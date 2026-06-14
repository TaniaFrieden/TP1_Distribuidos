import logging
import json
import threading
import os
from base.base import BaseWorker
from common.sharding import normalizar_valor_hash
from common.logging_setup import setup_logging
from common.persistencia import PersistidorEstado
from bank_shard_config import ShardConfig
from processor import PayloadProcessor

logger = logging.getLogger(__name__)


INTERVALO_PERSISTENCIA = 500


class AgregadorBancarioWorker(BaseWorker):
    def __init__(self):
        super().__init__()
        self.aggregator_state = {}
        self.eof_state = {}
        self.client_locks = {}
        self.global_lock = threading.Lock()
        self._processed_request_ids = {}
        self._barreras_para_iniciar = []
        self._mensajes_desde_flush = {}

        self.shard_config = ShardConfig(self.config.node_id)
        self.processor = PayloadProcessor()

        self._recover_state_from_disk()
        logger.info("[AgregadorBancario] Worker inicializado con soporte multicliente y persistencia atómica.")

    def _get_persistidor(self, client_id: str) -> PersistidorEstado:
        return PersistidorEstado(f"{self.shard_config.node_name_prefix}_{client_id}", base_dir=self.shard_config.base_dir)

    def _get_client_lock(self, client_id: str) -> threading.Lock:
        with self.global_lock:
            if client_id not in self.client_locks:
                self.client_locks[client_id] = threading.Lock()
            return self.client_locks[client_id]

    def _recover_state_from_disk(self):
        if not os.path.exists(self.shard_config.base_dir):
            return

        prefix = f"{self.shard_config.node_name_prefix}_"
        for folder_name in os.listdir(self.shard_config.base_dir):
            if not folder_name.startswith(prefix):
                continue

            client_id = folder_name[len(prefix):]
            persistidor = self._get_persistidor(client_id)
            saved_state = persistidor.cargar()

            if not saved_state:
                continue

            trans_cerrado = saved_state.get("transacciones_cerrado", False)
            bancos_cerrado = saved_state.get("bancos_cerrado", False)
            barrier_completada = saved_state.get("barrier_completada", False)
            eof_hex = saved_state.get("eof_mensaje_bytes_hex")

            if trans_cerrado and bancos_cerrado and barrier_completada:
                # La barrera completó pero borrar() no corrió (crash entre al_completar_cliente y borrar).
                # No hay nada que relanzar: solo limpiar el archivo remanente.
                persistidor.borrar()
                logger.info(f"[Recuperación] Cliente {client_id}: barrera ya completada, limpiando remanente.")
                continue

            with self._get_client_lock(client_id):
                self.aggregator_state[client_id] = saved_state.get("bancos", {})
                self._processed_request_ids[client_id] = set(saved_state.get("processed_request_ids", []))
                self.eof_state[client_id] = {
                    "transacciones_cerrado": trans_cerrado,
                    "bancos_cerrado": bancos_cerrado,
                    "eof_mensaje": bytes.fromhex(eof_hex) if eof_hex else None,
                    "flush_iniciado": saved_state.get("flush_iniciado", False),
                    "barrier_completada": False,
                }

                if trans_cerrado and bancos_cerrado:
                    # flush_iniciado=True estaba persistido pero iniciar_barrera() nunca se llamó.
                    # Forzamos flush_iniciado=True en memoria para que el EOF reenviado no dispare
                    # un segundo iniciar_barrera() cuando llegue a interceptar_eof.
                    self.eof_state[client_id]["flush_iniciado"] = True
                    with self.coordinator._coordinacion_lock:
                        self.coordinator._local_eof_completed.add(client_id)
                    self._barreras_para_iniciar.append((client_id, self.eof_state[client_id]["eof_mensaje"]))
                    logger.info(f"[Recuperación] Cliente {client_id}: barrera pendiente, se iniciará al arrancar.")
                else:
                    logger.info(f"[Recuperación] Estado parcial cargado para cliente {client_id}.")

    def _build_serializable_state(self, client_id: str) -> dict:
        current_eof_state = self.eof_state.get(client_id, {})
        eof_msg = current_eof_state.get("eof_mensaje")
        return {
            "client_id": client_id,
            "transacciones_cerrado": current_eof_state.get("transacciones_cerrado", False),
            "bancos_cerrado": current_eof_state.get("bancos_cerrado", False),
            "eof_mensaje_bytes_hex": eof_msg.hex() if eof_msg else None,
            "flush_iniciado": current_eof_state.get("flush_iniciado", False),
            "barrier_completada": current_eof_state.get("barrier_completada", False),
            "bancos": self.aggregator_state.get(client_id, {}),
            "processed_request_ids": list(self._processed_request_ids.get(client_id, set())),
        }

    def procesar_payload(self, queue_name: str, client_id: str, payload: dict, mensaje_original: bytes, ack, nack):
        try:
            request_id = payload.get("request_id")
            client_lock = self._get_client_lock(client_id)

            with client_lock:
                # Dedup propio: cubre la ventana entre persistidor.guardar() y dedup_filter.marcar_procesado()
                if request_id and request_id in self._processed_request_ids.get(client_id, set()):
                    logger.info(f"[BankShard] Duplicado propio descartado request_id={request_id} client_id={client_id}.")
                    ack()
                    return

                if client_id not in self.aggregator_state:
                    logger.info(f"[CLIENTE NUEVO] Inicializando estado para {client_id}")
                    self.aggregator_state[client_id] = {}

                if "batches" in payload:
                    for batch in payload["batches"]:
                        header = batch["header"]
                        schema = header["schema"]
                        records = batch["payload"]

                        if "transactions" in queue_name:
                            self.processor.process_transactions(self.aggregator_state[client_id], schema, records)
                        elif "banks" in queue_name:
                            self.processor.process_banks(self.aggregator_state[client_id], schema, records)
                else:
                    if "transactions" in queue_name:
                        self.processor.process_single_transaction(self.aggregator_state[client_id], payload)
                    elif "banks" in queue_name:
                        self.processor.process_single_bank(self.aggregator_state[client_id], payload)
                    else:
                        ack()
                        return

                if request_id:
                    self._processed_request_ids.setdefault(client_id, set()).add(request_id)

                self._mensajes_desde_flush[client_id] = self._mensajes_desde_flush.get(client_id, 0) + 1
                if self._mensajes_desde_flush[client_id] >= INTERVALO_PERSISTENCIA:
                    self._mensajes_desde_flush[client_id] = 0
                    self._get_persistidor(client_id).guardar(self._build_serializable_state(client_id))

            ack()

        except ValueError as e:
            logger.error(f"Error de conversión numérica para el cliente {client_id}: {e}")
            nack()
        except Exception as e:
            logger.error(f"Error procesando mensaje: {e}", exc_info=True)
            nack()

    def interceptar_eof(self, queue_name: str, client_id: str, payload: dict, mensaje_original: bytes) -> bool:
        trigger_flush = False
        barrier_message = None
        client_lock = self._get_client_lock(client_id)

        with client_lock:
            if client_id not in self.eof_state:
                self.eof_state[client_id] = {
                    "transacciones_cerrado": False,
                    "bancos_cerrado": False,
                    "eof_mensaje": None,
                    "flush_iniciado": False,
                    "barrier_completada": False,
                }

            state = self.eof_state[client_id]

            if not state["eof_mensaje"]:
                state["eof_mensaje"] = mensaje_original

            if "transactions" in queue_name:
                logger.info(f"[BankShard] EOF Transacciones recibido para {client_id}.")
                state["transacciones_cerrado"] = True
            elif "banks" in queue_name:
                logger.info(f"[BankShard] EOF Bancos recibido para {client_id}.")
                state["bancos_cerrado"] = True

            persistidor = self._get_persistidor(client_id)
            persistidor.guardar(self._build_serializable_state(client_id))

            if state["transacciones_cerrado"] and state["bancos_cerrado"] and not state["flush_iniciado"]:
                logger.info(f"[BankShard] Ambas colas cerradas para {client_id}. Solicitando barrera de flush.")
                state["flush_iniciado"] = True
                persistidor.guardar(self._build_serializable_state(client_id))

                trigger_flush = True
                barrier_message = state["eof_mensaje"]

        if trigger_flush:
            self.coordinator.iniciar_barrera(client_id, barrier_message)

        return True

    def al_iniciar_post_arranque(self):
        for client_id, eof_mensaje in self._barreras_para_iniciar:
            logger.info(f"[BankShard] Iniciando barrera diferida para {client_id} post-recovery.")
            self.coordinator.iniciar_barrera(client_id, eof_mensaje)
        self._barreras_para_iniciar.clear()

    def al_completar_cliente(self, client_id: str):
        """Callback ejecutado cuando la barrera de EOFs local y global se completó."""
        client_lock = self._get_client_lock(client_id)
        with client_lock:
            if client_id in self.aggregator_state:
                records = []
                for bank_id, bank_data in self.aggregator_state[client_id].items():
                    if bank_data["max_amount"] <= 0.0:
                        continue
                    if bank_data["bank_name"] == "Desconocido":
                        logger.warning(f"[FILTRO] Descartando banco {bank_id} para cliente {client_id}: Nombre desconocido.")
                        continue

                    records.append([bank_id, bank_data["account"], bank_data["bank_name"], bank_data["max_amount"]])

                if records:
                    batch_payload = {
                        "client_id": client_id,
                        "batches": [
                            {
                                "header": {
                                    "schema": ["From Bank", "Account", "Bank Name", "Amount Paid"],
                                    "client_id": client_id,
                                    "count": len(records)
                                },
                                "payload": records
                            }
                        ]
                    }
                    mensaje_bytes = json.dumps(batch_payload).encode('utf-8')
                    self._thread_local.current_request_id = f"bank_shard_output:{client_id}:{self.config.node_id}"
                    try:
                        self._enviar(mensaje_bytes, payload=batch_payload)
                    finally:
                        self._thread_local.current_request_id = None

                logger.info(f"[BARRERA CONTROL] Envío finalizado con éxito para cliente {client_id}.")
                del self.aggregator_state[client_id]
            else:
                logger.warning(f"[BARRERA CONTROL] Se disparó al_completar_cliente para {client_id} sin datos locales registrados.")

            # Marcar barrier_completada=True antes de borrar para que un crash aquí sea inocuo en recovery.
            if client_id in self.eof_state:
                self.eof_state[client_id]["barrier_completada"] = True
                self._get_persistidor(client_id).guardar(self._build_serializable_state(client_id))

            self.eof_state.pop(client_id, None)
            self._processed_request_ids.pop(client_id, None)
            self._mensajes_desde_flush.pop(client_id, None)

            self._get_persistidor(client_id).borrar()

        with self.global_lock:
            self.client_locks.pop(client_id, None)

    def al_desconectar_cliente(self, client_id: str):
        client_lock = self._get_client_lock(client_id)
        with client_lock:
            self.aggregator_state.pop(client_id, None)
            self.eof_state.pop(client_id, None)
            self._processed_request_ids.pop(client_id, None)
            self._mensajes_desde_flush.pop(client_id, None)
            self._get_persistidor(client_id).borrar()
        with self.global_lock:
            self.client_locks.pop(client_id, None)

    def al_cerrar(self):
        logger.info("[AgregadorBancario] Solicitud de apagado recibida de las señales del sistema.")


def __main__():
    setup_logging("bank_shard")
    worker = AgregadorBancarioWorker()
    worker.iniciar()


if __name__ == "__main__":
    __main__()
