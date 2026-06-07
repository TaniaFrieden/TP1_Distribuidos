import logging
import json
import threading
from base import BaseWorker
from common.logging_setup import setup_logging
from procesador import ProcesadorRegistros
from persistidor import PersistidorEstadoShard

logger = logging.getLogger(__name__)

class AgregadorBancarioWorker(BaseWorker):
    def __init__(self):
        super().__init__()
        self.lock_creacion_locks = threading.Lock()
        self.client_locks = {}
        
        nombre_nodo = f"{self.config.node_prefix}_{self.config.node_id:02d}"
        self.persistidor = PersistidorEstadoShard(nombre_nodo)
        self.procesador = ProcesadorRegistros()
        
        self._recuperar_barreras_pendientes()
        
        logger.info(f"[AgregadorBancario] Worker inicializado con arquitectura limpia y modular.")

    def _obtener_lock_cliente(self, client_id: str) -> threading.Lock:
        cid = str(client_id).strip()
        with self.lock_creacion_locks:
            if cid not in self.client_locks:
                self.client_locks[cid] = threading.Lock()
            return self.client_locks[cid]

    def _borrar_lock_cliente(self, client_id: str):
        cid = str(client_id).strip()
        with self.lock_creacion_locks:
            self.client_locks.pop(cid, None)

    def procesar_payload(self, queue_name: str, client_id: str, payload: dict, mensaje_original: bytes, ack, nack):
        try:
            lock = self._obtener_lock_cliente(client_id)
            with lock:
                estado_agregador, estado_eof = self.persistidor.cargar_estado_cliente(client_id)

                if client_id not in estado_agregador:
                    logger.info(f"[CLIENTE NUEVO] Inicializando estado para {client_id}")
                    estado_agregador[client_id] = {}

                hubo_cambio = False

                if "batches" in payload:
                    for batch in payload["batches"]:
                        schema = batch["header"]["schema"]
                        records = batch["payload"]

                        if "transactions" in queue_name:
                            cambio = self.procesador.procesar_batch_transacciones(client_id, schema, records, estado_agregador)
                        elif "banks" in queue_name:
                            cambio = self.procesador.procesar_batch_bancos(client_id, schema, records, estado_agregador)
                        else:
                            cambio = False
                        
                        hubo_cambio = hubo_cambio or cambio
                else:
                    hubo_cambio = self.procesador.procesar_registro_individual(queue_name, client_id, payload, estado_agregador)

                if hubo_cambio:
                    exito = self.persistidor.guardar_estado_cliente(client_id, estado_agregador, estado_eof)
                    if not exito:
                        raise RuntimeError(f"Error al guardar estado en disco para cliente {client_id}")

            ack()
        except ValueError as e:
            logger.error(f"Error de conversión numérica para el cliente {client_id}: {e}")
            nack()
        except Exception as e:
            logger.error(f"Error procesando mensaje: {e}", exc_info=True)
            nack()

    def interceptar_eof(self, queue_name: str, client_id: str, payload: dict, mensaje_original: bytes) -> bool:
        disparar_flush = False
        mensaje_barrera = None

        lock = self._obtener_lock_cliente(client_id)
        with lock:
            estado_agregador, estado_eof = self.persistidor.cargar_estado_cliente(client_id)

            if client_id not in estado_eof:
                estado_eof[client_id] = {
                    "transacciones_cerrado": False,
                    "bancos_cerrado": False,
                    "eof_mensaje": None,
                    "flush_iniciado": False
                }

            estado = estado_eof[client_id]

            if not estado["eof_mensaje"]:
                estado["eof_mensaje"] = mensaje_original

            if "transactions" in queue_name:
                logger.info(f"[BankShard] EOF Transacciones recibido para {client_id}.")
                estado["transacciones_cerrado"] = True
            elif "banks" in queue_name:
                logger.info(f"[BankShard] EOF Bancos recibido para {client_id}.")
                estado["bancos_cerrado"] = True

            if estado["transacciones_cerrado"] and estado["bancos_cerrado"] and not estado["flush_iniciado"]:
                logger.info(f"[BankShard] Ambas colas cerradas para {client_id}. Solicitando barrera de flush.")
                estado["flush_iniciado"] = True
                disparar_flush = True
                mensaje_barrera = estado["eof_mensaje"]
            
            exito = self.persistidor.guardar_estado_cliente(client_id, estado_agregador, estado_eof)
            if not exito:
                raise RuntimeError(f"Error al guardar estado de EOF en disco para cliente {client_id}")

        if disparar_flush:
            self.coordinator.iniciar_barrera(client_id, mensaje_barrera)

        return True

    def al_completar_cliente(self, client_id: str):
        lock = self._obtener_lock_cliente(client_id)
        with lock:
            estado_agregador, estado_eof = self.persistidor.cargar_estado_cliente(client_id)

            if client_id in estado_agregador:
                records = []
                for bank_id, datos in estado_agregador[client_id].items():
                    if datos["max_amount"] <= 0.0:
                        continue
                    if datos["bank_name"] == "Desconocido":
                        logger.warning(f"[FILTRO] Descartando banco {bank_id} para cliente {client_id}: Nombre desconocido.")
                        continue
                    
                    records.append([bank_id, datos["account"], datos["bank_name"], datos["max_amount"]])

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
                    self._enviar(mensaje_bytes, payload=batch_payload)

                logger.info(f"[BARRERA CONTROL] Envío finalizado con éxito para cliente {client_id}.")
            else:
                logger.warning(f"[BARRERA CONTROL] Se disparó al_completar_cliente para {client_id} sin datos locales registrados.")

            self.persistidor.borrar_estado_cliente(client_id)
        self._borrar_lock_cliente(client_id)

    def al_desconectar_cliente(self, client_id: str):
        lock = self._obtener_lock_cliente(client_id)
        with lock:
            self.persistidor.borrar_estado_cliente(client_id)
        self._borrar_lock_cliente(client_id)

    def al_cerrar(self):
        logger.info("[AgregadorBancario] Solicitud de apagado recibida de las señales del sistema.")

    def _recuperar_barreras_pendientes(self):
        clientes_pendientes = self.persistidor.detectar_clientes_pendientes()
        for client_id in clientes_pendientes:
            _, estado_eof = self.persistidor.cargar_estado_cliente(client_id)
            if client_id in estado_eof:
                estado = estado_eof[client_id]
                if estado.get("flush_iniciado") and estado.get("eof_mensaje"):
                    logger.warning(
                        f"[AgregadorBancario] Recuperando barrera pendiente para {client_id} tras reinicio."
                    )
                    self.coordinator.iniciar_barrera(client_id, estado["eof_mensaje"])

def __main__():
    setup_logging("bank_shard")
    worker = AgregadorBancarioWorker()
    worker.iniciar()

if __name__ == "__main__":
    __main__()