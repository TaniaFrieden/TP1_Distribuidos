import logging
import json
import threading
import os
from base import BaseWorker
from common.logging_setup import setup_logging
from common.persistencia import PersistidorEstado

logger = logging.getLogger(__name__)

BASE_DIR = "/app/volumen"


class FormatShardWorker(BaseWorker):
    def __init__(self):
        super().__init__()
        self.estado_clientes = {}
        self.lock = threading.Lock()
        self._barreras_para_iniciar = []
        self._recover_state_from_disk()
        logger.info("[FormatShard] Worker inicializado.")

    # ------------------------------------------------------------------ #
    # Persistencia                                                         #
    # ------------------------------------------------------------------ #

    def _node_prefix(self) -> str:
        return f"format_shard_{self.config.node_id}"

    def _get_persistidor(self, client_id: str) -> PersistidorEstado:
        return PersistidorEstado(f"{self._node_prefix()}_{client_id}", base_dir=BASE_DIR)

    def _get_cache_file_path(self, client_id: str) -> str:
        name = f"{self._node_prefix()}_{client_id}"
        directory = os.path.join(BASE_DIR, name)
        os.makedirs(directory, exist_ok=True)
        return os.path.join(directory, "cache_tardio.jsonl")

    def _append_to_cache_file(self, client_id: str, request_id, schema: list, records: list):
        """Escribe un batch al archivo JSONL. No reescribe el archivo completo."""
        path = self._get_cache_file_path(client_id)
        line = json.dumps({"request_id": request_id, "schema": schema, "records": records}, ensure_ascii=False)
        with open(path, "a", encoding="utf-8") as f:
            f.write(line + "\n")
            f.flush()
            os.fsync(f.fileno())

    def _recover_state_from_disk(self):
        if not os.path.exists(BASE_DIR):
            return

        prefix = f"{self._node_prefix()}_"
        for folder_name in os.listdir(BASE_DIR):
            if not folder_name.startswith(prefix):
                continue
            client_id = folder_name[len(prefix):]
            persistidor = self._get_persistidor(client_id)
            saved = persistidor.cargar()
            cache_path = self._get_cache_file_path(client_id)
            
            if not saved and not os.path.exists(cache_path):
                continue
                
            saved = saved or {}

            eof_hex = saved.get("eof_mensaje_bytes_hex")
            barrier_completada = saved.get("barrier_completada", False)
            estado = {
                "temprano_cerrado": saved.get("temprano_cerrado", False),
                "tardio_cerrado": saved.get("tardio_cerrado", False),
                "promedios_listos": saved.get("promedios_listos", False),
                "promedios": saved.get("promedios", {}),
                "datos_temprano": saved.get("datos_temprano", {}),
                "eof_mensaje": bytes.fromhex(eof_hex) if eof_hex else None,
                "cache_procesado": saved.get("cache_procesado", False),
                "barrier_completada": barrier_completada,
                "processed_request_ids": set(saved.get("processed_request_ids", [])),
            }

            # Reconstruye request_ids del cache file para manejar crashes entre append y persist
            cache_path = self._get_cache_file_path(client_id)
            if os.path.exists(cache_path):
                with open(cache_path, "r", encoding="utf-8") as f:
                    for line in f:
                        line = line.strip()
                        if not line:
                            continue
                        try:
                            entry = json.loads(line)
                            rid = entry.get("request_id")
                            if rid:
                                estado["processed_request_ids"].add(rid)
                        except (json.JSONDecodeError, KeyError):
                            pass  # línea parcial por crash, se ignora

            if estado["temprano_cerrado"] and estado["tardio_cerrado"] and estado["cache_procesado"]:
                if barrier_completada:
                    # La barrera completó pero borrar() no corrió. Limpiar sin relanzar.
                    self._get_persistidor(client_id).borrar()
                    self._borrar_cache_file(client_id)
                    logger.info(f"[Recuperación] Cliente {client_id}: barrera ya completada, limpiando remanente.")
                else:
                    # La barrera nunca se inició (o no completó). Encolamos para lanzarla al arrancar.
                    self.estado_clientes[client_id] = estado
                    with self.coordinator._coordinacion_lock:
                        self.coordinator._local_eof_completed.add(client_id)
                    self._barreras_para_iniciar.append((client_id, estado["eof_mensaje"]))
                    logger.info(f"[Recuperación] Cliente {client_id}: barrera pendiente, se iniciará al arrancar.")
            else:
                self.estado_clientes[client_id] = estado
                logger.info(f"[Recuperación] Estado parcial cargado para cliente {client_id}.")

    def _guardar_estado(self, client_id: str, estado: dict):
        eof_msg = estado.get("eof_mensaje")
        serializable = {
            "client_id": client_id,
            "temprano_cerrado": estado["temprano_cerrado"],
            "tardio_cerrado": estado["tardio_cerrado"],
            "promedios_listos": estado["promedios_listos"],
            "promedios": estado["promedios"],
            "datos_temprano": estado["datos_temprano"],
            "eof_mensaje_bytes_hex": eof_msg.hex() if eof_msg else None,
            "cache_procesado": estado["cache_procesado"],
            "barrier_completada": estado.get("barrier_completada", False),
            "processed_request_ids": list(estado["processed_request_ids"]),
        }
        self._get_persistidor(client_id).guardar(serializable)

    # ------------------------------------------------------------------ #
    # Estado en memoria                                                    #
    # ------------------------------------------------------------------ #

    def _get_estado(self, client_id: str) -> dict:
        if client_id not in self.estado_clientes:
            self.estado_clientes[client_id] = {
                "temprano_cerrado": False,
                "tardio_cerrado": False,
                "promedios_listos": False,
                "promedios": {},
                "datos_temprano": {},
                "eof_mensaje": None,
                "cache_procesado": False,
                "barrier_completada": False,
                "processed_request_ids": set(),
            }
        return self.estado_clientes[client_id]

    # ------------------------------------------------------------------ #
    # Procesamiento de mensajes                                            #
    # ------------------------------------------------------------------ #

    def procesar_payload(self, queue_name: str, client_id: str, payload: dict, mensaje_original: bytes, ack, nack):
        try:
            request_id = payload.get("request_id")

            with self.lock:
                estado = self._get_estado(client_id)

                if request_id and request_id in estado["processed_request_ids"]:
                    logger.info(f"[FormatShard] Batch duplicado descartado request_id={request_id} client_id={client_id}.")
                    ack()
                    return

                if "batches" in payload:
                    for batch in payload["batches"]:
                        header = batch["header"]
                        schema = header["schema"]
                        records = batch["payload"]

                        if "temprano" in queue_name:
                            self._acumular_temprano(estado, schema, records)
                        elif "tardio" in queue_name:
                            self._append_to_cache_file(client_id, request_id, schema, records)
                else:
                    if "temprano" in queue_name:
                        formato = payload.get("Payment Format", "")
                        monto = float(payload.get("Amount Paid", 0))
                        if formato not in estado["datos_temprano"]:
                            estado["datos_temprano"][formato] = {"suma": 0, "count": 0}
                        estado["datos_temprano"][formato]["suma"] += int(round(monto * 100))
                        estado["datos_temprano"][formato]["count"] += 1
                    elif "tardio" in queue_name:
                        schema = list(payload.keys())
                        record_values = list(payload.values())
                        self._append_to_cache_file(client_id, request_id, schema, [record_values])

                if request_id:
                    estado["processed_request_ids"].add(request_id)
                self._guardar_estado(client_id, estado)

            ack()

        except Exception as e:
            logger.error(f"Error procesando mensaje en {queue_name}: {e}", exc_info=True)
            nack()

    def _acumular_temprano(self, estado: dict, schema: list, records: list):
        formato_idx = schema.index("Payment Format") if "Payment Format" in schema else None
        monto_idx = schema.index("Amount Paid") if "Amount Paid" in schema else None
        for record_values in records:
            formato = record_values[formato_idx] if formato_idx is not None else ""
            monto = float(record_values[monto_idx] if monto_idx is not None else 0)
            if formato not in estado["datos_temprano"]:
                estado["datos_temprano"][formato] = {"suma": 0, "count": 0}
            estado["datos_temprano"][formato]["suma"] += int(round(monto * 100))
            estado["datos_temprano"][formato]["count"] += 1

    # ------------------------------------------------------------------ #
    # Manejo de EOFs                                                       #
    # ------------------------------------------------------------------ #

    def interceptar_eof(self, queue_name: str, client_id: str, payload: dict, mensaje_original: bytes) -> bool:
        disparar_flush = False

        with self.lock:
            estado = self._get_estado(client_id)

            if not estado["eof_mensaje"]:
                estado["eof_mensaje"] = mensaje_original

            if "temprano" in queue_name:
                logger.info(f"[Q3] EOF Temprano recibido para {client_id}.")
                estado["temprano_cerrado"] = True
                self._calcular_promedios(estado)
            elif "tardio" in queue_name:
                logger.info(f"[Q3] EOF Tardío recibido para {client_id}.")
                estado["tardio_cerrado"] = True

            self._guardar_estado(client_id, estado)

            if estado["temprano_cerrado"] and estado["tardio_cerrado"] and not estado["cache_procesado"]:
                cache_path = self._get_cache_file_path(client_id)
                cache_size = os.path.getsize(cache_path) if os.path.exists(cache_path) else 0
                logger.info(f"[Q3] Ambas fases cerradas para {client_id}. Procesando caché tardío ({cache_size} bytes).")

                output_request_id = f"format_shard_output:{client_id}:{self.config.node_id}"
                self._thread_local.current_request_id = output_request_id
                try:
                    self._procesar_cache_tardio(client_id, estado)
                finally:
                    self._thread_local.current_request_id = None

                estado["cache_procesado"] = True
                self._guardar_estado(client_id, estado)

                with self.coordinator._coordinacion_lock:
                    self.coordinator._local_eof_completed.add(client_id)

                disparar_flush = True

        if disparar_flush:
            logger.info(f"[Q3] Caché procesado. Iniciando barrera distribuida para {client_id}.")
            self.coordinator.iniciar_barrera(client_id, estado["eof_mensaje"])

        return True

    # ------------------------------------------------------------------ #
    # Lógica de negocio                                                    #
    # ------------------------------------------------------------------ #

    def _calcular_promedios(self, estado: dict):
        for formato, stats in estado["datos_temprano"].items():
            if stats["count"] > 0:
                promedio_centavos = stats["suma"] / stats["count"]
                estado["promedios"][formato] = promedio_centavos / 100.0
        estado["promedios_listos"] = True

    def _procesar_cache_tardio(self, client_id: str, estado: dict):
        """Emite transacciones tardías cuyo monto es inferior al 1% del promedio de su formato."""
        promedios = estado["promedios"]
        cache_path = self._get_cache_file_path(client_id)

        if not os.path.exists(cache_path):
            return

        schema_indices_cache = {}
        records = []

        with open(cache_path, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    entry = json.loads(line)
                except json.JSONDecodeError:
                    continue  # línea parcial por crash

                schema = entry["schema"]
                batch_records = entry["records"]

                schema_key = tuple(schema)
                if schema_key not in schema_indices_cache:
                    schema_indices_cache[schema_key] = {
                        "from_bank": schema.index("From Bank") if "From Bank" in schema else None,
                        "formato": schema.index("Payment Format") if "Payment Format" in schema else None,
                        "monto": schema.index("Amount Paid") if "Amount Paid" in schema else None,
                        "account": schema.index("Account") if "Account" in schema else None,
                    }
                idx = schema_indices_cache[schema_key]

                for record_values in batch_records:
                    formato = record_values[idx["formato"]] if idx["formato"] is not None else ""
                    monto = float(record_values[idx["monto"]] if idx["monto"] is not None else 0)
                    promedio = promedios.get(formato)

                    if promedio is None or monto >= promedio * 0.01:
                        continue

                    from_bank = record_values[idx["from_bank"]] if idx["from_bank"] is not None else ""
                    if isinstance(from_bank, str) and from_bank.isdigit():
                        from_bank = from_bank.lstrip("0") or "0"
                    
                    account = record_values[idx["account"]] if idx["account"] is not None else ""
                    records.append([from_bank, account, formato, monto])

        if records:
            output_payload = {
                "client_id": client_id,
                "batches": [
                    {
                        "header": {
                            "schema": ["From Bank", "Account", "Payment Format", "Amount Paid"],
                            "client_id": client_id,
                            "count": len(records),
                        },
                        "payload": records,
                    }
                ],
            }
            self._enviar(json.dumps(output_payload).encode("utf-8"), payload=output_payload)

        try:
            os.remove(cache_path)
        except Exception as e:
            logger.warning(f"[FormatShard] No se pudo eliminar cache file para {client_id}: {e}")

    # ------------------------------------------------------------------ #
    # Hooks de ciclo de vida                                               #
    # ------------------------------------------------------------------ #

    def al_completar_cliente(self, client_id: str):
        with self.lock:
            estado = self.estado_clientes.get(client_id)
            if estado:
                logger.info(f"[FormatShard] Limpiando estado en memoria para {client_id}.")
                estado["barrier_completada"] = True
                self._guardar_estado(client_id, estado)
                del self.estado_clientes[client_id]
        self._get_persistidor(client_id).borrar()
        self._borrar_cache_file(client_id)

    def al_iniciar_post_arranque(self):
        for client_id, eof_mensaje in self._barreras_para_iniciar:
            logger.info(f"[FormatShard] Iniciando barrera diferida para {client_id} post-recovery.")
            self.coordinator.iniciar_barrera(client_id, eof_mensaje)
        self._barreras_para_iniciar.clear()

    def al_desconectar_cliente(self, client_id: str):
        with self.lock:
            self.estado_clientes.pop(client_id, None)
        self._get_persistidor(client_id).borrar()
        self._borrar_cache_file(client_id)

    def _borrar_cache_file(self, client_id: str):
        cache_path = self._get_cache_file_path(client_id)
        if os.path.exists(cache_path):
            try:
                os.remove(cache_path)
            except Exception as e:
                logger.warning(f"[FormatShard] No se pudo eliminar cache file para {client_id}: {e}")

    def al_cerrar(self):
        logger.info("[FormatShard] Apagado exitoso.")


def __main__():
    setup_logging("format_shard")
    worker = FormatShardWorker()
    worker.iniciar()


if __name__ == "__main__":
    __main__()
