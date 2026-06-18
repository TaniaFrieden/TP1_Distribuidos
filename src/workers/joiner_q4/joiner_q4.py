import json
import threading
import os

from base import WorkerBase
from common.logger import Logger, obtener_logger
from common.persistencia import PersistidorEstado, TAMANIO_BATCH_PERSISTENCIA

logger = obtener_logger(__name__)

BASE_DIR = "/app/volumen"


class JoinerQ4Worker(WorkerBase):
    """
    Box 2 (Gather join): sharded por B = (to_bank, to_account) del scatter /
                                          (From Bank, Account) de las transacciones.

    Recibe dos flujos:
      - scatter_edges : aristas (A→B) donde A dispersó a exactamente 5 Bs
      - transacciones : todas las transacciones del período (para detectar B→C)

    Al flush emite tripletas (A, B, C) para armar los caminos del scatter-gather.
    Incluye "Filtro celes": descarta caminos degenerados donde A==B, B==C o A==C.
    """
    # Guardar estado y liberar acks en lote. Los mensajes en el lote permanecen
    # unacked en RabbitMQ hasta el save, así que un crash los reenvía todos
    # y _vistos (guardado junto con el estado) filtra los duplicados.
    SAVE_BATCH = TAMANIO_BATCH_PERSISTENCIA

    def __init__(self):
        super().__init__()
        self._scatter: dict = {}
        self._txns: dict = {}
        self._vistos: dict[str, set] = {}
        self._pending_acks: dict[str, list] = {}
        self._lock = threading.Lock()
        self._recover_state_from_disk()

    def _nombre_nodo(self, client_id: str) -> str:
        return f"joiner_q4_{self.configuracion.id_nodo}_{client_id}"

    def _recover_state_from_disk(self):
        if not os.path.exists(BASE_DIR):
            logger.info(f"[JoinerQ4] Directorio {BASE_DIR} no existe. Arrancando limpio.")
            return
        prefijo = f"joiner_q4_{self.configuracion.id_nodo}_"
        carpetas = [c for c in os.listdir(BASE_DIR) if c.startswith(prefijo)]
        if not carpetas:
            logger.info(f"[JoinerQ4] Sin estado previo en disco. Arrancando limpio.")
            return
        for carpeta in carpetas:
            client_id = carpeta[len(prefijo):]
            persistidor = PersistidorEstado(carpeta, base_dir=BASE_DIR)
            estado = persistidor.cargar()
            if not estado:
                continue
            if estado.get("barrier_completada", False):
                persistidor.borrar()
                logger.info(f"[JoinerQ4] barrier_completada detectada para client_id={client_id}. Limpiando remanente.")
                continue
            scatter = {k: [tuple(a) for a in v] for k, v in estado.get("scatter", {}).items()}
            txns = {k: set(tuple(c) for c in v) for k, v in estado.get("txns", {}).items()}
            with self._lock:
                self._scatter[client_id] = scatter
                self._txns[client_id] = txns
                self._vistos[client_id] = set(estado.get("vistos", []))
            logger.info(
                f"[JoinerQ4] Recuperado estado para client_id={client_id}: "
                f"scatter_keys={len(scatter)}, txns_keys={len(txns)}, vistos={len(self._vistos[client_id])}"
            )

    def _guardar_estado(self, client_id: str):
        scatter_serial = {k: [list(a) for a in v] for k, v in self._scatter.get(client_id, {}).items()}
        txns_serial = {k: [list(c) for c in v] for k, v in self._txns.get(client_id, {}).items()}
        PersistidorEstado(self._nombre_nodo(client_id), base_dir=BASE_DIR).guardar({
            "client_id": client_id,
            "scatter": scatter_serial,
            "txns": txns_serial,
            "vistos": list(self._vistos.get(client_id, set())),
        })

    def _norm(self, v) -> str:
        return str(v).strip().lstrip("0") or "0"

    def procesar_payload(self, queue_name: str, client_id: str, payload: dict,
                        mensaje_original: bytes, ack, nack):
        acks_a_liberar = []
        try:
            with self._lock:
                request_id = payload.get("request_id")

                if request_id and request_id in self._vistos.get(client_id, set()):
                    logger.warning(f"[JoinerQ4] Duplicado propio ignorado: request_id={request_id} client_id={client_id}")
                    acks_a_liberar = [ack]
                else:
                    if "batches" in payload:
                        for batch in payload["batches"]:
                            header = batch["header"]
                            schema = header["schema"]
                            records = batch["payload"]

                            if "scatter" in queue_name:
                                to_bank_idx = schema.index("to_bank") if "to_bank" in schema else None
                                to_account_idx = schema.index("to_account") if "to_account" in schema else None
                                from_bank_idx = schema.index("from_bank") if "from_bank" in schema else None
                                from_account_idx = schema.index("from_account") if "from_account" in schema else None

                                for record_values in records:
                                    to_bank = record_values[to_bank_idx] if to_bank_idx is not None else ""
                                    to_account = record_values[to_account_idx] if to_account_idx is not None else ""
                                    from_bank = record_values[from_bank_idx] if from_bank_idx is not None else ""
                                    from_account = record_values[from_account_idx] if from_account_idx is not None else ""

                                    b_key = f"{self._norm(to_bank)}|{self._norm(to_account)}"
                                    a_info = (self._norm(from_bank), self._norm(from_account))
                                    self._scatter.setdefault(client_id, {}).setdefault(b_key, []).append(a_info)
                            else:
                                from_bank_idx = schema.index("From Bank") if "From Bank" in schema else None
                                account_idx = schema.index("Account") if "Account" in schema else None
                                to_bank_idx = schema.index("To Bank") if "To Bank" in schema else None
                                to_account_idx = schema.index("Account.1") if "Account.1" in schema else None

                                for record_values in records:
                                    from_bank = record_values[from_bank_idx] if from_bank_idx is not None else ""
                                    account = record_values[account_idx] if account_idx is not None else ""
                                    to_bank = record_values[to_bank_idx] if to_bank_idx is not None else ""
                                    to_account = record_values[to_account_idx] if to_account_idx is not None else ""

                                    b_key = f"{self._norm(from_bank)}|{self._norm(account)}"
                                    c_info = (self._norm(to_bank), self._norm(to_account))
                                    self._txns.setdefault(client_id, {}).setdefault(b_key, set()).add(c_info)
                    else:
                        if "scatter" in queue_name:
                            b_key = f"{self._norm(payload['to_bank'])}|{self._norm(payload['to_account'])}"
                            a_info = (self._norm(payload["from_bank"]), self._norm(payload["from_account"]))
                            self._scatter.setdefault(client_id, {}).setdefault(b_key, []).append(a_info)
                        else:
                            b_key = f"{self._norm(payload.get('From Bank', ''))}|{self._norm(payload.get('Account', ''))}"
                            c_info = (self._norm(payload.get("To Bank", "")), self._norm(payload.get("Account.1", "")))
                            self._txns.setdefault(client_id, {}).setdefault(b_key, set()).add(c_info)

                    if request_id:
                        self._vistos.setdefault(client_id, set()).add(request_id)

                    self._pending_acks.setdefault(client_id, []).append(ack)
                    total_pending = sum(len(v) for v in self._pending_acks.values())
                    if total_pending >= self.SAVE_BATCH:
                        for cid in list(self._pending_acks.keys()):
                            self._guardar_estado(cid)
                        for cid in list(self._pending_acks.keys()):
                            acks_a_liberar.extend(self._pending_acks.pop(cid, []))

        except Exception as e:
            logger.error(f"Error procesando payload: {e}", exc_info=True)
            nack()
            return

        for fn in acks_a_liberar:
            fn()

    FLUSH_BATCH_SIZE = 1000
    SCHEMA = ["a_bank", "a_account", "b_bank", "b_account", "c_bank", "c_account"]

    def _enviar_batch(self, client_id: str, records: list):
        output_payload = {
            "client_id": client_id,
            "batches": [
                {
                    "header": {
                        "schema": self.SCHEMA,
                        "client_id": client_id,
                        "count": len(records)
                    },
                    "payload": records
                }
            ]
        }
        self._enviar(json.dumps(output_payload).encode("utf-8"), payload=output_payload)

    def al_completar_eof_local(self, client_id: str):
        """Libera los acks pendientes del último lote parcial antes de que el
        coordinator espere vuelos=0. Si esperáramos a al_completar_cliente,
        el coordinator ya sostendría _vuelo_lock al llamarla → deadlock."""
        acks_a_liberar = []
        with self._lock:
            self._guardar_estado(client_id)
            acks_a_liberar = self._pending_acks.pop(client_id, [])
        for fn in acks_a_liberar:
            fn()

    def al_completar_cliente(self, client_id: str):
        with self._lock:
            self._guardar_estado(client_id)
            scatter = self._scatter.pop(client_id, {})
            txns    = self._txns.pop(client_id, {})
            self._vistos.pop(client_id, None)
            self._pending_acks.pop(client_id, None)

        logger.info(f"[JoinerQ4] scatter_keys={len(scatter)} txns_keys={len(txns)}")

        matches = [k for k in scatter if k in txns]
        logger.info(f"[JoinerQ4] keys que matchean scatter∩txns: {len(matches)}")
        if matches:
            logger.info(f"[JoinerQ4] match sample: {matches[:3]}")

        batch = []
        enviados = 0
        for b_key, a_list in scatter.items():
            if b_key not in txns:
                continue
            b_bank, b_account = b_key.split("|", 1)
            for c_bank, c_account in txns[b_key]:
                for a_bank, a_account in a_list:
                    if (a_bank, a_account) == (b_bank, b_account):
                        continue
                    if (b_bank, b_account) == (c_bank, c_account):
                        continue
                    if (a_bank, a_account) == (c_bank, c_account):
                        continue

                    batch.append([a_bank, a_account, b_bank, b_account, c_bank, c_account])

                    if len(batch) >= self.FLUSH_BATCH_SIZE:
                        self._enviar_batch(client_id, batch)
                        enviados += len(batch)
                        batch = []

        if batch:
            self._enviar_batch(client_id, batch)
            enviados += len(batch)

        logger.info(f"[JoinerQ4] Flush completo para client_id={client_id}. Registros emitidos: {enviados}.")

        if os.environ.get("CRASH_AFTER_FLUSH") == "true":
            bandera = os.path.join(BASE_DIR, "crash_flush_done")
            if not os.path.exists(bandera):
                open(bandera, "w").close()
                logger.warning("[JoinerQ4] CRASH_AFTER_FLUSH — muriendo después del envío, antes de barrier_completada")
                os._exit(1)

        PersistidorEstado(self._nombre_nodo(client_id), base_dir=BASE_DIR).guardar({"barrier_completada": True})
        PersistidorEstado(self._nombre_nodo(client_id), base_dir=BASE_DIR).borrar()

    def al_desconectar_cliente(self, client_id: str):
        acks_a_liberar = []
        with self._lock:
            self._scatter.pop(client_id, None)
            self._txns.pop(client_id, None)
            self._vistos.pop(client_id, None)
            acks_a_liberar = self._pending_acks.pop(client_id, [])
        PersistidorEstado(self._nombre_nodo(client_id), base_dir=BASE_DIR).borrar()
        for fn in acks_a_liberar:
            fn()
        logger.info(f"[JoinerQ4] Estado descartado para {client_id}.")

    def al_cerrar(self):
        logger.info("[JoinerQ4] Apagado.")


def main():
    Logger.configurar("joiner_q4")
    JoinerQ4Worker().iniciar()


if __name__ == "__main__":
    main()
