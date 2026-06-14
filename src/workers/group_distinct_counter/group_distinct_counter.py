import logging
import json
import threading
import os

from base import BaseWorker
from common.logging_setup import setup_logging
from common.persistencia import PersistidorEstado, TAMANIO_BATCH_PERSISTENCIA

logger = logging.getLogger(__name__)

BASE_DIR = "/app/volumen"


class GroupDistinctCounterWorker(BaseWorker):
    """
    Worker genérico: agrupa mensajes por GROUP_FIELDS, acumula un set de valores
    distintos de VALUE_FIELDS y, al flush, emite los grupos donde |set| == EXPECTED_COUNT.

    EMIT_MODE=explode   → un mensaje por ítem del set; incluye campos de grupo y de valor.
                          (Usado como sumador_destinos en Q4.)
    EMIT_MODE=aggregate → un mensaje por grupo con el conteo en COUNT_OUTPUT_FIELD.
                          (Usado como contador_caminos en Q4.)

    Variables de entorno:
      GROUP_FIELDS         campos por los que agrupar (CSV)
      GROUP_OUTPUT_FIELDS  nombres de salida para los campos de grupo (CSV, mismo orden)
      VALUE_FIELDS         campos cuyo valor se acumula como set distinto (CSV)
      VALUE_OUTPUT_FIELDS  nombres de salida para los campos de valor (CSV, solo en explode)
      EXPECTED_COUNT       tamaño exacto del set requerido (default: 5)
      EMIT_MODE            "explode" | "aggregate" (default: aggregate)
      COUNT_OUTPUT_FIELD   nombre del campo de conteo en modo aggregate (default: Amount Transactions)
    """
    SAVE_BATCH = TAMANIO_BATCH_PERSISTENCIA

    def __init__(self):
        super().__init__()

        def _parse(env, default=""):
            raw = os.environ.get(env, default)
            return [f.strip() for f in raw.split(",") if f.strip()]

        self.group_fields = _parse("GROUP_FIELDS")
        self.value_fields = _parse("VALUE_FIELDS")
        self.group_out    = _parse("GROUP_OUTPUT_FIELDS") or self.group_fields
        self.value_out    = _parse("VALUE_OUTPUT_FIELDS") or self.value_fields
        self.expected     = int(os.environ.get("EXPECTED_COUNT", "5"))
        self.operator     = os.environ.get("COMPARISON_OPERATOR", "eq").lower()
        self.emit_mode    = os.environ.get("EMIT_MODE", "aggregate").lower()
        self.count_field  = os.environ.get("COUNT_OUTPUT_FIELD", "Amount Transactions")

        self._grupos: dict = {}
        self._vistos: dict[str, set] = {}
        self._pending_acks: dict[str, list] = {}
        self._lock = threading.Lock()

        self._recover_state_from_disk()

        logger.info(
            f"[GroupDistinctCounter] group={self.group_fields} value={self.value_fields} "
            f"expected={self.expected} operator={self.operator} mode={self.emit_mode}"
        )

    def _nombre_nodo(self, client_id: str) -> str:
        return f"gdc_{self.config.node_prefix}_{self.config.node_id}_{client_id}"

    def _recover_state_from_disk(self):
        if not os.path.exists(BASE_DIR):
            logger.info(f"[GroupDistinctCounter] Directorio {BASE_DIR} no existe. Arrancando limpio.")
            return
        prefijo = f"gdc_{self.config.node_prefix}_{self.config.node_id}_"
        carpetas = [c for c in os.listdir(BASE_DIR) if c.startswith(prefijo)]
        if not carpetas:
            logger.info(f"[GroupDistinctCounter] Sin estado previo en disco. Arrancando limpio.")
            return
        for carpeta in carpetas:
            client_id = carpeta[len(prefijo):]
            persistidor = PersistidorEstado(carpeta, base_dir=BASE_DIR)
            estado = persistidor.cargar()
            if not estado:
                continue
            if estado.get("barrier_completada", False):
                persistidor.borrar()
                logger.info(f"[GroupDistinctCounter] barrier_completada detectada para client_id={client_id}. Limpiando remanente.")
                continue
            grupos_serial = estado.get("grupos", {})
            grupos = {}
            for k, vlist in grupos_serial.items():
                gkey = tuple(json.loads(k))
                grupos[gkey] = set(tuple(v) for v in vlist)
            with self._lock:
                self._grupos[client_id] = grupos
                self._vistos[client_id] = set(estado.get("vistos", []))
            logger.info(
                f"[GroupDistinctCounter] Recuperado estado para client_id={client_id}: "
                f"grupos={len(grupos)}, vistos={len(self._vistos[client_id])}"
            )

    def _guardar_estado(self, client_id: str):
        grupos_serial = {}
        for gkey, vset in self._grupos.get(client_id, {}).items():
            k = json.dumps(list(gkey))
            grupos_serial[k] = [list(vkey) for vkey in vset]
        PersistidorEstado(self._nombre_nodo(client_id), base_dir=BASE_DIR).guardar({
            "client_id": client_id,
            "grupos": grupos_serial,
            "vistos": list(self._vistos.get(client_id, set())),
        })

    def _make_key(self, payload: dict, fields: list) -> tuple:
        return tuple(str(payload.get(f, "")) for f in fields)

    def procesar_payload(self, queue_name: str, client_id: str, payload: dict,
                         mensaje_original: bytes, ack, nack):
        acks_a_liberar = []
        try:
            with self._lock:
                request_id = payload.get("request_id")

                if request_id and request_id in self._vistos.get(client_id, set()):
                    logger.warning(f"[GroupDistinctCounter] Duplicado propio ignorado: request_id={request_id} client_id={client_id}")
                    acks_a_liberar = [ack]
                else:
                    if "batches" in payload:
                        for batch in payload["batches"]:
                            header = batch["header"]
                            schema = header["schema"]
                            records = batch["payload"]

                            group_indices = [schema.index(f) if f in schema else None for f in self.group_fields]
                            value_indices = [schema.index(f) if f in schema else None for f in self.value_fields]

                            for record_values in records:
                                gkey = tuple(str(record_values[idx]) if idx is not None else "" for idx in group_indices)
                                vkey = tuple(str(record_values[idx]) if idx is not None else "" for idx in value_indices)
                                self._grupos.setdefault(client_id, {}).setdefault(gkey, set()).add(vkey)
                    else:
                        gkey = self._make_key(payload, self.group_fields)
                        vkey = self._make_key(payload, self.value_fields)
                        self._grupos.setdefault(client_id, {}).setdefault(gkey, set()).add(vkey)

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

    def _enviar_batch(self, client_id: str, schema: list, records: list):
        output_payload = {
            "client_id": client_id,
            "batches": [
                {
                    "header": {
                        "schema": schema,
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
            grupos = self._grupos.pop(client_id, {})
            self._vistos.pop(client_id, None)
            self._pending_acks.pop(client_id, None)

        logger.info(f"[GroupDistinctCounter] grupos totales: {len(grupos)}")
        top = sorted(grupos.items(), key=lambda x: len(x[1]), reverse=True)[:5]
        for gkey, vset in top:
            logger.info(f"[GroupDistinctCounter] grupo {gkey}: {len(vset)} valores distintos")

        if self.emit_mode == "explode":
            schema = self.group_out + self.value_out
        else:
            schema = self.group_out + [self.count_field]

        batch = []
        enviados = 0
        for gkey, vset in grupos.items():
            if self.operator == "gt":
                if len(vset) <= self.expected:
                    continue
            elif self.operator == "gte":
                if len(vset) < self.expected:
                    continue
            else:  # eq
                if len(vset) != self.expected:
                    continue

            if self.emit_mode == "explode":
                for vkey in vset:
                    batch.append(list(gkey) + list(vkey))
            else:
                batch.append(list(gkey) + [len(vset)])

            if len(batch) >= self.FLUSH_BATCH_SIZE:
                self._enviar_batch(client_id, schema, batch)
                enviados += len(batch)
                batch = []
        if batch:
            self._enviar_batch(client_id, schema, batch)
            enviados += len(batch)

        logger.info(f"[GroupDistinctCounter] Flush completo para client_id={client_id}. Registros emitidos: {enviados}.")

        if os.environ.get("CRASH_AFTER_FLUSH") == "true":
            bandera = os.path.join(BASE_DIR, "crash_flush_done")
            if not os.path.exists(bandera):
                open(bandera, "w").close()
                logger.warning("[GroupDistinctCounter] CRASH_AFTER_FLUSH — muriendo después del envío, antes de barrier_completada")
                os._exit(1)

        PersistidorEstado(self._nombre_nodo(client_id), base_dir=BASE_DIR).guardar({"barrier_completada": True})
        PersistidorEstado(self._nombre_nodo(client_id), base_dir=BASE_DIR).borrar()

    def al_desconectar_cliente(self, client_id: str):
        acks_a_liberar = []
        with self._lock:
            self._grupos.pop(client_id, None)
            self._vistos.pop(client_id, None)
            acks_a_liberar = self._pending_acks.pop(client_id, [])
        PersistidorEstado(self._nombre_nodo(client_id), base_dir=BASE_DIR).borrar()
        for fn in acks_a_liberar:
            fn()
        logger.info(f"[GroupDistinctCounter] Estado descartado para {client_id}.")

    def al_cerrar(self):
        logger.info("[GroupDistinctCounter] Apagado.")


def main():
    setup_logging("group_distinct_counter")
    GroupDistinctCounterWorker().iniciar()


if __name__ == "__main__":
    main()
