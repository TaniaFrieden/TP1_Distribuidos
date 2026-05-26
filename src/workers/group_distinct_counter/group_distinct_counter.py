import logging
import json
import threading
import os

from base import BaseWorker
from common.logging_setup import setup_logging

logger = logging.getLogger(__name__)


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
        self.emit_mode    = os.environ.get("EMIT_MODE", "aggregate").lower()
        self.count_field  = os.environ.get("COUNT_OUTPUT_FIELD", "Amount Transactions")

        # { client_id: { group_key: set(value_key) } }
        self._grupos: dict = {}
        self._lock = threading.Lock()

        logger.info(
            f"[GroupDistinctCounter] group={self.group_fields} value={self.value_fields} "
            f"expected={self.expected} mode={self.emit_mode}"
        )

    def _make_key(self, payload: dict, fields: list) -> tuple:
        return tuple(str(payload.get(f, "")) for f in fields)

    def procesar_payload(self, queue_name: str, client_id: str, payload: dict,
                         mensaje_original: bytes, ack, nack):
        try:
            gkey = self._make_key(payload, self.group_fields)
            vkey = self._make_key(payload, self.value_fields)

            with self._lock:
                self._grupos.setdefault(client_id, {}).setdefault(gkey, set()).add(vkey)

            ack()
        except Exception as e:
            logger.error(f"Error procesando payload: {e}", exc_info=True)
            nack()

    def al_completar_cliente(self, client_id: str):
        with self._lock:
            grupos = self._grupos.pop(client_id, {})

        for gkey, vset in grupos.items():
            if len(vset) != self.expected:
                continue

            if self.emit_mode == "explode":
                for vkey in vset:
                    msg = {"client_id": client_id}
                    for name, val in zip(self.group_out, gkey):
                        msg[name] = val
                    for name, val in zip(self.value_out, vkey):
                        msg[name] = val
                    self._enviar(json.dumps(msg).encode("utf-8"), payload=msg)
            else:
                msg = {"client_id": client_id}
                for name, val in zip(self.group_out, gkey):
                    msg[name] = val
                msg[self.count_field] = self.expected
                self._enviar(json.dumps(msg).encode("utf-8"))

        logger.info(f"[GroupDistinctCounter] Flush completo para client_id={client_id}.")

    def al_cerrar(self):
        logger.info("[GroupDistinctCounter] Apagado.")


def main():
    setup_logging("group_distinct_counter")
    GroupDistinctCounterWorker().iniciar()


if __name__ == "__main__":
    main()
