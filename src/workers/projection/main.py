import logging
import os
import json

try:
    from base import BaseWorker  # Docker runtime (base files copiados al root)
    from common.logging_setup import setup_logging
except ImportError:
    from workers.base.base import BaseWorker  # entorno de tests
    from common.logging_setup import setup_logging

logger = logging.getLogger(__name__)


class ProjectionWorker(BaseWorker):
    def __init__(self):
        super().__init__()
        campos_str = os.environ.get("CAMPOS", "")
        self.campos = [c.strip() for c in campos_str.split(",") if c.strip()]

        int_fields_str = os.environ.get("INT_FIELDS", "")
        self.int_fields = {f.strip() for f in int_fields_str.split(",") if f.strip()}

        logger.info(f"[ProjectionWorker] Campos: {self.campos} | INT_FIELDS: {self.int_fields}")

    def procesar_payload(self, queue_name: str, client_id: str, payload: dict, mensaje_original: bytes, ack, nack):
        try:
            proyectado = {"client_id": payload.get("client_id", client_id)}
            for campo in self.campos:
                if campo in payload:
                    valor = payload[campo]
                    if campo in self.int_fields:
                        try:
                            valor = int(valor)
                        except (ValueError, TypeError):
                            pass
                    proyectado[campo] = valor

            msg_bytes = json.dumps(proyectado).encode("utf-8")
            self._enviar(msg_bytes, payload=proyectado)
            ack()
        except Exception as e:
            logger.error(f"Error proyectando payload: {e}", exc_info=True)
            nack()

    def al_cerrar(self):
        logger.info("ProjectionWorker apagado.")


def main():
    setup_logging("projection")
    ProjectionWorker().iniciar()


if __name__ == "__main__":
    main()
