import logging
import json

from base.base import BaseWorker
from common.logging_setup import setup_logging
from projection_config import ProjectionConfig
from processor import ProjectionProcessor

logger = logging.getLogger(__name__)


class ProjectionWorker(BaseWorker):
    def __init__(self):
        super().__init__()
        self.projection_config = ProjectionConfig()
        self.processor = ProjectionProcessor(
            self.projection_config.fields,
            self.projection_config.int_fields
        )

        logger.info(
            f"[ProjectionWorker] Campos: {self.projection_config.fields} | "
            f"INT_FIELDS: {self.projection_config.int_fields}"
        )

    def procesar_payload(self, queue_name: str, client_id: str, payload: dict, mensaje_original: bytes, ack, nack):
        try:
            if payload.get("EOF"):
                self._enviar(mensaje_original)
                ack()
                return

            if "batches" in payload:
                result = self.processor.process_payload(payload, client_id)
                if result:
                    msg_bytes = json.dumps(result).encode("utf-8")
                    self._enviar(msg_bytes, payload=result)
            else:
                projected = self.processor.process_single(payload, client_id)
                msg_bytes = json.dumps(projected).encode("utf-8")
                self._enviar(msg_bytes, payload=projected)
            
            ack()
        except Exception as e:
            logger.error(f"Error proyectando payload: {e}", exc_info=True)
            nack()

    def al_completar_cliente(self, client_id: str):
        # Cada worker envía su propio EOF por su propia conexión TCP, garantizando
        # que sus datos lleguen a RabbitMQ antes que este EOF (ordering por conexión).
        eof_sintetico = json.dumps({
            "client_id": client_id,
            "EOF": True,
            "request_id": f"_peof_{self.config.node_id}_{client_id[:8]}"
        }).encode('utf-8')
        self._enviar(eof_sintetico)

    def al_cerrar(self):
        logger.info("ProjectionWorker apagado.")


def main():
    setup_logging("projection")
    ProjectionWorker().iniciar()


if __name__ == "__main__":
    main()
