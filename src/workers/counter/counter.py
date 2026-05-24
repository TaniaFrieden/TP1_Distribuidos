import logging
import os
import json
import threading
from base import BaseWorker

logger = logging.getLogger(__name__)


class CounterWorker(BaseWorker):

    def __init__(self):
        super().__init__()
        self._conteos: dict = {}
        self._conteos_lock = threading.Lock()

    def procesar_payload(self, queue_name: str, client_id: str, payload: str, mensaje_original: bytes, ack, nack):
        try:
            with self._conteos_lock:
                self._conteos[client_id] = self._conteos.get(client_id, 0) + 1
            ack()
        except Exception as e:
            logger.error(f"Error contando mensaje: {e}", exc_info=True)
            nack()

    def al_completar_cliente(self, client_id: str):
        with self._conteos_lock:
            count = self._conteos.pop(client_id, 0)
        resultado = json.dumps({"client_id": client_id, "count": count}).encode("utf-8")
        self._enviar(resultado)
        logger.info(f"Q5 count emitido para {client_id}: {count} transacciones.")

    def al_cerrar(self):
        logger.info("Counter apagado.")


def main():
    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
    logging.getLogger("pika").setLevel(logging.WARNING)
    CounterWorker().iniciar()


if __name__ == "__main__":
    main()
