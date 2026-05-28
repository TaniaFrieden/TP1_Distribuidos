import logging
import os
import json
import threading
from base import BaseWorker
from common.logging_setup import setup_logging

logger = logging.getLogger(__name__)


class CounterWorker(BaseWorker):

    def __init__(self):
        super().__init__()
        self._conteos: dict = {}
        self._conteos_lock = threading.Lock()

    def procesar_payload(self, queue_name: str, client_id: str, payload: dict | str, mensaje_original: bytes, ack, nack):
        try:
            t = payload if isinstance(payload, dict) else json.loads(payload)
            with self._conteos_lock:
                if "batches" in t:
                    added_count = sum(int(batch["header"].get("count", len(batch["payload"]))) for batch in t["batches"])
                    self._conteos[client_id] = self._conteos.get(client_id, 0) + added_count
                else:
                    self._conteos[client_id] = self._conteos.get(client_id, 0) + 1
            ack()
        except Exception as e:
            logger.error(f"Error contando mensaje: {e}", exc_info=True)
            nack()

    def al_completar_cliente(self, client_id: str):
        with self._conteos_lock:
            count = self._conteos.pop(client_id, 0)
        output_payload = {
            "client_id": client_id,
            "batches": [
                {
                    "header": {
                        "schema": ["count"],
                        "client_id": client_id,
                        "count": 1
                    },
                    "payload": [[count]]
                }
            ]
        }
        self._enviar(json.dumps(output_payload).encode("utf-8"), payload=output_payload)
        logger.info(f"Q5 count emitido para {client_id}: {count} transacciones.")

    def al_desconectar_cliente(self, client_id: str):
        with self._conteos_lock:
            self._conteos.pop(client_id, None)
        logger.info(f"Counter: estado descartado para {client_id}.")

    def al_cerrar(self):
        logger.info("Counter apagado.")


def main():
    setup_logging("counter")
    CounterWorker().iniciar()


if __name__ == "__main__":
    main()
