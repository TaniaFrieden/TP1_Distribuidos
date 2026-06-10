import logging
import os
import json
import threading
from base import BaseWorker
from common.logging_setup import setup_logging
from common.persistencia import PersistidorEstado

logger = logging.getLogger(__name__)

BASE_DIR = "/app/volumen"


class CounterWorker(BaseWorker):

    def __init__(self):
        super().__init__()
        self._conteos: dict = {}
        self._conteos_lock = threading.Lock()
        self._recover_state_from_disk()

    def _node_name(self, client_id: str) -> str:
        return f"counter_{self.config.node_id}_{client_id}"

    def _recover_state_from_disk(self):
        if not os.path.exists(BASE_DIR):
            return
        prefix = f"counter_{self.config.node_id}_"
        for folder_name in os.listdir(BASE_DIR):
            if folder_name.startswith(prefix):
                client_id = folder_name[len(prefix):]
                persistidor = PersistidorEstado(folder_name, base_dir=BASE_DIR)
                saved = persistidor.cargar()
                if saved:
                    with self._conteos_lock:
                        self._conteos[client_id] = saved.get("count", 0)
                    logger.info(f"[Counter] Recuperado estado de disco para {client_id}: {self._conteos[client_id]}")

    def procesar_payload(self, queue_name: str, client_id: str, payload: dict | str, mensaje_original: bytes, ack, nack):
        try:
            t = payload if isinstance(payload, dict) else json.loads(payload)
            with self._conteos_lock:
                if "batches" in t:
                    added_count = sum(int(batch["header"].get("count", len(batch["payload"]))) for batch in t["batches"])
                    self._conteos[client_id] = self._conteos.get(client_id, 0) + added_count
                else:
                    self._conteos[client_id] = self._conteos.get(client_id, 0) + 1

                PersistidorEstado(self._node_name(client_id), base_dir=BASE_DIR).guardar(
                    {"client_id": client_id, "count": self._conteos[client_id]}
                )
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
        PersistidorEstado(self._node_name(client_id), base_dir=BASE_DIR).borrar()

    def al_desconectar_cliente(self, client_id: str):
        with self._conteos_lock:
            self._conteos.pop(client_id, None)
        logger.info(f"Counter: estado descartado para {client_id}.")
        PersistidorEstado(self._node_name(client_id), base_dir=BASE_DIR).borrar()

    def al_cerrar(self):
        logger.info("Counter apagado.")


def main():
    setup_logging("counter")
    CounterWorker().iniciar()


if __name__ == "__main__":
    main()
