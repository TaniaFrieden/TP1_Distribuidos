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
        self._conteos: dict[str, int] = {}
        self._vistos: dict[str, set] = {}
        self._lock = threading.Lock()
        self._recover_state_from_disk()

    def _nombre_nodo(self, client_id: str) -> str:
        return f"counter_{self.config.node_id}_{client_id}"

    def _recover_state_from_disk(self):
        if not os.path.exists(BASE_DIR):
            return
        prefijo = f"counter_{self.config.node_id}_"
        for carpeta in os.listdir(BASE_DIR):
            if carpeta.startswith(prefijo):
                client_id = carpeta[len(prefijo):]
                persistidor = PersistidorEstado(carpeta, base_dir=BASE_DIR)
                estado = persistidor.cargar()
                if estado:
                    with self._lock:
                        self._conteos[client_id] = estado.get("count", 0)
                        self._vistos[client_id] = set(estado.get("vistos", []))
                    logger.info(f"[Counter] Recuperado estado de disco para {client_id}: count={self._conteos[client_id]}, vistos={len(self._vistos[client_id])}")

    def _guardar_estado(self, client_id: str):
        PersistidorEstado(self._nombre_nodo(client_id), base_dir=BASE_DIR).guardar({
            "client_id": client_id,
            "count": self._conteos[client_id],
            "vistos": list(self._vistos[client_id])
        })

    def procesar_payload(self, queue_name: str, client_id: str, payload: dict | str, mensaje_original: bytes, ack, nack):
        try:
            t = payload if isinstance(payload, dict) else json.loads(payload)
            msg_id = t.get("msg_id")

            with self._lock:
                if msg_id and msg_id in self._vistos.get(client_id, set()):
                    logger.info(f"[Counter] Mensaje duplicado ignorado: msg_id={msg_id} para {client_id}")
                    ack()
                    return

                if "batches" in t:
                    cantidad = sum(int(batch["header"].get("count", len(batch["payload"]))) for batch in t["batches"])
                    self._conteos[client_id] = self._conteos.get(client_id, 0) + cantidad
                else:
                    self._conteos[client_id] = self._conteos.get(client_id, 0) + 1

                if msg_id:
                    if client_id not in self._vistos:
                        self._vistos[client_id] = set()
                    self._vistos[client_id].add(msg_id)

                self._guardar_estado(client_id)

            ack()
        except Exception as e:
            logger.error(f"Error contando mensaje: {e}", exc_info=True)
            nack()

    def al_completar_cliente(self, client_id: str):
        with self._lock:
            count = self._conteos.pop(client_id, 0)
            self._vistos.pop(client_id, None)

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
        PersistidorEstado(self._nombre_nodo(client_id), base_dir=BASE_DIR).borrar()

    def al_desconectar_cliente(self, client_id: str):
        with self._lock:
            self._conteos.pop(client_id, None)
            self._vistos.pop(client_id, None)
        logger.info(f"Counter: estado descartado para {client_id}.")
        PersistidorEstado(self._nombre_nodo(client_id), base_dir=BASE_DIR).borrar()

    def al_cerrar(self):
        logger.info("Counter apagado.")


def main():
    setup_logging("counter")
    CounterWorker().iniciar()


if __name__ == "__main__":
    main()
