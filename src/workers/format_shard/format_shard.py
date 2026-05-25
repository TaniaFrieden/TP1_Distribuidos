import logging
import json
import threading
from base import BaseWorker
from common.logging_setup import setup_logging

logger = logging.getLogger(__name__)

class FormatShardWorker(BaseWorker):
    def __init__(self):
        super().__init__()
        # { client_id: { formato: { suma: float, count: int } } }
        self.promedios_por_formato = {}
        # { client_id: [ {payload} ] }
        self.cache_tardio = {}
        self.lock = threading.Lock()
        logger.info("[FormatShard] Worker inicializado.")

    def procesar_payload(self, queue_name: str, client_id: str, payload: dict, mensaje_original: bytes, ack, nack):
        try:
            if "temprano" in queue_name:
                formato = payload.get("Payment Format", "")
                monto = float(payload.get("Amount Paid", 0))
                with self.lock:
                    if client_id not in self.promedios_por_formato:
                        self.promedios_por_formato[client_id] = {}
                    if formato not in self.promedios_por_formato[client_id]:
                        self.promedios_por_formato[client_id][formato] = {"suma": 0.0, "count": 0}
                    self.promedios_por_formato[client_id][formato]["suma"] += monto
                    self.promedios_por_formato[client_id][formato]["count"] += 1

            elif "tardio" in queue_name:
                with self.lock:
                    if client_id not in self.cache_tardio:
                        self.cache_tardio[client_id] = []
                    self.cache_tardio[client_id].append(payload)

            ack()

        except Exception as e:
            logger.error(f"Error procesando mensaje: {e}", exc_info=True)
            nack()

    def al_completar_cliente(self, client_id: str):
        with self.lock:
            total_temprano = sum(s["count"] for s in self.promedios_por_formato.get(client_id, {}).values())
            total_tardio = len(self.cache_tardio.get(client_id, []))
            logger.info(f"[Q3] temprano={total_temprano} tardio={total_tardio}")
            promedios = {
                formato: stats["suma"] / stats["count"]
                for formato, stats in self.promedios_por_formato.get(client_id, {}).items()
                if stats["count"] > 0
            }
            cache = self.cache_tardio.get(client_id, [])

        for payload in cache:
            formato = payload.get("Payment Format", "")
            monto = float(payload.get("Amount Paid", 0))
            promedio = promedios.get(formato)

            if promedio is None:
                continue

            if monto < promedio * 0.01:
                resultado = {
                    "client_id": client_id,
                    "From Account": payload.get("Account", ""),
                    "Amount Paid": monto
                }
                self._enviar(json.dumps(resultado).encode('utf-8'))

        with self.lock:
            self.promedios_por_formato.pop(client_id, None)
            self.cache_tardio.pop(client_id, None)
    def al_cerrar(self):
        logger.info("[FormatShard] Apagado.")

def __main__():
    setup_logging("format_shard")
    worker = FormatShardWorker()
    worker.iniciar()

if __name__ == "__main__":
    __main__()