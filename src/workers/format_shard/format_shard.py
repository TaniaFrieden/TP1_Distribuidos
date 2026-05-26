import logging
import json
import threading
from base import BaseWorker
from common.logging_setup import setup_logging

logger = logging.getLogger(__name__)

class FormatShardWorker(BaseWorker):
    def __init__(self):
        super().__init__()
        # Máquina de estados centralizada por client_id
        self.estado_clientes = {}
        self.lock = threading.Lock()
        logger.info("[FormatShard] Worker inicializado con coordinación en dos fases (temprano/tardío).")

    def _get_estado(self, client_id: str):
        if client_id not in self.estado_clientes:
            self.estado_clientes[client_id] = {
                "temprano_cerrado": False,
                "tardio_cerrado": False,
                "promedios_listos": False,
                "promedios": {},
                "cache_tardio": [],
                "datos_temprano": {},
                "eof_mensaje": None,
                "cache_procesado": False
            }
        return self.estado_clientes[client_id]

    def procesar_payload(self, queue_name: str, client_id: str, payload: dict, mensaje_original: bytes, ack, nack):
        try:
            with self.lock:
                estado = self._get_estado(client_id)
                
                if "temprano" in queue_name:
                    formato = payload.get("Payment Format", "")
                    monto = float(payload.get("Amount Paid", 0))
                    
                    if formato not in estado["datos_temprano"]:
                        estado["datos_temprano"][formato] = {"suma": 0.0, "count": 0}
                    estado["datos_temprano"][formato]["suma"] += monto
                    estado["datos_temprano"][formato]["count"] += 1
                    
                elif "tardio" in queue_name:
                    estado["cache_tardio"].append(payload)

            ack()

        except Exception as e:
            logger.error(f"Error procesando mensaje en {queue_name}: {e}", exc_info=True)
            nack()

    def interceptar_eof(self, queue_name: str, client_id: str, payload: dict, mensaje_original: bytes) -> bool:
        """
        Sobrescribe el flujo base para orquestar las fases independiente del orden de llegada.
        """
        disparar_flush = False
        estado = None

        with self.lock:
            estado = self._get_estado(client_id)
            # Guardamos un EOF válido para usarlo en la barrera cuando sea el momento
            if not estado["eof_mensaje"]:
                estado["eof_mensaje"] = mensaje_original

            # Resolvemos qué fase acaba de llegar
            if "temprano" in queue_name:
                logger.info(f"[Q3] EOF Temprano recibido para {client_id}. Calculando promedios...")
                estado["temprano_cerrado"] = True
                self._calcular_promedios(estado)
            
            elif "tardio" in queue_name:
                logger.info(f"[Q3] EOF Tardío recibido para {client_id}. Cerrando fase de caché...")
                estado["tardio_cerrado"] = True

            # Si ambas fases están completas y aún no procesamos, ejecutamos el cierre local
            if estado["temprano_cerrado"] and estado["tardio_cerrado"] and not estado["cache_procesado"]:
                logger.info(f"[Q3] Ambas fases cerradas para {client_id}. Procesando caché tardío ({len(estado['cache_tardio'])} items).")
                self._procesar_cache_tardio(client_id, estado)
                estado["cache_procesado"] = True
                disparar_flush = True

        # Importante: Disparamos la barrera fuera del lock para no bloquear colas
        if disparar_flush:
            logger.info(f"[Q3] Caché procesado. Delegando al coordinador para iniciar barrera distribuida.")
            self.coordinator.iniciar_barrera(client_id, estado["eof_mensaje"])

        return True  # Le avisamos a base.py que no haga nada más

    def _calcular_promedios(self, estado: dict):
        for formato, stats in estado["datos_temprano"].items():
            if stats["count"] > 0:
                estado["promedios"][formato] = stats["suma"] / stats["count"]
        estado["promedios_listos"] = True

    def _procesar_cache_tardio(self, client_id: str, estado: dict):
        promedios = estado["promedios"]
        for payload in estado["cache_tardio"]:
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
        
        # Vaciamos la memoria inmediatamente después de procesar
        estado["cache_tardio"].clear()
        estado["datos_temprano"].clear()

    def al_completar_cliente(self, client_id: str):
        """Hook llamado por base.py tras finalizar TODO el flush distribuido."""
        with self.lock:
            if client_id in self.estado_clientes:
                logger.info(f"[FormatShard] Limpiando estado final para {client_id}")
                del self.estado_clientes[client_id]

    def al_cerrar(self):
        logger.info("[FormatShard] Apagado exitoso.")

def __main__():
    setup_logging("format_shard")
    worker = FormatShardWorker()
    worker.iniciar()

if __name__ == "__main__":
    __main__()