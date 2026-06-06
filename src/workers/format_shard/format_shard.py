import logging
import json
import threading
from base import BaseWorker
from common.logging_setup import setup_logging

logger = logging.getLogger(__name__)

class FormatShardWorker(BaseWorker):
    def __init__(self):
        super().__init__()
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
            if "batches" in payload:
                with self.lock:
                    estado = self._get_estado(client_id)
                    for batch in payload["batches"]:
                        header = batch["header"]
                        schema = header["schema"]
                        records = batch["payload"]
                        
                        if "temprano" in queue_name:
                            formato_idx = schema.index("Payment Format") if "Payment Format" in schema else None
                            monto_idx = schema.index("Amount Paid") if "Amount Paid" in schema else None
                            
                            for record_values in records:
                                formato = record_values[formato_idx] if formato_idx is not None else ""
                                monto = float(record_values[monto_idx] if monto_idx is not None else 0)
                                
                                if formato not in estado["datos_temprano"]:
                                    estado["datos_temprano"][formato] = {"suma": 0.0, "count": 0}
                                estado["datos_temprano"][formato]["suma"] += monto
                                estado["datos_temprano"][formato]["count"] += 1
                        elif "tardio" in queue_name:
                            for record_values in records:
                                estado["cache_tardio"].append((schema, record_values))
            else:
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
                        schema = list(payload.keys())
                        record_values = list(payload.values())
                        estado["cache_tardio"].append((schema, record_values))

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
            if not estado["eof_mensaje"]:
                estado["eof_mensaje"] = mensaje_original

            if "temprano" in queue_name:
                logger.info(f"[Q3] EOF Temprano recibido para {client_id}. Calculando promedios...")
                estado["temprano_cerrado"] = True
                self._calcular_promedios(estado)
            
            elif "tardio" in queue_name:
                logger.info(f"[Q3] EOF Tardío recibido para {client_id}. Cerrando fase de caché...")
                estado["tardio_cerrado"] = True

            if estado["temprano_cerrado"] and estado["tardio_cerrado"] and not estado["cache_procesado"]:
                logger.info(f"[Q3] Ambas fases cerradas para {client_id}. Procesando caché tardío ({len(estado['cache_tardio'])} items).")
                self._procesar_cache_tardio(client_id, estado)
                estado["cache_procesado"] = True
                disparar_flush = True

        # Disparamos la barrera fuera del lock para evitar deadlock con las colas de entrada
        if disparar_flush:
            logger.info(f"[Q3] Caché procesado. Delegando al coordinador para iniciar barrera distribuida.")
            self.coordinator.iniciar_barrera(client_id, estado["eof_mensaje"])

        return True

    def _calcular_promedios(self, estado: dict):
        for formato, stats in estado["datos_temprano"].items():
            if stats["count"] > 0:
                estado["promedios"][formato] = stats["suma"] / stats["count"]
        estado["promedios_listos"] = True

    def _procesar_cache_tardio(self, client_id: str, estado: dict):
        """Emite transacciones tardías cuyo monto es inferior al 1% del promedio de su formato."""
        promedios = estado["promedios"]

        records = []
        for schema, record_values in estado["cache_tardio"]:
            from_bank_idx = schema.index("From Bank") if "From Bank" in schema else None
            formato_idx = schema.index("Payment Format") if "Payment Format" in schema else None
            monto_idx = schema.index("Amount Paid") if "Amount Paid" in schema else None
            account_idx = schema.index("Account") if "Account" in schema else None
            
            from_bank = record_values[from_bank_idx] if from_bank_idx is not None else ""
            formato = record_values[formato_idx] if formato_idx is not None else ""
            monto = float(record_values[monto_idx] if monto_idx is not None else 0)
            promedio = promedios.get(formato)
            
            if promedio is None:
                continue
                
            if monto < promedio * 0.01:
                account = record_values[account_idx] if account_idx is not None else ""
                records.append([from_bank, account, formato, monto])
                
        if records:
            output_payload = {
                "client_id": client_id,
                "batches": [
                    {
                        "header": {
                            "schema": ["From Bank", "Account", "Payment Format", "Amount Paid"],
                            "client_id": client_id,
                            "count": len(records)
                        },
                        "payload": records
                    }
                ]
            }
            self._enviar(json.dumps(output_payload).encode('utf-8'), payload=output_payload)
        
        estado["cache_tardio"] = []
        estado["datos_temprano"] = {}
        estado["promedios"] = {}


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