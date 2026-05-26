import logging
import json
import threading
from base import BaseWorker
from common.sharding import normalizar_valor_hash
from common.logging_setup import setup_logging

logger = logging.getLogger(__name__)

class AgregadorBancarioWorker(BaseWorker):
    def __init__(self):
        super().__init__()
        # Estructura: { "client_id": { "bank_id": {"bank_name": str, "max_amount": float, "account": str} } }
        self.estado_agregador = {}
        # Estructura para el control de EOFs en dos fases: { "client_id": {...} }
        self.estado_eof = {}
        self.lock_estado = threading.Lock()
        logger.info("[AgregadorBancario] Worker inicializado con coordinación estricta de dos fases.")

    def procesar_payload(self, queue_name: str, client_id: str, payload: dict, mensaje_original: bytes, ack, nack):
        logger.debug(f"[MENSAJE ENTRANTE] Cola: '{queue_name}' | Cliente: {client_id}")

        try:
            # 1. Extraemos y normalizamos el Bank ID dependiendo del origen
            if "transactions" in queue_name:
                bank_id = normalizar_valor_hash(payload.get("From Bank"))
            elif "banks" in queue_name:
                bank_id = normalizar_valor_hash(payload.get("Bank ID"))
            else:
                ack()
                return

            with self.lock_estado:
                # 2. Inicialización segura del estado del cliente y del banco
                if client_id not in self.estado_agregador:
                    logger.info(f"[CLIENTE NUEVO] Inicializando estado para {client_id}")
                    self.estado_agregador[client_id] = {}
                
                if bank_id not in self.estado_agregador[client_id]:
                    self.estado_agregador[client_id][bank_id] = {
                        "bank_name": "Desconocido",
                        "max_amount": 0.0,
                        "account": "Desconocida"
                    }

                # 3. Lógica de actualización según la cola de origen
                if "banks" in queue_name:
                    # Metadatos del Banco
                    self.estado_agregador[client_id][bank_id]["bank_name"] = payload.get("Bank Name", "Desconocido")
                    
                    # Guardamos la cuenta del banco como fallback por si no llega ninguna transacción
                    if self.estado_agregador[client_id][bank_id]["account"] == "Desconocida":
                        self.estado_agregador[client_id][bank_id]["account"] = payload.get("Account Number", "Desconocida")

                elif "transactions" in queue_name:
                    # Datos de la Transacción
                    monto_str = payload.get("Amount Paid", payload.get("Amount Received", "0"))
                    monto = float(monto_str)
                    
                    # Si encontramos un nuevo máximo, actualizamos el monto Y guardamos la cuenta responsable
                    if monto > self.estado_agregador[client_id][bank_id]["max_amount"]:
                        self.estado_agregador[client_id][bank_id]["max_amount"] = monto
                        self.estado_agregador[client_id][bank_id]["account"] = payload.get("Account", "Desconocida")

            # Confirmamos a RabbitMQ que procesamos el mensaje con éxito
            ack()

        except ValueError as e:
            logger.error(f"Error de conversión numérica para el cliente {client_id}: {e}")
            nack()
        except Exception as e:
            logger.error(f"Error procesando mensaje: {e}", exc_info=True)
            nack()

    def interceptar_eof(self, queue_name: str, client_id: str, payload: dict, mensaje_original: bytes) -> bool:
        disparar_flush = False
        mensaje_barrera = None

        with self.lock_estado:
            if client_id not in self.estado_eof:
                self.estado_eof[client_id] = {
                    "transacciones_cerrado": False,
                    "bancos_cerrado": False,
                    "eof_mensaje": None,
                    "flush_iniciado": False
                }

            estado = self.estado_eof[client_id]

            if not estado["eof_mensaje"]:
                estado["eof_mensaje"] = mensaje_original

            if "transactions" in queue_name:
                logger.info(f"[BankShard] EOF Transacciones recibido para {client_id}.")
                estado["transacciones_cerrado"] = True
            elif "banks" in queue_name:
                logger.info(f"[BankShard] EOF Bancos recibido para {client_id}.")
                estado["bancos_cerrado"] = True

            if estado["transacciones_cerrado"] and estado["bancos_cerrado"] and not estado["flush_iniciado"]:
                logger.info(f"[BankShard] Ambas colas cerradas para {client_id}. Solicitando barrera de flush.")
                estado["flush_iniciado"] = True
                disparar_flush = True
                mensaje_barrera = estado["eof_mensaje"]

        if disparar_flush:
            self.coordinator.iniciar_barrera(client_id, mensaje_barrera)

        return True
    def al_completar_cliente(self, client_id: str):
        """Callback ejecutado cuando la barrera de EOFs local y global se completó."""
        with self.lock_estado:
            if client_id in self.estado_agregador:
                for bank_id, datos in self.estado_agregador[client_id].items():
                    
                    # Criterios de descarte
                    if datos["max_amount"] <= 0.0:
                        continue

                    if datos["bank_name"] == "Desconocido":
                        logger.warning(f"[FILTRO] Descartando banco {bank_id} para cliente {client_id}: Nombre desconocido.")
                        continue
                    
                    # Formato JSON final solicitado
                    payload_final = {
                        "client_id": client_id,
                        "Bank ID": bank_id,
                        "Bank Name": datos["bank_name"],
                        "Account": datos["account"],
                        "Max Amount": datos["max_amount"]
                    }
                    
                    mensaje_bytes = json.dumps(payload_final).encode('utf-8')
                    self._enviar(mensaje_bytes)

                logger.info(f"[BARRERA CONTROL] Envío finalizado con éxito para cliente {client_id}.")
                
                # Liberamos la memoria del cliente
                del self.estado_agregador[client_id]
            else:
                logger.warning(f"[BARRERA CONTROL] Se disparó al_completar_cliente para {client_id} sin datos locales registrados.")
            
            # Limpiamos también el estado de control de los EOFs
            if client_id in self.estado_eof:
                del self.estado_eof[client_id]

    def al_cerrar(self):
        logger.info("[AgregadorBancario] Solicitud de apagado recibida de las señales del sistema.")

def __main__():
    setup_logging("bank_shard")
    worker = AgregadorBancarioWorker()
    worker.iniciar()

if __name__ == "__main__":
    __main__()