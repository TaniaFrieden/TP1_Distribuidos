import logging
import json
import threading
from base import BaseWorker

# puede ser que no este funciona porque es otra version

# Configuración de logs limpia y visible
logging.basicConfig(level=logging.INFO, format="%(message)s")
logger = logging.getLogger(__name__)

class AgregadorBancarioWorker(BaseWorker):
    def __init__(self):
        super().__init__()
        # Estructura: { "client_id": { "bank_id": {"bank_name": str, "max_amount": float} } }
        self.estado_agregador = {}
        self.lock_estado = threading.Lock()
        logger.info("[AgregadorBancario] Worker inicializado y listo para doble escucha.")

    def procesar_payload(self, queue_name: str, client_id: str, payload: dict, mensaje_original: bytes, ack, nack):
        # ---------------------------------------------------------
        # NUEVO LOG: Muestra absolutamente todo lo que llega, tal cual
        # ---------------------------------------------------------
        logger.info(f"[MENSAJE ENTRANTE] Cola: '{queue_name}' | Cliente: {client_id} | Payload: {payload}")
        
        # Si de verdad necesitas ver los bytes crudos literales antes de ser diccionario, podés descomentar esto:
        # logger.info(f"[MENSAJE RAW] Cola: '{queue_name}' | Raw Bytes: {mensaje_original.decode('utf-8', errors='ignore')}")
        
        try:
            bank_id = payload.get("bank_id", payload.get("Bank ID", "N/A"))
            

            logger.debug(f"Intentando adquirir LOCK para procesar mensaje de la cola: {queue_name}")
            if "transactions" in queue_name:
                bank_id = payload.get("From Bank", "N/A")
            elif "banks" in queue_name:
                bank_id = payload.get("Bank ID", payload.get("bank_id", "N/A"))
            else:
                bank_id = "N/A"

            logger.debug(f"Procesando para Cliente: {client_id} | Banco identificado: {bank_id}")

            with self.lock_estado:
                # Inicializar estructuras jerárquicas con logs explícitos
                if client_id not in self.estado_agregador:
                    logger.info(f"[CLIENTE NUEVO] Inicializando estructuras para client_id: {client_id}")
                    self.estado_agregador[client_id] = {}
                
                if bank_id not in self.estado_agregador[client_id]:
                    self.estado_agregador[client_id][bank_id] = {
                        "bank_name": "Desconocido",
                        "max_amount": 0.0
                    }

                # --- RAMAL DE TRANSACCIONES ---
                if "transactions" in queue_name:
                    amount_str = payload.get("Amount Received", payload.get("amount", "0"))
                    logger.debug(f"Procesando transacción para Cliente {client_id} -> Banco {bank_id}: Monto bruto extraído: '{amount_str}'")
                    account = payload.get("Account", "N/A")
                    try:
                        amount = float(amount_str)
                    except ValueError:
                        logger.error(f"Error al castear monto de transacción: '{amount_str}'")
                        amount = 0.0

                    max_actual = self.estado_agregador[client_id][bank_id]["max_amount"]
                    if amount > max_actual:
                        logger.info(f"[NUEVO MÁXIMO] Cliente {client_id} -> Banco {bank_id}: Viejo: {max_actual} -> Nuevo: {amount}")
                        self.estado_agregador[client_id][bank_id]["max_amount"] = amount
                        self.estado_agregador[client_id][bank_id]["origin_account"] = account
                
                # --- RAMAL DE BANCOS ---
                elif "banks" in queue_name:
                    b_name = payload.get("Bank Name", payload.get("bank_name", "Desconocido"))
                    # logger.info(f"[INFO BANCO] Cliente {client_id} -> Banco {bank_id}: Nombre registrado: '{b_name}'")
                    self.estado_agregador[client_id][bank_id]["bank_name"] = b_name

            # Confirmación explícita al Middleware
            ack()

        except Exception as e:
            logger.error(f"CRÍTICO: Excepción en procesar_payload sobre la cola {queue_name}: {e}", exc_info=True)
            nack()

    def al_completar_cliente(self, client_id: str):
        with self.lock_estado:
            if client_id in self.estado_agregador:
                bancos_del_cliente = self.estado_agregador.pop(client_id)
                logger.info(f"[BARRERA CONTROL] Vaciando estado. Despachando {len(bancos_del_cliente)} registros de bancos consolidados.")
                
                for bank_id, datos in bancos_del_cliente.items():
                    payload_final = {
                        "client_id": client_id,
                        "Bank ID": bank_id,
                        "Bank Name": datos["bank_name"],
                        "Account": datos["origin_account"],
                        "Max Amount": datos["max_amount"]
                    }
                    mensaje_bytes = json.dumps(payload_final).encode('utf-8')
                    self._enviar(mensaje_bytes)
                logger.info(f"[BARRERA CONTROL] Envío finalizado con éxito para cliente {client_id}.")
            else:
                logger.warning(f"[BARRERA CONTROL] Se disparó al_completar_cliente para {client_id} pero no tenía datos locales registrados.")

    def al_cerrar(self):
        logger.info("[AgregadorBancario] Solicitud de apagado recibida de las señales del sistema.")

def __main__():
    worker = AgregadorBancarioWorker()
    worker.iniciar()

if __name__ == "__main__":
    __main__()