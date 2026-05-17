import logging
from common.worker_base.base import BaseWorker

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
logger = logging.getLogger(__name__)

class FilterWorker(BaseWorker):
    def __init__(self):
        super().__init__()
        logger.info("Iniciado Filtro: US Dollar")

    def procesar_mensaje(self, mensaje: bytes, ack, nack):
        try:
            mensaje_str = mensaje.decode('utf-8')
            
            # 2. Separamos el UUID del cliente del resto del mensaje
            # Esto asume el formato que armamos: "client_id|datos"
            partes = mensaje_str.split("|", 1)
            if len(partes) != 2:
                logger.warning(f"Mensaje mal formado descartado: {mensaje_str[:30]}...")
                ack()
                return
                
            client_id, datos = partes

            if datos == "EOF":
                logger.info(f"EOF recibido del cliente {client_id}. Reenviando a la cola...")
                self._enviar(mensaje)
                ack()
                return

            columnas = datos.split(",")
            
            # El CSV tiene las siguientes posiciones:
            # 0:Timestamp, 1:From Bank, 2:Account, 3:To Bank, 4:Account.1, 
            # 5:Amount Received, 6:Receiving Currency, 7:Amount Paid, 8:Payment Currency
            if len(columnas) > 8:
                payment_currency = columnas[8].strip()
                
                if payment_currency == "US Dollar":
                    self._enviar(mensaje)
            
            ack()

        except Exception as e:
            logger.error(f"Error procesando mensaje: {e}")
            nack()

    def al_cerrar(self):
        logger.info("Cerrando conexiones al middleware...")
        pass

def __main__():
    worker = FilterWorker()
    worker.iniciar()

if __name__ == "__main__":
    __main__()