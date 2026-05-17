import logging
import json
from common.worker_base.base import BaseWorker

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
logger = logging.getLogger(__name__)

# --- CONSTANTES DE LIMPIEZA DE CÓDIGO (Adiós magia) ---
SEPARADOR_CLIENTE = "|"
SEPARADOR_CSV = ","
SEÑAL_EOF = "EOF"
COLUMNA_FILTRO = "Payment Currency"
VALOR_FILTRO = "US Dollar"

# Tipos de mensajes para el canal de control
TIPO_BROADCAST_CABECERA = "HEADER_BROADCAST"


class FilterWorker(BaseWorker):
    def __init__(self):
        super().__init__()
        self._mapeo_columnas = None  # Guardará dinámicamente { "NombreColumna": índice }
        logger.info(f"Filtro iniciado de forma dinámica para: {VALOR_FILTRO}")

    def procesar_mensaje(self, mensaje: bytes, ack, nack):
        try:
            mensaje_str = mensaje.decode('utf-8')
            partes = mensaje_str.split(SEPARADOR_CLIENTE, 1)
            
            if len(partes) != 2:
                logger.warning(f"Mensaje corrupto omitido: {mensaje_str[:30]}...")
                ack()
                return
                
            client_id, datos = partes

            # 1. Instancia Clave: Detección de Fin de Transmisión (EOF)
            if datos == SEÑAL_EOF:
                logger.info(f"[EOF] Recibido para cliente {client_id}. Iniciando barrera de control...")
                self.coordinar_eof(client_id, mensaje)
                ack()
                return

            # 2. Parseo de la fila común
            columnas = [col.strip() for col in datos.split(SEPARADOR_CSV)]

            # Instancia Clave: Descubrimiento local del Header y emisión del Megáfono
            if COLUMNA_FILTRO in columnas:
                self._registrar_cabeceras(columnas, fuente="descubrimiento local")
                
                logger.info(f"[BROADCAST] Compartiendo estructura del CSV con el grupo para cliente {client_id}...")
                msg_control = {
                    "type": TIPO_BROADCAST_CABECERA,
                    "headers": columnas
                }
                self.control_exchange.send(json.dumps(msg_control).encode('utf-8'))
                
                # 👇 NUEVA BUENA PRÁCTICA: Reenviamos las cabeceras intactas río abajo 👇
                logger.info(f"[CABECERA] Reenviando fila de títulos al output para cliente {client_id}.")
                self._enviar(mensaje)
                
                ack()
                return  # Ahora sí podemos salir, porque ya la enviamos.

            # Instancia Clave: Mensaje adelantado al canal de control
            if not self._mapeo_columnas:
                logger.warning(f"[ESPERA] Fila recibida sin cabeceras listas para cliente {client_id}. Reencolando...")
                nack() 
                return

            # 3. Filtrado Mensaje a Mensaje
            indice_objetivo = self._mapeo_columnas.get(COLUMNA_FILTRO)
            if indice_objetivo is not None and len(columnas) > indice_objetivo:
                valor_actual = columnas[indice_objetivo]
                
                if valor_actual == VALOR_FILTRO:
                    logger.info(f"[PASÓ] Cliente {client_id}: {VALOR_FILTRO} (Enviado)")
                    self._enviar(mensaje)
                else:
                    logger.info(f"[FILTRADO] Cliente {client_id}: {valor_actual} (Descartado)")
            else:
                logger.warning(f"[FALTA_COLUMNA] Fila del cliente {client_id} no contiene el índice {indice_objetivo}")
            
            ack()

        except Exception as e:
            logger.error(f"Error procesando transacción: {e}", exc_info=True)
            nack()

    def _process_control_message(self, message, ack, nack):
        """
        Sobrescribimos el canal de control para escuchar las cabeceras
        que compartan nuestros hermanos del grupo.
        """
        try:
            msg_dict = json.loads(message.decode('utf-8'))
            
            if msg_dict.get("type") == TIPO_BROADCAST_CABECERA:
                cabeceras_recibidas = msg_dict.get("headers")
                logger.info("[CONTROL] Señal de cabeceras compartidas recibida desde el grupo.")
                self._registrar_cabeceras(cabeceras_recibidas, fuente="broadcast de grupo")
                
        except Exception as e:
            logger.error(f"Error procesando mensaje de control en Filtro: {e}")
        finally:
            # Crucial para mantener la sincronización del EOF en la clase base
            super()._process_control_message(message, ack, nack)

    def _registrar_cabeceras(self, lista_cabeceras, fuente):
        """Asigna el mapa de columnas de forma limpia y evita logs repetidos si ya se conocía."""
        nuevo_mapeo = {nombre: idx for idx, nombre in enumerate(lista_cabeceras)}
        if self._mapeo_columnas != nuevo_mapeo:
            self._mapeo_columnas = nuevo_mapeo
            logger.info(f"Estructura del CSV aprendida vía [{fuente}]: {self._mapeo_columnas}")

    def al_cerrar(self):
        logger.info("Limpieza finalizada.")


def __main__():
    worker = FilterWorker()
    worker.iniciar()

if __name__ == "__main__":
    __main__()