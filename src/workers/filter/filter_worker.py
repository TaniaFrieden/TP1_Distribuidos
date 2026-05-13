"""
FilterWorker
============
Worker de filtrado genérico configurable por variables de entorno.
Hereda de BaseWorker y descarta mensajes que no cumplan las condiciones.

Variables de entorno requeridas:
    RABBITMQ_HOST   - host de RabbitMQ (default: "rabbitmq")
    COLA_ENTRADA    - cola de la que se consumen mensajes
    COLA_SALIDA     - cola a la que se publican los mensajes que pasan

Variables de entorno opcionales:
    FILTROS         - condiciones separadas por coma. Formato por condición:
                        campo:operador:valor
                      Operadores soportados:
                        lt, lte, gt, gte, eq, neq   → numéricos / fechas
                        eq, neq, contiene, no_contiene → strings
                      Ejemplos:
                        "amount_paid:lt:50"
                        "timestamp:gte:2023-09-01,timestamp:lte:2023-09-05"
                        "payment_format:eq:Wire"
                        "payment_currency:neq:USD"

"""

import os
import json
import logging
from datetime import datetime

from common.worker_base.base import BaseWorker
from common.middleware.middleware_rabbitmq import DirectQueueRabbitMQ

logger = logging.getLogger(__name__)

# ------------------------------------------------------------------
# Operadores soportados
# ------------------------------------------------------------------

OPERADORES = {
    "lt":           lambda valor_msg, valor_cfg: valor_msg < valor_cfg,
    "lte":          lambda valor_msg, valor_cfg: valor_msg <= valor_cfg,
    "gt":           lambda valor_msg, valor_cfg: valor_msg > valor_cfg,
    "gte":          lambda valor_msg, valor_cfg: valor_msg >= valor_cfg,
    "eq":           lambda valor_msg, valor_cfg: valor_msg == valor_cfg,
    "neq":          lambda valor_msg, valor_cfg: valor_msg != valor_cfg,
    "contiene":     lambda valor_msg, valor_cfg: valor_cfg in str(valor_msg),
    "no_contiene":  lambda valor_msg, valor_cfg: valor_cfg not in str(valor_msg),
}


# ------------------------------------------------------------------
# Parseo y tipado de valores
# ------------------------------------------------------------------

def _parsear_valor(valor_str: str):
    """
    Intenta convertir el valor de configuración al tipo más apropiado.
    Orden de intento: int → float → datetime → string.
    """
    # Entero
    try:
        return int(valor_str)
    except ValueError:
        pass

    # Float
    try:
        return float(valor_str)
    except ValueError:
        pass

    # Fecha (soporta YYYY-MM-DD y YYYY-MM-DD HH:MM:SS)
    for formato in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d"):
        try:
            return datetime.strptime(valor_str, formato)
        except ValueError:
            pass

    # String puro
    return valor_str


def _parsear_valor_mensaje(valor_raw):
    """
    Convierte el valor que viene en el mensaje al tipo correcto para comparar.
    Si es string que parece fecha, lo parsea como datetime.
    """
    if not isinstance(valor_raw, str):
        return valor_raw

    for formato in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d"):
        try:
            return datetime.strptime(valor_raw, formato)
        except ValueError:
            pass

    return valor_raw


# ------------------------------------------------------------------
# Parseo de la variable de entorno FILTROS
# ------------------------------------------------------------------

def parsear_filtros(filtros_str: str) -> list[dict]:
    """
    Parsea el string de la variable de entorno FILTROS.

    Formato: "campo:operador:valor,campo:operador:valor,..."

    Retorna lista de dicts:
        [{"campo": ..., "operador": ..., "valor": ...}, ...]

    Lanza ValueError si alguna condición está mal formada.
    """
    if not filtros_str or not filtros_str.strip():
        return []

    condiciones = []
    for parte in filtros_str.split(","):
        parte = parte.strip()
        if not parte:
            continue

        segmentos = parte.split(":")
        if len(segmentos) < 3:
            raise ValueError(
                f"Condición mal formada: '{parte}'. "
                f"Formato esperado: campo:operador:valor"
            )

        campo = segmentos[0].strip()
        operador = segmentos[1].strip()
        # El valor puede contener ':' (ej: timestamps con hora)
        valor_str = ":".join(segmentos[2:]).strip()

        if operador not in OPERADORES:
            raise ValueError(
                f"Operador desconocido: '{operador}'. "
                f"Operadores válidos: {list(OPERADORES.keys())}"
            )

        condiciones.append({
            "campo":    campo,
            "operador": operador,
            "valor":    _parsear_valor(valor_str),
        })

    return condiciones


# ------------------------------------------------------------------
# FilterWorker
# ------------------------------------------------------------------

class FilterWorker(BaseWorker):
    """
    Worker de filtrado genérico.

    Descarta los mensajes que NO cumplan TODAS las condiciones configuradas
    (comportamiento AND). Los que sí las cumplen se publican en COLA_SALIDA.
    """

    def __init__(self):
        super().__init__()
        self._host         = os.environ.get("RABBITMQ_HOST", "rabbitmq")
        self._cola_entrada = os.environ["COLA_ENTRADA"]
        self._cola_salida  = os.environ["COLA_SALIDA"]
        self._condiciones  = parsear_filtros(os.environ.get("FILTROS", ""))
        self._middleware_salida = None

        logger.info(
            f"[FilterWorker] Condiciones cargadas: {self._condiciones}"
        )

    # ------------------------------------------------------------------
    # BaseWorker API
    # ------------------------------------------------------------------

    def inicializar_middleware(self):
        self._middleware_salida = DirectQueueRabbitMQ(
            host=self._host,
            queue_name=self._cola_salida,
        )
        return DirectQueueRabbitMQ(
            host=self._host,
            queue_name=self._cola_entrada,
        )

    def procesar_mensaje(self, mensaje: bytes, ack, nack):
        datos = json.loads(mensaje)

        if self._cumple_condiciones(datos):
            self._middleware_salida.send(mensaje)
            logger.debug(f"[FilterWorker] Mensaje pasó → {self._cola_salida}")
        else:
            logger.debug("[FilterWorker] Mensaje descartado.")

        ack()

    def al_cerrar(self):
        if self._middleware_salida:
            try:
                self._middleware_salida.close()
            except Exception as e:
                logger.warning(f"[FilterWorker] Error al cerrar middleware salida: {e}")

    # ------------------------------------------------------------------
    # Lógica de filtrado
    # ------------------------------------------------------------------

    def _cumple_condiciones(self, datos: dict) -> bool:
        """
        Retorna True si el mensaje cumple TODAS las condiciones (AND).
        Si no existe el campo en el mensaje, descarta el mensaje.
        """
        for condicion in self._condiciones:
            campo    = condicion["campo"]
            operador = condicion["operador"]
            valor    = condicion["valor"]

            if campo not in datos:
                logger.debug(
                    f"[FilterWorker] Campo '{campo}' no encontrado en el mensaje. Descartando."
                )
                return False

            valor_msg = _parsear_valor_mensaje(datos[campo])

            try:
                if not OPERADORES[operador](valor_msg, valor):
                    return False
            except TypeError as e:
                logger.warning(
                    f"[FilterWorker] Error comparando campo '{campo}': {e}. Descartando."
                )
                return False

        return True