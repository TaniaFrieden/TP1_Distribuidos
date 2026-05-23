import logging
import os
import operator
import json

from base import BaseWorker

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
logger = logging.getLogger(__name__)

class GenericFilterWorker(BaseWorker):
    def __init__(self):
        super().__init__()
        
        self.campo_objetivo = os.environ["FILTER_FIELD"]
        self.valor_objetivo = os.environ["FILTER_VALUE"]
        
        # Guardamos el string del operador para saber si es matemático
        self.operador_str = os.environ.get("FILTER_OPERATOR", "eq").lower()
        
        operaciones = {
            "eq": operator.eq,              # ==
            "neq": operator.ne,             # !=
            "contains": lambda a, b: b in str(a), # a contiene b
            "lt": operator.lt,              # <  (Less Than)
            "gt": operator.gt,              # >  (Greater Than)
            "lte": operator.le,             # <= (Less Than or Equal)
            "gte": operator.ge,              # >= (Greater Than or Equal)
            "between": lambda a, b: b[0] <= a <= b[1],
            "in": lambda a, b: a in b
        }
        self.operacion = operaciones.get(self.operador_str, operator.eq)

        logger.info(f"[GenericFilter] Iniciado: Campo '{self.campo_objetivo}' {self.operador_str} '{self.valor_objetivo}'")

    def procesar_payload(self, queue_name: str, client_id: str, payload: dict | str, mensaje_original: bytes, ack, nack):
        try:
            if isinstance(payload, dict):
                transaccion = payload
            else:
                # Si viene como string o bytes, lo parseamos
                transaccion = json.loads(payload)

            if transaccion.get("EOF"):
                logger.info(f"[EOF] Reenviando señal de fin para cliente {client_id}.")
                self._enviar(mensaje_original)
                ack()
                return

            if self.campo_objetivo in transaccion:
                valor_actual = transaccion[self.campo_objetivo]
                valor_referencia = self.valor_objetivo

                # --- Si es un operador matemático, forzamos a que sean números (float) ---
                if self.operador_str == "between":
                    # Esperamos que valor_objetivo sea "valor1,valor2"
                    limites = [limite.strip() for limite in str(self.valor_objetivo).split(",")]
                    if len(limites) == 2:
                        valor_referencia = limites
                        # Truncar al largo del límite para comparar solo la parte relevante
                        # (ej: "2022/09/05 00:00:00" ->"2022/09/05")
                        max_len = min(len(limites[0]), len(limites[1]))
                        valor_actual = str(valor_actual)[:max_len]
                    else:
                        logger.error(f"[ERROR_RANGO] FILTER_VALUE debe ser 'min,max' para 'between'. Recibido: {self.valor_objetivo}")
                        ack()
                        return
                elif self.operador_str == "in":
                    # Convierte "Wire, ACH" en una lista: ['Wire', 'ACH']
                    valor_referencia = [opcion.strip() for opcion in str(self.valor_objetivo).split(",")]
                    valor_actual = str(valor_actual)

                elif self.operador_str in ["lt", "gt", "lte", "gte"]:
                    try:
                        # Intentamos convertir a float (para números)
                        valor_actual = float(valor_actual)
                        valor_referencia = float(valor_referencia)
                    except (ValueError, TypeError):
                        # Si falla (ej. son fechas '2022-09-01'), las comparamos como strings
                        valor_actual = str(valor_actual)
                        valor_referencia = str(valor_referencia)
                else:
                    # eq, neq, contains
                    valor_actual = str(valor_actual)
                    valor_referencia = str(valor_referencia)

                # Evaluamos la regla
                if self.operacion(valor_actual, valor_referencia):
                    #logger.info(f"[PASÓ] Cliente {client_id}: {valor_actual} (Enviado)")
                    self._enviar(mensaje_original)
                #else:
                    #logger.info(f"[FILTRADO] Cliente {client_id}: {valor_actual} (Descartado)")

            else:
                logger.warning(f"[FALTA_CAMPO] El JSON del cliente {client_id} no contiene el campo '{self.campo_objetivo}'")

            ack()

        except json.JSONDecodeError:
            logger.error(f"Error parseando JSON del cliente {client_id}: {payload}")
            nack() 
        except Exception as e:
            logger.error(f"Error procesando regla genérica: {e}", exc_info=True)
            nack()

    def al_cerrar(self):
        logger.info("Filtro genérico apagado.")

def __main__():
    worker = GenericFilterWorker()
    worker.iniciar()

if __name__ == "__main__":
    __main__()