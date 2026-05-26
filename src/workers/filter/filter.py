import logging
import os
import operator
import json

from base import BaseWorker
from common.logging_setup import setup_logging

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
            logger.info(f'Transaccion recibida {payload}')
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

            if "batches" in transaccion:
                filtered_batches = []
                for batch in transaccion["batches"]:
                    header = batch["header"]
                    schema = header["schema"]
                    records = batch["payload"]
                    
                    filtered_records = []
                    for record_values in records:
                        record_dict = dict(zip(schema, record_values))
                        if self._match_filter(record_dict, client_id):
                            filtered_records.append(record_values)
                    
                    if filtered_records:
                        filtered_batches.append({
                            "header": {
                                "schema": schema,
                                "client_id": header.get("client_id", client_id),
                                "count": len(filtered_records)
                            },
                            "payload": filtered_records
                        })
                
                if filtered_batches:
                    output_payload = {
                        "client_id": client_id,
                        "batches": filtered_batches
                    }
                    msg_bytes = json.dumps(output_payload).encode("utf-8")
                    self._enviar(msg_bytes, payload=output_payload)
            else:
                # Fallback para formato de registro unico anterior
                if self._match_filter(transaccion, client_id):
                    self._enviar(mensaje_original, payload=transaccion)  # Reenviamos el mensaje original sin modificar

            ack()

        except json.JSONDecodeError:
            logger.error(f"Error parseando JSON del cliente {client_id}: {payload}")
            nack() 
        except Exception as e:
            logger.error(f"Error procesando regla genérica: {e}", exc_info=True)
            nack()

    def _match_filter(self, transaccion: dict, client_id: str) -> bool:
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
                    return False
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

            return self.operacion(valor_actual, valor_referencia)
        else:
            logger.warning(f"[FALTA_CAMPO] El JSON del cliente {client_id} no contiene el campo '{self.campo_objetivo}'")
            return False

    def al_cerrar(self):
        logger.info("Filtro genérico apagado.")

def __main__():
    setup_logging("filter")
    worker = GenericFilterWorker()
    worker.iniciar()

if __name__ == "__main__":
    __main__()