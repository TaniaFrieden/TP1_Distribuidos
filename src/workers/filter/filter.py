import logging
import json
import operator
from base.base import BaseWorker
from common.logging_setup import setup_logging
from filter_config import FilterConfig
from batch_processor import BatchProcessor
from operators import OPERATORS, OP_BETWEEN, OP_IN, NUMERIC_OPERATORS

logger = logging.getLogger(__name__)


class GenericFilterWorker(BaseWorker):
    def __init__(self):
        super().__init__()
        self.filter_config = FilterConfig()
        
        self.field = self.filter_config.target_field
        self.op_str = self.filter_config.operator_str
        self.ref_val = self._pre_parse_value(self.filter_config.raw_target_value)
        self.operation = OPERATORS.get(self.op_str, operator.eq)
        
        self.processor = BatchProcessor(self)

        logger.info(f"[GenericFilter] Iniciado: Campo '{self.field}' {self.op_str} '{self.ref_val}'")

    def _pre_parse_value(self, raw_value: str):
        """Pre-procesa el valor objetivo según el operador para optimizar comparaciones."""
        if self.op_str == OP_BETWEEN:
            limits = [lim.strip() for lim in raw_value.split(",")]
            if len(limits) != 2:
                raise ValueError(f"FILTER_VALUE debe ser 'min,max' para 'between'. Recibido: {raw_value}")
            return limits
            
        if self.op_str == OP_IN:
            return {opt.strip() for opt in raw_value.split(",")}
            
        if self.op_str in NUMERIC_OPERATORS:
            try:
                return float(raw_value)
            except ValueError:
                return raw_value
                
        return raw_value

    def matches(self, transaction: dict) -> bool:
        """Determina si una transacción individual cumple con la regla de filtrado."""
        if self.field not in transaction:
            return False

        val = transaction[self.field]
        if isinstance(self.ref_val, float):
            try:
                val = float(val)
            except (ValueError, TypeError):
                val = str(val)
        elif self.op_str != OP_BETWEEN:
            val = str(val)

        return self.operation(val, self.ref_val)

    def procesar_payload(self, queue_name: str, client_id: str, payload: dict | str, mensaje_original: bytes, ack, nack):
        try:
            transaction = payload if isinstance(payload, dict) else json.loads(payload)
            
            if transaction.get("EOF"):
                logger.info(f"[EOF] Reenviando señal de fin para cliente {client_id}.")
                self._enviar(mensaje_original)
                ack()
                return

            if "batches" in transaction:
                result = self.processor.process_payload(transaction)
                if result:
                    msg_bytes = json.dumps(result).encode("utf-8")
                    self._enviar(msg_bytes, payload=result)
            else:
                if self.matches(transaction):
                    self._enviar(mensaje_original, payload=transaction)

            ack()

        except json.JSONDecodeError:
            logger.error(f"Error parseando JSON del cliente {client_id}: {payload}")
            nack()
        except Exception as e:
            logger.error(f"Error procesando regla genérica: {e}", exc_info=True)
            nack()

    def al_cerrar(self):
        logger.info("Filtro genérico apagado.")


def main():
    setup_logging("filter")
    worker = GenericFilterWorker()
    worker.iniciar()

if __name__ == "__main__":
    main()