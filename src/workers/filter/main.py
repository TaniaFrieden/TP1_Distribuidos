import logging
import os
import operator
from base import BaseWorker

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
logger = logging.getLogger(__name__)

class GenericFilterWorker(BaseWorker):
    def __init__(self):
        super().__init__()
        
        # 1. Leemos la regla de negocio desde el entorno
        self.indice = int(os.environ["FILTER_INDEX"])
        self.valor_objetivo = os.environ["FILTER_VALUE"]
        
        # Operador lógico (por defecto es "igual a")
        operador_str = os.environ.get("FILTER_OPERATOR", "eq").lower()
        
        # Mapeo mágico: convierte un string en una operación matemática real
        operaciones = {
            "eq": operator.eq,              # ==
            "neq": operator.ne,             # !=
            "contains": lambda a, b: b in a # a contiene b
        }
        self.operacion = operaciones.get(operador_str, operator.eq)
        
        # 2. Configuración para dejar pasar la cabecera sin filtrarla
        self.nombre_cabecera = os.environ.get("HEADER_NAME", "Payment Currency")

        logger.info(f"[GenericFilter] Iniciado: Columna {self.indice} {operador_str} '{self.valor_objetivo}'")

    def procesar_payload(self, client_id: str, payload: str, mensaje_original: bytes, ack, nack):
        try:
            columnas = [col.strip() for col in payload.split(",")]

            if len(columnas) > self.indice:
                valor_actual = columnas[self.indice]

                # 1. Si es la cabecera, la dejamos pasar río abajo
                if valor_actual == self.nombre_cabecera:
                    logger.info(f"[CABECERA] Reenviando fila de títulos al output para cliente {client_id}.")
                    self._enviar(mensaje_original)
                
                # 2. Evaluamos la regla dinámica (ej: valor_actual == valor_objetivo)
                elif self.operacion(valor_actual, self.valor_objetivo):
                    logger.info(f"[PASÓ] Cliente {client_id}: {valor_actual} (Enviado)")
                    self._enviar(mensaje_original)
                
                # 3. No cumple la regla, se descarta
                else:
                    logger.info(f"[FILTRADO] Cliente {client_id}: {valor_actual} (Descartado)")

            else:
                logger.warning(f"[FALTA_COLUMNA] Fila del cliente {client_id} no contiene el índice {self.indice}")

            ack()

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