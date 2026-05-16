"""
FilterWorker
============
Worker de filtrado configurable por tipo.
Hereda de BaseWorker y descarta mensajes que no cumplan la condición
del filtro indicado.

Variables de entorno requeridas:
    FILTER_TYPE  - tipo de filtro a aplicar. Soportados: "USD", "QUERY1"
"""

import os
import json
import logging

from common.worker_base.base import BaseWorker

logger = logging.getLogger(__name__)

FILTER_TYPE = os.getenv("FILTER_TYPE")
ID = int(os.getenv("ID"))

# ------------------------------------------------------------------
# Funciones de filtrado
# ------------------------------------------------------------------

def _filtrar_usd(datos: dict) -> bool:
    """Pasa solo transacciones en moneda USD."""
    return datos.get("payment_currency") == "USD"


def _filtrar_query1(datos: dict) -> bool:
    """Pasa solo transaccionescon monto menor a 50."""
    return float(datos.get("amount_paid", float("inf"))) < 50


# ------------------------------------------------------------------
# Dispatch table
# ------------------------------------------------------------------

FILTROS = {
    "USD":    _filtrar_usd,
    "QUERY1": _filtrar_query1,
}

# ------------------------------------------------------------------
# FilterWorker
# ------------------------------------------------------------------

class FilterWorker(BaseWorker):

    def __init__(self):
        super().__init__()

        if FILTER_TYPE not in FILTROS:
            raise ValueError(
                f"[FilterWorker] FILTER_TYPE='{FILTER_TYPE}' no reconocido. "
                f"Valores válidos: {list(FILTROS.keys())}"
            )

        self._filtrar = FILTROS[FILTER_TYPE]
        logger.info(f"[FilterWorker] Filtro activo: {FILTER_TYPE}")

    def iniciar(self):
        logger.info(f"[FilterWorker] Iniciando worker con ID={ID} y filtro={FILTER_TYPE}")
        super().iniciar()

    # ------------------------------------------------------------------
    # BaseWorker API
    # ------------------------------------------------------------------

    def procesar_mensaje(self, mensaje: bytes, ack, nack):
        try:
            datos = json.loads(mensaje)
        except json.JSONDecodeError as e:
            logger.warning(f"[FilterWorker] Mensaje no es JSON válido: {e}. Descartando.")
            ack()
            return

        # EOF: se propaga sin filtrar, el base worker lo maneja
        if self._es_eof(datos):
            ack()
            return

        if self._filtrar(datos):
            self._enviar(mensaje)
            logger.debug(f"[FilterWorker] Mensaje pasó filtro {FILTER_TYPE}")
        else:
            logger.debug(f"[FilterWorker] Mensaje descartado por filtro {FILTER_TYPE}")

        ack()

    def al_cerrar(self):
        pass  # El base worker cierra las colas


    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _es_eof(self, datos: dict) -> bool:
        return "client_id" in datos and len(datos) == 1
  


def __main__():
    worker = FilterWorker()
    worker.iniciar()


if __name__ == "__main__":
    __main__()