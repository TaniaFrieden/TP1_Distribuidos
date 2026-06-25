import time
import requests
from datetime import date, timedelta
from common.logger import obtener_logger
from common.constantes_protocolo import URL_API_FRANKFURTER
from constantes import REINTENTOS_DELAY_INICIAL, REINTENTOS_DELAY_MAXIMO, TIMEOUT_SOLICITUD

logger = obtener_logger(__name__)


class ClienteCotizaciones:
    def __init__(self, fecha_inicio: str, fecha_fin: str):
        self.fecha_inicio = fecha_inicio
        self.fecha_fin = fecha_fin

    def obtener_cotizaciones(self) -> dict:
        url = f"{URL_API_FRANKFURTER}{self.fecha_inicio}..{self.fecha_fin}?base=USD"
        intento = 1
        delay = REINTENTOS_DELAY_INICIAL
        while True:
            try:
                resp = requests.get(url, timeout=TIMEOUT_SOLICITUD)
                resp.raise_for_status()
                raw = resp.json().get("rates", {})
                break
            except requests.exceptions.RequestException as e:
                logger.warning(f"Error conectando con Frankfurter (intento {intento}): {e}. Reintentando en {delay}s...")
                time.sleep(delay)
                intento += 1
                delay = min(delay * 2, REINTENTOS_DELAY_MAXIMO)

        cotizaciones = {}
        last_rates = None
        dia = date.fromisoformat(self.fecha_inicio)
        fin = date.fromisoformat(self.fecha_fin)
        while dia <= fin:
            key = dia.isoformat()
            if key in raw:
                last_rates = raw[key]
            if last_rates is not None:
                cotizaciones[key] = last_rates
            dia += timedelta(days=1)

        logger.info(f"Cotizaciones cargadas: {len(cotizaciones)} días.")
        return cotizaciones
