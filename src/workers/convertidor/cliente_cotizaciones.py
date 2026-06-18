from common.logger import obtener_logger
import requests
from datetime import date, timedelta
from common.constantes_protocolo import URL_API_FRANKFURTER

logger = obtener_logger(__name__)

class ClienteCotizaciones:
    def __init__(self, fecha_inicio: str, fecha_fin: str):
        self.fecha_inicio = fecha_inicio
        self.fecha_fin = fecha_fin

    def obtener_cotizaciones(self) -> dict:
        url = f"{URL_API_FRANKFURTER}{self.fecha_inicio}..{self.fecha_fin}?base=USD"
        try:
            resp = requests.get(url, timeout=10)
            resp.raise_for_status()
            raw = resp.json().get("rates", {})
        except requests.exceptions.RequestException as e:
            logger.error(f"Error conectando con Frankfurter: {e}")
            raise

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
