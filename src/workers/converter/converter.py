import logging
import os
import json
import requests
from datetime import date, timedelta
from base import BaseWorker
from common.logging_setup import setup_logging

logger = logging.getLogger(__name__)

CURRENCY_MAP = {
    "US Dollar":         "USD",
    "Euro":              "EUR",
    "UK Pound":          "GBP",
    "Yen":               "JPY",
    "Australian Dollar": "AUD",
    "Bitcoin":           "BTC",
    "Brazil Real":       "BRL",
    "Canadian Dollar":   "CAD",
    "Mexican Peso":      "MXN",
    "Ruble":             "RUB",
    "Rupee":             "INR",
    "Saudi Riyal":       "SAR",
    "Shekel":            "ILS",
    "Swiss Franc":       "CHF",
    "Yuan":              "CNY",
}


class CurrencyConverterWorker(BaseWorker):

    def __init__(self):
        super().__init__()
        self._start_date = os.environ.get("START_DATE", "2022-09-01")
        self._end_date   = os.environ.get("END_DATE",   "2022-09-05")
        self._cotizaciones = {}
        self._cargar_cotizaciones()

    def _cargar_cotizaciones(self):
        url = f"https://api.frankfurter.app/{self._start_date}..{self._end_date}"
        try:
            resp = requests.get(url, timeout=10)
            resp.raise_for_status()
            raw = resp.json().get("rates", {})
        except requests.exceptions.RequestException as e:
            logger.error(f"Error conectando con Frankfurter: {e}")
            raise

        # Forward-fill: weekends/holidays inherit the last known rate
        self._cotizaciones = {}
        last_rates = None
        dia = date.fromisoformat(self._start_date)
        fin = date.fromisoformat(self._end_date)
        while dia <= fin:
            key = dia.isoformat()
            if key in raw:
                last_rates = raw[key]
            if last_rates is not None:
                self._cotizaciones[key] = last_rates
            dia += timedelta(days=1)

        logger.info(f"Cotizaciones cargadas: {len(self._cotizaciones)} días (con forward-fill).")

    def _convertir_a_usd(self, monto: float, iso: str, fecha: str):
        if iso == "USD":
            return monto
        rates = self._cotizaciones.get(fecha)
        if not rates:
            return None
        rate_usd = rates.get("USD")
        if not rate_usd:
            return None
        if iso == "EUR":
            return monto * rate_usd
        # Triangulation: source -> EUR -> USD
        rate_origen = rates.get(iso)
        if not rate_origen:
            return None
        return (monto / rate_origen) * rate_usd

    def procesar_payload(self, queue_name: str, client_id: str, payload: dict | str, mensaje_original: bytes, ack, nack):
        try:
            t = payload if isinstance(payload, dict) else json.loads(payload)
            iso = CURRENCY_MAP.get(t.get("Receiving Currency", ""))
            if not iso:
                ack()
                return

            fecha = t.get("Timestamp", "").split(" ")[0].replace("/", "-")
            monto = float(t.get("Amount Received", 0))
            amount_usd = self._convertir_a_usd(monto, iso, fecha)

            if amount_usd is None:
                ack()
                return

            if amount_usd < 1.0:
                self._enviar(mensaje_original)

            ack()

        except (ValueError, KeyError) as e:
            logger.warning(f"Error parseando transacción: {e}. Descartando.")
            ack()
        except Exception as e:
            logger.error(f"Error inesperado: {e}", exc_info=True)
            nack()

    def al_cerrar(self):
        logger.info("Converter apagado.")


def main():
    setup_logging("converter")
    CurrencyConverterWorker().iniciar()


if __name__ == "__main__":
    main()
