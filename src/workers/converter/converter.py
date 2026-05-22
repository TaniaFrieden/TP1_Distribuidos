import logging
import os
import json
import threading
import requests
from datetime import date, timedelta
from base import BaseWorker

logger = logging.getLogger(__name__)

# Nombres del CSV -> código ISO 4217 (Frankfurter usa EUR como base)
CURRENCY_MAP = {
    "US Dollar":        "USD",
    "Euro":             "EUR",
    "UK Pound":         "GBP",
    "Yen":              "JPY",
    "Australian Dollar":"AUD",
    "Bitcoin":          "BTC",
    "Brazil Real":      "BRL",
    "Canadian Dollar":  "CAD",
    "Mexican Peso":     "MXN",
    "Ruble":            "RUB",
    "Rupee":            "INR",
    "Saudi Riyal":      "SAR",
    "Shekel":           "ILS",
    "Swiss Franc":      "CHF",
    "Yuan":             "CNY",
}


class CurrencyConverterWorker(BaseWorker):

    def __init__(self):
        super().__init__()
        self._start_date = os.environ.get("START_DATE", "2022-09-01")
        self._end_date   = os.environ.get("END_DATE",   "2022-09-05")
        self._cotizaciones = {}
        # Conteo de transacciones < 1 USD por cliente
        self._conteos: dict = {}
        self._conteos_lock = threading.Lock()
        self._cargar_cotizaciones()

    # ------------------------------------------------------------------

    def _cargar_cotizaciones(self):
        """Descarga cotizaciones del período y hace forward-fill para días sin mercado."""
        url = f"https://api.frankfurter.app/{self._start_date}..{self._end_date}"
        try:
            resp = requests.get(url, timeout=10)
            resp.raise_for_status()
            raw = resp.json().get("rates", {})
        except requests.exceptions.RequestException as e:
            logger.error(f"Error conectando con Frankfurter: {e}")
            raise

        # Forward-fill: fines de semana/feriados heredan la última cotización conocida
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
        """Devuelve el monto en USD, o None si no hay cotización disponible."""
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
        # Triangulación: origen → EUR → USD
        rate_origen = rates.get(iso)
        if not rate_origen:
            return None
        return (monto / rate_origen) * rate_usd

    # ------------------------------------------------------------------
    # BaseWorker API
    # ------------------------------------------------------------------

    def procesar_payload(self, client_id: str, payload: str, mensaje_original: bytes, ack, nack):
        try:
            t = json.loads(payload)
            iso = CURRENCY_MAP.get(t.get("Receiving Currency", ""))
            if not iso:
                logger.warning(f"Divisa no mapeada: {t.get('Receiving Currency')}. Descartando.")
                ack()
                return

            fecha = t.get("Timestamp", "").split(" ")[0].replace("/", "-")
            monto = float(t.get("Amount Received", 0))
            amount_usd = self._convertir_a_usd(monto, iso, fecha)

            if amount_usd is None:
                logger.warning(f"Sin cotización para {iso} el {fecha}. Descartando.")
                ack()
                return

            if amount_usd < 1.0:
                with self._conteos_lock:
                    self._conteos[client_id] = self._conteos.get(client_id, 0) + 1

            ack()

        except (ValueError, KeyError) as e:
            logger.warning(f"Error parseando transacción: {e}. Descartando.")
            ack()
        except Exception as e:
            logger.error(f"Error inesperado: {e}", exc_info=True)
            nack()

    def al_completar_cliente(self, client_id: str):
        """Emite el conteo final antes de que BaseWorker propague el EOF."""
        with self._conteos_lock:
            count = self._conteos.pop(client_id, 0)
        resultado = json.dumps({"client_id": client_id, "count": count}).encode("utf-8")
        self._enviar(resultado)
        logger.info(f"Q5 resultado emitido para {client_id}: {count} transacciones < 1 USD.")

    def al_cerrar(self):
        logger.info("Converter apagado.")


def main():
    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
    CurrencyConverterWorker().iniciar()


if __name__ == "__main__":
    main()
