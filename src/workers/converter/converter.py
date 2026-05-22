import logging
import os
import json
import requests
from base import BaseWorker

logger = logging.getLogger(__name__)

class CurrencyConverterWorker(BaseWorker):
    def __init__(self):
        # 1. Llamamos a init de BaseWorker para levantar middleware y estado distribuido
        super().__init__()
        
        self.start_date = os.environ.get("START_DATE", "2022-09-01")
        self.end_date = os.environ.get("END_DATE", "2022-09-05")
        
        # 2. Diccionario traductor: Nombre de tu CSV -> Código ISO 4217
        self.currency_map = {
            "US Dollar": "USD",
            "Euro": "EUR",
            "British Pound": "GBP",
            "Yen": "JPY"
            # TODO: Agrega aquí el resto de las divisas que vengan en tu CSV
        }
        
        self.cotizaciones = {}
        self._cargar_cotizaciones()

    def _cargar_cotizaciones(self):
        """Descarga las cotizaciones de todo el periodo en memoria."""
        logger.info(f"[{self.__class__.__name__}] Descargando cotizaciones ({self.start_date} a {self.end_date})...")
        
        url = f"https://api.frankfurter.app/{self.start_date}..{self.end_date}"
        
        try:
            respuesta = requests.get(url)
            respuesta.raise_for_status()
            datos = respuesta.json()
            
            self.cotizaciones = datos.get("rates", {})
            logger.info(f"[{self.__class__.__name__}] Cotizaciones cacheadas en memoria para {len(self.cotizaciones)} días.")
            
        except requests.exceptions.RequestException as e:
            logger.error(f"[{self.__class__.__name__}] Error crítico conectando con Frankfurter: {e}")
            raise e

    def procesar_payload(self, client_id: str, payload: str, mensaje_original: bytes, ack, nack):
        """
        BaseWorker ya se encarga del EOF. Solo manejamos datos reales aquí.
        """
        try:
            transaccion = json.loads(payload)

            # Extraemos datos clave
            timestamp_crudo = transaccion.get("Timestamp", "")
            if not timestamp_crudo:
                # Transacción inválida, la dejamos pasar o la descartamos (aquí descartamos)
                logger.warning(f"[{client_id}] Falta Timestamp. Descartando.")
                ack()
                return

            # Formatear "2022/09/01 04:01" a "2022-09-01"
            fecha_corta = timestamp_crudo.split(" ")[0].replace("/", "-")
            moneda_origen = transaccion.get("Receiving Currency")
            monto_recibido = float(transaccion.get("Amount Received", 0))

            iso_currency = self.currency_map.get(moneda_origen)

            if not iso_currency:
                logger.warning(f"[{client_id}] Divisa no mapeada: {moneda_origen}. Descartando.")
                ack()
                return

            # --- MATEMÁTICA DE CONVERSIÓN ---
            amount_usd = 0.0
            
            if iso_currency == "USD":
                amount_usd = monto_recibido
            else:
                cotizacion_del_dia = self.cotizaciones.get(fecha_corta)
                
                if not cotizacion_del_dia:
                    logger.warning(f"[{client_id}] Sin datos de API para {fecha_corta}. Descartando.")
                    ack()
                    return
                
                # Rescatamos el valor del USD de ese día (respecto al EUR)
                rate_usd = cotizacion_del_dia.get("USD")
                
                if iso_currency == "EUR":
                    # De EUR a USD directo
                    amount_usd = monto_recibido * rate_usd
                else:
                    # Triangulación: Origen -> EUR -> USD
                    rate_origen = cotizacion_del_dia.get(iso_currency)
                    if not rate_origen:
                        logger.warning(f"[{client_id}] Moneda {iso_currency} no cotizada el {fecha_corta}. Descartando.")
                        ack()
                        return
                        
                    monto_eur = monto_recibido / rate_origen
                    amount_usd = monto_eur * rate_usd

            # --- FILTRO FINAL ---
            if amount_usd >= 1.0:
                # Pasó el filtro. Enviamos a la siguiente cola.
                # NOTA: Si necesitas inyectar el valor convertido en el JSON para el siguiente nodo,
                # puedes hacer transaccion["amount_usd"] = amount_usd y enviar json.dumps(transaccion).encode()
                # Por ahora envío el mensaje original crudo.
                
                logger.info(f"[PASÓ] {client_id}: {monto_recibido} {iso_currency} = {amount_usd:.2f} USD")
                self._enviar(mensaje_original)
            else:
                logger.info(f"[FILTRADO] {client_id}: {monto_recibido} {iso_currency} = {amount_usd:.2f} USD (Menor a 1)")
            
            # Siempre confirmar el procesamiento al middleware
            ack()

        except ValueError:
            logger.warning(f"[{client_id}] Amount Received no es un número válido. Descartando.")
            ack()
        except Exception as e:
            logger.error(f"[{client_id}] Error procesando conversión: {e}", exc_info=True)
            nack()

    def al_cerrar(self):
        logger.info(f"[{self.__class__.__name__}] Apagando de forma limpia...")

def __main__():
    worker = CurrencyConverterWorker()
    worker.iniciar()

if __name__ == "__main__":
    __main__()