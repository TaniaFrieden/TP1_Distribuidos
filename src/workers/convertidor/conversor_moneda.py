from constantes import MAPA_DIVISAS


class ConversorMoneda:
    def __init__(self, cotizaciones: dict):
        self.cotizaciones = cotizaciones

    def obtener_iso(self, moneda: str) -> str | None:
        return MAPA_DIVISAS.get(moneda)

    def convertir_a_usd(self, monto: float, iso: str, fecha: str) -> float | None:
        if iso == "USD":
            return monto
        rates = self.cotizaciones.get(fecha)
        if not rates:
            return None
        rate_origen = rates.get(iso)
        if not rate_origen:
            return None
        return monto / rate_origen
