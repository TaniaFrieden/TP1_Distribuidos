import pandas as pd
import requests
from datetime import date, timedelta
from pathlib import Path

def ejecutar_query(nombre_dataset):
    base_dir = Path(__file__).resolve().parent
    ruta_datasets = base_dir.parents[1] / "datasets"
    ruta_resultado = base_dir / "q5_solucion.csv"

    transacciones = pd.read_csv(ruta_datasets / nombre_dataset)

    INICIO = "2022/09/01"
    FIN    = "2022/09/06"

    filtro_periodo = transacciones[
        (transacciones["Timestamp"] >= INICIO) &
        (transacciones["Timestamp"] < FIN)
    ]

    FORMATOS = ["Wire", "ACH"]
    filtro_formato = filtro_periodo[filtro_periodo["Payment Format"].isin(FORMATOS)]

    # Descarga de cotizaciones
    url = "https://api.frankfurter.app/2022-09-01..2022-09-05"
    resp = requests.get(url, timeout=10)
    resp.raise_for_status()
    cotizaciones_raw = resp.json()["rates"]

    cotizaciones = {}
    last_rates = None
    dia = date(2022, 9, 1)
    fin = date(2022, 9, 5)
    while dia <= fin:
        key = dia.strftime("%Y-%m-%d")
        if key in cotizaciones_raw:
            last_rates = cotizaciones_raw[key]
        if last_rates is not None:
            cotizaciones[key] = last_rates
        dia += timedelta(days=1)

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

    def convertir_a_usd(row):
        iso = CURRENCY_MAP.get(row["Receiving Currency"])
        if not iso:
            return None
        if iso == "USD":
            return float(row["Amount Received"])

        fecha = row["Timestamp"].split(" ")[0].replace("/", "-")
        rates = cotizaciones.get(fecha)
        if not rates:
            return None

        rate_usd = rates.get("USD")
        if not rate_usd:
            return None

        monto = float(row["Amount Received"])
        if iso == "EUR":
            return monto * rate_usd

        rate_origen = rates.get(iso)
        if not rate_origen:
            return None
        return (monto / rate_origen) * rate_usd

    filtro_formato = filtro_formato.copy()
    filtro_formato["amount_usd"] = filtro_formato.apply(convertir_a_usd, axis=1)

    menores_1_usd = filtro_formato.dropna(subset=["amount_usd"])
    menores_1_usd = menores_1_usd[menores_1_usd["amount_usd"] < 1.0]

    count = len(menores_1_usd)
    resultado = pd.DataFrame({"count": [count]})
    resultado.to_csv(ruta_resultado, index=False)

if __name__ == "__main__":
    ejecutar_query("transacciones_sample.csv")
