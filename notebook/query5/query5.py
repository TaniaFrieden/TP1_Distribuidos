import pandas as pd
import requests
from datetime import date, timedelta
from pathlib import Path

def ejecutar_query(nombre_dataset):
    base_dir = Path(__file__).resolve().parent
    ruta_datasets = base_dir.parents[1] / "datasets"
    ruta_resultado = base_dir / "q5_solucion.csv"

    INICIO = "2022/09/01"
    FIN    = "2022/09/06"
    FORMATOS = ["Wire", "ACH"]

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

    # Precompute conversion factors to avoid row-by-row apply
    factors = {}
    for fecha, rates in cotizaciones.items():
        rate_usd = rates.get("USD")
        if rate_usd is None:
            continue
        for cur_name, iso in CURRENCY_MAP.items():
            if iso == "USD":
                factors[(fecha, iso)] = 1.0
            elif iso == "EUR":
                factors[(fecha, iso)] = float(rate_usd)
            else:
                rate_origen = rates.get(iso)
                if rate_origen is not None:
                    factors[(fecha, iso)] = float(rate_usd) / float(rate_origen)

    chunksize = 100000
    total_count = 0

    for chunk in pd.read_csv(
        ruta_datasets / nombre_dataset,
        dtype={"Timestamp": "string", "Receiving Currency": "string"},
        chunksize=chunksize
    ):
        filtro_periodo = chunk[
            (chunk["Timestamp"] >= INICIO) &
            (chunk["Timestamp"] < FIN)
        ]
        if filtro_periodo.empty:
            continue

        filtro_formato = filtro_periodo[filtro_periodo["Payment Format"].isin(FORMATOS)].copy()
        if filtro_formato.empty:
            continue

        # Extract date YYYY-MM-DD from YYYY/MM/DD HH:MM
        dates = filtro_formato["Timestamp"].str[:10].str.replace("/", "-", regex=False)
        isos = filtro_formato["Receiving Currency"].map(CURRENCY_MAP)

        # Build list of keys (date, iso) and retrieve multipliers
        keys = list(zip(dates, isos))
        multipliers = pd.Series([factors.get(k) for k in keys], index=filtro_formato.index)

        # Vectorized calculation of amount_usd
        monto_recibido = pd.to_numeric(filtro_formato["Amount Received"], errors="coerce")
        filtro_formato["amount_usd"] = monto_recibido * multipliers

        menores_1_usd = filtro_formato.dropna(subset=["amount_usd"])
        menores_1_usd = menores_1_usd[menores_1_usd["amount_usd"] < 1.0]

        total_count += len(menores_1_usd)

    resultado = pd.DataFrame({"count": [total_count]})
    resultado.to_csv(ruta_resultado, index=False)

if __name__ == "__main__":
    ejecutar_query("transacciones_sample.csv")
