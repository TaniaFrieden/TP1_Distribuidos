import pandas as pd
from pathlib import Path

def ejecutar_query(nombre_dataset):
    base_dir = Path(__file__).resolve().parent
    ruta_datasets = base_dir.parents[1] / "datasets"
    ruta_resultado = base_dir / "q3_solucion.csv"

    transacciones_completas = pd.read_csv(
        ruta_datasets / nombre_dataset,
        dtype={"From Bank": "string", "Account": "string",
               "To Bank": "string", "Account.1": "string"}
    )

    transacciones = transacciones_completas[
        transacciones_completas["Payment Currency"] == "US Dollar"
    ].copy()

    transacciones["Timestamp"]   = pd.to_datetime(transacciones["Timestamp"])
    transacciones["Amount Paid"] = pd.to_numeric(transacciones["Amount Paid"], errors="coerce")
    transacciones = transacciones.dropna(subset=["Amount Paid", "Payment Format"])

    periodo_temprano = transacciones[
        (transacciones["Timestamp"] >= "2022-09-01") &
        (transacciones["Timestamp"] < "2022-09-06")
    ]
    periodo_tardio = transacciones[
        (transacciones["Timestamp"] >= "2022-09-06") &
        (transacciones["Timestamp"] < "2022-09-16")
    ]

    stats_por_formato = (
        periodo_temprano
        .groupby("Payment Format")["Amount Paid"]
        .agg(suma="sum", count="count")
    )
    stats_por_formato["promedio"] = stats_por_formato["suma"] / stats_por_formato["count"]

    df = periodo_tardio.copy()
    df["Promedio Formato"] = df["Payment Format"].map(stats_por_formato["promedio"])
    df = df.dropna(subset=["Promedio Formato"])

    resultado_query3 = df[
        df["Amount Paid"] < df["Promedio Formato"] * 0.01
    ][["Account", "Amount Paid"]].rename(columns={"Account": "From Account"}).reset_index(drop=True)

    resultado_query3.to_csv(ruta_resultado, index=False)

if __name__ == "__main__":
    ejecutar_query("transacciones_sample.csv")
