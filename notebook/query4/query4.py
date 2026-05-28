import pandas as pd
from pathlib import Path

def ejecutar_query(nombre_dataset):
    base_dir = Path(__file__).resolve().parent
    ruta_datasets = base_dir.parents[1] / "datasets"
    ruta_resultado = base_dir / "q4_solucion.csv"

    transacciones = pd.read_csv(
        ruta_datasets / nombre_dataset,
        dtype={
            "From Bank": "string", 
            "Account": "string",
            "To Bank": "string", 
            "Account.1": "string"
        }
    )

    if "Account" in transacciones.columns and "Account.1" in transacciones.columns:
        transacciones = transacciones.rename(columns={
            "Account": "From Account", 
            "Account.1": "To Account"
        })

    transacciones["Timestamp"] = pd.to_datetime(transacciones["Timestamp"])
    transacciones["Amount Paid"] = pd.to_numeric(transacciones["Amount Paid"], errors="coerce")

    df = transacciones[
        (transacciones["Payment Currency"] == "US Dollar") & 
        (transacciones["Amount Paid"].notna())
    ]

    df_periodo = df[
        (df["Timestamp"] >= "2022-09-01") & 
        (df["Timestamp"] <= "2022-09-05 23:59:59")
    ].copy()

    df_periodo["From"] = df_periodo["From Bank"] + "|" + df_periodo["From Account"]
    df_periodo["To"]   = df_periodo["To Bank"]   + "|" + df_periodo["To Account"]

    # PASO 1: SCATTER
    relaciones_ab = df_periodo[["From", "To"]].drop_duplicates()
    conteo_b_por_a = relaciones_ab.groupby("From")["To"].count()
    cuentas_a_validas = conteo_b_por_a[conteo_b_por_a == 5].index
    scatter_df = relaciones_ab[relaciones_ab["From"].isin(cuentas_a_validas)]

    # PASO 2: GATHER
    relaciones_bc = df_periodo[["From", "To"]].drop_duplicates()
    relaciones_bc = relaciones_bc.rename(columns={
        "From": "Intermediate", 
        "To": "Final"
    })

    patrones = pd.merge(
        scatter_df, 
        relaciones_bc, 
        left_on="To", 
        right_on="Intermediate",
        how="inner"
    )

    conteo_caminos = patrones.groupby(["From", "Final"])["Intermediate"].nunique().reset_index()
    resultado_scatter_gather = conteo_caminos[conteo_caminos["Intermediate"] == 5].copy()

    if len(resultado_scatter_gather) > 0:
        resultado_scatter_gather[["From Bank", "From Account"]] = resultado_scatter_gather["From"].str.split("|", expand=True)
        resultado_scatter_gather[["To Bank", "To Account"]]     = resultado_scatter_gather["Final"].str.split("|", expand=True)
        resultado_scatter_gather = resultado_scatter_gather.rename(columns={"Intermediate": "Amount Transactions"})
        resultado_scatter_gather = resultado_scatter_gather[
            ["From Bank", "From Account", "To Bank", "To Account", "Amount Transactions"]
        ].reset_index(drop=True)
    else:
        resultado_scatter_gather = pd.DataFrame(
            columns=["From Bank", "From Account", "To Bank", "To Account", "Amount Transactions"]
        )

    resultado_scatter_gather.to_csv(ruta_resultado, index=False)

if __name__ == "__main__":
    ejecutar_query("transacciones_sample.csv")
