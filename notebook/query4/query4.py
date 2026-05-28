import pandas as pd
from pathlib import Path

def ejecutar_query(nombre_dataset):
    base_dir = Path(__file__).resolve().parent
    ruta_datasets = base_dir.parents[1] / "datasets"
    ruta_resultado = base_dir / "q4_solucion.csv"

    chunksize = 100000
    relaciones_list = []

    for chunk in pd.read_csv(
        ruta_datasets / nombre_dataset,
        dtype={
            "From Bank": "string", 
            "Account": "string",
            "To Bank": "string", 
            "Account.1": "string",
            "Timestamp": "string"
        },
        chunksize=chunksize
    ):
        if "Account" in chunk.columns and "Account.1" in chunk.columns:
            chunk = chunk.rename(columns={
                "Account": "From Account", 
                "Account.1": "To Account"
            })

        chunk["Amount Paid"] = pd.to_numeric(chunk["Amount Paid"], errors="coerce")

        df = chunk[
            (chunk["Payment Currency"] == "US Dollar") & 
            (chunk["Amount Paid"].notna()) &
            (chunk["Timestamp"] >= "2022/09/01") & 
            (chunk["Timestamp"] <= "2022/09/05 23:59:59")
        ]

        if df.empty:
            continue

        df_periodo = df.copy()
        df_periodo["From"] = df_periodo["From Bank"].astype(str) + "|" + df_periodo["From Account"].astype(str)
        df_periodo["To"]   = df_periodo["To Bank"].astype(str)   + "|" + df_periodo["To Account"].astype(str)

        relaciones_chunk = df_periodo[["From", "To"]].drop_duplicates()
        relaciones_list.append(relaciones_chunk)

    if relaciones_list:
        relaciones_ab = pd.concat(relaciones_list, ignore_index=True).drop_duplicates()
    else:
        relaciones_ab = pd.DataFrame(columns=["From", "To"])

    # PASO 1: SCATTER
    conteo_b_por_a = relaciones_ab.groupby("From")["To"].count()
    cuentas_a_validas = conteo_b_por_a[conteo_b_por_a == 5].index
    scatter_df = relaciones_ab[relaciones_ab["From"].isin(cuentas_a_validas)]

    # PASO 2: GATHER
    relaciones_bc = relaciones_ab.rename(columns={
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
