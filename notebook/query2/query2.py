import pandas as pd
from pathlib import Path

def ejecutar_query(nombre_dataset, dataset_accounts):
    base_dir = Path(__file__).resolve().parent
    ruta_datasets = base_dir.parents[1] / "datasets"
    
    ruta_transacciones_norm = base_dir / "transacciones_normalizadas.csv"
    ruta_cuentas_norm = base_dir / "cuentas_normalizadas.csv"
    ruta_resultado = base_dir / "q2_solucion.csv"

    def normalizar_bank_id(serie):
        return serie.str.strip().str.lstrip("0").replace("", "0")

    # ── PASO 1: Creación de archivos normalizados ─────────────────────────────
    transacciones = pd.read_csv(
        ruta_datasets / nombre_dataset,
        dtype={"From Bank": "string", "Account": "string",
               "To Bank": "string", "Account.1": "string"}
    )
    cuentas = pd.read_csv(
        ruta_datasets / dataset_accounts,
        dtype={"Bank ID": "string", "Account Number": "string"}
    )

    transacciones["From Bank Normalized"] = normalizar_bank_id(transacciones["From Bank"])
    transacciones["To Bank Normalized"]   = normalizar_bank_id(transacciones["To Bank"])
    cuentas["Bank ID Normalized"]         = normalizar_bank_id(cuentas["Bank ID"])

    transacciones.to_csv(ruta_transacciones_norm, index=False)
    cuentas.to_csv(ruta_cuentas_norm, index=False)

    # ── PASO 2: Procesar la consulta desde los nuevos archivos ────────────────
    df_transacciones = pd.read_csv(
        ruta_transacciones_norm,
        dtype={"From Bank Normalized": "string", "Payment Currency": "string"}
    )
    df_cuentas = pd.read_csv(
        ruta_cuentas_norm,
        dtype={"Bank ID Normalized": "string", "Bank ID": "string"}
    )

    transacciones_usd = df_transacciones[df_transacciones["Payment Currency"] == "US Dollar"].copy()
    transacciones_usd["Amount Paid"] = pd.to_numeric(transacciones_usd["Amount Paid"], errors="coerce")
    transacciones_usd = transacciones_usd.dropna(subset=["Amount Paid"])

    bancos = df_cuentas[["Bank ID Normalized", "Bank ID", "Bank Name"]].rename(
        columns={"Bank ID Normalized": "From Bank Normalized"}
    ).drop_duplicates(subset=["From Bank Normalized"])

    transacciones_con_banco = transacciones_usd.merge(
        bancos, on="From Bank Normalized", how="inner"
    )

    idx_maximos = transacciones_con_banco.groupby("From Bank Normalized")["Amount Paid"].idxmax()
    resultado_query2 = transacciones_con_banco.loc[
        idx_maximos, ["Bank ID", "Bank Name", "Account", "Amount Paid"]
    ].rename(columns={"Amount Paid": "Max Amount"}).sort_values(by="Bank ID").reset_index(drop=True)

    resultado_query2.to_csv(ruta_resultado, index=False)

if __name__ == "__main__":
    ejecutar_query("transacciones_sample.csv", "accounts_sample.csv")
