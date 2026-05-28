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

    chunksize = 100000

    # ── PASO 1: Creación de archivos normalizados ─────────────────────────────
    # Cuentas
    first_chunk = True
    for chunk in pd.read_csv(
        ruta_datasets / dataset_accounts,
        dtype={"Bank ID": "string", "Account Number": "string"},
        chunksize=chunksize
    ):
        chunk["Bank ID Normalized"] = normalizar_bank_id(chunk["Bank ID"])
        if first_chunk:
            chunk.to_csv(ruta_cuentas_norm, mode='w', index=False)
            first_chunk = False
        else:
            chunk.to_csv(ruta_cuentas_norm, mode='a', header=False, index=False)

    # Transacciones
    first_chunk = True
    for chunk in pd.read_csv(
        ruta_datasets / nombre_dataset,
        dtype={"From Bank": "string", "Account": "string",
               "To Bank": "string", "Account.1": "string"},
        chunksize=chunksize
    ):
        chunk["From Bank Normalized"] = normalizar_bank_id(chunk["From Bank"])
        chunk["To Bank Normalized"]   = normalizar_bank_id(chunk["To Bank"])
        if first_chunk:
            chunk.to_csv(ruta_transacciones_norm, mode='w', index=False)
            first_chunk = False
        else:
            chunk.to_csv(ruta_transacciones_norm, mode='a', header=False, index=False)

    # ── PASO 2: Procesar la consulta desde los nuevos archivos ────────────────
    # Construir dataframe de bancos únicos en memoria
    bancos_list = []
    for chunk in pd.read_csv(
        ruta_cuentas_norm,
        dtype={"Bank ID Normalized": "string", "Bank ID": "string"},
        chunksize=chunksize
    ):
        df_b = chunk[["Bank ID Normalized", "Bank ID", "Bank Name"]].rename(
            columns={"Bank ID Normalized": "From Bank Normalized"}
        )
        bancos_list.append(df_b)
    
    bancos = pd.concat(bancos_list).drop_duplicates(subset=["From Bank Normalized"])

    max_by_bank = pd.DataFrame(columns=["Bank ID", "Bank Name", "Account", "Amount Paid", "From Bank Normalized"])

    for chunk in pd.read_csv(
        ruta_transacciones_norm,
        dtype={"From Bank Normalized": "string", "Payment Currency": "string"},
        chunksize=chunksize
    ):
        transacciones_usd = chunk[chunk["Payment Currency"] == "US Dollar"].copy()
        transacciones_usd["Amount Paid"] = pd.to_numeric(transacciones_usd["Amount Paid"], errors="coerce")
        transacciones_usd = transacciones_usd.dropna(subset=["Amount Paid"])

        transacciones_con_banco = transacciones_usd.merge(
            bancos, on="From Bank Normalized", how="inner"
        )

        if not transacciones_con_banco.empty:
            idx_maximos = transacciones_con_banco.groupby("From Bank Normalized")["Amount Paid"].idxmax()
            chunk_max = transacciones_con_banco.loc[
                idx_maximos, ["Bank ID", "Bank Name", "Account", "Amount Paid", "From Bank Normalized"]
            ]
            max_by_bank = pd.concat([max_by_bank, chunk_max], ignore_index=True)
            
            # Keep only the global maximum transaction per bank seen so far
            idx_global_max = max_by_bank.groupby("From Bank Normalized")["Amount Paid"].idxmax()
            max_by_bank = max_by_bank.loc[idx_global_max].reset_index(drop=True)

    resultado_query2 = max_by_bank[["Bank ID", "Bank Name", "Account", "Amount Paid"]].rename(
        columns={"Amount Paid": "Max Amount"}
    ).sort_values(by="Bank ID").reset_index(drop=True)

    resultado_query2.to_csv(ruta_resultado, index=False)

if __name__ == "__main__":
    ejecutar_query("transacciones_sample.csv", "accounts_sample.csv")
