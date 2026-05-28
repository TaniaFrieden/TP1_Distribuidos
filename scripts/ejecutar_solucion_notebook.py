#!/usr/bin/env python3
import os
import sys
import pandas as pd
import requests
from datetime import date, timedelta
from pathlib import Path

# =====================================================================
# CONFIGURACIÓN EDITABLE
# =====================================================================
# Rutas y nombres de los datasets de entrada
RUTA_DATASETS = "datasets"
DATASET_TRANS = "HI-Large_Trans_sample_30.csv"
DATASET_ACCOUNTS = "HI-Large_accounts.csv"

# Directorio de salida y nombres de archivos de destino (seteados con un bucle for)
RUTA_SALIDAS = "output/Hi-Large-30"
OUTPUTS = {}
for q in range(1, 6):
    OUTPUTS[q] = f"{RUTA_SALIDAS}/q{q}_solucion.csv"
# =====================================================================

def main():
    # Encontrar la raíz del proyecto para asegurar rutas correctas
    project_root = Path(__file__).resolve().parents[1]
    
    input_trans = project_root / RUTA_DATASETS / DATASET_TRANS
    input_accounts = project_root / RUTA_DATASETS / DATASET_ACCOUNTS
    
    # Crear carpeta de salida si no existe
    Path(project_root / RUTA_SALIDAS).mkdir(parents=True, exist_ok=True)
    
    print(f"--- Cargando Datasets ---")
    print(f"Cargando transacciones desde: {input_trans}")
    if not input_trans.exists():
        print(f"Error: No existe el archivo de transacciones: {input_trans}", file=sys.stderr)
        sys.exit(1)
    trans_df = pd.read_csv(input_trans)
    print(f"Tamaño de transacciones (filas, columnas): {trans_df.shape}")
    
    print(f"Cargando cuentas desde: {input_accounts}")
    if not input_accounts.exists():
        print(f"Error: No existe el archivo de cuentas: {input_accounts}", file=sys.stderr)
        sys.exit(1)
    accounts_df = pd.read_csv(input_accounts)
    print(f"Tamaño de cuentas (filas, columnas): {accounts_df.shape}")

    # Rango de Timestamps
    if "Timestamp" in trans_df.columns:
        print(f"Rango de Timestamps: [{trans_df['Timestamp'].min()}, {trans_df['Timestamp'].max()}]")
    else:
        print("Advertencia: No se encontró la columna 'Timestamp' en las transacciones.")

    # Filtrar transacciones que no sean en USD
    print("\nFiltrando transacciones que no son en dólares (US Dollar)...")
    trans_usd_df = trans_df[trans_df['Payment Currency'] == "US Dollar"]
    print(f"Cantidad de transacciones USD: {trans_usd_df.shape[0]}")

    # Filtrar transacciones USD en el primer período (01 al 06 de Septiembre de 2022)
    trans_usd_sept_1st_df = trans_usd_df[(trans_usd_df["Timestamp"] >= '2022/09/01') & (trans_usd_df["Timestamp"] <= '2022/09/06')]
    
    # -----------------------------------------------------------------
    # QUERY 1
    # -----------------------------------------------------------------
    print("\nProcesando Query 1...")
    low_profile_transactions = trans_usd_df[trans_usd_df['Amount Paid'] < 50]
    low_profile_transactions = low_profile_transactions[['From Bank', 'Account', 'To Bank', 'Account.1', 'Amount Paid']]
    q1_path = project_root / OUTPUTS[1]
    low_profile_transactions.to_csv(q1_path, index=False)
    print(f"Query 1 guardada en: {q1_path} ({low_profile_transactions.shape[0]} filas)")

    # -----------------------------------------------------------------
    # QUERY 2
    # -----------------------------------------------------------------
    print("\nProcesando Query 2...")
    max_amount_trans_usd_idx = trans_usd_df.groupby(["From Bank"])["Amount Paid"].idxmax()
    max_amount_trans_usd = trans_usd_df.loc[max_amount_trans_usd_idx]
    max_amount_bank = max_amount_trans_usd.merge(accounts_df, left_on="From Bank", right_on="Bank ID")
    q2_solucion = max_amount_bank[["From Bank", "Account", "Bank Name", "Amount Paid"]].drop_duplicates()
    q2_path = project_root / OUTPUTS[2]
    q2_solucion.to_csv(q2_path, index=False)
    print(f"Query 2 guardada en: {q2_path} ({q2_solucion.shape[0]} filas)")

    # -----------------------------------------------------------------
    # QUERY 3
    # -----------------------------------------------------------------
    print("\nProcesando Query 3...")
    avg_amounts_per_type = trans_usd_sept_1st_df.groupby(["Payment Format"])["Amount Paid"].mean().reset_index()
    trans_usd_sept_2nd_df = trans_usd_df[(trans_usd_df["Timestamp"] >= '2022/09/06') & (trans_usd_df["Timestamp"] <= '2022/09/15')]
    trans_usd_sept_2nd_with_avg_df = trans_usd_sept_2nd_df.merge(avg_amounts_per_type, left_on=["Payment Format"], right_on=["Payment Format"]).rename(columns={
        "Amount Paid_x": "Amount Paid",
        "Amount Paid_y": "AVG",
    })
    lower_trans_usd_sept_2nd_with_avg_df = trans_usd_sept_2nd_with_avg_df[trans_usd_sept_2nd_with_avg_df["Amount Paid"] < trans_usd_sept_2nd_with_avg_df["AVG"] * 0.01]
    q3_solucion = lower_trans_usd_sept_2nd_with_avg_df[["From Bank", "Account", "Payment Format", "Amount Paid"]]
    q3_path = project_root / OUTPUTS[3]
    q3_solucion.to_csv(q3_path, index=False)
    print(f"Query 3 guardada en: {q3_path} ({q3_solucion.shape[0]} filas)")

    # -----------------------------------------------------------------
    # QUERY 4
    # -----------------------------------------------------------------
    print("\nProcesando Query 4...")
    ranged_trans_usd_sept_df = trans_usd_sept_1st_df\
        .groupby(["From Bank", "Account"])\
        .filter(lambda x: x.groupby(["To Bank", "Account.1"]).size().size > 5)



    accounts_ab = ranged_trans_usd_sept_df[["From Bank", "Account", "To Bank", "Account.1"]]
    accounts_bc = trans_usd_sept_1st_df[["From Bank", "Account", "To Bank", "Account.1"]]

    account_pairs_df = accounts_ab.merge(accounts_bc, left_on=["To Bank", "Account.1"], right_on=["From Bank", "Account"]).rename(columns={
        "From Bank_x": "From Bank",
        "Account_x": "From Account",
        "To Bank_y": "To Bank",
        "Account.1_y": "To Account"
    })
    account_pairs_df = account_pairs_df[(account_pairs_df["From Bank"] != account_pairs_df["To Bank"]) | (account_pairs_df["From Account"] != account_pairs_df["To Account"])]
    account_pairs_df = account_pairs_df.drop_duplicates(subset=["From Bank", "From Account", "To Bank_x", "Account.1_x", "To Bank", "To Account"])
    account_pairs_df = account_pairs_df.groupby(["From Bank", "From Account", "To Bank", "To Account"], as_index=False).size()
    account_pairs_df = account_pairs_df[(account_pairs_df["size"] > 5)]

    from_account_pairs_df = account_pairs_df[["From Bank", "From Account"]].rename(columns={
        "From Bank": "Bank",
        "From Account": "Account"
    })
    to_account_pairs_df = account_pairs_df[["To Bank", "To Account"]].rename(columns={
        "To Bank": "Bank",
        "To Account": "Account"
    })
    unique_accounts = pd.concat([from_account_pairs_df, to_account_pairs_df]).drop_duplicates()
    q4_path = project_root / OUTPUTS[4]
    unique_accounts.to_csv(q4_path, index=False)
    print(f"Query 4 guardada en: {q4_path} ({unique_accounts.shape[0]} filas)")

    # -----------------------------------------------------------------
    # QUERY 5
    # -----------------------------------------------------------------
    print("\nProcesando Query 5...")
    CURRENCY_MAP_Q5 = {
        "US Dollar": "USD", "Euro": "EUR", "UK Pound": "GBP", "Yen": "JPY",
        "Australian Dollar": "AUD", "Brazil Real": "BRL", "Canadian Dollar": "CAD",
        "Mexican Peso": "MXN", "Rupee": "INR", "Shekel": "ILS",
        "Swiss Franc": "CHF", "Yuan": "CNY", "Ruble": "RUB",
        "Saudi Riyal": "SAR", "Bitcoin": "BTC",
    }
    ISO_TO_NAME = {v: k for k, v in CURRENCY_MAP_Q5.items()}

    try:
        resp = requests.get("https://api.frankfurter.app/2022-09-01..2022-09-05?base=USD", timeout=10)
        resp.raise_for_status()
        raw = resp.json()["rates"]
    except Exception as e:
        print(f"Error al obtener tasas de cambio desde Frankfurter API: {e}", file=sys.stderr)
        sys.exit(1)

    daily_rates = {}
    last = None
    d = date(2022, 9, 1)
    while d <= date(2022, 9, 5):
        iso_key = d.isoformat()
        slash_key = d.strftime("%Y/%m/%d")
        if iso_key in raw:
            last = raw[iso_key]
        if last:
            row_data = {"US Dollar": 1.0}
            for iso, rate in last.items():
                name = ISO_TO_NAME.get(iso)
                if name:
                    row_data[name] = float(rate)
            daily_rates[slash_key] = row_data
        d += timedelta(days=1)

    conversion_rates_df = pd.DataFrame(daily_rates).T
    conversion_rates_df.index.name = "Date"

    trans_sept_1st_df = trans_df[(trans_df["Timestamp"] >= '2022/09/01') & (trans_df["Timestamp"] <= '2022/09/06')]
    trans_sept_1st_wire_or_ach_df = trans_sept_1st_df[trans_sept_1st_df["Payment Format"].isin(["Wire", "ACH"])]
    trans_sept_1st_wire_or_ach_converted_df = trans_sept_1st_wire_or_ach_df.copy()

    def convertir_a_usd(row):
        try:
            rate = conversion_rates_df[row['Payment Currency']][row["Timestamp"].split(" ")[0]]
            return row['Amount Paid'] / rate
        except (KeyError, ZeroDivisionError):
            return float('nan')

    trans_sept_1st_wire_or_ach_converted_df['Amount'] = trans_sept_1st_wire_or_ach_converted_df.apply(convertir_a_usd, axis=1)
    trans_sept_1st_wire_or_ach_filtered = trans_sept_1st_wire_or_ach_converted_df[trans_sept_1st_wire_or_ach_converted_df['Amount'] < 1.0]
    
    q5_solucion = pd.DataFrame({"count": [trans_sept_1st_wire_or_ach_filtered.shape[0]]})
    q5_path = project_root / OUTPUTS[5]
    q5_solucion.to_csv(q5_path, index=False)
    print(f"Query 5 guardada en: {q5_path} (Valor: {trans_sept_1st_wire_or_ach_filtered.shape[0]})")
    print("\n¡Ejecución de queries completada con éxito!")

if __name__ == "__main__":
    main()
