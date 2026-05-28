import pandas as pd
from pathlib import Path

def ejecutar_query(nombre_dataset):
    base_dir = Path(__file__).resolve().parent
    ruta_datasets = base_dir.parents[1] / "datasets"
    solucion_path = base_dir / "q1_solucion.csv"

    transacciones = pd.read_csv(ruta_datasets / nombre_dataset)

    transacciones_usd = transacciones[transacciones['Payment Currency'] == "US Dollar"]
    monto_limite = 50  
    transacciones_menores_50 = transacciones_usd[transacciones_usd['Amount Paid'] < monto_limite]
    resultado = transacciones_menores_50[['From Bank', 'Account', 'To Bank', 'Account.1', 'Amount Paid']]
    
    resultado.to_csv(solucion_path, index=False)

if __name__ == "__main__":
    ejecutar_query("transacciones_sample.csv")
