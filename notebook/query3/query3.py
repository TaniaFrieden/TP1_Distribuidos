import pandas as pd
from pathlib import Path

def ejecutar_query(nombre_dataset):
    base_dir = Path(__file__).resolve().parent
    ruta_datasets = base_dir.parents[1] / "datasets"
    ruta_resultado = base_dir / "q3_solucion.csv"

    chunksize = 100000
    
    # Paso 1: calcular estadísticas en el periodo temprano
    stats = {}
    
    for chunk in pd.read_csv(
        ruta_datasets / nombre_dataset,
        dtype={"From Bank": "string", "Account": "string",
               "To Bank": "string", "Account.1": "string",
               "Timestamp": "string"},
        chunksize=chunksize
    ):
        transacciones = chunk[chunk["Payment Currency"] == "US Dollar"].copy()
        if transacciones.empty:
            continue
        transacciones["Amount Paid"] = pd.to_numeric(transacciones["Amount Paid"], errors="coerce")
        transacciones = transacciones.dropna(subset=["Amount Paid", "Payment Format", "Timestamp"])
        
        # String comparison is extremely fast and works because format is YYYY/MM/DD HH:MM
        periodo_temprano = transacciones[
            (transacciones["Timestamp"] >= "2022/09/01") &
            (transacciones["Timestamp"] < "2022/09/06")
        ]
        
        if not periodo_temprano.empty:
            grouped = periodo_temprano.groupby("Payment Format")["Amount Paid"].agg(["sum", "count"])
            for fmt, row in grouped.iterrows():
                if fmt not in stats:
                    stats[fmt] = {"sum": 0.0, "count": 0}
                stats[fmt]["sum"] += row["sum"]
                stats[fmt]["count"] += row["count"]

    promedios = {}
    for fmt, val in stats.items():
        if val["count"] > 0:
            promedios[fmt] = val["sum"] / val["count"]

    # Paso 2: filtrar periodo tardío usando las medias obtenidas
    first_chunk = True
    for chunk in pd.read_csv(
        ruta_datasets / nombre_dataset,
        dtype={"From Bank": "string", "Account": "string",
               "To Bank": "string", "Account.1": "string",
               "Timestamp": "string"},
        chunksize=chunksize
    ):
        transacciones = chunk[chunk["Payment Currency"] == "US Dollar"].copy()
        if transacciones.empty:
            continue
        transacciones["Amount Paid"] = pd.to_numeric(transacciones["Amount Paid"], errors="coerce")
        transacciones = transacciones.dropna(subset=["Amount Paid", "Payment Format", "Timestamp"])
        
        periodo_tardio = transacciones[
            (transacciones["Timestamp"] >= "2022/09/06") &
            (transacciones["Timestamp"] < "2022/09/16")
        ].copy()
        
        if periodo_tardio.empty:
            continue
            
        periodo_tardio["Promedio Formato"] = periodo_tardio["Payment Format"].map(promedios)
        periodo_tardio = periodo_tardio.dropna(subset=["Promedio Formato"])
        
        resultado_chunk = periodo_tardio[
            periodo_tardio["Amount Paid"] < periodo_tardio["Promedio Formato"] * 0.01
        ][["Account", "Amount Paid"]].rename(columns={"Account": "From Account"})
        
        if not resultado_chunk.empty:
            if first_chunk:
                resultado_chunk.to_csv(ruta_resultado, mode='w', index=False)
                first_chunk = False
            else:
                resultado_chunk.to_csv(ruta_resultado, mode='a', header=False, index=False)

    if first_chunk:
        resultado = pd.DataFrame(columns=["From Account", "Amount Paid"])
        resultado.to_csv(ruta_resultado, index=False)

if __name__ == "__main__":
    ejecutar_query("transacciones_sample.csv")
