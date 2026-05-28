import sys
from pathlib import Path

# Agregar el directorio notebook al sys.path para poder importar los módulos fácilmente
BASE_DIR = Path(__file__).resolve().parent
sys.path.append(str(BASE_DIR))

DATASET_TRANSACCIONES = "transacciones_sample_30.csv"
DATASET_ACCOUNTS = "HI-Large_accounts.csv"

from query1.query1 import ejecutar_query as q1
from query2.query2 import ejecutar_query as q2
from query3.query3 import ejecutar_query as q3
from query4.query4 import ejecutar_query as q4
from query5.query5 import ejecutar_query as q5

def obtener_todas_las_soluciones(transacciones=DATASET_TRANSACCIONES, accounts=DATASET_ACCOUNTS ):
    print(f"Iniciando ejecución de todas las queries usando el dataset: {transacciones} y {accounts}\n")
    
    print("Ejecutando Query 1...")
    q1(transacciones)
    print("Query 1 completada exitosamente.")
    
    print("Ejecutando Query 2...")
    q2(transacciones, accounts)
    print("Query 2 completada exitosamente.")
    
    print("Ejecutando Query 3...")
    q3(transacciones)
    print("Query 3 completada exitosamente.")
    
    print("Ejecutando Query 4...")
    q4(transacciones)
    print("Query 4 completada exitosamente.")
    
    print("Ejecutando Query 5...")
    q5(transacciones)
    print("Query 5 completada exitosamente.")
    
    print("\n¡Todas las soluciones han sido generadas y guardadas correctamente!")

if __name__ == "__main__":
    if len(sys.argv) > 2:
        dataset = sys.argv[1]
        accounts = sys.argv[2]
    else:
        dataset = DATASET_TRANSACCIONES
        accounts = DATASET_ACCOUNTS

    obtener_todas_las_soluciones(dataset, accounts)
