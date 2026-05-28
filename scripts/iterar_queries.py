#!/usr/bin/env python3
import os
import sys
import subprocess
from pathlib import Path

# =====================================================================
# CONFIGURACIÓN POR DEFECTO
# =====================================================================
CANTIDAD_EJECUCIONES = 2
ACTUAL_TEMPLATE = "output/output_{q}.csv"
EXPECTED_TEMPLATE = "notebook/q{q}_solucion.csv"

# Archivos de datasets a pasar al cliente
TRANSACTIONS_FILE = "datasets/transacciones_sample.csv"
ACCOUNTS_FILE = "datasets/LI-Small_accounts.csv"
# =====================================================================

def main():
    # Encontrar la raíz del proyecto (un nivel arriba del directorio scripts/)
    project_root = Path(__file__).resolve().parents[1]
    
    # Agregar scripts/ al path de importación para importar los scripts auxiliares
    scripts_dir = Path(__file__).resolve().parent
    sys.path.append(str(scripts_dir))
    
    try:
        from obtener_queries import obtener_queries_desde_compose
        from comparar_datasets import comparar_csv_sin_orden
        from titular import imprimir_titulo
    except ImportError as e:
        print(f"Error al importar módulos auxiliares desde scripts/: {e}")
        sys.exit(1)
        
    # Obtener las queries configuradas en el docker-compose
    try:
        queries = obtener_queries_desde_compose(project_root / "docker-compose.yml")
    except Exception as e:
        print(f"No se pudo detectar queries del compose. Usando fallback [1, 2, 3, 4, 5]. Error: {e}")
        queries = [1, 2, 3, 4, 5]

    fallas_por_query = {q: 0 for q in queries}

    # Leer cantidad de iteraciones si se pasa por parámetro (ej. ./iterar_queries.py 5)
    cantidad_iteraciones = CANTIDAD_EJECUCIONES
    args = sys.argv[1:]
    if args:
        try:
            cantidad_iteraciones = int(args[0])
        except ValueError:
            print(f"Argumento inválido '{args[0]}', se usará el valor por defecto de {CANTIDAD_EJECUCIONES} iteraciones.")

    for indice in range(1, cantidad_iteraciones + 1):
        imprimir_titulo(f"Iteración {indice}/{cantidad_iteraciones}")
        
        # Ejecutar el cliente usando make client
        try:
            subprocess.run(
                [
                    "make",
                    "-C", str(project_root),
                    "client",
                    f"TRANSACTIONS_FILE={TRANSACTIONS_FILE}",
                    f"ACCOUNTS_FILE={ACCOUNTS_FILE}",
                    f"OUTPUT_DIR={ACTUAL_TEMPLATE.split('/')[0]}"
                ],
                check=True
            )
        except subprocess.CalledProcessError as e:
            print(f"\n[ERROR] El cliente falló con código de salida {e.returncode} en la iteración {indice}.")
            for query in queries:
                fallas_por_query[query] += 1
            continue

        # Comparar los resultados de cada query obtenida frente a su dataset solución esperado
        for query in queries:
            actual_csv = project_root / ACTUAL_TEMPLATE.format(q=query)
            expected_csv = project_root / EXPECTED_TEMPLATE.format(q=query)

            print(f"\nComparando CSVs para Query {query}:")
            son_iguales, mensaje = comparar_csv_sin_orden(actual_csv, expected_csv)
            print(mensaje)

            if son_iguales:
                print("Resultado: iguales")
            else:
                fallas_por_query[query] += 1
                print("Resultado: diferentes")

    imprimir_titulo("Resumen")
    print(f"\nResumen de fallas por query después de {cantidad_iteraciones} iteraciones:")
    for query, fallas in fallas_por_query.items():
        print(f"Query {query}: {fallas} fallas en {cantidad_iteraciones} ejecuciones")

    # Si hay fallas detectadas, salir con código de error
    total_fallas = sum(fallas_por_query.values())
    if total_fallas > 0:
        sys.exit(1)
    else:
        sys.exit(0)

if __name__ == "__main__":
    main()
