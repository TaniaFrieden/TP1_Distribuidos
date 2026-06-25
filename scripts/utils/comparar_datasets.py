#!/usr/bin/env python3
import argparse
import sys
import pandas as pd
from pathlib import Path


def comparar_csv_sin_orden(archivo1, archivo2):
    try:
        df1 = pd.read_csv(archivo1)
        df2 = pd.read_csv(archivo2)

        if set(df1.columns) != set(df2.columns):
            return False, f"Columnas diferentes: {list(df1.columns)} vs {list(df2.columns)}"

        df2 = df2[df1.columns]
        df1_sorted = df1.sort_values(by=list(df1.columns)).reset_index(drop=True)
        df2_sorted = df2.sort_values(by=list(df1.columns)).reset_index(drop=True)

        if df1_sorted.equals(df2_sorted):
            return True, ""

        return False, "El contenido de las filas difiere"

    except FileNotFoundError as exc:
        return False, f"Archivo no encontrado: {exc.filename}"
    except Exception as exc:
        return False, f"Error: {exc}"


def comparar_cliente(cid, output_dir, soluciones_dir, queries):
    correctas = 0
    total = len(queries)
    cid_corto = cid[:8] if len(cid) > 8 else cid
    print(f"\n  ── cliente {cid_corto} ──")
    for q in queries:
        actual = Path(output_dir) / f"q{q}_solucion.csv"
        expected = Path(soluciones_dir) / f"q{q}_solucion.csv"
        if not actual.exists():
            print(f"  ⚠ Q{q}  FALTA — {actual}")
        else:
            ok, msg = comparar_csv_sin_orden(actual, expected)
            if ok:
                print(f"  ✔ Q{q}  OK")
                correctas += 1
            else:
                print(f"  ✘ Q{q}  FAIL — {msg}")
    return correctas == total


def comparar_todos_clientes(output_base, soluciones_dir, queries):
    output_path = Path(output_base)
    clientes = sorted([d for d in output_path.iterdir() if d.is_dir()])
    if not clientes:
        print("No se encontraron directorios de clientes en", output_base)
        return False
    total = len(clientes)
    exitosos = 0
    for d in clientes:
        if comparar_cliente(d.name, str(d), soluciones_dir, queries):
            exitosos += 1
    if exitosos == total:
        print(f"═══ Resultado: {total}/{total} clientes OK ═══")
    else:
        print(f"═══ Resultado: {total - exitosos}/{total} clientes FALLARON ═══")
    return exitosos == total


def _obtener_queries():
    scripts_dir = Path(__file__).resolve().parent
    if str(scripts_dir) not in sys.path:
        sys.path.append(str(scripts_dir))
    project_root = Path(__file__).resolve().parents[2]
    try:
        from obtener_queries import obtener_queries_desde_compose
        return obtener_queries_desde_compose(project_root / "docker-compose.yml")
    except Exception:
        return [1, 2, 3, 4, 5]


def main():
    parser = argparse.ArgumentParser(description="Compara resultados de queries contra soluciones esperadas.")
    parser.add_argument("output_dir", help="Directorio de output (base con subdirs de clientes, o directorio de un cliente).")
    parser.add_argument("soluciones_dir", help="Directorio con las soluciones esperadas.")
    args = parser.parse_args()

    project_root = Path(__file__).resolve().parents[2]
    queries = _obtener_queries()

    output = Path(args.output_dir)
    if not output.is_absolute():
        output = project_root / output
    soluciones = Path(args.soluciones_dir)
    if not soluciones.is_absolute():
        soluciones = project_root / soluciones

    tiene_csvs = any(output.glob("q*_solucion.csv"))
    if tiene_csvs:
        ok = comparar_cliente(output.name, str(output), str(soluciones), queries)
    else:
        ok = comparar_todos_clientes(str(output), str(soluciones), queries)

    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()
