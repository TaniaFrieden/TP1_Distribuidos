import pandas as pd
from pathlib import Path


def comparar_csv_sin_orden(archivo1: Path | str, archivo2: Path | str) -> tuple[bool, str]:
    """Compara dos CSV ignorando el orden de las filas.

    Devuelve (True, mensaje) si son iguales, (False, mensaje) si no lo son
    o si ocurre un error (archivo no encontrado u otro).
    """
    try:
        df1 = pd.read_csv(archivo1)
        df2 = pd.read_csv(archivo2)

        if set(df1.columns) != set(df2.columns):
            mensaje = (
                "Los archivos NO son iguales: Tienen columnas diferentes.\n"
                f"Columnas CSV 1: {list(df1.columns)}\n"
                f"Columnas CSV 2: {list(df2.columns)}"
            )
            return False, mensaje

        df2 = df2[df1.columns]
        df1_ordenado = df1.sort_values(by=list(df1.columns)).reset_index(drop=True)
        df2_ordenado = df2.sort_values(by=list(df1.columns)).reset_index(drop=True)

        if df1_ordenado.equals(df2_ordenado):
            return True, "¡Los archivos CSV son exactamente IGUALES! (ignorando el orden)"

        return False, "Los archivos NO son iguales: El contenido de las filas difiere."

    except FileNotFoundError as exc:
        return False, f"Error: No se pudo encontrar uno de los archivos: {exc}"
    except Exception as exc:
        return False, f"Ocurrió un error inesperado: {exc}"
