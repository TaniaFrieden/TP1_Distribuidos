#!/usr/bin/env python3
import json
import sys
from pathlib import Path

WORKERS_FILE = Path(__file__).resolve().parents[2] / "config" / "workers.json"


def cargar_workers():
    with open(WORKERS_FILE, "r") as f:
        return json.load(f)


def guardar_workers(data):
    with open(WORKERS_FILE, "w") as f:
        json.dump(data, f, indent=2)
        f.write("\n")


def listar():
    data = cargar_workers()
    claves = sorted(k for k in data if k.startswith("CANT_"))
    if not claves:
        print("No se encontraron variables CANT_* en workers.json")
        return

    ancho_nombre = max(len(k) for k in claves)
    print(f"\n  {'Worker':<{ancho_nombre}}  Réplicas")
    print(f"  {'─' * ancho_nombre}  ────────")
    for k in claves:
        nombre = k.replace("CANT_", "").lower()
        print(f"  {nombre:<{ancho_nombre}}  {data[k]}")
    print()


def establecer(nombre, cantidad):
    data = cargar_workers()

    clave = f"CANT_{nombre.upper()}"
    if clave not in data:
        claves = [k for k in data if k.startswith("CANT_")]
        print(f"Error: '{nombre}' no encontrado.")
        print(f"Workers disponibles: {', '.join(k.replace('CANT_', '').lower() for k in sorted(claves))}")
        sys.exit(1)

    anterior = data[clave]
    data[clave] = cantidad
    guardar_workers(data)
    print(f"  {nombre.lower()}: {anterior} → {cantidad}")
    print(f"\n  Ejecutá 'make generar <queries>' para regenerar el docker-compose.")


def main():
    args = sys.argv[1:]

    if not args or args[0] == "listar":
        listar()
    elif args[0] == "set":
        if len(args) < 3:
            print("Uso: gestionar_workers.py set <nombre_worker> <cantidad>")
            print("Ejemplo: gestionar_workers.py set Q1_PROJECTION 4")
            sys.exit(1)
        nombre = args[1]
        try:
            cantidad = int(args[2])
        except ValueError:
            print(f"Error: '{args[2]}' no es un número válido.")
            sys.exit(1)
        establecer(nombre, cantidad)
    else:
        print("Uso:")
        print("  gestionar_workers.py listar              Ver réplicas actuales")
        print("  gestionar_workers.py set <worker> <N>    Cambiar réplicas")
        sys.exit(1)


if __name__ == "__main__":
    main()
