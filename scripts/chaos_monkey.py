#!/usr/bin/env python3
import time
import random
import sys
import docker

# Lista de servicios críticos que el Chaos Monkey NO debe apagar jamás
SERVICIOS_CRITICOS = ["client", "rabbitmq", "gateway", "watchdog", "actuador"]

def run_chaos_monkey(intervalo_min=10, intervalo_max=20):
    try:
        client = docker.from_env()
    except Exception as e:
        print(f"Error al conectar con Docker: {e}")
        print("Asegúrate de que el daemon de Docker esté corriendo y tengas permisos.")
        sys.exit(1)

    print("=== Chaos Monkey Iniciado ===")
    print(f"Intervalo aleatorio de fallas: {intervalo_min}s - {intervalo_max}s")
    print(f"Excluyendo servicios críticos: {SERVICIOS_CRITICOS}")

    try:
        while True:
            # Esperar un tiempo aleatorio antes del siguiente ataque
            delay = random.uniform(intervalo_min, intervalo_max)
            print(f"\nSiguiente ataque en {delay:.1f} segundos...")
            time.sleep(delay)

            # Obtener lista de contenedores corriendo en el proyecto, tolerando condiciones de carrera
            contenedores = []
            for _ in range(3):
                try:
                    contenedores = client.containers.list(filters={"status": "running"})
                    break
                except docker.errors.NotFound:
                    time.sleep(0.1)
                except Exception as e:
                    print(f"Error al listar contenedores: {e}")
                    break
            
            # Filtrar para obtener solo workers (excluyendo bases y servicios de control/clientes)
            workers = [
                c for c in contenedores
                if not (c.name.startswith("client") or any(crit in c.name for crit in SERVICIOS_CRITICOS))
            ]

            if not workers:
                print("No se encontraron workers activos para derribar.")
                continue

            # Seleccionar un worker aleatorio
            victima = random.choice(workers)
            
            # Decidir método de ataque (50% stop normal / 50% kill abrupto)
            metodo = random.choice(["stop", "kill"])

            print(f"[Chaos Monkey] 💥 ATACANDO a '{victima.name}' usando método '{metodo}'...")
            
            try:
                if metodo == "stop":
                    victima.stop(timeout=2)
                else:
                    victima.kill()
                print(f"[Chaos Monkey] ✅ '{victima.name}' fue derribado exitosamente.")
            except Exception as e:
                print(f"[Chaos Monkey] ❌ Error derribando a '{victima.name}': {e}")

    except KeyboardInterrupt:
        print("\n=== Chaos Monkey Finalizado ===")

if __name__ == "__main__":
    # Permite pasar el intervalo por parámetro si se desea
    min_time = int(sys.argv[1]) if len(sys.argv) > 1 else 10
    max_time = int(sys.argv[2]) if len(sys.argv) > 2 else 20
    run_chaos_monkey(min_time, max_time)
