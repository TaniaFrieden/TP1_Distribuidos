#!/usr/bin/env python3
import time
import random
import sys
import docker
from datetime import datetime

# Lista de servicios críticos que el Chaos Monkey NO debe apagar jamás
SERVICIOS_CRITICOS = ["client", "rabbitmq", "gateway", "watchdog", "actuador"]

def log(msg):
    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    if msg.startswith('\n'):
        print(f"\n[{timestamp}] {msg[1:]}", flush=True)
    else:
        print(f"[{timestamp}] {msg}", flush=True)

def run_chaos_monkey(intervalo_min=10, intervalo_max=20, filtros=None):
    try:
        client = docker.from_env()
    except Exception as e:
        log(f"Error al conectar con Docker: {e}")
        log("Asegúrate de que el daemon de Docker esté corriendo y tengas permisos.")
        sys.exit(1)

    log("=== Chaos Monkey Iniciado ===")
    log(f"Intervalo aleatorio de fallas: {intervalo_min}s - {intervalo_max}s")
    if filtros:
        log(f"Apuntando solo a contenedores que contengan: {filtros}")
    else:
        log(f"Excluyendo servicios críticos: {SERVICIOS_CRITICOS}")

    try:
        while True:
            delay = random.uniform(intervalo_min, intervalo_max)
            log(f"\nSiguiente ataque en {delay:.1f} segundos...")
            time.sleep(delay)

            contenedores = []
            for _ in range(3):
                try:
                    contenedores = client.containers.list(filters={"status": "running"})
                    break
                except docker.errors.NotFound:
                    time.sleep(0.1)
                except Exception as e:
                    log(f"Error al listar contenedores: {e}")
                    break

            if filtros:
                # Modo apuntado: solo contenedores que matcheen algún filtro
                # Si el usuario pasa un servicio crítico explícitamente en filtros, se le permite atacarlo
                workers = [
                    c for c in contenedores
                    if any(f in c.name for f in filtros)
                    and not any(crit in c.name for crit in SERVICIOS_CRITICOS if crit not in filtros)
                ]
            else:
                # Modo general: todos los workers no críticos
                workers = [
                    c for c in contenedores
                    if not (c.name.startswith("client") or any(crit in c.name for crit in SERVICIOS_CRITICOS))
                ]

            if not workers:
                log("No se encontraron workers activos para derribar.")
                continue

            victima = random.choice(workers)
            metodo = random.choice(["stop", "kill"])

            log(f"[Chaos Monkey] ATACANDO a '{victima.name}' usando método '{metodo}'...")

            try:
                if metodo == "stop":
                    victima.stop(timeout=2)
                else:
                    victima.kill()
                log(f"[Chaos Monkey] '{victima.name}' fue derribado exitosamente.")
            except Exception as e:
                log(f"[Chaos Monkey] Error derribando a '{victima.name}': {e}")

    except KeyboardInterrupt:
        log("\n=== Chaos Monkey Finalizado ===")

if __name__ == "__main__":
    min_time = int(sys.argv[1]) if len(sys.argv) > 1 else 10
    max_time = int(sys.argv[2]) if len(sys.argv) > 2 else 20
    filtros = sys.argv[3:] if len(sys.argv) > 3 else None
    run_chaos_monkey(min_time, max_time, filtros)
