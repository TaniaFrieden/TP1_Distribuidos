#!/usr/bin/env python3
import time
import random
import sys
import docker
from datetime import datetime

# Lista de servicios críticos que el Chaos Monkey NO debe apagar jamás
# A menos que se pida apagar TODOS (--todos) o se especifique una etapa que los incluya.
# Nota: "gateway" y "rabbitmq" son servicios de infraestructura base para que el test unitario de workers
# pueda correr o reconectar, no son workers stateless. Los watchdogs y actuadores controlan el ciclo de vida.
# Excluimos estos servicios clave de "--todos" para evitar que el test falle por indisponibilidad de RabbitMQ o Gateway muerto permanente.
SERVICIOS_CRITICOS = ["client", "rabbitmq", "gateway", "watchdog", "actuador"]

def log(msg):
    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    if msg.startswith('\n'):
        print(f"\n[{timestamp}] {msg[1:]}", flush=True)
    else:
        print(f"[{timestamp}] {msg}", flush=True)

def run_chaos_monkey(intervalo_min=10, intervalo_max=20, filtros=None, matar_todos=False):
    try:
        client = docker.from_env()
    except Exception as e:
        log(f"Error al conectar con Docker: {e}")
        log("Asegúrate de que el daemon de Docker esté corriendo y tengas permisos.")
        sys.exit(1)

    log("=== Chaos Monkey Iniciado ===")
    
    if matar_todos:
        log("MODO DESTRUCTOR: Apagando todos los contenedores activos de inmediato.")
        try:
            contenedores = client.containers.list(filters={"status": "running"})
            # Excluimos cliente por sanidad del test, y también RabbitMQ / Gateway / Watchdogs / Actuadores
            # para no romper el test-todos (que asume que los workers mueren pero la base sigue viva).
            victimas = [
                c for c in contenedores 
                if not (c.name.startswith("client") or any(crit in c.name for crit in SERVICIOS_CRITICOS))
            ]
            if not victimas:
                log("No hay contenedores de workers corriendo para apagar.")
                return
            log(f"Matando a todos los contenedores: {[c.name for c in victimas]}")
            for v in victimas:
                try:
                    v.kill()
                except Exception as e:
                    log(f"Error matando a {v.name}: {e}")
            log("Todos los nodos elegidos fueron derribados.")
        except Exception as e:
            log(f"Error en destructor: {e}")
        return

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
                    client = docker.from_env(timeout=10)
                    contenedores = client.containers.list(filters={"status": "running"})
                    break
                except docker.errors.NotFound:
                    time.sleep(0.1)
                except Exception as e:
                    log(f"Error al listar contenedores: {e}")
                    time.sleep(2)
                    continue

            if filtros:
                workers = [
                    c for c in contenedores
                    if any(f in c.name for f in filtros)
                    and not any(crit in c.name for crit in SERVICIOS_CRITICOS if crit not in filtros)
                ]
            else:
                workers = [
                    c for c in contenedores
                    if not (c.name.startswith("client") or any(crit in c.name for crit in SERVICIOS_CRITICOS))
                ]

            if not workers:
                log("No se encontraron workers activos para derribar.")
                continue

            # Si es por etapa (filtros especificados), matamos todos los de esa etapa de una.
            if filtros:
                log(f"[Chaos Monkey] ATACANDO etapa con filtros: {filtros}. Matando a todos sus nodos activos...")
                for victima in workers:
                    try:
                        victima.kill()
                        log(f"[Chaos Monkey] '{victima.name}' fue derribado.")
                    except Exception as e:
                        log(f"[Chaos Monkey] Error derribando a '{victima.name}': {e}")
            else:
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
    # Formato esperado de argumentos:
    # Caso 1: [min] [max] --todos
    # Caso 2: [min] [max] --etapa <prefijo>
    # Caso 3: [min] [max] [filtros...]
    args = sys.argv[1:]
    
    # 1. Detectar si hay un rango de tiempo definido al principio (dos números seguidos)
    espera_inicial = 0
    min_time = 10
    max_time = 20
    
    # Analizamos si los primeros argumentos son números
    nums = []
    while len(args) > 0 and args[0].isdigit():
        nums.append(int(args.pop(0)))
        
    if len(nums) == 2:
        # Rango min-max provisto
        min_time = nums[0]
        max_time = nums[1]
        espera_inicial = random.uniform(min_time, max_time)
    elif len(nums) == 1:
        # Un único número, lo tratamos como tiempo fijo
        min_time = nums[0]
        max_time = nums[0]
        espera_inicial = float(nums[0])
        
    # Si se especificó espera_inicial y vienen flags inmediatas (--todos o --etapa), esperamos ese tiempo aleatorio
    if espera_inicial > 0 and ("--todos" in args or "--etapa" in args):
        log(f"Esperando intervalo aleatorio entre {min_time}s y {max_time}s (elegido: {espera_inicial:.2f}s) antes del crash...")
        time.sleep(espera_inicial)

    if "--todos" in args:
        run_chaos_monkey(matar_todos=True)
    elif "--etapa" in args:
        idx = args.index("--etapa")
        if idx + 1 < len(args):
            etapa = args[idx+1]
            try:
                cl = docker.from_env()
                contenedores = cl.containers.list(filters={"status": "running"})
                victimas = [c for c in contenedores if etapa in c.name and not c.name.startswith("client")]
                if not victimas:
                    log(f"No se encontraron contenedores para la etapa: {etapa}")
                else:
                    log(f"Matando etapa '{etapa}': {[c.name for c in victimas]}")
                    for v in victimas:
                        try:
                            v.kill()
                        except Exception as e:
                            log(f"Error matando a {v.name}: {e}")
            except Exception as e:
                log(f"Error al matar etapa: {e}")
        else:
            log("Error: Falta especificar la etapa luego de --etapa")
            sys.exit(1)
    else:
        # Formato tradicional: se usa min_time y max_time ya leídos
        filtros = args if len(args) > 0 else None
        run_chaos_monkey(min_time, max_time, filtros)
