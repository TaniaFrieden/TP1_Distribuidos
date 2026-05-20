import json
import os

CONFIG_FILE = "config.json"
OUTPUT_FILE = "docker-compose.yml"

def generar_docker_compose():
    if not os.path.exists(CONFIG_FILE):
        print(f"Error: No se encontró el archivo {CONFIG_FILE}")
        return

    with open(CONFIG_FILE, 'r', encoding='utf-8') as f:
        config = json.load(f)

    # 1. Base del archivo y servicios estáticos
    yaml_content = "services:\n"
    
    yaml_content += """  gateway:
    build:
      context: ./src
      dockerfile: gateway/Dockerfile
    container_name: gateway
    depends_on:
      rabbitmq:
        condition: service_healthy
    ports:
    - "8080:8080"
    - "5678:5678"
    environment:
    - PYTHONUNBUFFERED=1
    - SERVER_HOST=gateway
    - SERVER_PORT=5678
    - MOM_HOST=rabbitmq
    - OUTPUT_QUEUE=raw_data
    - INPUT_QUEUE=filtered_data
    - HTTP_HOST=0.0.0.0
    - HTTP_PORT=8080

  rabbitmq:
    image: rabbitmq:3-management
    container_name: rabbitmq
    environment:
    - RABBITMQ_LOG_LEVELS=error
    healthcheck:
      interval: 5s
      retries: 10
      start_period: 50s
      test: rabbitmq-diagnostics check_port_connectivity
      timeout: 3s
    ports:
    - "5672:5672"
    - "15672:15672"
"""

    # 2. Generación dinámica de workers
    total_workers_creados = 0
    worker_groups = config.get("worker_groups", [])

    for group in worker_groups:
        prefix = group["prefix"]
        replicas = group["replicas"]
        
        for i in range(1, replicas + 1):
            worker_name = f"{prefix}_{str(i).zfill(2)}"  # Ej: filter_usd_01
            total_workers_creados += 1
            
            yaml_content += f"""
  {worker_name}:
    build:
      context: ./src
      dockerfile: workers/filter/Dockerfile
    container_name: {worker_name}
    depends_on:
      rabbitmq:
        condition: service_healthy
      gateway:
        condition: service_started
    environment:
    - MOM_HOST=rabbitmq
    - INPUT_QUEUE={group['input_queue']}
    - OUTPUT_QUEUE={group['output_queue']}
    - PYTHONUNBUFFERED=1
    - NODE_PREFIX={prefix}
    - ID={i}
    - TOTAL_WORKERS={replicas}
    - FILTER_FIELD={group['filter_field']}
    - FILTER_VALUE={group['filter_value']}
    - FILTER_OPERATOR={group.get('filter_operator', 'eq')}"""

    # 3. Escribir el archivo
    with open(OUTPUT_FILE, 'w', encoding='utf-8') as f:
        f.write(yaml_content + "\n")

    print(f"¡Éxito! '{OUTPUT_FILE}' generado correctamente.")
    print(f"Se configuraron {total_workers_creados} workers en total distribuidos en {len(worker_groups)} grupos.")

if __name__ == "__main__":
    generar_docker_compose()