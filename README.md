# TP1 Distribuidos

## Requisitos previos

- Docker y Docker Compose instalados
- Python 3 con entorno virtual (para correr el cliente localmente)
- Dataset: carpeta `datasets/` con los archivos CSV de transacciones y cuentas

## Configuración inicial

```bash
make venv
make install
```

## Generar el docker-compose

El `docker-compose.yml` se genera a partir de los archivos de configuración en `config/queries/`. Hay 5 queries disponibles (1 a 5). Indicá cuáles querés incluir:

```bash
make generar 1 2 5      # genera solo las queries 1, 2 y 5
make generar 1 2 3 4 5  # genera todas las queries
```

Esto configura automáticamente el Gateway y los workers correspondientes.

## Levantar el lado del servidor (Docker)

Una vez generado el compose:

```bash
make start          # levanta RabbitMQ, Gateway y workers en segundo plano
make start !logs    # igual pero con logs en consola
```

Para detener:

```bash
make down
```

## Levantar el lado del cliente

### Opción A — clientes en contenedores Docker

```bash
make run-clients            # lanza 2 clientes (por defecto)
make run-clients SCALE=N    # lanza N clientes en paralelo
```

Cada cliente escribe sus resultados en `output/<hostname>/`.

### Opción B — cliente local (sin Docker)

Útil para desarrollo o debug. Requiere levantar el gateway localmente primero.

**Terminal 1 — Gateway:**
```bash
make gateway
```

**Terminal 2+ — Clientes:**
```bash
make client OUTPUT_DIR=output/c1
make client OUTPUT_DIR=output/c2   # cada cliente en su propia terminal
```

#### Variables configurables del cliente

| Variable           | Default       | Descripción                         |
|--------------------|---------------|-------------------------------------|
| `TRANSACTIONS_FILE`| —             | Path al CSV de transacciones        |
| `ACCOUNTS_FILE`    | —             | Path al CSV de cuentas              |
| `OUTPUT_DIR`       | `output`      | Carpeta donde se guardan resultados |
| `SERVER_HOST`      | `127.0.0.1`   | Host del Gateway                    |
| `SERVER_PORT`      | `5678`        | Puerto del Gateway                  |
| `BATCH_SIZE`       | `10000`       | Tamaño del batch de envío           |

Ejemplo con override:
```bash
make client TRANSACTIONS_FILE=datasets/trans.csv ACCOUNTS_FILE=datasets/acc.csv OUTPUT_DIR=output/prueba
```

## Limpiar todo

```bash
make clean
```

Detiene los contenedores, libera los puertos 5678, 5672 y 15672, y elimina caches y archivos temporales.

## Comandos útiles

```bash
make log gateway          # logs del gateway en tiempo real
make log filter_usd_01    # logs de un worker específico
make test                 # corre todos los tests
make help                 # lista todos los targets disponibles
```
