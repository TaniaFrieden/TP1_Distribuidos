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
make start            # levanta RabbitMQ, Gateway y workers en segundo plano (detached)
make start --verbose  # levanta mostrando los logs en consola
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
make cliente OUTPUT_DIR=output/c1
make cliente OUTPUT_DIR=output/c2   # cada cliente en su propia terminal
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

Ejemplo con parámetros posicionales:
```bash
make cliente HI-Large_Trans_sample_30 HI-Large_accounts OUTPUT_DIR=output/prueba
```

## Validación de Soluciones

### Generar solución de referencia (Notebook)

Ejecuta la lógica de pandas de referencia para crear los archivos CSV de soluciones esperadas.
```bash
make solucionar <transacciones> <cuentas> <dir>
```

*Ejemplo:*
```bash
make solucionar HI-Large_Trans_sample_30 HI-Large_accounts Hi-Large-30
```

### Iterar clientes y comparar resultados

Corre N clientes de forma secuencial (sin caos) y compara cada resultado contra las soluciones de referencia.
```bash
make iterar [N] [tx] [acc] [sol]
```

Todos los parámetros son opcionales — usa los defaults del Makefile (`TEST_TX`, `TEST_ACC`, `TEST_SOL`).

*Ejemplo:*
```bash
make iterar                    # 5 clientes con datasets por defecto
make iterar 3                  # 3 clientes con datasets por defecto
make iterar 5 HI-Large_Trans_sample_30 HI-Large_accounts Hi-Large-30
```

## Tolerancia a Fallos y Pruebas de Caos

El sistema cuenta con un mecanismo de tolerancia a fallos autocurativo y herramientas para simular caídas:

### Componentes del Sistema
* **Gateway**: Punto de entrada que recibe las transacciones y cuentas del cliente, distribuye batches a la cola de entrada de RabbitMQ, consolida los resultados de las queries en archivos temporales por cliente y realiza deduplicación de lotes duplicados mediante hash MD5.
* **Workers**: Nodos de procesamiento del pipeline (filtros, conversores de monedas, proyecciones, agregadores y acumuladores) configurados de forma dinámica y con logs detallados de inicialización.
* **Coordinador Distribuido**: Gestiona las barreras de sincronización EOF a nivel de etapa a través de colas de control dedicadas, garantizando una finalización ordenada incluso ante caídas de workers.
* **Watchdog**: Monitorea de forma centralizada la llegada de heartbeats de cada worker y reporta fallos al actuador ante la pérdida reiterada de señales.
* **Actuador**: Escucha reportes del watchdog e interactúa con el daemon de Docker (`docker.sock`) para reiniciar automáticamente las réplicas caídas.

### Simulación de Caos (Chaos Monkey)

Para validar la tolerancia a fallos de forma manual en dos terminales:

**Terminal 1 (Clientes):**
```bash
make iterar 5
```

**Terminal 2 (Chaos Monkey):**
```bash
make tirar-nodos                   # aleatorio cada 10s
make tirar-nodos 5                 # aleatorio cada 5s
make tirar-nodos 5 todos           # mata todos los workers cada 5s
make tirar-nodos 5 etapa           # mata una etapa al azar cada 5s
make tirar-nodos 5 etapa counter   # mata etapa counter cada 5s
```

## Suite de Tests Automatizados

### Estructura del Makefile

El Makefile está modularizado en `make/`:

| Archivo | Contenido |
|---|---|
| `make/config.mk` | Variables globales (python, docker compose, puertos) |
| `make/datasets.mk` | Datasets y soluciones para tests (`TEST_TX`, `TEST_ACC`, `TEST_SOL`) |
| `make/helpers.mk` | Helpers internos (`_full_clean`, `_light_clean`, `_start_env`) |
| `make/utils.mk` | Utilidades (venv, install, clean, generar, solucionar) |
| `make/run.mk` | Ejecución manual (start, down, client, gateway, tirar-nodos) |
| `make/tests.mk` | Todos los targets de test |

### Datasets de test

Todos los tests usan tres variables que se definen en `make/datasets.mk`:

| Variable   | Default             | Descripción                                    |
|------------|---------------------|------------------------------------------------|
| `TEST_TX`  | `trans_sample`      | Archivo de transacciones (en `datasets/`)      |
| `TEST_ACC` | `LI-Small_accounts` | Archivo de cuentas (en `datasets/`)            |
| `TEST_SOL` | `sample`            | Carpeta de soluciones esperadas (en `solutions/`) |

Para cambiar el dataset de toda la suite:
```bash
make test-todos TEST_TX=LI-Small_Trans TEST_ACC=LI-Small_accounts TEST_SOL=LI-Small
```

### Crash Hooks (determinísticos)

Inyectan un crash en un punto exacto del código. El worker/gateway muere una vez y se recupera.

```bash
make test-crash-worker-pre-confirm        # crash pre-confirmación de fin
make test-crash-worker-pre-barrera        # crash pre-disparo de barrera
make test-crash-worker-post-flush         # crash post-flush (default etapa: counter)
make test-crash-worker-post-flush q4_sumador 2  # etapa y clientes custom
make test-crash-gateway                   # 10 hooks del gateway
make test-crash-watchdog                  # 4 hooks del watchdog
```

### Caos (kill externo durante operación)

Matan contenedores externamente durante el procesamiento.

```bash
make test-caos-total 3              # mata todos los workers, 3 clientes
make test-caos-aleatorio 70 2       # chaos monkey 70s, 2 clientes
make test-caos-etapa q4_sumador 2   # mata etapa específica, 2 clientes
make test-caos-gateway 2            # mata gateway, 2 clientes
make test-caos-gateway-resultados   # mata gateway entregando resultados
make test-caos-cliente 3            # mata un cliente a mitad de envío
```

### Suites

```bash
make test-unit                # tests unitarios y de persistencia
make iterar 5                 # 5 clientes secuenciales sin caos
make test-todos               # suite completa (unit + crash + caos)
make test-todos-multi         # suite solo multicliente (default 3 clientes)
make test-todos-multi 5       # suite multicliente con 5 clientes
make test-stress-crash 10     # 10 iteraciones de crash hooks
make test-stress-caos 5 3     # 5 iteraciones de caos, 3 clientes
```

### Suite Completa (`test-todos`)

Ejecuta 16 pasos de validación. Se detiene al primer error.

| # | Test | Descripción |
|---|------|-------------|
| 1 | `test-unit` | Tests unitarios y de persistencia |
| 2 | `test-crash-watchdog` | Crash hooks del watchdog (4 puntos) |
| 3 | `test-crash-worker-pre-confirm` | Crash worker pre-confirmación |
| 4 | `test-crash-worker-pre-barrera` | Crash worker pre-barrera |
| 5 | `test-crash-worker-post-flush` | Crash worker post-flush |
| 6 | `test-crash-gateway` | Crash hooks del gateway (10 puntos) |
| 7-9 | `test-caos-etapa` | Caída de etapas (q2_agregador, q4_sumador, q3_format) |
| 10 | `test-caos-cliente` | Cliente cae a mitad de envío |
| 11 | `test-caos-gateway` | Gateway cae en caliente |
| 12 | `test-caos-aleatorio` | Chaos monkey aleatorio |
| 13 | `test-caos-total` | Mata todos los workers de golpe |
| 14 | `test-caos-gateway-resultados` | Gateway cae entregando resultados |
| 15 | `test-stress-crash` | Stress de crash hooks (2 iter) |
| 16 | `test-stress-caos` | Stress de caos (2 iter) |

### Suite Multicliente (`test-todos-multi`)

Corre solo los tests de caos con N clientes simultáneos:

| # | Test | Descripción |
|---|------|-------------|
| 1 | `test-caos-cliente` | Mata un cliente a mitad de envío |
| 2 | `test-caos-gateway` | Mata gateway mid-operación |
| 3 | `test-caos-aleatorio` | Chaos monkey 70s |
| 4 | `test-caos-total` | Mata todos los workers de golpe |
| 5 | `test-caos-gateway-resultados` | Mata gateway entregando resultados |
| 6 | `test-stress-caos` | 2 iteraciones de caos |

## Limpiar todo

```bash
make clean          # limpieza completa (contenedores, volúmenes, caches, puertos e imágenes Docker)
make docker-clean   # solo limpia imágenes dangling y build cache de Docker
```

`make clean` detiene los contenedores, libera los puertos 5678, 5672 y 15672, elimina caches y archivos temporales, y además ejecuta `docker-clean` para remover imágenes huérfanas y build cache acumulado.

`make docker-clean` se puede correr de forma independiente cuando se quiere liberar espacio de disco sin detener nada.

## Comandos útiles

```bash
make log gateway          # logs del gateway en tiempo real
make log filter_usd_01    # logs de un worker específico
make help                 # lista todos los targets disponibles
```
