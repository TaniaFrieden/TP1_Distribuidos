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

Ejemplo con parámetros posicionales (con los mismos atajos sin carpeta ni extensión):
```bash
make client HI-Large_Trans_sample_30 HI-Large_accounts OUTPUT_DIR=output/prueba
```

## Validación de Soluciones

Para validar el correcto funcionamiento de las queries contra datasets de prueba, se disponen de comandos para generar soluciones locales y contrastar iterativamente los resultados.

### Generar solución de referencia (Notebook)
Ejecuta la lógica de pandas de referencia para crear los archivos CSV de soluciones esperadas.
```bash
make solucionar <dataset> <dir>
```
* **`<dataset>`**: Nombre del archivo del dataset en la carpeta `datasets/` (no requiere indicar el directorio ni la extensión `.csv`).
* **`<dir>`**: Carpeta de destino de soluciones bajo `solutions/` (no requiere escribir el prefijo `solutions/`). Si ya existe la carpeta de destino, se borra por completo y se vuelve a crear.

*Ejemplo:*
```bash
make solucionar HI-Large_Trans_sample_30 Hi-Large-30
```

### Iterar y comparar resultados del cliente
Ejecuta el cliente iterativamente y contrasta sus resultados CSV con las soluciones de referencia de forma automatizada.
```bash
make iterar [iteraciones] [transacciones] [cuentas] [soluciones]
```
* **`[iteraciones]`**: Número de iteraciones a ejecutar (opcional, default `1`).
* **`[transacciones]`**: Nombre del dataset de transacciones (opcional, busca en `datasets/` por defecto).
* **`[cuentas]`**: Nombre del dataset de cuentas (opcional, busca en `datasets/` por defecto).
* **`[soluciones]`**: Nombre de la carpeta de soluciones esperadas bajo `solutions/` (opcional, default `Hi-Large-30`).

*Ejemplo:*
```bash
make iterar 5 HI-Large_Trans_sample_30 HI-Large_accounts Hi-Large-30
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
