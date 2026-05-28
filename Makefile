PYTHON := .venv/bin/python
PIP := $(PYTHON) -m pip
PYTEST := PYTHONPATH=src $(PYTHON) -m pytest
START_VERBOSE := $(if $(filter --verbose,$(MAKECMDGOALS)),1,0)

# Variables del cliente
OUTPUT_DIR ?= output
SERVER_HOST ?= 127.0.0.1
SERVER_PORT ?= 5678
BATCH_SIZE ?= 10000
SCALE ?= 2

# Variables del gateway
MOM_HOST ?= localhost
INPUT_QUEUE ?= input_queue
OUTPUT_QUEUE ?= output_queue

.PHONY: help venv install test test-worker-base clean free-ports client run-clients test-server gateway start down docker-logs iterar solucionar generar log

help:
	@echo "Targets disponibles:"
	@echo "  make venv                        - Crea el entorno virtual .venv"
	@echo "  make install                     - Instala las dependencias en .venv"
	@echo "  make test                        - Corre todos los tests"
	@echo "  make test-worker-base            - Corre solo el test de BaseWorker"
	@echo "  make clean                       - Limpia caches, temporales y libera puertos"
	@echo "  make test-server                 - Inicia servidor de prueba"
	@echo "  make start                       - Levanta docker-compose en segundo plano (detached)"
	@echo "  make start --verbose             - Levanta docker-compose con logs en consola"
	@echo "  make down                        - Detiene docker-compose"
	@echo "  make docker-logs                 - Muestra logs de docker"
	@echo "  make log <servicio>              - Muestra logs de un servicio específico"
	@echo "  make generar <queries>           - Genera el docker-compose para las queries dadas"
	@echo "  make iterar [iteraciones] [transacciones] [cuentas] [soluciones] - Itera queries pasándole número iteraciones, datasets y carpeta de soluciones"
	@echo "  make solucionar <dataset> <dir>  - Ejecuta la solución del notebook (sin dir ni extensión) en el dir destino (sin solutions/)"

venv:
	python3 -m venv .venv

install:
	$(PIP) install --upgrade pip
	$(PIP) install -r requirements.txt

test:
	@echo "\n══════════════════════════════════════════"
	@echo " worker_base"
	@echo "══════════════════════════════════════════"
	@$(PYTEST) test/common/worker_base/test_worker_base.py -v --tb=short --no-header -q || true
	@echo "\n══════════════════════════════════════════"
	@echo " filter"
	@echo "══════════════════════════════════════════"
	@$(PYTEST) test/common/workers/test_filter.py -v --tb=short --no-header -q || true
	@echo "\n══════════════════════════════════════════"
	@echo " adder"
	@echo "══════════════════════════════════════════"
	@$(PYTEST) test/common/workers/test_add.py -v --tb=short --no-header -q || true
	@echo "\n══════════════════════════════════════════"
	@echo " aggregator"
	@echo "══════════════════════════════════════════"
	@$(PYTEST) test/common/workers/test_aggregator.py -v --tb=short --no-header -q || true
	@echo "\n══════════════════════════════════════════"
	@echo " projection"
	@echo "══════════════════════════════════════════"
	@$(PYTEST) test/common/workers/test_projection.py -v --tb=short --no-header -q || true

test-worker-base:
	$(PYTEST) test/common/worker_base/test_worker_base.py -q

clean:
	-@$(MAKE) free-ports
	-@$(MAKE) down
	-docker compose down --vols --remove-orphans 2>/dev/null || true
	-docker network prune -f 2>/dev/null || true
	rm -rf .pytest_cache
	find . -type d -name '__pycache__' -prune -exec rm -rf {} +
	rm -f /tmp/client_output.txt
	rm -f logs/*.txt
	rm -f output/*.csv
	rm -rf output/*/

free-ports:
	@echo "=== Liberando puerto $(SERVER_PORT) (Gateway) ==="
	@if command -v fuser >/dev/null 2>&1; then \
		fuser -k $(SERVER_PORT)/tcp >/dev/null 2>&1 || true; \
	fi
	@echo "=== Deteniendo posibles servicios locales de RabbitMQ ==="
	-@sudo systemctl stop rabbitmq-server 2>/dev/null || true
	-@sudo service rabbitmq-server stop 2>/dev/null || true
	@echo "=== Forzando cierre de puertos 5672 y 15672 ==="
	@if command -v fuser >/dev/null 2>&1; then \
		sudo fuser -k 5672/tcp >/dev/null 2>&1 || true; \
		sudo fuser -k 15672/tcp >/dev/null 2>&1 || true; \
	fi

test-server:
	PYTHONPATH=src $(PYTHON) scripts/test_server.py

client:
	@ARGS="$(filter-out $@,$(MAKECMDGOALS))"; \
	TX=$$(echo $$ARGS | cut -d' ' -f1); \
	ACC=$$(echo $$ARGS | cut -d' ' -f2); \
	TRANSACTIONS_FILE=$${TX:-$(TRANSACTIONS_FILE)} \
	ACCOUNTS_FILE=$${ACC:-$(ACCOUNTS_FILE)} \
	OUTPUT_DIR=$(OUTPUT_DIR) \
	SERVER_HOST=$(SERVER_HOST) \
	SERVER_PORT=$(SERVER_PORT) \
	BATCH_SIZE=$(BATCH_SIZE) \
	PYTHONPATH=src $(PYTHON) src/client/client.py

run-clients:
	docker compose --profile clients up --build --scale client=$(SCALE)

gateway:
	@if command -v fuser >/dev/null 2>&1; then fuser -k $(SERVER_PORT)/tcp >/dev/null 2>&1 || true; fi
	SERVER_HOST=$(SERVER_HOST) \
	SERVER_PORT=$(SERVER_PORT) \
	MOM_HOST=$(MOM_HOST) \
	INPUT_QUEUE=$(INPUT_QUEUE) \
	OUTPUT_QUEUE=$(OUTPUT_QUEUE) \
	PYTHONPATH=src $(PYTHON) src/gateway/gateway.py

start:
	@if [ "$(START_VERBOSE)" = "1" ]; then \
		docker compose up --build; \
	else \
		docker compose up -d --build; \
	fi

down:
	docker compose down

generar:
	@ARGS="$(filter-out $@,$(MAKECMDGOALS))"; \
	if [ -z "$$ARGS" ]; then \
		echo "Error: Debes especificar las queries a generar."; \
		echo "Uso: make generar <numeros>"; \
		echo "Ejemplo: make generar 1 2 5"; \
		exit 1; \
	else \
		python3 generar_compose.py $$ARGS; \
	fi

log:
	@if [ -z "$(filter-out $@,$(MAKECMDGOALS))" ]; then \
		echo "Error: Debes especificar el nombre del servicio."; \
		echo "Ejemplo: make log gateway"; \
		exit 1; \
	fi
	docker compose logs -f $(filter-out $@,$(MAKECMDGOALS))

iterar:
	@ARGS="$(filter-out $@,$(MAKECMDGOALS))"; \
	if [ -z "$$ARGS" ]; then \
		echo "Error: Debes especificar al menos el número de iteraciones."; \
		echo "Uso: make iterar [iteraciones] [transacciones] [cuentas] [soluciones]"; \
		echo "Ejemplo: make iterar 5 HI-Large_Trans_sample_30 HI-Large_accounts Hi-Large-30"; \
		exit 1; \
	else \
		PYTHONPATH=src $(PYTHON) scripts/iterar_queries.py $$ARGS; \
	fi

solucionar:
	@ARGS="$(filter-out $@,$(MAKECMDGOALS))"; \
	if [ -z "$$ARGS" ]; then \
		echo "Error: Debes especificar el dataset y la carpeta de destino de soluciones."; \
		echo "Uso: make solucionar <dataset> <dir carpeta solutions>"; \
		echo "Ejemplo: make solucionar HI-Large_Trans_sample_30 Hi-Large-30"; \
		exit 1; \
	else \
		$(PYTHON) scripts/ejecutar_solucion_notebook.py $$ARGS; \
	fi

# Ignorar argumentos pasados a targets dinámicos
%:
	@: