PYTHON := .venv/bin/python
PIP := $(PYTHON) -m pip
PYTEST := PYTHONPATH=src $(PYTHON) -m pytest
START_VERBOSE := $(if $(filter !logs,$(MAKECMDGOALS)),0,1)

# Variables del cliente
# TRANSACTIONS_FILE ?= datasets/LI-Small_Trans.csv
TRANSACTIONS_FILE ?= datasets/transacciones_sample.csv
ACCOUNTS_FILE ?= datasets/LI-Small_accounts.csv
OUTPUT_DIR ?= output
SERVER_HOST ?= 127.0.0.1
SERVER_PORT ?= 5678
BATCH_SIZE ?= 10000


# Variables del gateway
MOM_HOST ?= localhost
INPUT_QUEUE ?= input_queue
OUTPUT_QUEUE ?= output_queue

.PHONY: help venv install test test-worker-base clean free-ports client test-server gateway start down docker-logs

help:
	@echo "Targets disponibles:"
	@echo "  make venv              - crea el entorno virtual .venv"
	@echo "  make install           - instala las dependencias en .venv"
	@echo "  make test              - corre todos los tests"
	@echo "  make test-worker-base  - corre solo el test de BaseWorker"
	@echo "  make clean             - limpia caches, artefactos temporales y libera todos los puertos"
	@echo "  make test-server       - inicia servidor de prueba"
	@echo "  make start             - levanta docker-compose con logs en consola"
	@echo "  make start !logs       - levanta docker-compose en segundo plano"
	@echo "  make down              - detiene docker-compose"
	@echo "  make docker-logs       - muestra logs de docker"
	@echo ""
	@echo "Uso local (sin docker):"
	@echo "  Terminal 1: make gateway"
	@echo "  Terminal 2: make client"
	@echo ""
	@echo "Uso con docker:"
	@echo "  1. make start           (levanta RabbitMQ y contenedores con logs)"
	@echo "  2. make start !logs     (opcional: no mostrar logs al arrancar)"
	@echo "  3. make docker-logs     (opcional: ver logs luego)"
	@echo "  4. Ctrl+C para detener"
	@echo "  5. make down            (detiene servicios)"
	@echo ""
	@echo "Variables del cliente (override con: make client TRANSACTIONS_FILE=...):"
	@echo "  TRANSACTIONS_FILE=$(TRANSACTIONS_FILE)"
	@echo "  ACCOUNTS_FILE=$(ACCOUNTS_FILE)"
	@echo "  OUTPUT_DIR=$(OUTPUT_DIR)"
	@echo "  SERVER_HOST=$(SERVER_HOST)"
	@echo "  SERVER_PORT=$(SERVER_PORT)"
	@echo "  BATCH_SIZE=$(BATCH_SIZE)"
	@echo ""
	@echo "Variables del gateway:"
	@echo "  MOM_HOST=$(MOM_HOST)"
	@echo "  INPUT_QUEUE=$(INPUT_QUEUE)"
	@echo "  OUTPUT_QUEUE=$(OUTPUT_QUEUE)"

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
	TRANSACTIONS_FILE=$(TRANSACTIONS_FILE) \
	ACCOUNTS_FILE=$(ACCOUNTS_FILE) \
	OUTPUT_DIR=$(OUTPUT_DIR) \
	SERVER_HOST=$(SERVER_HOST) \
	SERVER_PORT=$(SERVER_PORT) \
	BATCH_SIZE=$(BATCH_SIZE) \
	PYTHONPATH=src $(PYTHON) src/client/client.py

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

.PHONY: generar
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

%:
	@:

log:
	@if [ -z "$(filter-out $@,$(MAKECMDGOALS))" ]; then \
		echo "Error: Debes especificar el nombre del servicio."; \
		echo "Ejemplo: make log gateway"; \
		exit 1; \
	fi
	docker compose logs -f $(filter-out $@,$(MAKECMDGOALS))