PYTHON := .venv/bin/python
PIP := $(PYTHON) -m pip
PYTEST := PYTHONPATH=src $(PYTHON) -m pytest

# Variables del cliente
INPUT_FILE ?= datasets/transacciones_sample.csv
OUTPUT_FILE ?= output/client_output.csv
SERVER_HOST ?= 127.0.0.1
SERVER_PORT ?= 5678
BATCH_SIZE ?= 2000

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
	@echo "  make start             - levanta docker-compose (RabbitMQ + servicios)"
	@echo "  make down              - detiene docker-compose"
	@echo "  make docker-logs       - muestra logs de docker"
	@echo ""
	@echo "Uso local (sin docker):"
	@echo "  Terminal 1: make gateway"
	@echo "  Terminal 2: make client"
	@echo ""
	@echo "Uso con docker:"
	@echo "  1. make start           (levanta RabbitMQ y contenedores)"
	@echo "  2. Espera a que esté listo"
	@echo "  3. make docker-logs     (opcional: ver logs)"
	@echo "  4. Ctrl+C para detener"
	@echo "  5. make down            (detiene servicios)"
	@echo ""
	@echo "Variables del cliente (override con: make client INPUT_FILE=...):"
	@echo "  INPUT_FILE=$(INPUT_FILE)"
	@echo "  OUTPUT_FILE=$(OUTPUT_FILE)"
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
	rm -f output/client_output.txt
	rm -f output/client_output.csv

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
	INPUT_FILE=$(INPUT_FILE) \
	OUTPUT_FILE=$(OUTPUT_FILE) \
	SERVER_HOST=$(SERVER_HOST) \
	SERVER_PORT=$(SERVER_PORT) \
	BATCH_SIZE=$(BATCH_SIZE) \
	PYTHONPATH=src $(PYTHON) src/client/main.py

gateway:
	@if command -v fuser >/dev/null 2>&1; then fuser -k $(SERVER_PORT)/tcp >/dev/null 2>&1 || true; fi
	SERVER_HOST=$(SERVER_HOST) \
	SERVER_PORT=$(SERVER_PORT) \
	MOM_HOST=$(MOM_HOST) \
	INPUT_QUEUE=$(INPUT_QUEUE) \
	OUTPUT_QUEUE=$(OUTPUT_QUEUE) \
	PYTHONPATH=src $(PYTHON) src/gateway/main.py

start:
	docker compose up --build

down:
	docker compose down

generar:
	$(PYTHON) ./generar_compose.py