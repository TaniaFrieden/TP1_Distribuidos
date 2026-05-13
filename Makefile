PYTHON := .venv/bin/python
PIP := $(PYTHON) -m pip
PYTEST := PYTHONPATH=src $(PYTHON) -m pytest

# Variables del cliente
INPUT_FILE ?= test/notebook/query1/transacciones_menores_50.csv
OUTPUT_FILE ?= /tmp/client_output.txt
SERVER_HOST ?= 127.0.0.1
SERVER_PORT ?= 5678
BATCH_SIZE ?= 2

.PHONY: help venv install test test-worker-base client test-server

help:
	@echo "Targets disponibles:"
	@echo "  make venv              - crea el entorno virtual .venv"
	@echo "  make install           - instala las dependencias en .venv"
	@echo "  make test              - corre todos los tests"
	@echo "  make test-worker-base  - corre solo el test de BaseWorker"
	@echo "  make test-server       - inicia servidor de prueba"
	@echo "  make client            - ejecuta el cliente"
	@echo ""
	@echo "Variables del cliente (override con: make client INPUT_FILE=...):"
	@echo "  INPUT_FILE=$(INPUT_FILE)"
	@echo "  OUTPUT_FILE=$(OUTPUT_FILE)"
	@echo "  SERVER_HOST=$(SERVER_HOST)"
	@echo "  SERVER_PORT=$(SERVER_PORT)"
	@echo "  BATCH_SIZE=$(BATCH_SIZE)"

venv:
	python3 -m venv .venv

install:
	$(PIP) install --upgrade pip
	$(PIP) install -r requirements.txt

test:
	$(PYTEST) -q

test-worker-base:
	$(PYTEST) test/common/worker_base/test_worker_base.py -q

test-server:
	PYTHONPATH=src $(PYTHON) scripts/test_server.py

client:
	INPUT_FILE=$(INPUT_FILE) \
	OUTPUT_FILE=$(OUTPUT_FILE) \
	SERVER_HOST=$(SERVER_HOST) \
	SERVER_PORT=$(SERVER_PORT) \
	BATCH_SIZE=$(BATCH_SIZE) \
	PYTHONPATH=src $(PYTHON) src/client/main.py
