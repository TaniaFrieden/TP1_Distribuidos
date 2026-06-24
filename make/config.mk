MAKEFLAGS += --no-print-directory
PYTHON := .venv/bin/python
PIP := .venv/bin/pip
PYTEST := PYTHONPATH=src $(PYTHON) -m pytest
START_VERBOSE := $(if $(filter --verbose,$(MAKECMDGOALS)),1,0)

# Deteccion automatica de Docker Compose (v1 o v2)
DOCKER_COMPOSE := $(shell docker compose version >/dev/null 2>&1 && echo "docker compose" || echo "docker-compose")

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
