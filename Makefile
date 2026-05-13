PYTHON := .venv/bin/python
PIP := $(PYTHON) -m pip
PYTEST := PYTHONPATH=src $(PYTHON) -m pytest

.PHONY: help venv install test test-worker-base

help:
	@echo "Targets disponibles:"
	@echo "  make venv              - crea el entorno virtual .venv"
	@echo "  make install           - instala las dependencias en .venv"
	@echo "  make test              - corre todos los tests"
	@echo "  make test-worker-base  - corre solo el test de BaseWorker"

venv:
	python3 -m venv .venv

install:
	$(PIP) install --upgrade pip
	$(PIP) install -r requirements.txt

test:
	$(PYTEST) -q

test-worker-base:
	$(PYTEST) test/common/worker_base/test_worker_base.py -q
