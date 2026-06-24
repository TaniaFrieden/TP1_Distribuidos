# Utilidades: venv, install, clean, generacion de datos, etc.

.PHONY: venv install clean free-ports generar generar-sample solucionar iterar

venv:
	python3 -m venv .venv

install:
	$(PIP) install --upgrade pip
	$(PIP) install -r requirements.txt

clean:
	-@$(MAKE) free-ports
	-@$(MAKE) down
	-$(DOCKER_COMPOSE) down -v --remove-orphans 2>/dev/null || true
	rm -rf .pytest_cache
	find . -type d -name '__pycache__' -prune -exec rm -rf {} +
	-docker rm -f $$(docker ps -a -q --filter "name=client_") 2>/dev/null || true
	rm -f /tmp/client_output.txt
	rm -f logs/*.txt
	rm -f logs/*.log
	find output/ -mindepth 1 ! -name '.gitkeep' -delete 2>/dev/null || true
	@if [ -d volume ]; then \
		docker run --rm -v "$$(pwd)/volume:/vol" alpine sh -c "rm -rf /vol/* && chmod -R 777 /vol" 2>/dev/null || true; \
	fi

free-ports:
	@echo "=== Liberando puerto $(SERVER_PORT) (Gateway) ==="
	@if command -v fuser >/dev/null 2>&1; then \
		fuser -k $(SERVER_PORT)/tcp >/dev/null 2>&1 || true; \
	fi
	@echo "=== Liberando puertos de RabbitMQ ==="
	@if command -v fuser >/dev/null 2>&1; then \
		fuser -k 5672/tcp >/dev/null 2>&1 || true; \
		fuser -k 15672/tcp >/dev/null 2>&1 || true; \
	fi

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

generar-sample:
	@ARGS="$(filter-out $@,$(MAKECMDGOALS))"; \
	DB=$$(echo $$ARGS | cut -d' ' -f1); \
	PCT=$$(echo $$ARGS | cut -d' ' -f2); \
	PCT=$${PCT:-30}; \
	if [ -z "$$DB" ]; then \
		echo "Error: Debes especificar el nombre del dataset."; \
		echo "Uso: make generar-sample <dataset> [porcentaje]"; \
		echo "Ejemplo: make generar-sample HI-Large_Trans 30"; \
		exit 1; \
	fi; \
	$(PYTHON) scripts/utils/sample_dataset.py \
		--input datasets/$$DB.csv \
		--output datasets/$${DB}_sample_$${PCT}.csv \
		--percentage $$PCT

solucionar:
	@ARGS="$(filter-out $@,$(MAKECMDGOALS))"; \
	if [ -z "$$ARGS" ]; then \
		echo "Error: Debes especificar al menos el dataset de transacciones y el de cuentas."; \
		echo "Uso: make solucionar <dataset_transacciones> <dataset_cuentas> [dir_carpeta_solutions]"; \
		echo "Ejemplo: make solucionar HI-Large_Trans_sample_30 HI-Large_accounts Hi-Large-30"; \
		exit 1; \
	else \
		$(PYTHON) scripts/utils/ejecutar_solucion_notebook.py $$ARGS; \
	fi

iterar:
	@ARGS="$(filter-out $@,$(MAKECMDGOALS))"; \
	if [ -z "$$ARGS" ]; then \
		echo "Error: Debes especificar al menos el numero de iteraciones."; \
		echo "Uso: make iterar [iteraciones] [transacciones] [cuentas] [soluciones]"; \
		echo "Ejemplo: make iterar 5 HI-Large_Trans_sample_30 HI-Large_accounts Hi-Large-30"; \
		exit 1; \
	else \
		PYTHONPATH=src $(PYTHON) scripts/utils/iterar_queries.py $$ARGS; \
	fi
