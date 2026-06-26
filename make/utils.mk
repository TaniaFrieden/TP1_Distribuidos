# Utilidades: venv, install, clean, generacion de datos, etc.

.PHONY: venv install clean docker-clean free-ports generar generar-sample solucionar workers-lista workers-set

venv:
	python3 -m venv .venv

install:
	$(PIP) install --upgrade pip
	$(PIP) install -r requirements.txt

clean:
	-@pkill -f "make iterar\|make cliente\|test_helpers\|chaos_monkey" 2>/dev/null || true
	-@$(MAKE) free-ports
	-@timeout 10 docker rm -f $$(docker ps -a -q --filter "name=client_") 2>/dev/null || true
	-@timeout 15 $(DOCKER_COMPOSE) down -v --remove-orphans 2>/dev/null || true
	@rm -rf .pytest_cache
	@find . -type d -name '__pycache__' -prune -exec rm -rf {} +
	@rm -f logs/*.txt logs/*.log
	@find output/ -mindepth 1 ! -name '.gitkeep' -delete 2>/dev/null || true
	@if [ -d volume ] && [ -n "$$(ls -A volume/ 2>/dev/null)" ]; then \
		timeout 15 docker run --rm -v "$$(pwd)/volume:/vol" \
			alpine sh -c "rm -rf /vol/* && chmod -R 777 /vol" 2>/dev/null || true; \
	fi
	@$(MAKE) docker-clean

docker-clean:
	@echo "=== Limpiando imágenes dangling ==="
	@docker image prune -f 2>/dev/null || true
	@echo "=== Limpiando build cache ==="
	@docker builder prune -f 2>/dev/null || true
	@echo "=== Docker limpio ==="

free-ports:
	@echo "=== Liberando puerto $(SERVER_PORT) (Gateway) ==="
	@if command -v lsof >/dev/null 2>&1; then \
		timeout 3 lsof -t -n -P -i :$(SERVER_PORT) | xargs timeout 3 kill -9 >/dev/null 2>&1 || true; \
	elif command -v fuser >/dev/null 2>&1; then \
		timeout 3 fuser -k $(SERVER_PORT)/tcp >/dev/null 2>&1 || true; \
	fi
	@echo "=== Liberando puertos de RabbitMQ ==="
	@if command -v lsof >/dev/null 2>&1; then \
		timeout 3 lsof -t -n -P -i :5672 -i :15672 | xargs timeout 3 kill -9 >/dev/null 2>&1 || true; \
	elif command -v fuser >/dev/null 2>&1; then \
		timeout 3 fuser -k 5672/tcp >/dev/null 2>&1 || true; \
		timeout 3 fuser -k 15672/tcp >/dev/null 2>&1 || true; \
	fi

workers-lista:
	@python3 scripts/utils/gestionar_workers.py listar

workers-set:
	@ARGS="$(filter-out $@,$(MAKECMDGOALS))"; \
	if [ -z "$$ARGS" ]; then \
		echo "Uso: make workers-set <worker> <cantidad>"; \
		echo "Ejemplo: make workers-set Q1_PROJECTION 4"; \
		exit 1; \
	fi; \
	python3 scripts/utils/gestionar_workers.py set $$ARGS

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
		$(PYTHON) scripts/utils/ejecutar_solucion_polars.py $$ARGS; \
	fi

