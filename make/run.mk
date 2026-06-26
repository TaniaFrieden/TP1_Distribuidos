# Targets de ejecucion manual (levantar, bajar, correr cliente, gateway, etc.)

.PHONY: build start down cliente run-clients gateway log caos tirar-nodos

build:
	@mkdir -p logs
	@echo "=== Buildeando imágenes... ==="
	@$(DOCKER_COMPOSE) build > logs/run_build.log 2>&1
	@echo "=== Build completado ==="

start:
	@mkdir -p logs
	@if [ "$(START_VERBOSE)" = "1" ]; then \
		$(DOCKER_COMPOSE) up -d; \
		$(DOCKER_COMPOSE) logs -f; \
	else \
		$(DOCKER_COMPOSE) up -d > logs/run_start_env.log 2>&1; \
	fi

rebuild:
	@mkdir -p logs
	@if [ "$(START_VERBOSE)" = "1" ]; then \
		$(DOCKER_COMPOSE) up -d --build; \
		$(DOCKER_COMPOSE) logs -f; \
	else \
		$(DOCKER_COMPOSE) up -d --build > logs/run_start_env.log 2>&1; \
	fi

down:
	@mkdir -p logs
	-@docker rm -f $$(docker ps -a -q --filter "name=client_") 2>/dev/null || true
	@$(DOCKER_COMPOSE) down > logs/run_clean_full.log 2>&1 || true

cliente:
	@ARGS="$(filter-out $@,$(MAKECMDGOALS))"; \
	TX=$$(echo $$ARGS | cut -d' ' -f1); \
	ACC=$$(echo $$ARGS | cut -d' ' -f2); \
	SOL=$$(echo $$ARGS | cut -d' ' -f3); \
	TX_FILE=$${TRANSACTIONS_FILE:-$${TX:-$(TEST_TX)}}; \
	ACC_FILE=$${ACCOUNTS_FILE:-$${ACC:-$(TEST_ACC)}}; \
	SOL_DIR=$${SOL:-$(TEST_SOL)}; \
	CLIENT_SUFFIX=$$(date +%s%N); \
	CLIENT_ID_SUFFIX=$${CLIENT_ID_SUFFIX:-$$CLIENT_SUFFIX}; \
	mkdir -p "$(OUTPUT_DIR)" && \
	docker build -q -t tp1-client -f src/client/Dockerfile src >/dev/null 2>&1 && \
	TTY_FLAG=""; [ -t 0 ] && TTY_FLAG="-t"; \
	docker run --rm -i $$TTY_FLAG \
		--name client_$$CLIENT_SUFFIX \
		--network host \
		--user "$$(id -u):$$(id -g)" \
		-v "$(shell pwd)/datasets:/app/datasets" \
		-v "$(shell pwd)/$(OUTPUT_DIR):/app/$(OUTPUT_DIR)" \
		-e LOG_FILE="/app/$(OUTPUT_DIR)/client_$$CLIENT_SUFFIX.log" \
		-e OUTPUT_APPEND_HOSTNAME="false" \
		-e CLIENT_ID_SUFFIX="$$CLIENT_ID_SUFFIX" \
		-e TRANSACTIONS_FILE="$$TX_FILE" \
		-e ACCOUNTS_FILE="$$ACC_FILE" \
		-e OUTPUT_DIR="$(OUTPUT_DIR)" \
		-e SERVER_HOST="$(SERVER_HOST)" \
		-e SERVER_PORT="$(SERVER_PORT)" \
		-e BATCH_SIZE="$(BATCH_SIZE)" \
		-e PROGRESS_BAR="$${PROGRESS_BAR:-1}" \
		tp1-client && \
	LAST_DIR=$$(ls -td $(OUTPUT_DIR)/*/ 2>/dev/null | head -1); \
	if [ -z "$$LAST_DIR" ]; then \
		echo "No se encontró output del cliente"; \
		exit 1; \
	fi; \
	echo ""; \
	echo "=== Comparando resultados ($$LAST_DIR vs solutions/$$SOL_DIR) ==="; \
	$(PYTHON) scripts/utils/comparar_datasets.py "$$LAST_DIR" "solutions/$$SOL_DIR"; \
	CMP_RC=$$?; \
	rm -f "$(OUTPUT_DIR)/client_id_$$CLIENT_ID_SUFFIX.txt"; \
	exit $$CMP_RC

run-clients:
	$(DOCKER_COMPOSE) --profile clients up --build --scale client=$(SCALE)

gateway:
	@if command -v fuser >/dev/null 2>&1; then fuser -k $(SERVER_PORT)/tcp >/dev/null 2>&1 || true; fi
	SERVER_HOST=$(SERVER_HOST) \
	SERVER_PORT=$(SERVER_PORT) \
	MOM_HOST=$(MOM_HOST) \
	INPUT_QUEUE=$(INPUT_QUEUE) \
	OUTPUT_QUEUE=$(OUTPUT_QUEUE) \
	PYTHONPATH=src $(PYTHON) src/gateway/gateway.py

log:
	@if [ -z "$(filter-out $@,$(MAKECMDGOALS))" ]; then \
		echo "Error: Debes especificar el nombre del servicio."; \
		echo "Ejemplo: make log gateway"; \
		exit 1; \
	fi
	$(DOCKER_COMPOSE) logs -f $(filter-out $@,$(MAKECMDGOALS))

caos:
	@mkdir -p logs
	@ARGS="$(filter-out $@,$(MAKECMDGOALS))"; \
	bash scripts/tests/test_caos_continuo.sh $$ARGS

tirar-nodos:
	@ARGS="$(filter-out $@,$(MAKECMDGOALS))"; \
	if [ -z "$$ARGS" ]; then \
		python3 scripts/chaos/chaos_monkey.py 10; \
	else \
		CMD_ARGS=""; \
		FOR_PY=""; \
		for arg in $$ARGS; do \
			if [ "$$arg" = "todos" ]; then \
				FOR_PY="$$FOR_PY --todos"; \
			elif [ "$$arg" = "etapa" ]; then \
				FOR_PY="$$FOR_PY --etapa"; \
			else \
				FOR_PY="$$FOR_PY $$arg"; \
			fi; \
		done; \
		python3 scripts/chaos/chaos_monkey.py $$FOR_PY; \
	fi
