# Helpers internos para test-todos (silencian docker y guardan logs en logs/run_*.log)
_kill_zombies = docker rm -f $$(docker ps -a -q --filter "name=client_") 2>/dev/null; true

_full_clean = { $(_kill_zombies); $(DOCKER_COMPOSE) down -v --remove-orphans; \
	docker run --rm -v "$$(pwd)/volume:/vol" -v "$$(pwd)/output:/out" \
		alpine sh -c "rm -rf /vol/* /out/*/ /out/client_id*.txt && chmod -R 777 /vol /out"; } > logs/run_clean_full.log 2>&1

_light_clean = { $(DOCKER_COMPOSE) down --remove-orphans; \
	docker run --rm -v "$$(pwd)/output:/out" \
		alpine sh -c "rm -rf /out/*/ /out/client_id*.txt && chmod -R 777 /out"; } > logs/run_clean_light.log 2>&1

_start_env = { $(DOCKER_COMPOSE) up -d && \
	ESPERADOS=$$($(DOCKER_COMPOSE) config --services | wc -l); \
	for i in $$(seq 1 60); do \
		CORRIENDO=$$($(DOCKER_COMPOSE) ps --status running --format '{{.Name}}' | wc -l); \
		[ "$$CORRIENDO" -ge "$$ESPERADOS" ] && break; \
		sleep 2; \
	done; } > logs/run_start_env.log 2>&1
