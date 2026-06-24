# Tests end-to-end: unitarios, caos, crash, stress, suites

.PHONY: test test-worker-base test-server test-unitarios
.PHONY: test-caos-todos test-caos-aleatorio test-caos-secuencial test-secuencial
.PHONY: test-caos-etapa test-caos-cliente test-caos-gateway test-caos-gateway-resultados
.PHONY: test-crash-flush test-crash-caso6 test-crash-caso7 test-crash-leader
.PHONY: test-crash-gateway-hooks test-crash-watchdog-hooks
.PHONY: test-stress-caos test-stress-crash
.PHONY: test-todos test-todos-multi

# --- UNIT TESTS ---
test:
	./scripts/run_local_tests.sh

test-worker-base:
	$(PYTEST) test/common/worker_base/test_worker_base.py -q

test-server:
	PYTHONPATH=src $(PYTHON) scripts/test_server.py

test-unitarios:
	./scripts/tests/run_local_tests.sh

# --- CAOS / FALLAS END-TO-END ---
test-caos-todos:
	@ARGS="$(filter-out $@,$(MAKECMDGOALS))"; \
	bash scripts/tests/test_todos.sh $$ARGS

test-caos-aleatorio:
	@ARGS="$(filter-out $@,$(MAKECMDGOALS))"; \
	bash scripts/tests/test_caos_continuo.sh $$ARGS

test-caos-secuencial:
	@ARGS="$(filter-out $@,$(MAKECMDGOALS))"; \
	SEQUENTIAL=1 SEQUENTIAL_SOL="$${TEST_SOL:-sample}" bash scripts/tests/test_caos_continuo.sh $$ARGS

test-secuencial:
	@CANT="$(filter-out $@,$(MAKECMDGOALS))"; \
	CANT=$${CANT:-5}; \
	SEQUENTIAL=1 SEQUENTIAL_SOL="$(TEST_SOL)" CANT="$$CANT" bash -c 'source scripts/tests/test_helpers.sh && lanzar_clientes "$$CANT" $(TEST_TX) $(TEST_ACC)'

test-caos-etapa:
	@ARGS="$(filter-out $@,$(MAKECMDGOALS))"; \
	if [ -z "$$ARGS" ]; then \
		echo "Error: Debes especificar el prefix de la etapa."; \
		echo "Uso: make test-caos-etapa <prefix> [cant_clientes] [tx] [acc] [soluciones] [espera|random]"; \
		exit 1; \
	fi; \
	bash scripts/tests/test_etapa.sh $$ARGS

test-caos-cliente:
	@ARGS="$(filter-out $@,$(MAKECMDGOALS))"; \
	bash scripts/tests/test_cliente.sh $$ARGS

test-caos-gateway:
	@ARGS="$(filter-out $@,$(MAKECMDGOALS))"; \
	bash scripts/tests/test_gateway.sh $$ARGS

test-caos-gateway-resultados:
	@ARGS="$(filter-out $@,$(MAKECMDGOALS))"; \
	bash scripts/tests/test_crash_gateway_resultados.sh $$ARGS

# --- CASOS DE FRONTERA / CRITICAL CORNER CASES ---
test-crash-flush:
	@ARGS="$(filter-out $@,$(MAKECMDGOALS))"; \
	bash scripts/tests/test_crash_flush.sh $$ARGS

test-crash-caso6:
	@ARGS="$(filter-out $@,$(MAKECMDGOALS))"; \
	bash scripts/tests/test_crash_caso6.sh $$ARGS

test-crash-caso7:
	@ARGS="$(filter-out $@,$(MAKECMDGOALS))"; \
	bash scripts/tests/test_crash_caso7.sh $$ARGS

test-crash-leader:
	@ARGS="$(filter-out $@,$(MAKECMDGOALS))"; \
	bash scripts/tests/test_crash_leader.sh $$ARGS

test-crash-gateway-hooks:
	@ARGS="$(filter-out $@,$(MAKECMDGOALS))"; \
	bash scripts/tests/test_crash_gateway_hooks.sh $$ARGS

test-crash-watchdog-hooks:
	@ARGS="$(filter-out $@,$(MAKECMDGOALS))"; \
	bash scripts/tests/test_crash_watchdog_hooks.sh $$ARGS

# --- STRESS TESTING (BUCLES) ---
test-stress-caos:
	@ARGS="$(filter-out $@,$(MAKECMDGOALS))"; \
	bash scripts/tests/test_stress_todos.sh $$ARGS

test-stress-crash:
	@ARGS="$(filter-out $@,$(MAKECMDGOALS))"; \
	bash scripts/tests/test_stress_crash.sh $$ARGS

# --- TEST TODOS MULTI (solo tests multicliente) ---
test-todos-multi:
	@N="$(filter-out $@,$(MAKECMDGOALS))"; \
	N=$${N:-3}; \
	echo "========================================================="; \
	echo "=== TEST MULTICLIENTE ($$N clientes) ==="; \
	echo "========================================================="; \
	$(_full_clean); \
	$(_start_env); \
	echo "--- 1/6. Caos cliente ($$N clientes) ---"; \
	$(MAKE) test-caos-cliente $$N $(TEST_TX) $(TEST_ACC) $(TEST_SOL); \
	$(_light_clean); \
	$(_start_env); \
	echo "--- 2/6. Caos gateway ($$N clientes) ---"; \
	$(MAKE) test-caos-gateway $$N $(TEST_TX) $(TEST_ACC) $(TEST_SOL); \
	$(_light_clean); \
	$(_start_env); \
	echo "--- 3/6. Caos aleatorio ($$N clientes) ---"; \
	$(MAKE) test-caos-aleatorio 70 $$N; \
	$(_light_clean); \
	$(_start_env); \
	echo "--- 4/6. Caos todos los workers ($$N clientes) ---"; \
	$(MAKE) test-caos-todos $$N $(TEST_TX) $(TEST_ACC) $(TEST_SOL) 75; \
	$(_light_clean); \
	$(_start_env); \
	echo "--- 5/6. Caos gateway con resultados ($$N clientes) ---"; \
	$(MAKE) test-caos-gateway-resultados $(TEST_TX) $(TEST_ACC) $(TEST_SOL); \
	$(_light_clean); \
	$(_start_env); \
	echo "--- 6/6. Stress caos (2 iter, $$N clientes) ---"; \
	$(MAKE) test-stress-caos 2 $$N $(TEST_TX) $(TEST_ACC) $(TEST_SOL) 70; \
	echo "========================================================="; \
	echo "  Todos los tests multicliente pasaron ($$N clientes)"; \
	echo "========================================================="

# --- TEST TODOS ---
test-todos:
	@$(_full_clean)
	@echo "========================================================="
	@echo "=== 1. Ejecutando tests unitarios y de persistencia ==="
	@echo "========================================================="
	@$(MAKE) test-unitarios
	@echo "========================================================="
	@echo "=== 2. Ejecutando tests de crash hooks del watchdog ==="
	@echo "========================================================="
	@$(MAKE) test-crash-watchdog-hooks 1 $(TEST_TX) $(TEST_ACC) $(TEST_SOL)
	@echo "========================================================="
	@echo "=== 3. Ejecutando test de caidas en frio (caso 6) ==="
	@echo "========================================================="
	@$(MAKE) test-crash-caso6 1 $(TEST_TX) $(TEST_ACC) $(TEST_SOL)
	@echo "========================================================="
	@echo "=== 4. Ejecutando test de caidas en frio (caso 7) ==="
	@echo "========================================================="
	@$(MAKE) test-crash-caso7 1 $(TEST_TX) $(TEST_ACC) $(TEST_SOL)
	@echo "========================================================="
	@echo "=== 5. Ejecutando test de caida de lider de eleccion ==="
	@echo "========================================================="
	@$(MAKE) test-crash-leader 1 $(TEST_TX) $(TEST_ACC) $(TEST_SOL)
	@echo "========================================================="
	@echo "=== 6. Ejecutando test crash flush (caso 8) ==="
	@echo "========================================================="
	@$(_full_clean)
	@$(MAKE) test-crash-flush counter $(TEST_TX) $(TEST_ACC) $(TEST_SOL)
	@echo "========================================================="
	@echo "=== 7-13. Ejecutando tests de caos (entorno compartido) ==="
	@echo "========================================================="
	@$(_full_clean)
	@$(_start_env)
	@echo "--- 7. Caos etapa: q2_agregador_shard ---"
	@$(MAKE) test-caos-etapa q2_agregador_shard 1 $(TEST_TX) $(TEST_ACC) $(TEST_SOL) 70
	@$(_light_clean)
	@$(_start_env)
	@echo "--- 8. Caos etapa: q4_sumador ---"
	@$(MAKE) test-caos-etapa q4_sumador 1 $(TEST_TX) $(TEST_ACC) $(TEST_SOL) 70
	@$(_light_clean)
	@$(_start_env)
	@echo "--- 9. Caos etapa: q3_format_shard ---"
	@$(MAKE) test-caos-etapa q3_format_shard 1 $(TEST_TX) $(TEST_ACC) $(TEST_SOL) 70
	@$(_light_clean)
	@$(_start_env)
	@echo "--- 10. Caos cliente ---"
	@$(MAKE) test-caos-cliente 2 $(TEST_TX) $(TEST_ACC) $(TEST_SOL)
	@$(_light_clean)
	@$(_start_env)
	@echo "--- 11. Caos gateway ---"
	@$(MAKE) test-caos-gateway 2 $(TEST_TX) $(TEST_ACC) $(TEST_SOL)
	@$(_light_clean)
	@$(_start_env)
	@echo "--- 12. Caos aleatorio ---"
	@$(MAKE) test-caos-aleatorio 70 2
	@$(_full_clean)
	@$(_start_env)
	@echo "--- 13. Caos total (todos los workers) ---"
	@$(MAKE) test-caos-todos 2 $(TEST_TX) $(TEST_ACC) $(TEST_SOL) 75
	@echo "========================================================="
	@echo "=== 14. Ejecutando tests de crash del gateway (hooks) ==="
	@echo "========================================================="
	@$(MAKE) test-crash-gateway-hooks 1 $(TEST_TX) $(TEST_ACC) $(TEST_SOL)
	@echo "========================================================="
	@echo "=== 15. Ejecutando test caos gateway con resultados ==="
	@echo "========================================================="
	@$(MAKE) test-caos-gateway-resultados $(TEST_TX) $(TEST_ACC) $(TEST_SOL)
	@echo "========================================================="
	@echo "=== 16. Ejecutando stress test crash (2 iteraciones) ==="
	@echo "========================================================="
	@$(MAKE) test-stress-crash 2 1 $(TEST_TX) $(TEST_ACC) $(TEST_SOL)
	@echo "========================================================="
	@echo "=== 17. Ejecutando stress test caos (2 iteraciones) ==="
	@echo "========================================================="
	@$(_light_clean)
	@$(MAKE) test-stress-caos 2 2 $(TEST_TX) $(TEST_ACC) $(TEST_SOL) 70
	@echo "========================================================="
	@echo "  Todos los tests del sistema pasaron exitosamente"
	@echo "========================================================="
