# Tests end-to-end: unitarios, crash hooks, caos, suites

# ─── UNITARIOS ───
.PHONY: test-unit test-worker-base test-server

test-unit:
	./scripts/tests/run_local_tests.sh

test-worker-base:
	$(PYTEST) test/common/worker_base/test_worker_base.py -q

test-server:
	PYTHONPATH=src $(PYTHON) scripts/test_server.py

# ─── CRASH HOOKS (determinísticos, 1 crash inyectado) ───
.PHONY: test-crash-worker-pre-confirm test-crash-worker-pre-barrera test-crash-worker-post-flush
.PHONY: test-crash-gateway test-crash-watchdog

test-crash-worker-pre-confirm:
	@ARGS="$(filter-out $@,$(MAKECMDGOALS))"; \
	bash scripts/tests/test_crash_worker_pre_confirm.sh $$ARGS

test-crash-worker-pre-barrera:
	@ARGS="$(filter-out $@,$(MAKECMDGOALS))"; \
	bash scripts/tests/test_crash_worker_pre_barrera.sh $$ARGS

test-crash-worker-post-flush:
	@ARGS="$(filter-out $@,$(MAKECMDGOALS))"; \
	bash scripts/tests/test_crash_worker_post_flush.sh $$ARGS

test-crash-gateway:
	@ARGS="$(filter-out $@,$(MAKECMDGOALS))"; \
	bash scripts/tests/test_crash_gateway.sh $$ARGS

test-crash-watchdog:
	@ARGS="$(filter-out $@,$(MAKECMDGOALS))"; \
	bash scripts/tests/test_crash_watchdog.sh $$ARGS

# ─── CAOS (kill externo durante operación) ───
.PHONY: iterar test-caos-etapa test-caos-total test-caos-aleatorio test-caos-secuencial
.PHONY: test-caos-gateway test-caos-gateway-resultados test-caos-cliente

test-caos-etapa:
	@ARGS="$(filter-out $@,$(MAKECMDGOALS))"; \
	if [ -z "$$ARGS" ]; then \
		echo "Error: Debes especificar el prefix de la etapa."; \
		echo "Uso: make test-caos-etapa <prefix> [cant_clientes] [tx] [acc] [soluciones] [espera|random]"; \
		exit 1; \
	fi; \
	bash scripts/tests/test_etapa.sh $$ARGS

test-caos-total:
	@ARGS="$(filter-out $@,$(MAKECMDGOALS))"; \
	bash scripts/tests/test_caos_total.sh $$ARGS

test-caos-aleatorio:
	@ARGS="$(filter-out $@,$(MAKECMDGOALS))"; \
	bash scripts/tests/test_caos_continuo.sh $$ARGS

test-caos-secuencial:
	@ARGS="$(filter-out $@,$(MAKECMDGOALS))"; \
	SEQUENTIAL=1 SEQUENTIAL_SOL="$${TEST_SOL:-sample}" bash scripts/tests/test_caos_continuo.sh $$ARGS

iterar:
	@ARGS="$(filter-out $@,$(MAKECMDGOALS))"; \
	set -- $$ARGS; \
	CANT=$${1:-5}; \
	TX=$${2:-$(TEST_TX)}; \
	ACC=$${3:-$(TEST_ACC)}; \
	SOL=$${4:-$(TEST_SOL)}; \
	export SEQUENTIAL=1 SEQUENTIAL_SOL="$$SOL"; \
	source scripts/tests/test_helpers.sh && lanzar_clientes "$$CANT" "$$TX" "$$ACC"

test-caos-gateway:
	@ARGS="$(filter-out $@,$(MAKECMDGOALS))"; \
	bash scripts/tests/test_gateway.sh $$ARGS

test-caos-gateway-resultados:
	@ARGS="$(filter-out $@,$(MAKECMDGOALS))"; \
	bash scripts/tests/test_crash_gateway_resultados.sh $$ARGS

test-caos-cliente:
	@ARGS="$(filter-out $@,$(MAKECMDGOALS))"; \
	bash scripts/tests/test_cliente.sh $$ARGS

# ─── SUITES (corren muchos juntos) ───
.PHONY: test-todos test-todos-multi test-stress-crash test-stress-caos

test-stress-crash:
	@ARGS="$(filter-out $@,$(MAKECMDGOALS))"; \
	bash scripts/tests/test_stress_crash.sh $$ARGS

test-stress-caos:
	@ARGS="$(filter-out $@,$(MAKECMDGOALS))"; \
	bash scripts/tests/test_stress_todos.sh $$ARGS

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
	echo "--- 4/6. Caos total ($$N clientes) ---"; \
	$(MAKE) test-caos-total $$N $(TEST_TX) $(TEST_ACC) $(TEST_SOL) 75; \
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

test-todos:
	@$(_full_clean)
	@echo "========================================================="
	@echo "=== 1. Tests unitarios ==="
	@echo "========================================================="
	@$(MAKE) test-unit
	@echo "========================================================="
	@echo "=== 2. Crash watchdog (hooks) ==="
	@echo "========================================================="
	@$(MAKE) test-crash-watchdog 1 $(TEST_TX) $(TEST_ACC) $(TEST_SOL)
	@echo "========================================================="
	@echo "=== 3. Crash worker pre-confirmación ==="
	@echo "========================================================="
	@$(MAKE) test-crash-worker-pre-confirm 1 $(TEST_TX) $(TEST_ACC) $(TEST_SOL)
	@echo "========================================================="
	@echo "=== 4. Crash worker pre-barrera ==="
	@echo "========================================================="
	@$(MAKE) test-crash-worker-pre-barrera 1 $(TEST_TX) $(TEST_ACC) $(TEST_SOL)
	@echo "========================================================="
	@echo "=== 5. Crash worker post-flush ==="
	@echo "========================================================="
	@$(_full_clean)
	@$(MAKE) test-crash-worker-post-flush counter 1 $(TEST_TX) $(TEST_ACC) $(TEST_SOL)
	@echo "========================================================="
	@echo "=== 6. Crash gateway (hooks) ==="
	@echo "========================================================="
	@$(MAKE) test-crash-gateway 1 $(TEST_TX) $(TEST_ACC) $(TEST_SOL)
	@echo "========================================================="
	@echo "=== 7-12. Tests de caos ==="
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
	@echo "--- 13. Caos total ---"
	@$(MAKE) test-caos-total 2 $(TEST_TX) $(TEST_ACC) $(TEST_SOL) 75
	@echo "========================================================="
	@echo "=== 14. Caos gateway con resultados ==="
	@echo "========================================================="
	@$(MAKE) test-caos-gateway-resultados $(TEST_TX) $(TEST_ACC) $(TEST_SOL)
	@echo "========================================================="
	@echo "=== 15. Stress crash (2 iteraciones) ==="
	@echo "========================================================="
	@$(MAKE) test-stress-crash 2 1 $(TEST_TX) $(TEST_ACC) $(TEST_SOL)
	@echo "========================================================="
	@echo "=== 16. Stress caos (2 iteraciones) ==="
	@echo "========================================================="
	@$(_light_clean)
	@$(MAKE) test-stress-caos 2 2 $(TEST_TX) $(TEST_ACC) $(TEST_SOL) 70
	@echo "========================================================="
	@echo "  Todos los tests pasaron exitosamente"
	@echo "========================================================="
