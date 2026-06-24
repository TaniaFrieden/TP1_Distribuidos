include make/config.mk
include make/datasets.mk
include make/helpers.mk
include make/utils.mk
include make/run.mk
include make/tests.mk

.DEFAULT_GOAL := help

.PHONY: help

help:
	@echo "Targets disponibles:"
	@echo ""
	@echo "=== EJECUCIÓN ==="
	@echo "  make start                       - Levanta docker-compose (detached)"
	@echo "  make start --verbose             - Levanta docker-compose con logs en consola"
	@echo "  make down                        - Detiene docker-compose"
	@echo "  make client <trans> <cuentas>    - Corre un cliente"
	@echo "  make log <servicio>              - Muestra logs de un servicio"
	@echo "  make tirar-nodos [seg] [todos|etapa [p]] - Chaos monkey manual"
	@echo ""
	@echo "=== UTILIDADES ==="
	@echo "  make venv                        - Crea el entorno virtual .venv"
	@echo "  make install                     - Instala dependencias en .venv"
	@echo "  make clean                       - Limpia caches, temporales y libera puertos"
	@echo "  make generar <queries>           - Genera docker-compose para queries específicas"
	@echo "  make generar-sample <ds> [%]     - Genera sample de un dataset"
	@echo "  make solucionar <tx> <acc> <dir> - Genera soluciones de referencia"
	@echo ""
	@echo "=== CRASH HOOKS (determinísticos) ==="
	@echo "  make test-crash-worker-pre-confirm [cli] - Crash worker pre-confirmación"
	@echo "  make test-crash-worker-pre-barrera [cli] - Crash worker pre-barrera"
	@echo "  make test-crash-worker-post-flush [etapa]- Crash worker post-flush"
	@echo "  make test-crash-gateway [cli]            - Crash gateway (10 hooks)"
	@echo "  make test-crash-watchdog [cli]           - Crash watchdog (4 hooks)"
	@echo ""
	@echo "=== CAOS (kill externo durante operación) ==="
	@echo "  make test-caos-total [cli]       - Mata todos los workers de golpe"
	@echo "  make test-caos-aleatorio [seg] [cli] - Chaos monkey continuo"
	@echo "  make test-caos-secuencial [seg] [cli]- Igual pero clientes secuenciales"
	@echo "  make test-caos-etapa <pref> [cli]- Mata una etapa específica en loop"
	@echo "  make test-caos-gateway [cli]     - Mata gateway mid-operación"
	@echo "  make test-caos-gateway-resultados- Mata gateway entregando resultados"
	@echo "  make test-caos-cliente [cli]     - Mata un cliente a mitad de envío"
	@echo ""
	@echo "=== SUITES ==="
	@echo "  make test-unit                   - Tests unitarios y de persistencia"
	@echo "  make iterar [N] [tx] [acc] [sol] - N clientes secuenciales sin caos (default 5)"
	@echo "  make test-todos                  - Suite completa (unit + crash + caos)"
	@echo "  make test-todos-multi [N]        - Suite solo multicliente (default 3)"
	@echo "  make test-stress-crash [iter]    - Stress loop de crash hooks"
	@echo "  make test-stress-caos [iter] [cli] - Stress loop de caos"

%:
	@:
