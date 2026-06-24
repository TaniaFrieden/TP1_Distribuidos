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
	@echo "  make start                         Levanta docker-compose (detached, con build)"
	@echo "  make down                          Detiene docker-compose"
	@echo "  make client [trans] [cuentas]      Corre un cliente"
	@echo "  make log <servicio>                Muestra logs de un servicio"
	@echo "  make tirar-nodos [seg] [todos|etapa [p]]  Chaos monkey manual"
	@echo ""
	@echo "=== UTILIDADES ==="
	@echo "  make venv                          Crea el entorno virtual .venv"
	@echo "  make install                       Instala dependencias en .venv"
	@echo "  make clean                         Limpia caches, temporales y libera puertos"
	@echo "  make generar <queries>             Genera docker-compose (ej: make generar 5, make generar 1 2 3 4 5)"
	@echo "  make generar-sample <ds> [%]       Genera sample de un dataset"
	@echo "  make solucionar <tx> <acc> <dir>   Genera soluciones de referencia"
	@echo ""
	@echo "=== CRASH HOOKS (determinísticos, por instancia) ==="
	@echo "  Cada worker recibe CRASH_HOOK via <INSTANCIA>_CRASH=<hook> make start"
	@echo "  Hooks: CRASH_BEFORE_DATA_ACK, CRASH_PRE_BARRERA, CRASH_BEFORE_FINISHED_CONFIRMATION,"
	@echo "         CRASH_AFTER_FLUSH, CRASH_BEFORE_EOF_FORWARD"
	@echo "  Ejemplo: Q5_FILTER_PERIOD_01_CRASH=CRASH_PRE_BARRERA make start"
	@echo ""
	@echo "  make test-crash-worker-pre-confirm [cli] [tx] [acc] [sol] [TARGET]"
	@echo "  make test-crash-worker-pre-barrera [cli] [tx] [acc] [sol] [TARGET]"
	@echo "  make test-crash-worker-post-flush [etapa] [cli] [tx] [acc] [sol]"
	@echo "  make test-crash-agregador-pending-acks [cli]"
	@echo "  make test-crash-gateway [cli]       Crash gateway (10 hooks)"
	@echo "  make test-crash-watchdog [cli]       Crash watchdog"
	@echo "  make test-crash-pipeline             Todos los hooks x todas las etapas x 1,2 clientes"
	@echo ""
	@echo "=== CAOS (kill externo durante operación) ==="
	@echo "  make test-caos-total [cli] [espera]  Mata todos los workers de golpe"
	@echo "  make test-caos-aleatorio [seg] [cli] Chaos monkey continuo"
	@echo "  make test-caos-secuencial [seg] [cli] Igual pero clientes secuenciales"
	@echo "  make test-caos-etapa <pref> [cli]    Mata una etapa específica en loop"
	@echo "  make test-caos-gateway [cli]         Mata gateway mid-operación"
	@echo "  make test-caos-gateway-resultados    Mata gateway entregando resultados"
	@echo "  make test-caos-cliente [cli]         Mata un cliente a mitad de envío"
	@echo ""
	@echo "=== SUITES ==="
	@echo "  make test-unit                       Tests unitarios y de persistencia"
	@echo "  make iterar [N] [tx] [acc] [sol]     N clientes secuenciales sin caos (default 5)"
	@echo "  make test-todos                      Suite completa (unit + crash + caos, dinámico según compose)"
	@echo "  make test-todos-multi [N]            Suite solo multicliente (default 3)"
	@echo "  make test-stress-crash [iter]        Stress loop de crash hooks"
	@echo "  make test-stress-caos [iter] [cli]   Stress loop de caos"

%:
	@:
