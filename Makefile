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
	@echo "  make venv                        - Crea el entorno virtual .venv"
	@echo "  make install                     - Instala las dependencias en .venv"
	@echo "  make clean                       - Limpia caches, temporales y libera puertos"
	@echo "  make start                       - Levanta docker-compose en segundo plano (detached)"
	@echo "  make start --verbose             - Levanta docker-compose con logs en consola"
	@echo "  make down                        - Detiene docker-compose"
	@echo "  make log <servicio>              - Muestra logs de un servicio específico"
	@echo "  make client <trans> <cuentas>    - Corre un cliente enviando transacciones y cuentas"
	@echo "  make test-secuencial [cant_cli]  - Corre N clientes de forma secuencial y compara resultados"
	@echo "  make tirar-nodos [segundos]      - Lanza Chaos Monkey aleatorio independiente"
	@echo "  make tirar-nodos [seg] todos     - Matar todos los workers en loop continuo cada [seg]"
	@echo "  make tirar-nodos [seg] etapa     - Matar una etapa al azar en loop continuo cada [seg]"
	@echo "  make tirar-nodos [seg] etapa <p> - Matar etapa específica <p> en loop continuo cada [seg]"
	@echo ""
	@echo "=== CATEGORÍAS DE TESTING ==="
	@echo "  make test-unitarios              - Corre los tests unitarios y de persistencia"
	@echo "  make test-caos-todos [cant_cli]  - Test de caída de todos los workers en simultáneo con clientes"
	@echo "  make test-caos-aleatorio [seg] [cant_cli] - Test de caída continua aleatoria de workers"
	@echo "  make test-caos-secuencial [seg] [cant_cli] - Igual que aleatorio pero clientes secuenciales"
	@echo "  make test-caos-etapa <pref> [cant_cli]  - Test de caída de una etapa específica"
	@echo "  make test-caos-cliente [cant_cli] - Test de caída de un cliente a mitad de envío"
	@echo "  make test-caos-gateway [cant_cli] - Test de caída del gateway"
	@echo "  make test-caos-gateway-resultados- Test de caída del gateway con resultados parciales"
	@echo "  make test-crash-flush [etapa]    - Test del Caso 8 (crash post-flush / pre-barrera)"
	@echo "  make test-crash-caso6 [cant_cli] - Test del Caso 6 (pre-confirmación)"
	@echo "  make test-crash-caso7 [cant_cli] - Test del Caso 7 (pre-barrera)"
	@echo "  make test-crash-leader [cant_cli]- Test de Caída del Líder en Elección"
	@echo "  make test-crash-watchdog-hooks   - Tests de crash hooks del watchdog (topología, detección, elección)"
	@echo "  make test-stress-caos [iter] [cant_cli] - Stress test de caídas masivas en bucle"
	@echo "  make test-stress-crash [iter]    - Stress test de casos de frontera en bucle"
	@echo "  make test-todos                  - Corre toda la suite del sistema (unitarios + crash + caos)"
	@echo "  make test-todos-multi [N]        - Corre solo los tests multicliente con N clientes (default 3)"

# Ignorar argumentos pasados a targets dinámicos
%:
	@:
