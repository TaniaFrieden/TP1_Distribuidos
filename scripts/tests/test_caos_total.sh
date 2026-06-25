#!/usr/bin/env bash
set -e
source scripts/tests/test_helpers.sh

CANT_CLIENTES=${1:-3}
ESPERA_ANTES_DE_MATAR=${2:-5}
INTERVALO=${3:-30}
TX=${4:-${TEST_TX:-trans_sample}}
ACC=${5:-${TEST_ACC:-LI-Small_accounts}}
SOLUCIONES=${6:-${TEST_SOL:-sample}}

preparar_entorno

lanzar_clientes "$CANT_CLIENTES" "$TX" "$ACC"

# Limpiar el log al iniciar este test
> logs/chaos_monkey_run.log

# Usamos el Chaos Monkey unificado para esperar y luego matar todos los workers
python3 scripts/chaos/chaos_monkey.py "$ESPERA_ANTES_DE_MATAR" "$INTERVALO" --todos >> logs/chaos_monkey_run.log 2>&1 &
CHAOS_PID=$!

trap 'echo "=== Apagando Chaos Monkey... ==="; kill $CHAOS_PID 2>/dev/null || true' EXIT

esperar_clientes

echo "=== Clientes finalizaron. Deteniendo Chaos Monkey. ==="
kill $CHAOS_PID 2>/dev/null || true

comparar_resultados "$SOLUCIONES"
