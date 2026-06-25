#!/usr/bin/env bash
set -e
source scripts/tests/test_helpers.sh

ITERACIONES=${1:-50}
CANT_CLIENTES=${2:-2}
ESPERA_ANTES_DE_MATAR=${3:-5}
INTERVALO=${4:-30}
TX=${5:-${TEST_TX:-trans_sample}}
ACC=${6:-${TEST_ACC:-LI-Small_accounts}}
SOLUCIONES=${7:-${TEST_SOL:-sample}}

echo "========================================================="
echo "=== Stress Test: $ITERACIONES iter, $CANT_CLIENTES clientes, intervalo caos ${INTERVALO}s ==="
echo "========================================================="

echo "=== Limpieza completa y arranque limpio ==="
limpiar_y_arrancar

> logs/chaos_monkey_run.log

echo "=== Lanzando Chaos Monkey (persistente durante todas las iteraciones) ==="
echo "Primer ataque en ${ESPERA_ANTES_DE_MATAR}s, intervalo ${INTERVALO}s, modo --todos"
python3 scripts/chaos/chaos_monkey.py "$ESPERA_ANTES_DE_MATAR" "$INTERVALO" --todos >> logs/chaos_monkey_run.log 2>&1 &
CHAOS_PID=$!
trap 'echo "=== Apagando Chaos Monkey (PID: $CHAOS_PID)... ==="; kill $CHAOS_PID 2>/dev/null || true; limpiar_test_global' EXIT

for i in $(seq 1 "$ITERACIONES"); do
    echo ""
    echo ">>> ITERACIÓN $i / $ITERACIONES <<<"

    lanzar_clientes "$CANT_CLIENTES" "$TX" "$ACC"
    esperar_clientes

    echo "=== Comparando resultados (iter $i/$ITERACIONES) contra $SOLUCIONES ==="
    if ! comparar_resultados "$SOLUCIONES"; then
        echo "========================================================="
        echo "❌ ERROR: El test falló en la iteración $i"
        echo "Revisa los logs para ver qué ocurrió."
        echo "========================================================="
        exit 1
    fi
    echo "=== Iteración $i/$ITERACIONES exitosa ==="
done

echo "========================================================="
echo "✅ ÉXITO: Se completaron $ITERACIONES iteraciones sin errores."
echo "========================================================="
