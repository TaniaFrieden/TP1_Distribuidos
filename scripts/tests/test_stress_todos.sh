#!/usr/bin/env bash
set -e

ITERACIONES=${1:-50}
CANT_CLIENTES=${2:-2}
ESPERA_ANTES_DE_MATAR=${3:-75}
TX=${4:-${TEST_TX:-trans_sample}}
ACC=${5:-${TEST_ACC:-LI-Small_accounts}}
SOLUCIONES=${6:-${TEST_SOL:-sample}}

echo "========================================================="
echo "=== Iniciando Stress Test: $ITERACIONES iteraciones de test_todos ($CANT_CLIENTES clientes) ==="
echo "========================================================="

for i in $(seq 1 "$ITERACIONES"); do
    echo ""
    echo ">>> ITERACIÓN $i / $ITERACIONES <<<"

    if ! bash scripts/tests/test_caos_total.sh "$CANT_CLIENTES" "$ESPERA_ANTES_DE_MATAR" "$TX" "$ACC" "$SOLUCIONES"; then
        echo "========================================================="
        echo "❌ ERROR: El test falló en la iteración $i"
        echo "Revisa los logs para ver qué ocurrió."
        echo "========================================================="
        exit 1
    fi
done

echo "========================================================="
echo "✅ ÉXITO: Se completaron $ITERACIONES iteraciones sin errores."
echo "========================================================="
