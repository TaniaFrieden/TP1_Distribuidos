#!/usr/bin/env bash
# scripts/test_kill_gateway.sh
set -e
source scripts/test_helpers.sh

CANT_CLIENTES=${1:-3}
TX=${2:-${TEST_TX:-trans_sample}}
ACC=${3:-${TEST_ACC:-LI-Small_accounts}}
SOLUCIONES=${4:-${TEST_SOL:-sample}}
ESPERA_ANTES_DE_MATAR=${5:-3}

lanzar_clientes "$CANT_CLIENTES" "$TX" "$ACC"

echo "=== Esperando ${ESPERA_ANTES_DE_MATAR}s antes de matar gateway ==="
sleep "$ESPERA_ANTES_DE_MATAR"

echo "=== Matando gateway ==="
docker kill gateway_01

echo "=== Gateway caído. La recuperación queda a cargo de la solución que implementen ==="
echo "(réplica activa, restart manual, etc. - este script no hace nada más)"

esperar_clientes
comparar_resultados "$SOLUCIONES"