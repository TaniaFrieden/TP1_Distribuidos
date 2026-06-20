#!/usr/bin/env bash
# scripts/test_kill_gateway.sh
set -e
source scripts/test_helpers.sh

CANT_CLIENTES=${1:-3}
TX=${2:-HI-Large_Trans_sample_30}
ACC=${3:-HI-Large_accounts}
SOLUCIONES=${4:-Hi-Large-30}
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