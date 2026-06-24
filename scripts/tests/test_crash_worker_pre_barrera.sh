#!/usr/bin/env bash
set -e
source scripts/tests/test_helpers.sh

CANT_CLIENTES=${1:-1}
TX=${2:-${TEST_TX:-trans_sample}}
ACC=${3:-${TEST_ACC:-LI-Small_accounts}}
SOLUCIONES=${4:-${TEST_SOL:-sample}}
TARGET=${5:-Q5_COUNTER_01}

echo "=== [crash-worker-pre-barrera] Target: $TARGET, $CANT_CLIENTES cliente(s) ==="
make down
timeout 10s docker run --rm -v "$(pwd)/volume:/cleanup" alpine sh -c "rm -rf /cleanup/*" 2>/dev/null \
    || rm -rf volume/* 2>/dev/null || true

echo "=== Levantando sistema con ${TARGET}_CRASH=CRASH_PRE_BARRERA ==="
eval "${TARGET}_CRASH=CRASH_PRE_BARRERA make start"
esperar_sistema_listo

lanzar_clientes "$CANT_CLIENTES" "$TX" "$ACC"
esperar_clientes
comparar_resultados "$SOLUCIONES"

echo "=== [crash-worker-pre-barrera] OK ==="
