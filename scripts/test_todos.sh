#!/usr/bin/env bash
set -e
source scripts/test_helpers.sh

CANT_CLIENTES=${1:-3}
TX=${2:-${TEST_TX:-trans_sample}}
ACC=${3:-${TEST_ACC:-LI-Small_accounts}}
SOLUCIONES=${4:-${TEST_SOL:-sample}}
ESPERA_ANTES_DE_MATAR=${5:-5}

docker build -q -t client-image -f src/client/Dockerfile src >/dev/null 2>&1

lanzar_clientes "$CANT_CLIENTES" "$TX" "$ACC"

# Usamos el Chaos Monkey unificado para esperar y luego matar todos los workers
python3 scripts/chaos_monkey.py "$ESPERA_ANTES_DE_MATAR" --todos

esperar_clientes
comparar_resultados "$SOLUCIONES"
