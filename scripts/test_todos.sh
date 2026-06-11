#!/usr/bin/env bash
set -e
source scripts/test_helpers.sh

CANT_CLIENTES=${1:-3}
TX=${2:-HI-Large_Trans_sample_30}
ACC=${3:-HI-Large_accounts}
SOLUCIONES=${4:-Hi-Large-30}
ESPERA_ANTES_DE_MATAR=${5:-5}

lanzar_clientes "$CANT_CLIENTES" "$TX" "$ACC"

echo "=== Esperando ${ESPERA_ANTES_DE_MATAR}s antes de matar todo ==="
sleep "$ESPERA_ANTES_DE_MATAR"

echo "=== Matando todos los workers (excepto rabbitmq) ==="
CONTAINERS=$(docker compose ps --services | grep -v -E '^(rabbitmq)$')
docker kill $(docker compose ps -q $CONTAINERS) 2>/dev/null || true

echo "=== Todo caído. Relevantá manualmente cuando quieras (docker compose up -d) ==="

esperar_clientes
comparar_resultados "$SOLUCIONES"