#!/usr/bin/env bash
set -e
source scripts/test_helpers.sh

CANT_CLIENTES=${1:-3}
TX=${2:-LI-Small_Trans}
ACC=${3:-LI-Small_accounts}
SOLUCIONES=${4:-small}
ESPERA_ANTES_DE_MATAR=${5:-5}

lanzar_clientes "$CANT_CLIENTES" "$TX" "$ACC"

echo "=== Esperando ${ESPERA_ANTES_DE_MATAR}s antes de matar todo ==="
sleep "$ESPERA_ANTES_DE_MATAR"

echo "=== Matando solo los workers (rabbitmq, gateway, client, watchdogs y actuadores quedan vivos) ==="
CONTAINERS=$(docker compose ps --services | grep -v -E '^(rabbitmq|gateway|client|watchdog_[0-9]+|actuador_[0-9]+)$')
docker kill $(docker compose ps -q $CONTAINERS) 2>/dev/null || true

echo "=== Todo caído. Relevantá manualmente cuando quieras (docker compose up -d) ==="

esperar_clientes
comparar_resultados "$SOLUCIONES"