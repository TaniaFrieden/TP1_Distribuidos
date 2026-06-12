#!/usr/bin/env bash
set -e
source scripts/test_helpers.sh

PREFIX=$1
CANT_CLIENTES=${2:-3}
TX=${3:-HI-Large_Trans_sample_30}
ACC=${4:-HI-Large_accounts}
SOLUCIONES=${5:-Hi-Large-30}
ESPERA_PARAM=${6:-"random"}

if [ "$ESPERA_PARAM" = "random" ]; then
    MIN_ESPERA=2
    MAX_ESPERA=15
    ESPERA_ANTES_DE_MATAR=$((MIN_ESPERA + RANDOM % (MAX_ESPERA - MIN_ESPERA + 1)))
else
    ESPERA_ANTES_DE_MATAR=$ESPERA_PARAM
fi

if [ -z "$PREFIX" ]; then
    echo "Uso: $0 <prefix_etapa> [cant_clientes] [tx] [acc] [soluciones] [espera|random]"
    exit 1
fi

lanzar_clientes "$CANT_CLIENTES" "$TX" "$ACC"

echo "=== Esperando tiempo al azar de ${ESPERA_ANTES_DE_MATAR}s antes de matar etapa $PREFIX ==="
sleep "$ESPERA_ANTES_DE_MATAR"

NODOS=$(docker ps --format '{{.Names}}' | grep -E "^${PREFIX}_[0-9]+$" || true)
if [ -z "$NODOS" ]; then
    echo "No se encontraron contenedores con prefix '$PREFIX'"
    exit 1
fi

echo "=== Matando: $NODOS ==="
docker kill $NODOS

echo "=== Etapa caída. Relevantala manualmente cuando quieras (docker compose up -d $NODOS) ==="

esperar_clientes
comparar_resultados "$SOLUCIONES"