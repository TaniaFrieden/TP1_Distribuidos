#!/usr/bin/env bash
set -e
source scripts/test_helpers.sh

PREFIX=$1
CANT_CLIENTES=${2:-3}
TX=${3:-HI-Large_Trans_sample_30}
ACC=${4:-HI-Large_accounts}
SOLUCIONES=${5:-Hi-Large-30}
ESPERA_PARAM=${6:-"random"}

if [ -z "$PREFIX" ]; then
    echo "Uso: $0 <prefix_etapa> [cant_clientes] [tx] [acc] [soluciones] [espera|random]"
    exit 1
fi

if [ "$ESPERA_PARAM" = "random" ]; then
    MIN_ESPERA=2
    MAX_ESPERA=15
    # Pasamos el rango al Chaos Monkey
    ESPERA_ARG="$MIN_ESPERA $MAX_ESPERA"
else
    ESPERA_ARG="$ESPERA_PARAM"
fi

lanzar_clientes "$CANT_CLIENTES" "$TX" "$ACC"

# Usamos el Chaos Monkey unificado para esperar y matar la etapa
python3 scripts/chaos_monkey.py $ESPERA_ARG --etapa "$PREFIX"

esperar_clientes
comparar_resultados "$SOLUCIONES"
