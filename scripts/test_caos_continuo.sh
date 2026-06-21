#!/usr/bin/env bash
set -e
source scripts/test_helpers.sh

MIN_CAOS=10
MAX_CAOS=20
CANT_CLIENTES=3
TX="${TEST_TX:-trans_sample}"
ACC="${TEST_ACC:-LI-Small_accounts}"
SOL="${TEST_SOL:-sample}"

EXTRA_ARGS=""

NUMS=()
while [[ $# -gt 0 ]]; do
    case $1 in
        --todos)
            EXTRA_ARGS="$EXTRA_ARGS --todos"
            shift
            ;;
        --etapa)
            EXTRA_ARGS="$EXTRA_ARGS --etapa $2"
            shift 2
            ;;
        *)
            if [[ "$1" =~ ^[0-9]+$ ]]; then
                NUMS+=("$1")
            else
                EXTRA_ARGS="$EXTRA_ARGS $1"
            fi
            shift
            ;;
    esac
done

if [ ${#NUMS[@]} -ge 1 ]; then
    MIN_CAOS=${NUMS[0]}
    MAX_CAOS=${NUMS[0]}
fi
if [ ${#NUMS[@]} -ge 2 ]; then
    MAX_CAOS=${NUMS[1]}
fi
if [ ${#NUMS[@]} -ge 3 ]; then
    CANT_CLIENTES=${NUMS[2]}
fi

docker build -q -t client-image -f src/client/Dockerfile src >/dev/null 2>&1

lanzar_clientes "$CANT_CLIENTES" "$TX" "$ACC"

echo "=== Lanzando Chaos Monkey en segundo plano ==="
echo "Min: ${MIN_CAOS}s | Max: ${MAX_CAOS}s | Extra Args: $EXTRA_ARGS"
python3 scripts/chaos_monkey.py "$MIN_CAOS" "$MAX_CAOS" $EXTRA_ARGS > logs/chaos_monkey_run.log 2>&1 &
CHAOS_PID=$!

trap 'echo "=== Apagando Chaos Monkey... ==="; kill $CHAOS_PID 2>/dev/null || true' EXIT

esperar_clientes

echo "=== Clientes finalizaron. Deteniendo Chaos Monkey. ==="
kill $CHAOS_PID 2>/dev/null || true

comparar_resultados "$SOL"
