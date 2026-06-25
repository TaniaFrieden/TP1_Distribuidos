#!/usr/bin/env bash
set -euo pipefail

TX_FILE="${TRANSACTIONS_FILE:-$1}"
ACC_FILE="${ACCOUNTS_FILE:-${2:-}}"
SOL_DIR="${3:-${TEST_SOL:-sample}}"
OUT_DIR="${OUTPUT_DIR:-output}"
CLIENT_SUFFIX=$(date +%s%N)

mkdir -p "$OUT_DIR"
docker build -q -t client-image -f src/client/Dockerfile src >/dev/null 2>&1

CONTAINER="client_$CLIENT_SUFFIX"
trap 'docker kill "$CONTAINER" 2>/dev/null || true' INT TERM

docker run --rm --init \
    --name "$CONTAINER" \
    --network host \
    --user "$(id -u):$(id -g)" \
    -v "$(pwd)/datasets:/app/datasets" \
    -v "$(pwd)/$OUT_DIR:/app/$OUT_DIR" \
    -v "$(pwd)/logs:/app/logs" \
    -e LOG_FILE="/app/logs/client_$CLIENT_SUFFIX.txt" \
    -e OUTPUT_APPEND_HOSTNAME="false" \
    -e CLIENT_ID_SUFFIX="${CLIENT_ID_SUFFIX:-}" \
    -e TRANSACTIONS_FILE="$TX_FILE" \
    -e ACCOUNTS_FILE="$ACC_FILE" \
    -e OUTPUT_DIR="$OUT_DIR" \
    -e SERVER_HOST="${SERVER_HOST:-localhost}" \
    -e SERVER_PORT="${SERVER_PORT:-12345}" \
    -e BATCH_SIZE="${BATCH_SIZE:-1000}" \
    -e PROGRESS_BAR="${PROGRESS_BAR:-0}" \
    -e PYTHONUNBUFFERED=1 \
    client-image

LAST_DIR=$(ls -td "$OUT_DIR"/*/ 2>/dev/null | head -1)
if [ -n "$LAST_DIR" ] && [ -d "solutions/$SOL_DIR" ]; then
    .venv/bin/python scripts/utils/comparar_datasets.py "$LAST_DIR" "solutions/$SOL_DIR"
fi
