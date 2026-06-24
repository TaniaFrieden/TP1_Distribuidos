#!/usr/bin/env bash
# Verifica que un crash post-flush no produce resultados duplicados.
#
# Uso:
#   bash scripts/tests/test_crash_worker_post_flush.sh [etapa] [cli] [tx] [acc] [sol]
#
set -e
source scripts/tests/test_helpers.sh

ETAPA=${1:-counter}
CANT_CLIENTES=${2:-1}
TX=${3:-${TEST_TX:-trans_sample}}
ACC=${4:-${TEST_ACC:-LI-Small_accounts}}
SOLUCIONES=${5:-${TEST_SOL:-sample}}

# Buscar la primera instancia de la etapa para inyectar crash
TARGET=$(grep -oP '\$\{\K[A-Z0-9_]+(?=_CRASH:-)' docker-compose.yml | grep -i "$(echo "$ETAPA" | tr '-' '_')" | head -1)
if [ -z "$TARGET" ]; then
    echo "[ERROR] No se encontró instancia para etapa '$ETAPA' en docker-compose.yml"
    exit 1
fi

echo "=== [crash-worker-post-flush] etapa '$ETAPA', target $TARGET, $CANT_CLIENTES cliente(s) ==="

make down
timeout 10s docker run --rm -v "$(pwd)/volume:/cleanup" alpine sh -c "rm -rf /cleanup/*" 2>/dev/null \
    || rm -rf volume/* 2>/dev/null || true

echo "=== Levantando sistema con ${TARGET}_CRASH=CRASH_AFTER_FLUSH ==="
eval "${TARGET}_CRASH=CRASH_AFTER_FLUSH make start"
esperar_sistema_listo

lanzar_clientes "$CANT_CLIENTES" "$TX" "$ACC"
esperar_clientes

echo "=== Verificando que el hook se activó ==="
WORKER_LOG="logs/$(echo "$TARGET" | tr '[:upper:]' '[:lower:]').txt"
if [ -f "$WORKER_LOG" ] && grep -q "CRASH HOOK: CRASH_AFTER_FLUSH" "$WORKER_LOG" 2>/dev/null; then
    echo "[HOOK] CRASH_AFTER_FLUSH se activó en $TARGET"
else
    echo "[WARN] CRASH_AFTER_FLUSH no se activó en $TARGET"
fi

comparar_resultados "$SOLUCIONES"

echo "=== [crash-worker-post-flush] OK ==="
