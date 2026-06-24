#!/usr/bin/env bash
# Verifica que un crash del agregador bancario con acks pendientes sin flushear
# no produce un deadlock post-recovery.
set -e
source scripts/tests/test_helpers.sh

CANT_CLIENTES=${1:-1}
TX=${2:-${TEST_TX:-trans_sample}}
ACC=${3:-${TEST_ACC:-LI-Small_accounts}}
SOLUCIONES=${4:-${TEST_SOL:-sample}}

# Buscar primera instancia de agregador
TARGET=$(grep -oP '\$\{\K[A-Z0-9_]+(?=_CRASH:-)' docker-compose.yml | grep "AGREGADOR_SHARD" | head -1)
if [ -z "$TARGET" ]; then
    echo "[SKIP] No hay instancias de agregador_shard en el compose actual"
    exit 0
fi

echo "=== [crash-agregador-pending-acks] Target: $TARGET, $CANT_CLIENTES cliente(s) ==="

make down
timeout 10s docker run --rm -v "$(pwd)/volume:/cleanup" alpine sh -c "rm -rf /cleanup/*" 2>/dev/null \
    || rm -rf volume/* 2>/dev/null || true

echo "=== Levantando sistema con ${TARGET}_CRASH=CRASH_AGREGADOR_PENDING_ACKS ==="
eval "${TARGET}_CRASH=CRASH_AGREGADOR_PENDING_ACKS make start"
esperar_sistema_listo

lanzar_clientes "$CANT_CLIENTES" "$TX" "$ACC"
esperar_clientes
comparar_resultados "$SOLUCIONES"

echo "=== [crash-agregador-pending-acks] OK ==="
