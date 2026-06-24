#!/usr/bin/env bash
# Verifica que un crash post-flush (antes de persistir barrier_completada)
# no produce resultados duplicados.
#
# Uso:
#   bash scripts/tests/test_crash_worker_post_flush.sh [cli] [tx] [acc] [sol] [etapa]
#
#   etapa     : counter | q4_sumador | q4_joiner | q4_contador  (default: counter)
#
set -e
source scripts/tests/test_helpers.sh

ETAPA=${1:-counter}
CANT_CLIENTES=${2:-1}
TX=${3:-${TEST_TX:-trans_sample}}
ACC=${4:-${TEST_ACC:-LI-Small_accounts}}
SOLUCIONES=${5:-${TEST_SOL:-sample}}

echo "=== [crash-worker-post-flush] etapa '$ETAPA', $CANT_CLIENTES cliente(s) ==="

make down

echo "=== Limpiando banderas de crash previas ==="
find volume/ -name "crash_flush_done" -delete 2>/dev/null || true

echo "=== Levantando sistema con CRASH_AFTER_FLUSH=true ==="
CRASH_AFTER_FLUSH=true make start

echo "=== Esperando que el sistema esté listo ==="
esperar_sistema_listo

echo "=== Lanzando $CANT_CLIENTES cliente(s) ==="
lanzar_clientes "$CANT_CLIENTES" "$TX" "$ACC"

echo "=== Esperando finalización del cliente ==="
esperar_clientes

echo "=== Verificando que el worker crasheó y fue reiniciado ==="
NODOS=$(docker ps --format '{{.Names}}' | grep -E "${ETAPA}_[0-9]+" || true)
if [ -z "$NODOS" ]; then
    echo "[ERROR] No se encontraron nodos de la etapa '$ETAPA' corriendo después del crash."
    exit 1
fi
echo "Nodos activos: $NODOS"

echo "=== Verificando bandera de crash ==="
BANDERAS=$(find volume/ -name "crash_flush_done" 2>/dev/null || true)
if [ -z "$BANDERAS" ]; then
    echo "[WARN] No se encontró bandera crash_flush_done."
else
    echo "Banderas encontradas: $BANDERAS"
fi

echo "=== Comparando resultados (sin duplicados) ==="
comparar_resultados "$SOLUCIONES"

echo "=== [crash-worker-post-flush] OK ==="
