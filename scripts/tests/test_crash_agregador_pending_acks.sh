#!/usr/bin/env bash
# Verifica que un crash del agregador bancario con acks pendientes sin flushear
# no produce un deadlock post-recovery.
#
# El crash hook mata al worker después de acumular un ack en _acks_pendientes
# pero antes de flushearlo. Al reiniciar, RabbitMQ re-entrega el mensaje,
# incrementando el flight counter. Sin el fix, el flush thread queda bloqueado
# esperando vuelos=0 que nunca llega (deadlock).
#
set -e
source scripts/tests/test_helpers.sh

CANT_CLIENTES=${1:-1}
TX=${2:-${TEST_TX:-trans_sample}}
ACC=${3:-${TEST_ACC:-LI-Small_accounts}}
SOLUCIONES=${4:-${TEST_SOL:-sample}}

echo "=== [crash-agregador-pending-acks] $CANT_CLIENTES cliente(s) ==="

make down
timeout 10s docker run --rm -v "$(pwd)/volume:/cleanup" alpine sh -c "rm -rf /cleanup/*" 2>/dev/null \
    || rm -rf volume/* 2>/dev/null || true

echo "=== Levantando sistema con CRASH_AGREGADOR_PENDING_ACKS=true ==="
CRASH_AGREGADOR_PENDING_ACKS=true make start
esperar_sistema_listo

echo "=== Lanzando $CANT_CLIENTES cliente(s) ==="
lanzar_clientes "$CANT_CLIENTES" "$TX" "$ACC"

echo "=== Esperando finalización del cliente ==="
esperar_clientes

echo "=== Comparando resultados ==="
comparar_resultados "$SOLUCIONES"

echo "=== [crash-agregador-pending-acks] OK ==="
