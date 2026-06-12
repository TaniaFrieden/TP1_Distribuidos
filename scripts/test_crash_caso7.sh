#!/usr/bin/env bash
set -e
source scripts/test_helpers.sh

CANT_CLIENTES=${1:-1}
TX=${2:-LI-Small_Trans}
ACC=${3:-LI-Small_accounts}
SOLUCIONES=${4:-LI-Small}

echo "=== Preparando entorno para Test Caso 7 (Crash tras EOFs, pre-disparo de barrera) ==="
make down
rm -rf volume/

echo "=== Levantando sistema con inyección de falla ==="
CRASH_PRE_BARRERA=true make start

echo "=== Lanzando $CANT_CLIENTES cliente(s) ==="
lanzar_clientes "$CANT_CLIENTES" "$TX" "$ACC"

echo "=== Esperando finalización del cliente ==="
esperar_clientes

echo "=== Comparando resultados contra soluciones de '$SOLUCIONES' ==="
comparar_resultados "$SOLUCIONES"

echo "=== Test Caso 7 Finalizado. Los resultados son consistentes a pesar del crash pre-barrera. ==="
