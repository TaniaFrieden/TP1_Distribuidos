#!/usr/bin/env bash
set -e
source scripts/tests/test_helpers.sh

CANT_CLIENTES=${1:-3}
TX=${2:-LI-Small_Trans}
ACC=${3:-LI-Small_accounts}
SOLUCIONES=${4:-small}

SECCION_INICIO=0
TEST_INICIO=$SECONDS

iniciar_seccion() {
    SECCION_INICIO=$SECONDS
    echo ""
    echo "========================================================="
    printf "=== SECCIÓN %s: %s ===\n" "$1" "$2"
    echo "========================================================="
}

finalizar_seccion() {
    local elapsed=$(( SECONDS - SECCION_INICIO ))
    local mins=$(( elapsed / 60 ))
    local secs=$(( elapsed % 60 ))
    printf "✅ OK — %dm %02ds\n" "$mins" "$secs"
    echo "========================================================="
}

echo "========================================================="
echo "=== TEST DEMO — 4 escenarios de fault tolerance ==="
printf "=== Dataset: %s / %s | Clientes: %s ===\n" "$TX" "$ACC" "$CANT_CLIENTES"
echo "========================================================="

trap limpiar_test_global EXIT

# -----------------------------------------------------------
# SECCIÓN 1: Caída de todos los nodos de una etapa
# -----------------------------------------------------------
iniciar_seccion 1 "Caída de todos los nodos de una etapa aleatoria"

limpiar_y_arrancar
lanzar_clientes "$CANT_CLIENTES" "$TX" "$ACC"

> logs/chaos_monkey_run.log
python3 scripts/chaos/chaos_monkey.py 5 75 --etapa >> logs/chaos_monkey_run.log 2>&1 &
CHAOS_PID=$!

esperar_clientes
kill "$CHAOS_PID" 2>/dev/null || true
unset CHAOS_PID

comparar_resultados "$SOLUCIONES"
finalizar_seccion

# -----------------------------------------------------------
# SECCIÓN 2: Caída de todos los nodos (watchdog los recupera)
# -----------------------------------------------------------
iniciar_seccion 2 "Caída de todos los nodos — watchdog los recupera"

limpiar_y_arrancar
lanzar_clientes "$CANT_CLIENTES" "$TX" "$ACC"

> logs/chaos_monkey_run.log
python3 scripts/chaos/chaos_monkey.py 5 30 --todos >> logs/chaos_monkey_run.log 2>&1 &
CHAOS_PID=$!

esperar_clientes
kill "$CHAOS_PID" 2>/dev/null || true
unset CHAOS_PID

comparar_resultados "$SOLUCIONES"
finalizar_seccion

# -----------------------------------------------------------
# SECCIÓN 3: Caída del gateway
# -----------------------------------------------------------
iniciar_seccion 3 "Caída del gateway"

limpiar_y_arrancar
lanzar_clientes "$CANT_CLIENTES" "$TX" "$ACC"

echo "=== Esperando 5s antes de matar gateway ==="
sleep 5
echo "=== Matando gateway_01 ==="
docker kill gateway_01

esperar_clientes
comparar_resultados "$SOLUCIONES"
finalizar_seccion

# -----------------------------------------------------------
# SECCIÓN 4: Caída de un cliente
# -----------------------------------------------------------
iniciar_seccion 4 "Caída de un cliente"

limpiar_y_arrancar
lanzar_clientes "$CANT_CLIENTES" "$TX" "$ACC"

echo "=== Esperando 3s antes de matar un cliente ==="
sleep 3

CLIENTE_A_MATAR=$(docker ps --format '{{.Names}}' | grep -E '^client_' | head -n1 || true)
if [ -z "$CLIENTE_A_MATAR" ]; then
    echo "=== No se encontró contenedor client_* corriendo (¿ya terminaron todos?) ==="
else
    echo "=== Matando $CLIENTE_A_MATAR ==="
    docker kill "$CLIENTE_A_MATAR"
fi

esperar_clientes

TOTAL_QUERIES=$(obtener_queries | wc -w)
for dir in output/*/; do
    [ -d "$dir" ] || continue
    total_csvs=$(find "$dir" -maxdepth 1 -name 'q*_solucion.csv' 2>/dev/null | wc -l)
    if [ "$total_csvs" -lt "$TOTAL_QUERIES" ]; then
        echo "=== Descartando cliente incompleto $(basename "$dir") ($total_csvs/$TOTAL_QUERIES queries) ==="
        rm -rf "$dir"
    fi
done

comparar_resultados "$SOLUCIONES"
finalizar_seccion

# -----------------------------------------------------------
# RESUMEN FINAL
# -----------------------------------------------------------
TOTAL_ELAPSED=$(( SECONDS - TEST_INICIO ))
TOTAL_MINS=$(( TOTAL_ELAPSED / 60 ))
TOTAL_SECS=$(( TOTAL_ELAPSED % 60 ))

echo ""
echo "========================================================="
echo "✅ TEST DEMO COMPLETO — todas las secciones pasaron"
printf "   Tiempo total: %dm %02ds\n" "$TOTAL_MINS" "$TOTAL_SECS"
echo "========================================================="
