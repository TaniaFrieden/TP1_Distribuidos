#!/usr/bin/env bash
set -e
source scripts/tests/test_helpers.sh

TX=${1:-${TEST_TX:-trans_sample}}
ACC=${2:-${TEST_ACC:-LI-Small_accounts}}
SOL=${3:-${TEST_SOL:-sample}}

HOOKS=(
    "CRASH_BEFORE_DATA_ACK"
    "CRASH_PRE_BARRERA"
    "CRASH_BEFORE_FINISHED_CONFIRMATION"
    "CRASH_AFTER_FLUSH"
    "CRASH_BEFORE_EOF_FORWARD"
)

# Instancias target para inyectar crash (una por etapa del pipeline)
TARGETS=(
    "Q5_PROJECTION_01"
    "Q5_FILTER_PERIOD_01"
    "Q5_FILTER_FORMAT_01"
    "Q5_CONVERTER_01"
    "Q5_COUNTER_01"
)

CLIENTES_CONFIGS=(1 2)

RESULTADOS=()
FALLO_GLOBAL=0
TOTAL=0
PASARON=0

limpiar_entorno() {
    local clients
    clients=$(docker ps -a -q --filter "name=client_")
    if [ -n "$clients" ]; then
        timeout 10s docker rm -f $clients 2>/dev/null || true
    fi
    make down 2>/dev/null || true
    sleep 1
    timeout 10s docker run --rm \
        -v "$(pwd)/volume:/vol" -v "$(pwd)/output:/out" -v "$(pwd)/logs:/lg" \
        alpine sh -c "rm -rf /vol/* /out/*/ /out/client_id*.txt /lg/client_*.txt /lg/client_stdout_*.txt /lg/q5_*.txt /lg/shared_*.txt /lg/chaos_monkey_run.log" \
        2>/dev/null || true
}

levantar_con_crash() {
    local target="$1"
    local hook="$2"
    if [ -n "$target" ] && [ -n "$hook" ]; then
        local env_var="${target}_CRASH"
        echo "=== Levantando con ${env_var}=${hook} ==="
        eval "${env_var}=${hook} make start"
    else
        echo "=== Levantando sin crash hooks ==="
        make start
    fi
    esperar_sistema_listo
}

verificar_hook_activo() {
    local target="$1"
    local hook="$2"
    if [ -z "$hook" ]; then
        return 0
    fi
    local worker_log
    worker_log="logs/$(echo "$target" | tr '[:upper:]' '[:lower:]').txt"
    if [ -f "$worker_log" ] && grep -q "CRASH HOOK: $hook" "$worker_log" 2>/dev/null; then
        echo "    [HOOK] $hook se activo en $target"
        return 0
    fi
    echo "    [WARN] $hook NO se activo en $target"
    return 1
}

ejecutar_caso() {
    local nombre="$1"
    local target="$2"
    local hook="$3"
    local cant_clientes="$4"
    local es_caos="$5"

    TOTAL=$((TOTAL + 1))
    echo ""
    echo "========================================================="
    echo "=== [$TOTAL] $nombre (${cant_clientes}c) ==="
    echo "========================================================="

    limpiar_entorno
    levantar_con_crash "$target" "$hook"

    lanzar_clientes "$cant_clientes" "$TX" "$ACC"

    if [ "$es_caos" = "kill-all" ]; then
        > logs/chaos_monkey_run.log
        python3 scripts/chaos/chaos_monkey.py 75 --todos >> logs/chaos_monkey_run.log 2>&1 &
        CHAOS_PID=$!
        trap 'kill $CHAOS_PID 2>/dev/null || true' EXIT
    fi

    esperar_clientes

    if [ "$es_caos" = "kill-all" ]; then
        kill $CHAOS_PID 2>/dev/null || true
        trap - EXIT
    fi

    local hook_ok=1
    if [ -n "$hook" ]; then
        if ! verificar_hook_activo "$target" "$hook"; then
            hook_ok=0
        fi
    fi

    local resultado_ok=1
    if ! comparar_resultados "$SOL"; then
        resultado_ok=0
    fi

    if [ "$resultado_ok" -eq 1 ] && [ "$hook_ok" -eq 1 ]; then
        RESULTADOS+=("  OK    $nombre (${cant_clientes}c)")
        PASARON=$((PASARON + 1))
    elif [ "$resultado_ok" -eq 1 ] && [ "$hook_ok" -eq 0 ]; then
        RESULTADOS+=("  SKIP  $nombre (${cant_clientes}c) — hook no se activo")
        PASARON=$((PASARON + 1))
    else
        if [ "$hook_ok" -eq 0 ]; then
            RESULTADOS+=("  FAIL  $nombre (${cant_clientes}c) — resultado incorrecto + hook no se activo")
        else
            RESULTADOS+=("  FAIL  $nombre (${cant_clientes}c)")
        fi
        FALLO_GLOBAL=1
        echo ""
        echo "========================================================="
        echo "=== ABORTANDO — primer fallo detectado                ==="
        echo "=== Los logs y volumenes quedan intactos para debug   ==="
        echo "========================================================="
        for R in "${RESULTADOS[@]}"; do echo "$R"; done
        exit 1
    fi
}

echo "========================================================="
echo "=== Test de crash hooks del pipeline                  ==="
echo "=== Dataset: $TX / $ACC | Soluciones: $SOL            ==="
echo "========================================================="

# 1. Baseline sin crash
for C in "${CLIENTES_CONFIGS[@]}"; do
    ejecutar_caso "baseline (sin crash)" "" "" "$C" ""
done

# 2. Cada hook en cada etapa, con 1 y 2 clientes
for HOOK in "${HOOKS[@]}"; do
    for TARGET in "${TARGETS[@]}"; do
        for C in "${CLIENTES_CONFIGS[@]}"; do
            ejecutar_caso "$HOOK @ $TARGET" "$TARGET" "$HOOK" "$C" ""
        done
    done
done

# 3. Kill-all (chaos total) con 1 y 2 clientes
for C in "${CLIENTES_CONFIGS[@]}"; do
    ejecutar_caso "KILL-ALL (chaos total)" "" "" "$C" "kill-all"
done

echo ""
echo "========================================================="
echo "=== RESULTADOS ($PASARON/$TOTAL pasaron)              ==="
echo "========================================================="
for R in "${RESULTADOS[@]}"; do
    echo "$R"
done
echo "========================================================="

exit $FALLO_GLOBAL
