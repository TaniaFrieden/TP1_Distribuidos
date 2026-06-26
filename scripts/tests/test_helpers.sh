#!/usr/bin/env bash

DOCKER_COMPOSE="${DOCKER_COMPOSE:-docker compose}"
TEMP_DIR="${TEMP_DIR:-temp}"

limpiar_y_arrancar() {
    local clients
    clients=$(docker ps -a -q --filter "name=client_")
    if [ -n "$clients" ]; then
        timeout 10s docker rm -f $clients 2>/dev/null || true
    fi
    rm -rf "$TEMP_DIR"/* output/*/ output/client_id_*.txt 2>/dev/null || true
    make down 2>/dev/null || true
    sleep 2
    timeout 10s docker run --rm -v "$(pwd)/volume:/vol" \
        alpine sh -c "rm -rf /vol/*" 2>/dev/null || true
    make start
    esperar_sistema_listo
}

preparar_entorno() {
    local clients
    clients=$(docker ps -a -q --filter "name=client_")
    if [ -n "$clients" ]; then
        timeout 10s docker rm -f $clients 2>/dev/null || true
    fi
    rm -rf "$TEMP_DIR"/* output/*/ output/client_id_*.txt 2>/dev/null || true
    trap 'jobs -p | xargs -r kill 2>/dev/null; true' EXIT
    local esperados corriendo
    esperados=$($DOCKER_COMPOSE config --services 2>/dev/null | wc -l)
    corriendo=$($DOCKER_COMPOSE ps --status running --format '{{.Name}}' 2>/dev/null | wc -l)
    if [ "$corriendo" -lt "$esperados" ]; then
        echo "=== Sistema incompleto ($corriendo/$esperados). Limpiando y arrancando... ==="
        make down 2>/dev/null || true
        sleep 2
        timeout 10s docker run --rm -v "$(pwd)/volume:/vol" \
            alpine sh -c "rm -rf /vol/*" 2>/dev/null || true
        make start
        esperar_sistema_listo
    else
        echo "=== Sistema listo ($corriendo/$esperados servicios running) ==="
    fi
}

esperar_sistema_listo() {
    local timeout=${1:-120}
    local esperados
    esperados=$($DOCKER_COMPOSE config --services 2>/dev/null | wc -l)
    echo "=== Esperando que $esperados servicios estén running (timeout ${timeout}s) ==="
    local inicio=$SECONDS
    while true; do
        local corriendo
        corriendo=$($DOCKER_COMPOSE ps --status running --format '{{.Name}}' 2>/dev/null | wc -l)
        if [ "$corriendo" -ge "$esperados" ]; then
            echo "=== $corriendo/$esperados servicios running ($(( SECONDS - inicio ))s) ==="
            return 0
        fi
        if [ $(( SECONDS - inicio )) -ge "$timeout" ]; then
            echo "[ERROR] Timeout: solo $corriendo/$esperados servicios running tras ${timeout}s"
            $DOCKER_COMPOSE ps 2>/dev/null
            return 1
        fi
        sleep 2
    done
}

lanzar_clientes() {
    local cant=$1
    local tx=$2
    local acc=$3
    local run_id
    run_id=$(date +%s)
    PIDS=()
    mkdir -p "$TEMP_DIR" output
    DIRS_ANTES=$(ls -d output/*/ 2>/dev/null | sort)
    rm -f output/client_id_*.txt 2>/dev/null || true
    for i in $(seq 1 "$cant"); do
        if [ "${SEQUENTIAL:-0}" = "1" ]; then
            if [ "$i" -gt 1 ]; then
                echo ""
            fi
            echo "=== Iteracion $i/$cant iniciando ==="
            ( export CLIENT_ID_SUFFIX="${run_id}_$i"; make cliente TRANSACTIONS_FILE="$tx" ACCOUNTS_FILE="$acc" \
                > "$TEMP_DIR/client_stdout_$i.txt" )
            if [ -n "${SEQUENTIAL_SOL:-}" ]; then
                if ! comparar_ultimo_cliente "$SEQUENTIAL_SOL"; then
                    echo "=== FALLO en iteracion $i/$cant. Abortando. ==="
                    return 1
                fi
            fi
            echo "=== Iteracion $i/$cant finalizado exitosamente ==="
        else
            ( export CLIENT_ID_SUFFIX="${run_id}_$i" PROGRESS_BAR=0; make cliente TRANSACTIONS_FILE="$tx" ACCOUNTS_FILE="$acc" \
                > "$TEMP_DIR/client_stdout_$i.txt" 2>/dev/null ) &
            PIDS+=($!)
        fi
    done
}

esperar_clientes() {
    local inicio=$SECONDS
    local total=${#PIDS[@]}
    local restantes=("${PIDS[@]}")
    local spinner=("⠋" "⠙" "⠹" "⠸" "⠼" "⠴" "⠦" "⠧" "⠇" "⠏")
    local idx=0

    while [ ${#restantes[@]} -gt 0 ]; do
        local nuevos=()
        for pid in "${restantes[@]}"; do
            if kill -0 "$pid" 2>/dev/null; then
                nuevos+=("$pid")
            fi
        done
        restantes=("${nuevos[@]}")
        local terminados=$(( total - ${#restantes[@]} ))
        local elapsed=$(( SECONDS - inicio ))
        local mins=$(( elapsed / 60 ))
        local secs=$(( elapsed % 60 ))
        printf "\r\033[2K  %s %d/%d clientes completados (%02d:%02d)" \
            "${spinner[$idx]}" "$terminados" "$total" "$mins" "$secs" >&2
        idx=$(( (idx + 1) % ${#spinner[@]} ))
        if [ ${#restantes[@]} -gt 0 ]; then
            sleep 0.15
        fi
    done
    printf "\r\033[2K  ✔ %d/%d clientes completados (%02d:%02d)\n" \
        "$total" "$total" "$(( (SECONDS - inicio) / 60 ))" "$(( (SECONDS - inicio) % 60 ))" >&2

    for pid in "${PIDS[@]}"; do
        wait "$pid" || echo "[WARN] Proceso $pid (cliente) terminó con error. Ver $TEMP_DIR/client_stdout_*.txt"
    done
}

obtener_queries() {
    .venv/bin/python -c "
import sys
sys.path.append('scripts/utils')
from obtener_queries import obtener_queries_desde_compose
qs = obtener_queries_desde_compose('docker-compose.yml')
print(' '.join(str(q) for q in qs))
"
}

comparar_ultimo_cliente() {
    local soluciones_dir=$1
    local last_dir
    last_dir=$(ls -td output/*/ 2>/dev/null | head -1)
    if [ -z "$last_dir" ]; then
        echo "No se encontró output de cliente"
        return 1
    fi
    .venv/bin/python scripts/utils/comparar_datasets.py "$last_dir" "solutions/$soluciones_dir"
    local result=$?
    rm -f output/client_id_*.txt
    return $result
}

comparar_resultados() {
    local soluciones_dir=$1
    local dirs_despues
    dirs_despues=$(ls -d output/*/ 2>/dev/null | sort)
    local nuevos
    nuevos=$(comm -13 <(echo "$DIRS_ANTES") <(echo "$dirs_despues"))
    if [ -z "$nuevos" ]; then
        echo "No se encontraron carpetas de resultados nuevas"
        return 1
    fi
    local total=0 exitosos=0
    while IFS= read -r dir; do
        total=$((total + 1))
        if .venv/bin/python scripts/utils/comparar_datasets.py "$dir" "solutions/$soluciones_dir" 2>/dev/null; then
            exitosos=$((exitosos + 1))
        fi
    done <<< "$nuevos"
    if [ "$exitosos" -eq "$total" ]; then
        echo "═══ Resultado: $total/$total clientes OK ═══"
        return 0
    else
        echo "═══ Resultado: $((total - exitosos))/$total clientes FALLARON ═══"
        return 1
    fi
}

limpiar_test_global() {
    echo "=== Realizando limpieza de emergencia del test... ==="
    if [ -n "${CHAOS_PID:-}" ]; then
        echo "Apagando Chaos Monkey (PID: $CHAOS_PID)..."
        kill "$CHAOS_PID" 2>/dev/null || true
    fi
    if [ -n "${PIDS:-}" ]; then
        echo "Apagando procesos de clientes..."
        for pid in "${PIDS[@]}"; do
            kill "$pid" 2>/dev/null || true
        done
    fi
    echo "Removiendo contenedores cliente docker residuales..."
    local clients
    clients=$(docker ps -a -q --filter "name=client_")
    if [ -n "$clients" ]; then
        timeout 10s docker rm -f $clients 2>/dev/null || true
    fi
}
