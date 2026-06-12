#!/usr/bin/env bash

lanzar_clientes() {
    local cant=$1
    local tx=$2
    local acc=$3
    PIDS=()
    rm -rf output/*/ 2>/dev/null
    for i in $(seq 1 "$cant"); do
        ( make client TRANSACTIONS_FILE="$tx" ACCOUNTS_FILE="$acc" OUTPUT_DIR="output" \
            > "logs/client_stdout_$i.txt" 2>&1 ) &
        PIDS+=($!)
    done
}

esperar_clientes() {
    for pid in "${PIDS[@]}"; do
        wait "$pid" || echo "[WARN] Proceso $pid (cliente) terminó con error. Ver logs/client_stdout_*.txt"
    done
}

obtener_queries() {
    .venv/bin/python -c "
import sys
sys.path.append('scripts')
from obtener_queries import obtener_queries_desde_compose
qs = obtener_queries_desde_compose('docker-compose.yml')
print(' '.join(str(q) for q in qs))
"
}

comparar_resultados() {
    local soluciones_dir=$1
    local fallo=0
    local queries
    queries=$(obtener_queries)
    if [ -z "$queries" ]; then
        echo "No se pudieron determinar las queries desde docker-compose.yml. Usando fallback 1 2 3 4 5."
        queries="1 2 3 4 5"
    fi
    echo "Queries a comparar: $queries"

    for dir in output/*/; do
        cid=$(basename "$dir")
        for q in $queries; do
            actual="output/$cid/q${q}_solucion.csv"
            expected="solutions/$soluciones_dir/q${q}_solucion.csv"
            if [ -f "$actual" ]; then
                .venv/bin/python -c "
import sys
sys.path.append('scripts')
from comparar_datasets import comparar_csv_sin_orden
ok, msg = comparar_csv_sin_orden('$actual', '$expected')
print(f'[cliente $cid][q$q]', msg)
sys.exit(0 if ok else 1)
" || fallo=1
            else
                echo "[cliente $cid][q$q] FALTA archivo $actual"
                fallo=1
            fi
        done
    done
    return $fallo
}