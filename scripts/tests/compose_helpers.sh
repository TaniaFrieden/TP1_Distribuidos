#!/usr/bin/env bash
# Helpers para extraer info del docker-compose.yml actual

# Lista prefijos de etapas de workers (sin gateway/watchdog/actuador/rabbitmq/shared)
# Ej: q5_projection q5_filter_period q5_counter
listar_etapas_workers() {
    grep 'NODE_PREFIX:' docker-compose.yml 2>/dev/null \
        | sed 's/.*NODE_PREFIX: //' | sort -u
}

# Lista instancias de crash targets (Q5_COUNTER_01, etc.)
# Formato: UPPER con _CRASH suffix listo para usar
listar_crash_targets() {
    grep -oP '\$\{\K[A-Z0-9_]+(?=_CRASH:-)' docker-compose.yml 2>/dev/null \
        | grep -v "GATEWAY\|WATCHDOG"
}

# Devuelve el primer crash target de una etapa dada
# Uso: target_de_etapa "counter" → Q5_COUNTER_01
target_de_etapa() {
    local etapa="$1"
    listar_crash_targets | grep -i "$(echo "$etapa" | tr '-' '_')" | head -1
}

# Devuelve el primer crash target disponible (cualquier worker)
primer_target() {
    listar_crash_targets | head -1
}
