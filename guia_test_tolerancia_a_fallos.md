# Guía: cómo probar la tolerancia a fallos

## Prerrequisitos (una sola vez)

```bash
python3 generar_compose.py
make install
make start
```

Esperar que RabbitMQ esté healthy (~15s). Verificar con `docker compose ps`.

---

## Paso 1 — Unit tests (sin Docker)

Cubren los Casos 2, 3, 4 y 8 a nivel de lógica de código.

```bash
make test-persistencia
```

Corre en <1s y no necesita el sistema levantado. Verifica:
- **Caso 2**: `PersistidorEstado` escribe atómicamente; el estado anterior sobrevive un crash mid-write
- **Caso 3**: `DedupFilter` persiste IDs y los reconoce al reiniciar
- **Caso 4**: `_vistos` descarta el mensaje si el worker crasheó entre persist y ack
- **Caso 8**: `barrier_completada=True` en disco evita reflushar al reiniciar

Si los 44 pasan → la lógica de persistencia está bien. Lo que queda probar es el comportamiento en Docker.

---

## Paso 2 — Caso 1: recovery de estado (matar una etapa y reiniciarla)

Prueba que un worker que muere a mitad de procesamiento recupera su estado y el resultado sigue siendo correcto.

```bash
# Sintaxis completa:
# make test-etapa <prefix> [cant_clientes] [tx] [acc] [soluciones] [espera_segundos]

make test-etapa q4_joiner 2 LI-Small_Trans LI-Small_accounts small 5
```

Cuando el script diga `"Matando: q4_joiner_01 ..."`, en otra terminal:

```bash
docker compose up -d q4_joiner_01 q4_joiner_02 q4_joiner_03
```

Probarlo con distintas etapas (los prefijos son exactamente los nombres de contenedor sin el `_NN` final):

```bash
make test-etapa q4_sumador        2 LI-Small_Trans LI-Small_accounts small
make test-etapa q4_contador       2 LI-Small_Trans LI-Small_accounts small
make test-etapa q4_joiner         2 LI-Small_Trans LI-Small_accounts small
make test-etapa q5_counter        2 LI-Small_Trans LI-Small_accounts small
make test-etapa q2_agregador_shard 2 LI-Small_Trans LI-Small_accounts small
make test-etapa q3_format_shard   2 LI-Small_Trans LI-Small_accounts small
```

**Qué esperar**: los clientes completan y los resultados coinciden con la solución esperada.

---

## Paso 3 — Caso 4: crash entre persistir estado y hacer ack

Prueba que si el counter muere exactamente después de guardar el estado pero antes del ack, el mensaje reentregado no duplica el conteo.

```bash
make down
rm -rf volume/
CRASH_AFTER_PERSIST=true make start

make client datasets/HI-Large_Trans_sample_30.csv datasets/HI-Large_accounts.csv
```

**Qué esperar**: el log del counter muestra `CRASH_AFTER_PERSIST activado — muriendo antes del ack()`. El watchdog detecta el crash en ~30s y el actuador reinicia el container. Al reiniciar, el counter carga el estado del disco, recibe el mensaje reentregado, lo detecta en `_vistos` y lo descarta. El resultado final es correcto.

```bash
# Ver el crash y recovery
make log q5_counter_01
```

Limpiar la bandera para poder repetir el test:
```bash
rm volume/q5_counter_01/crash_once_done
```

---

## Paso 4 — Caso 8: crash durante el flush de resultados

Prueba que si el worker muere después de enviar datos downstream pero antes de marcar `barrier_completada`, al reiniciar **no reenvía** los datos.

```bash
make down
rm -rf volume/
CRASH_AFTER_FLUSH=true make start

make client datasets/HI-Large_Trans_sample_30.csv datasets/HI-Large_accounts.csv
```

O usando el script de integración que automatiza la verificación:

```bash
make test-crash-flush counter
make test-crash-flush q4_joiner
make test-crash-flush q4_sumador
```

**Qué esperar**: uno de los workers stateful crashea en `al_completar_cliente`, el actuador lo reinicia, al arrancar encuentra `barrier_completada=True` en disco y limpia sin reflushar. El resultado es correcto (sin duplicados).

Limpiar bandera para repetir:
```bash
find volume/ -name "crash_flush_done" -delete
```

---

## Paso 5 — Caso 9: el actuador reactiva un container detenido

Prueba que el actuador levanta un container que está `stopped` (no solo `killed`).

```bash
# Con el sistema corriendo y un cliente activo:
docker stop q4_joiner_01

# Verificar que está detenido
docker ps -a | grep q4_joiner_01   # status: Exited

# Esperar ~30s y verificar que el actuador lo levantó
watch docker ps | grep q4_joiner_01   # debe volver a "Up"

# O seguir el log del actuador
make log actuador_1
```

**Timing**: el watchdog detecta la ausencia de heartbeat tras 5s × 6 misses = ~30s. Luego el actuador consume el evento de la cola `caidas` y ejecuta `docker start`.

---

## Paso 6 — Chaos Monkey: stress test general

Mata workers aleatorios cada N segundos mientras corren múltiples clientes.

```bash
# Terminal 1: lanzar 3 clientes en paralelo
make test-todos 3

# Terminal 2: activar chaos monkey apuntando a q4
make caos 10 20 q4_joiner q4_sumador q4_contador
```

O para matar cualquier worker no crítico:
```bash
make caos 15 30
```

Al terminar, `test-todos` compara los resultados automáticamente.

---

## Paso 7 — Casos Críticos de Coordinación Inyectados (6, 7 y Líder)

Estos scripts levantan automáticamente la infraestructura completa inyectando fallas hiperespecíficas, corren la transacción, y comparan los resultados:

- **Caso 6 (Crash pre-confirmación de fin)**:
  `make test-crash-caso6 1 trans_sample LI-Small_accounts sample`

- **Caso 7 (Crash con colas EOF cerradas pre-barrera)**:
  `make test-crash-caso7 1 trans_sample LI-Small_accounts sample`

- **Caída del Líder de Watchdog (Prevención de Split-Brain)**:
  `make test-crash-leader 1 trans_sample LI-Small_accounts sample`

**Prueba de Estrés Iterativa:**
Para validar cualquiera de estos casos en bucle (o todos secuencialmente si se pasa solo un número), utilizá el test de estrés que abortará instantáneamente si se detecta un error de consistencia:

```bash
# Correr todos los casos 5 veces seguidas
make test-stress-crash 5 1 trans_sample LI-Small_accounts sample

# Correr solo el caso 6, 10 veces
make test-stress-crash caso6 10 1 trans_sample LI-Small_accounts sample
```

---

## Resumen

| Target | Caso | Necesita Docker |
|---|---|---|
| `make test-persistencia` | 2, 3, 4, 8 (lógica) | No |
| `make test-etapa <prefix>` | 1 (recovery de estado) | Sí |
| `CRASH_AFTER_PERSIST=true make start` | 4 (ventana persist-ack) | Sí |
| `make test-crash-flush <etapa>` | 8 (ventana flush-barrier) | Sí |
| `docker stop <worker>` manual | 9 (actuador reactiva) | Sí |
| `make caos` + `make test-todos` | 1, 5, 6, 7, 9 (stress) | Sí |
| `make test-stress-crash <n>` | 6, 7, Líder (stress automático) | Sí |
| `make test-crash-caso6` | 6 (crash pre-WORKER_FINISHED) | Sí |
| `make test-crash-caso7` | 7 (crash pre-iniciar_barrera) | Sí |
| `make test-crash-leader` | Caída del Líder en Elección | Sí |
