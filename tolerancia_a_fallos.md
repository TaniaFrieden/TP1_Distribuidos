# Tolerancia a Fallos — TP1 Distribuidos

## El problema a resolver

El sistema usa **entrega at-least-once**: RabbitMQ garantiza que un mensaje llegará *al menos una vez*, pero si un worker muere antes de hacer `ack()`, el mensaje se reenviará cuando ese worker (u otro) vuelva a estar disponible. Esto genera dos problemas:

1. **Pérdida de estado**: un worker stateful que muere pierde todo lo que tenía en memoria.
2. **Procesamiento doble**: si el worker murió *después* de procesar pero *antes* de hacer `ack()`, el mensaje se reenvía y se procesa de nuevo.

La solución tiene tres capas:
- **Persistencia atómica**: guardar el estado a disco antes del `ack()`.
- **Deduplicación por `request_id`**: detectar y descartar mensajes ya procesados.
- **Recovery al arrancar**: leer el estado previo desde disco antes de empezar a consumir.

---

## Bloque 1: Escritura atómica — `PersistidorEstado`

**Archivo**: `src/common/persistencia.py`

El mecanismo base para cualquier persistencia en el sistema. Garantiza que el estado en disco nunca quede a medias aunque el proceso muera a mitad de una escritura.

```
1. Escribir estado → archivo temporal (temp_XXXX.json) en el mismo directorio
2. fsync() → forzar flush físico a disco
3. os.replace(temp, destino) → reemplazo atómico (operación del kernel)
```

**Por qué es seguro**: `os.replace` es atómico en cualquier filesystem POSIX. O existe el archivo viejo, o existe el nuevo. Nunca hay un estado intermedio roto. El archivo temporal está en el mismo directorio que el destino para asegurar que están en el mismo filesystem (requerimiento de `os.replace`).

---

## Bloque 2: `request_id` — identificador de mensaje único

**Archivo**: `src/gateway/client_handler.py`

El Gateway asigna un `request_id` (UUID v4) a cada lote que publica en RabbitMQ:

```python
internal_msg = {
    "client_id": client_id,
    "request_id": str(uuid.uuid4()),
    "batches": [...]
}
```

Este ID viaja con el mensaje a través de **todos los workers del pipeline**. Cada worker lo propaga en su output (ver projection, filter, converter). Esto permite que cualquier worker downstream detecte si ya procesó ese mensaje.

Para mensajes shardados (`bank_shard`), el router deriva un ID por shard: `f"{upstream_request_id}:s{shard_id}"`, así cada shard tiene su propio ID único pero trazable al origen.

---

## Bloque 3: `DedupFilter` — deduplicación automática en todos los workers

**Archivo**: `src/common/dedup_filter.py`

Clase que mantiene un conjunto de `request_id` procesados por cliente, persistido en disco con `PersistidorEstado`.

```python
class DedupFilter:
    def es_duplicado(self, client_id, request_id) -> bool: ...
    def marcar_procesado(self, client_id, request_id): ...  # escribe a disco
    def limpiar_cliente(self, client_id): ...
```

**Archivo**: `src/workers/base/base.py`

`DedupFilter` está integrado directamente en `BaseWorker`. Esto significa que **todos los workers del sistema** obtienen deduplicación automáticamente sin código extra:

```
Mensaje llega con request_id=X
│
├── ¿DedupFilter.es_duplicado(X)? → SÍ → ack() y descartar
│
└── NO → registrar en vuelo → llamar procesar_payload()
                                        │
                                    ack_wrapper()
                                        ├── DedupFilter.marcar_procesado(X)  ← persiste a disco
                                        └── ack() real a RabbitMQ
```

### Escenario cubierto

```
1. Mensaje con request_id=X llega al worker W
2. W procesa el mensaje (sin crash)
3. W muere justo antes de hacer ack()
4. RabbitMQ reenvía X al worker W' (otra instancia)
5. W' verifica DedupFilter: X ya está marcado → descarta → ack()
```

**Limitación de este mecanismo**: el `DedupFilter` se actualiza en `ack_wrapper`, es decir, **después** de que `procesar_payload` retorna. Si el worker persiste estado de negocio (como un conteo) dentro de `procesar_payload` y luego muere *antes* de que `ack_wrapper` corra, el `DedupFilter` **no** tendrá registro del mensaje. Esto requiere deduplicación adicional en el worker. Ver Caso Q5 más abajo.

---

## Bloque 4: Workers stateful con persistencia propia

### Q5 — Counter (`src/workers/counter/counter.py`)

El counter suma transacciones por cliente. Su estado es un entero (`_conteos`) que crece con cada lote. Es el worker más sensible a procesamiento doble porque cada crash-antes-de-ack incrementaría el conteo.

#### Estado en disco

Por cada `client_id` activo se guarda:

```json
{
  "client_id": "abc123",
  "count": 4200,
  "vistos": ["uuid-1", "uuid-2", "uuid-3", ...]
}
```

El campo `vistos` es el conjunto de `request_id` ya procesados por este worker específicamente. Se persiste junto con el conteo en la **misma escritura atómica**. Esto es clave.

#### Por qué necesita su propio dedup además del DedupFilter del BaseWorker

```
Flujo normal (sin crash):
  procesar_payload():
    1. incrementar conteo en memoria
    2. agregar request_id a _vistos
    3. _guardar_estado() → escribe {count, vistos} en disco  ← PERSIST
    4. ack() → ack_wrapper() → DedupFilter.marcar_procesado() ← PERSIST DedupFilter
    5. ack() real a RabbitMQ

Escenario de crash (CRASH_AFTER_PERSIST):
  Crash entre paso 3 y 4
  - count=4200 está en disco ✓
  - request_id está en _vistos en disco ✓
  - DedupFilter NO tiene el request_id (no llegó al paso 4)

Recovery:
  - Counter carga count=4200 desde disco ✓
  - Counter carga _vistos desde disco ✓
  - RabbitMQ reenvía el mensaje (request_id=X)
  - BaseWorker llama DedupFilter: NO es duplicado (DedupFilter no lo vio)
  - BaseWorker llama procesar_payload()
  - Counter verifica _vistos: X ya está → descarta → ack()  ✓ NO HAY DOBLE CONTEO
```

La clave es que `count` y `vistos` se guardan **atómicamente juntos**: si el count fue persistido, el `request_id` también fue persistido. Nunca puede haber `count` guardado sin el `vistos` correspondiente.

#### Recovery al arrancar

```python
def _recover_state_from_disk(self):
    # busca carpetas counter_{node_id}_* en /app/volumen
    # para cada una: carga count + vistos desde estado.json
```

#### Limpieza

Cuando `al_completar_cliente` emite el resultado final, borra la carpeta de estado del cliente. Si `al_desconectar_cliente` recibe un CLIENT_DISCONNECT, también borra sin emitir resultado.

---

### Q4 — GroupDistinctCounter (`src/workers/group_distinct_counter/group_distinct_counter.py`)

Worker genérico usado en dos etapas de Q4: `q4_sumador` (agrupa nodos con más de 5 destinos distintos) y `q4_contador` (cuenta caminos intermedios por par A→C).

#### Estado en disco

Por cada `client_id` activo se guarda:

```json
{
  "client_id": "abc123",
  "grupos": {
    "[\"bank1\", \"acc1\"]": [["bank2", "acc2"], ["bank3", "acc3"]],
    ...
  },
  "vistos": ["uuid-1", "uuid-2", ...]
}
```

Las claves de `grupos` son tuplas serializadas con `json.dumps(list(gkey))`. Los valores son listas de listas (los sets de tuples serializados). Se usa `_vistos` para dedup propio porque `set.add()` es idempotente pero el conjunto de IDs vistos no se puede derivar del estado de negocio.

#### Recovery al arrancar

Busca carpetas con prefijo `gdc_{node_prefix}_{node_id}_` en `/app/volumen` y reconstruye `_grupos` (deserializando claves y valores) y `_vistos`.

---

### Q4 — JoinerQ4 (`src/workers/joiner_q4/joiner_q4.py`)

El joiner acumula dos estructuras en memoria por cliente:
- `_scatter`: diccionario de **listas** de aristas A→B → `list.append()` → **no-idempotente** → requiere `_vistos`
- `_txns`: diccionario de **sets** de aristas B→C → `set.add()` → idempotente

#### Estado en disco

Por cada `client_id` activo se guarda:

```json
{
  "client_id": "abc123",
  "scatter": {
    "bank1|acc1": [["a_bank", "a_acc"], ...]
  },
  "txns": {
    "bank1|acc1": [["c_bank", "c_acc"], ...]
  },
  "vistos": ["uuid-1", "uuid-2", ...]
}
```

`b_key` ya es string (`"bank|account"`), así que no requiere serialización especial. Los sets de tuples se convierten a listas de listas para JSON. El campo `_vistos` es necesario para `_scatter`: sin él, un crash-antes-de-ack podría insertar la misma arista A→B dos veces en la lista, generando caminos duplicados en el resultado.

#### Recovery al arrancar

Busca carpetas con prefijo `joiner_q4_{node_id}_` en `/app/volumen` y reconstruye `_scatter` (lista de tuples), `_txns` (set de tuples) y `_vistos`.

---

### Q3 — FormatShard (`src/workers/format_shard/format_shard.py`)

El format_shard tiene lógica de dos fases:
1. **Fase temprana**: acumula promedios de montos por categoría.
2. **Fase tardía**: filtra registros del período tardío usando los promedios calculados en la fase 1.

El desafío es que los datos de fase tardía pueden ser gigabytes. No se pueden guardar en un JSON (reescribir el archivo entero por cada mensaje es demasiado caro).

#### Cache JSONL append-only

Los registros de fase tardía se guardan en un archivo `cache_tardio.jsonl` donde **cada línea es un batch independiente**. Esto es append-only: nunca se reescribe el archivo, solo se agrega al final:

```python
def _append_to_cache_file(self, client_id, request_id, schema, records):
    line = json.dumps({"request_id": request_id, "schema": schema, "records": records})
    with open(path, "a") as f:
        f.write(line + "\n")
        f.flush()
        os.fsync(f.fileno())  # flush físico por línea
```

Cada línea incluye el `request_id` del batch. Esto permite reconstruir el conjunto de IDs ya procesados al hacer recovery (leyendo el archivo JSONL línea por línea), incluso si el estado JSON principal no fue actualizado todavía.

#### Precisión monetaria

Los montos se almacenan como **enteros en centavos** para evitar errores de punto flotante acumulados en los promedios:

```python
# guardar: 1234.56 → 123456  (× 100)
# leer:    123456 → 1234.56  (÷ 100)
```

#### Estado en disco por cliente

```json
{
  "temprano_cerrado": true,
  "tardio_cerrado": false,
  "promedios_listos": true,
  "promedios": {"cat_A": 154200, "cat_B": 89300},
  "eof_mensaje": "...",
  "processed_request_ids": ["uuid-1", "uuid-2"],
  "barrier_completada": false
}
```

---

### Q2 — BankShard (`src/workers/bank_shard/bank_shard.py`)

Agrega datos de transacciones con datos bancarios (join). State por cliente: datos agregados por banco + flags de EOF.

#### Mejoras para crash recovery

**`barrier_completada`**: antes de hacer `borrar()` del estado al finalizar, se marca `barrier_completada=True` y se persiste. Si el worker muere entre el flush de datos y el borrado, al arrancar detecta que la barrera ya estaba completa y no reprocesa.

**`_barreras_para_iniciar`**: si el worker detecta al recuperar que ambas colas estaban cerradas pero el flush nunca se disparó, encola la barrera en una lista. `al_iniciar_post_arranque()` (llamado por BaseWorker al terminar el recovery) las inicia.

---

## Bloque 5: Coordinator con recovery de barreras

**Archivo**: `src/workers/base/coordinator.py`

El `DistributedCoordinator` maneja la sincronización global entre réplicas (barrera de EOF distribuida). Si un worker muere durante este proceso, puede quedar el sistema en un estado inconsistente.

### Casos cubiertos

**`clientes_finalizados` recovery**: si el coordinator muere después de completar la barrera pero antes de enviar `BARRIER_COMPLETE`, al arrancar recuerda qué clientes ya estaban finalizados. Si llegan mensajes `WORKER_FINISHED` tardíos, responde correctamente.

**`flush_completados` recovery**: si el worker ya hizo el flush de datos downstream pero murió antes de confirmar `WORKER_FINISHED`, al arrancar reenvía esa confirmación.

---

## Bloque 6: Actuador — restart inteligente

**Archivo**: `src/watchdog/actuador.py`

El actuador reinicia contenedores caídos detectados por el watchdog. La corrección distingue el estado del contenedor:

```python
container.reload()
if container.status != "running":
    container.start()   # contenedor detenido → iniciar
else:
    container.restart() # contenedor colgado pero "running" → reiniciar
```

Antes siempre hacía `restart()`, lo que fallaba silenciosamente si el contenedor ya estaba detenido (Docker no puede "reiniciar" lo que está parado).

---

## Bloque 7: Test de crash controlado — `CRASH_AFTER_PERSIST`

Para demostrar que el mecanismo funciona, se puede forzar un crash en el counter exactamente en la ventana entre guardar estado y hacer ack:

### Cómo activarlo

```bash
CRASH_AFTER_PERSIST=true make start
```

El compose lo pasa al contenedor counter via `${CRASH_AFTER_PERSIST:-false}`.

### Comportamiento

El código del test (en `counter.py`) usa una bandera en disco para crashear **una sola vez**:

```python
if os.environ.get("CRASH_AFTER_PERSIST") == "true":
    bandera = os.path.join(BASE_DIR, "crash_once_done")
    if not os.path.exists(bandera):
        open(bandera, "w").close()
        logger.warning("[Counter] CRASH_AFTER_PERSIST activado — muriendo antes del ack()")
        os._exit(1)  # os._exit() porque estamos en un thread (signal.SIGKILL no funciona desde threads)
```

### Secuencia observable

```
1. make start (con CRASH_AFTER_PERSIST=true)
2. make client <datos>          → cliente envía transacciones
3. counter procesa primer batch → guarda count+vistos → CRASH (os._exit)
4. Docker reinicia el container automáticamente (restart: on-failure)
5. counter arranca → carga count desde disco
6. RabbitMQ reenvía el batch sin ackear
7. counter verifica _vistos → ya estaba → descarta → ack()
8. procesamiento continúa normalmente
9. resultado final: count correcto (sin doble conteo)
```

### Limpiar la bandera para repetir el test

```bash
rm volume/q5_counter_01/crash_once_done
```

---

## Bloque 8: Pruebas de Estrés y Fallos Inyectados (Stress Test)

Para garantizar la cobertura empírica, el repositorio provee *scripts* automatizados de inyección que derriban intencionalmente los contenedores en las ventanas críticas de ejecución usando banderas de entorno.

### Cómo testear todos los casos en bucle (Stress Test)
```bash
make test-stress-crash <iteraciones> 1 trans_sample LI-Small_accounts sample
```
Si se especifican iteraciones, por defecto el sistema bajará, inyectará y correrá secuencialmente el Caso 6, Caso 7 y Caída del Líder, validando en cada vuelta que no haya pérdida ni duplicación de datos contra la solución `sample`.

Para probar un solo caso repetidas veces:
```bash
make test-stress-crash caso6 5 1 trans_sample LI-Small_accounts sample
```

---

## Resumen de qué persiste cada worker

| Worker | Qué guarda en disco | Dónde |
|---|---|---|
| `counter` | `{count, vistos}` por cliente | `volume/q5_counter_NN/counter_N_CLIENT/estado.json` |
| `format_shard` | promedios, flags EOF, request_ids procesados + cache tardío | `volume/q3_format_shard_NN/` |
| `bank_shard` | datos agregados por banco, flags EOF, barrier_completada | `volume/q2_bank_shard_NN/` |
| `group_distinct_counter` | `{grupos, vistos}` por cliente | `volume/q4_{sumador,contador}_NN/gdc_*/estado.json` |
| `joiner_q4` | `{scatter, txns, vistos}` por cliente | `volume/q4_joiner_NN/joiner_q4_*/estado.json` |
| `coordinator` (interno de cada worker) | coordinaciones EOF, eofs locales, flush_completados | `volume/<worker>/coordinator_*/estado.json` |
| `DedupFilter` (en BaseWorker) | request_ids procesados por cliente | `volume/<worker>/dedup_*/estado.json` |

---

## Diagrama del flujo de tolerancia para un mensaje

```
Gateway
  │  asigna request_id=UUID
  ▼
RabbitMQ cola de entrada
  │
  ▼
Worker W (BaseWorker)
  │
  ├─1─ ¿request_id en DedupFilter? → SÍ → ack() [descarte general]
  │
  └─2─ NO → llama procesar_payload()
              │
              ├─a─ lógica de negocio (incrementar/filtrar/agregar)
              ├─b─ persistir estado + request_id a disco  ← PUNTO CRÍTICO
              │    (atómico: o ambos o ninguno)
              │
              [posible CRASH aquí]
              │
              └─c─ ack_wrapper()
                      ├── DedupFilter.marcar_procesado()  ← persiste a disco
                      └── ack() real a RabbitMQ

Si crash entre b y c:
  → estado de negocio: guardado ✓
  → request_id en dedup propio del worker: guardado ✓
  → DedupFilter de base: NO guardado
  → RabbitMQ reenvía el mensaje
  → BaseWorker: DedupFilter no lo conoce → pasa a procesar_payload
  → worker propio: request_id en _vistos → descarta → ack() ✓
```

---

## Casos de tolerancia a fallos cubiertos

### Caso 1 — Pérdida de estado al reiniciarse
**Problema**: un worker stateful muere y pierde todo lo que tenía en memoria.
**Solución**: guardar el estado a disco antes del `ack()`. Al arrancar, leer ese estado y continuar desde donde quedó.
**Cubierto en**: `counter`, `format_shard`, `bank_shard`, `group_distinct_counter`, `joiner_q4`, `coordinator` (interno de cada worker).

---

### Caso 2 — Escritura a disco a medias
**Problema**: el proceso muere mientras está escribiendo el archivo de estado, dejando un JSON corrupto o truncado.
**Solución**: `PersistidorEstado` escribe en un archivo temporal y luego hace `os.replace()` (atómico a nivel del kernel). O existe el archivo viejo completo, o el nuevo completo. Nunca uno roto.
**Cubierto en**: todos los workers que persisten estado.

---

### Caso 3 — Mensaje procesado pero no acked (crash entre procesamiento y ack)
**Problema**: RabbitMQ reenvía el mensaje y el worker lo procesa dos veces.
**Solución**: `DedupFilter` integrado en `BaseWorker` verifica el `request_id` antes de llamar a `procesar_payload`. Si ya fue procesado, hace `ack()` y descarta.
**Cubierto en**: todos los workers automáticamente vía `BaseWorker`.

---

### Caso 4 — Crash en la ventana entre persistir estado y persistir el DedupFilter
**Problema**: el worker guarda su estado de negocio a disco, luego muere antes de que `ack_wrapper` actualice el `DedupFilter`. En el recovery, el `DedupFilter` no sabe que ese `request_id` ya fue procesado, y deja pasar el mensaje nuevamente → doble conteo.
**Solución**: persistir el `request_id` **atómicamente junto con el estado de negocio** en la misma escritura. Si el estado fue guardado, el ID también lo fue.
**Cubierto en**: `counter` (campo `vistos` en el mismo JSON que `count`), `format_shard` (`request_id` embebido en cada línea del JSONL), `group_distinct_counter` y `joiner_q4` (campo `vistos` persistido junto con el estado de negocio).

---

### Caso 5 — Worker reiniciado cuando la barrera de EOF ya se había completado
**Problema**: el coordinator muere después de completar la barrera pero antes de enviar `BARRIER_COMPLETE`. Al arrancar, llegan mensajes tardíos `WORKER_FINISHED` y no sabe cómo responder.
**Solución**: al recuperar, el coordinator restaura el conjunto `clientes_finalizados` y responde correctamente a cualquier mensaje tardío.
**Cubierto en**: `coordinator`.

---

### Caso 6 — Worker que flusheó datos downstream pero murió antes de confirmar WORKER_FINISHED
**Problema**: los datos ya fueron enviados al siguiente stage pero el coordinator no sabe que ese worker terminó. El sistema queda esperando una confirmación que nunca llega.
**Solución**: el coordinator persiste `flush_completados`. Al arrancar, detecta esos casos y reenvía la confirmación `WORKER_FINISHED`.
**Cubierto en**: `coordinator`. *(Automatizado con `CRASH_BEFORE_FINISHED_CONFIRMATION=true` vía `make test-crash-caso6`)*.

---

### Caso 7 — Crash con ambas colas EOF cerradas pero flush no iniciado
**Problema**: el worker recibió todos los EOFs y debía iniciar la barrera de flush, pero murió antes de hacerlo. Al arrancar, las colas ya no van a enviar más EOFs, así que ese trigger nunca vuelve a llegar.
**Solución**: al recuperar, el worker detecta el estado "ambas colas cerradas, flush no iniciado" y encola la barrera en `_barreras_para_iniciar`. El hook `al_iniciar_post_arranque()` (llamado por `BaseWorker` al terminar el recovery) la dispara.
**Cubierto en**: `bank_shard`, `format_shard`. *(Automatizado con `CRASH_PRE_BARRERA=true` vía `make test-crash-caso7`)*.

---

### Caso 8 — Crash durante el flush de resultados finales
**Problema**: el worker ya envió los resultados downstream pero muere antes de borrar su estado. Al reiniciarse, detecta que tiene estado en disco y vuelve a enviar los resultados → duplicados en la salida.
**Solución**: antes de borrar el estado, persistir `barrier_completada=True`. Al arrancar con ese flag, el worker sabe que ya completó y no reenvía.
**Cubierto en**: `bank_shard`, `counter`, `group_distinct_counter`, `joiner_q4`.

---

### Caso 9 — Actuador intentando reiniciar un container ya detenido
**Problema**: `docker restart` falla silenciosamente si el container está en estado `stopped` (no `running`), dejando el worker caído indefinidamente.
**Solución**: el actuador verifica el estado del container antes de actuar: `start()` si está detenido, `restart()` si está colgado pero "running".
**Cubierto en**: `actuador`.

---

### Caso 10 — Caída del Líder durante la Elección (Split-Brain Prevention / Hito 3)
**Problema**: Matar al Watchdog Líder o Coordinador justo en momentos de alta sensibilidad, por ejemplo justo después de determinarse ganador de la elección pero antes de propagar el "coordinador" al anillo.
**Solución**: Si el líder muere, los standbys inician una nueva ronda por timeout. Al morir, el líder no llega a enviar sus heartbeats obligatorios. El anillo evita el `split-brain` salteando el nodo inactivo y proclamando un nuevo líder de manera atómica. 
**Cubierto en**: `watchdog/ring_election.py`. *(Automatizado con `CRASH_LEADER_MID_ELECTION=true` vía `make test-crash-leader`)*.

---
