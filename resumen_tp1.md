# TP1 — Sistema de Procesamiento Distribuido de Transacciones

## Descripción General

El sistema procesa un dataset de transacciones bancarias respondiendo cinco queries analíticas. La arquitectura es un pipeline de procesamiento distribuido basado en mensajes: los datos entran por un **gateway TCP**, se distribuyen a workers via **RabbitMQ**, y los resultados vuelven al cliente.

Todos los workers son réplicas sin estado compartido; el estado se coordina vía mensajes de control sobre exchanges fanout de RabbitMQ.

---

## Arquitectura de los Nodos

### Tipos de worker

| Tipo | Función |
|---|---|
| `filter` | Filtra registros por campo con operadores (`eq`, `lt`, `gt`, `between`, `in`) |
| `projection` | Selecciona columnas y convierte tipos |
| `bank_shard` | Join transacciones + datos bancarios, agrega máximo por banco |
| `format_shard` | Procesamiento en dos fases: calcula promedio y filtra por porcentaje (Q3) |
| `group_distinct_counter` | Agrupa y cuenta valores distintos con filtro de umbral |
| `joiner_q4` | Join de tres vías A→B→C con filtro de degeneración |
| `converter` | Convierte divisas via API externa, filtra por monto |
| `counter` | Agregación final de conteo |

### Clases base

- `BaseWorker` — orquesta `MessageRouter` y `DistributedCoordinator`; gestiona el ciclo de vida de cada worker
- `MessageRouter` — gestiona colas de entrada/salida, aplica sharding y routing condicional
- `DistributedCoordinator` — implementa la barrera global distribuida y el tracking de mensajes en vuelo
- `WorkerConfig` — lee variables de entorno con la topología del worker (colas, reglas de sharding)

---

## Distribución de Mensajes

### Formato interno de mensajes

Los workers se comunican con batches JSON:

```json
{
  "client_id": "123",
  "batches": [
    {
      "header": { "schema": ["From Bank", "Account", "Amount Paid"], "client_id": "123", "count": 2500 },
      "payload": [[1001, 10001, 150.50], ...]
    }
  ]
}
```

### Estrategia de sharding

El sharding se hace por hash MD5 del valor de uno o más campos clave concatenados con `|`. El shard destino es:

```
shard_id = (int(md5_hex, 16) % total_shards) + 1   # 1-indexed
```

Los valores numéricos son normalizados (se eliminan ceros a la izquierda) para evitar desalineación entre nodos.

### Tipos de routing

| Tipo | Descripción |
|---|---|
| **Directo** | El mensaje va a todas las colas directas sin modificación |
| **Sharded** | Los registros del batch se particionan por hash del campo clave; cada shard recibe sólo sus registros |
| **Condicional** | Los registros se enrutan a distintas colas según el valor de un campo (ej: por rango de fecha); luego se aplica sharding dentro de cada caso |

Las señales de **EOF** siempre se propagan a **todas** las colas de salida (directas, todos los shards, todos los casos condicionales).

---

## Coordinación entre Nodos y Manejo de EOF

### Protocolo de barrera distribuida (3 fases)

Cuando un grupo de workers del mismo tipo (mismo prefijo) necesita sincronizarse antes de hacer flush de su estado, usan un canal de control (`control_<prefijo>_exchange`, tipo fanout).

**Fase 1 — Acumulación local de EOFs**

Cada worker trackea cuántas de sus colas de entrada enviaron EOF. Cuando todas enviaron EOF, el worker está listo para participar en la barrera.

**Fase 2 — Elección de originador**

El primer worker que completa su EOF local se anuncia como originador:
```json
{ "type": "EOF_RECEIVED", "client_id": "X", "originator": "worker_id" }
```
Si dos workers se anuncian simultáneamente, **gana el de menor ID**. El originador es el responsable de esperar a todos y emitir la señal de completado.

**Fase 3 — Flush y finalización**

Cada worker, al terminar de procesar su último mensaje, emite:
```json
{ "type": "WORKER_FINISHED", "client_id": "X", "originator": "...", "worker_id": "..." }
```
El originador espera `WORKER_FINISHED` de todos los N workers. Una vez recibidos todos, emite:
```json
{ "type": "BARRIER_COMPLETE", "client_id": "X" }
```
Todos los workers hacen flush de estado, emiten resultados hacia downstream y liberan el estado del cliente.

### Tracking de mensajes en vuelo

Antes de hacer flush, cada worker verifica que no haya mensajes downstream pendientes de ACK. Usa un contador de mensajes en vuelo (`_mensajes_en_vuelo`): incrementa al enviar, decrementa al recibir ACK. El flush sólo procede cuando ese contador lleva ≥1 segundo en cero (chequeo de continuidad).

### Casos especiales de EOF

- **Workers con múltiples entradas (Q2, Q3, Q4):** esperan EOF de todas sus colas de entrada antes de iniciar la barrera
- **FormatShard (Q3):** sobreescribe `interceptar_eof()` para manejar manualmente las dos fases (early/late); evita flush prematuro antes de que ambas fases estén completas
- **JoinerQ4 (Q4):** tiene dos semánticas de entrada distintas (scatter edges vs. transacciones), cada una con su propia gestión de EOF
- **CLIENT_DISCONNECT:** limpia el estado del cliente sin emitir resultados (distinto a EOF que sí emite)

---

## Pipeline y Diseño por Query

### Q1 — Transacciones pequeñas en USD

**Objetivo:** Transacciones en dólares con monto < $50.

```
raw_data
  └→ shared_filter_usd (3 réplicas) — filtra Payment Currency = "US Dollar"
      └→ q1_projection (2 réplicas) — selecciona From Bank, Account, To Bank, Account, Amount Paid
          └→ q1_minor_than_50 (3 réplicas) — filtra Amount Paid < 50
              └→ q1_results
```

Pipeline lineal sin sharding. Los 3 filtros compartidos upstream (`shared_filter_usd`) también alimentan Q2, Q3 y Q4.

---

### Q2 — Monto máximo por banco en USD

**Objetivo:** Para cada banco, el monto máximo de transacciones recibidas en dólares.

```
raw_data ──→ shared_filter_usd (3 réplicas)
               └→ q2_projection (6 réplicas, shard por "From Bank")
                   └→ q2_shard_transactions_1..6

[Datos bancarios desde el cliente]
  └→ q2_bank_projection (3 réplicas, shard por "Bank ID" → rehash a 6)
      └→ q2_shard_banks_1..6

q2_shard_transactions_{id} + q2_shard_banks_{id}
  └→ q2_agregador_shard (6 réplicas, join + max por banco)
      └→ q2_results
```

**Diseño clave:** Dos flujos de datos entran al mismo worker de agregación por el mismo shard ID. El sharding garantiza que transacciones y datos bancarios del mismo banco siempre caen en el mismo agregador. El join se hace dentro de cada shard sin comunicación cruzada.

---

### Q3 — Transacciones tardías menores al 1% del promedio temprano

**Objetivo:** Del 1 al 14 de septiembre de 2022, transacciones del período tardío (6-14/9) cuyo monto es menor al 1% del promedio del período temprano (1-5/9), agrupado por Payment Format.

```
raw_data → shared_filter_usd
  └→ q3_filter_fechas (4 réplicas)
      ├→ q3_temprano_1..2 (shard por "Payment Format") — período 1-5/9
      └→ q3_tardio_1..2  (shard por "Payment Format") — período 6-14/9

q3_temprano_{id} + q3_tardio_{id}
  └→ q3_format_shard (2 réplicas, procesamiento en dos fases)
      └→ q3_results
```

**Diseño clave:** Routing condicional por rango de fecha + sharding por Payment Format. Cada `format_shard` recibe registros de ambas fases pero del mismo formato. El worker primero acumula los del período temprano para calcular promedios; cuando recibe EOF de temprano, procesa los tardíos filtrando contra esos promedios. El EOF de ambas fases se coordina manualmente con `interceptar_eof()`.

---

### Q4 — Caminos A→B→C con alta dispersión

**Objetivo:** Encontrar entidades `(A, C)` tales que existe algún `B` con: A→B (A transfiere a ≥5 B distintos), B→C (B transfiere a C). Se reportan los `(A, C)` con más de 5 `B` intermediarios distintos.

```
raw_data → shared_filter_usd → q4_projection (2 réplicas)
  └→ q4_filter_period (2 réplicas, filtra 1-5/9)
      ├→ q4_to_sumador_1..3 (shard por From Bank + Account)
      └→ q4_to_joiner_1..3  (mismo shard key)

q4_to_sumador_{id}
  └→ q4_sumador (3 réplicas) — cuenta destinos distintos por origen
      si |{B}| ≥ 5: emite aristas A→B válidas
      └→ q4_scatter_edges_1..3 (shard por to_bank + to_account)

q4_scatter_edges_{id} + q4_to_joiner_{id}
  └→ q4_joiner (3 réplicas) — join A→B con B→C
      filtra degeneración: A≠B, B≠C, A≠C
      └→ q4_paths_1..3 (shard por A + C)

q4_paths_{id}
  └→ q4_contador (3 réplicas) — cuenta B's distintos por par (A,C)
      filtra donde count > 5
      └→ q4_results
```

**Diseño clave (scatter-gather):** El mismo nodo de origen es enviado tanto al sumador (stream A: para detectar alta dispersión) como al joiner (stream B: para ser encontrado como destino). El sharding por `(From Bank, Account)` garantiza que todas las aristas del mismo origen caen en el mismo sumador. Luego las aristas validadas se reshardean por `(to_bank, to_account)` para juntarse con el flujo B→C. El joiner recibe dos streams: aristas A→B válidas y todas las transacciones B→C; el join es posible porque ambos flujos están shardados por el mismo campo B.

---

### Q5 — Transacciones Wire/ACH que valen menos de $1 USD

**Objetivo:** Conteo de transacciones en formato Wire o ACH, del 1-5/9, cuyo monto convertido a USD es menor a $1.

```
raw_data
  └→ q5_projection (4 réplicas) — selecciona Timestamp, Currency, Format, Amount
      └→ q5_filter_period (4 réplicas) — filtra 1-5/9
          └→ q5_filter_format (2 réplicas) — filtra Format in {Wire, ACH}
              └→ q5_converter (2 réplicas) — convierte a USD via API, filtra < $1
                  └→ q5_counter (1 réplica) — conteo final
                      └→ q5_results
```

**Diseño clave:** Pipeline secuencial sin sharding, con reducción progresiva del volumen de datos. La conversión via API externa (`frankfurter.app`) se hace en 2 workers paralelos para tolerar la latencia de red. El counter final es único para garantizar conteo global consistente.

---

## Infraestructura y Despliegue

### Generación dinámica del docker-compose

La topología se define en archivos JSON por query (`config/queries/q*.json`). El script `generar_compose.py` los lee y genera `docker-compose.yml` instanciando servicios con las variables de entorno correctas (colas, shards, IDs de nodo).

```bash
make generar q1 q2 q3 q4 q5   # genera para las queries seleccionadas
make start                     # levanta el sistema
make client                    # corre el cliente
```

### Prefetch

RabbitMQ usa `MAX_MESSAGES_PER_WORKER = 150` para limitar mensajes en memoria por worker y evitar desbordamiento con batches grandes.

---

## Resumen de patrones clave

| Patrón | Queries | Descripción |
|---|---|---|
| Pipeline lineal | Q1, Q5 | Workers en serie sin sharding cruzado |
| Sharding por clave | Q2, Q4 | Hash de campo garantiza que datos relacionados van al mismo shard |
| Routing condicional | Q3 | Registros enrutados a colas distintas según valor de campo |
| Barrera distribuida | Q2, Q3, Q4 | Workers se sincronizan via control fanout antes de hacer flush |
| Scatter-Gather | Q4 | Mismo dato enviado a dos streams; re-unificado aguas abajo por join shardado |
| Join inter-stream | Q2, Q4 | Dos fuentes de datos convergidas por sharding en la misma clave |
| Procesamiento en dos fases | Q3 | Primer batch calcula estadísticas; segundo batch las consume para filtrar |
