# Tolerancia a fallos en el Contador

## El escenario: crash entre persistir y ACK

La secuencia del contador es:

1. Llega mensaje con `request_id="req-42"`, `cantidad=5`
2. `estado.incrementar()` → persiste a disco `{count: 47, ids_procesados: {"req-42", ...}}`
3. **--- CRASH ACÁ ---**
4. `ack()` ← nunca se ejecuta

## Qué pasa al reiniciar

1. **El estado se recupera de disco**: el count ya es 47, y `"req-42"` está en `ids_procesados`.
2. **RabbitMQ reentrega el mensaje** (nunca recibió ACK).
3. El worker lo recibe de nuevo, pero hay **dos niveles de dedup**:
   - **Nivel worker base** (`dedup_filter.py`): chequea `es_duplicado(client_id, "req-42")` — si el batch de persistencia ya lo incluyó, lo descarta acá.
   - **Nivel contador** (`estado.py`): `incrementar()` chequea si `"req-42"` está en `_ids_procesados` — si sí, retorna `True` (ya procesado), hace ACK sin contar de nuevo.

**Resultado**: el mensaje se ACKea sin volver a sumar. El count queda correcto.

## Y si NO se manejara como duplicado?

Ahí tenés un **doble conteo**: el estado ya tiene `count=47` (se persistió), pero el mensaje llega de nuevo y se sumaría otra vez → `count=52`. Esto es el clásico problema de **at-least-once sin idempotencia**.

## La clave del patrón

El invariante que hace que funcione es:

```
persistir(estado + request_id)  →  ANTES de  →  ack()
```

- Si crashea **antes de persistir**: no pasa nada, el estado no cambió y el mensaje se reentrega.
- Si crashea **después de persistir pero antes de ACK**: el estado ya cambió, el mensaje se reentrega, pero el `request_id` guardado lo detecta como duplicado.
- Si crashea **después de ACK**: todo OK, flujo normal.

No hay ventana de inconsistencia porque la escritura es **atómica** (`temp file` + `os.replace` + `fsync`). El `request_id` actúa como clave de idempotencia.

## Mismo patrón en la barrera (flush)

Hay un caso análogo con la barrera: si el worker emite el resultado final pero crashea antes de marcar `barrera_completada=True`, al reiniciar volvería a emitir el resultado. Por eso `contador.py` hace:

1. Emitir resultado
2. `marcar_completado(client_id)` → persiste `{barrera_completada: True}`

En recovery, si `barrera_completada=True`, no se recarga el estado de ese cliente, evitando re-emisión.

## Resumen

Sin el tracking de `request_id` persistido, cualquier crash entre write y ACK produce doble conteo. El patrón **write-then-ACK + dedup por request_id** es lo que garantiza **exactly-once semántico** sobre un transporte at-least-once.
