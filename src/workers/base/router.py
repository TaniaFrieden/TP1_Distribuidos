import json
import logging
from common import middleware, sharding

logger = logging.getLogger(__name__)

class MessageRouter:
    """Maneja las conexiones I/O y las reglas de ruteo/sharding."""
    def __init__(self, config):
        self.config = config
        self.input_queues = {}
        self.output_queues_direct = []
        self.output_queues_sharded = []
        self.output_queues_conditional = []
        self._setup_queues()

    def _setup_queues(self):
        # Entradas
        for cola in self.config.input_queues:
            nombre_cola = cola.replace("{id}", str(self.config.node_id))
            self.input_queues[nombre_cola] = middleware.MessageMiddlewareQueueRabbitMQ(
                self.config.mom_host, nombre_cola
            )

        # Salidas
        for item in self.config.output_queues:
            if isinstance(item, str):
                self.output_queues_direct.append(
                    middleware.MessageMiddlewareQueueRabbitMQ(self.config.mom_host, item)
                )
            elif isinstance(item, dict):
                if item.get("type") == "conditional":
                    cases_setup = []
                    for case in item["cases"]:
                        routing = case["routing"]
                        prefix = routing["queue_shard_prefix"]
                        total = routing["total_workers"]
                        shard_queues = {
                            i: middleware.MessageMiddlewareQueueRabbitMQ(
                                self.config.mom_host, f"{prefix}_{i}"
                            )
                            for i in range(1, total + 1)
                        }
                        cases_setup.append({
                            "operator": case["operator"],
                            "value": case["value"],
                            "hash_field": routing["hash_field"],
                            "total_workers": total,
                            "queues": shard_queues
                        })
                    self.output_queues_conditional.append({
                        "condition_field": item["condition_field"],
                        "cases": cases_setup
                    })
                else:
                    prefix = item.get("queue_shard_prefix", item.get("shard_prefix"))
                    total = int(item.get("total_workers") or 0)
                    if total <= 0:
                        continue
                    # Soporta hash_fields (lista) o hash_field (string único, compat. hacia atrás)
                    raw = item.get("hash_fields") or ([item.get("hash_field")] if item.get("hash_field") else [])
                    hash_fields = [f for f in raw if f]
                    shard_queues = {
                        i: middleware.MessageMiddlewareQueueRabbitMQ(self.config.mom_host, f"{prefix}_{i}")
                        for i in range(1, total + 1)
                    }
                    self.output_queues_sharded.append({
                        "prefix": prefix,
                        "total_workers": total,
                        "hash_fields": hash_fields,
                        "queues": shard_queues
                    })

    def enviar(self, mensaje: bytes, payload: dict | None = None):
        try:
            if mensaje is None:
                return

            if payload is None:
                try:
                    payload = json.loads(mensaje.decode('utf-8'))
                except:
                    payload = {}
            assert payload is not None

            es_eof = payload.get("EOF", False)
            client_id = payload.get("client_id")

            # 1. ENVIAR A COLAS SIMPLES
            for q in self.output_queues_direct:
                q.send(mensaje)

            # Check if this is a batch message
            if "batches" in payload and not es_eof:
                # 2. ENVIAR A SHARDS (BATCH)
                for shard_meta in self.output_queues_sharded:
                    hash_fields = shard_meta.get("hash_fields", [])
                    hash_field = hash_fields[0] if hash_fields else shard_meta.get("hash_field")
                    
                    # Group records by target shard_id
                    records_by_shard = {}
                    original_schema = None
                    for batch in payload["batches"]:
                        header = batch["header"]
                        original_schema = header["schema"]
                        records = batch["payload"]
                        
                        if hash_field in original_schema:
                            hash_idx = original_schema.index(hash_field)
                        else:
                            hash_idx = None
                            
                        for record_values in records:
                            val = record_values[hash_idx] if hash_idx is not None else "default"
                            target_id = sharding.obtener_id_shard(val, shard_meta["total_workers"])
                            if target_id not in records_by_shard:
                                records_by_shard[target_id] = []
                            records_by_shard[target_id].append(record_values)
                            
                    for shard_id, shard_records in records_by_shard.items():
                        shard_payload = {
                            "client_id": client_id,
                            "batches": [
                                {
                                    "header": {
                                        "schema": original_schema,
                                        "client_id": client_id,
                                        "count": len(shard_records)
                                    },
                                    "payload": shard_records
                                }
                            ]
                        }
                        shard_meta["queues"][shard_id].send(json.dumps(shard_payload).encode("utf-8"))

                # 3. ENVIAR A CONDICIONALES (BATCH)
                for cond_meta in self.output_queues_conditional:
                    condition_field = cond_meta["condition_field"]
                    
                    # Group records by target queue (case index, shard_id)
                    records_by_queue = {}  # queue_object -> (original_schema, list_of_records)
                    
                    for batch in payload["batches"]:
                        header = batch["header"]
                        original_schema = header["schema"]
                        records = batch["payload"]
                        
                        cond_idx = original_schema.index(condition_field) if condition_field in original_schema else None
                        
                        for record_values in records:
                            valor_campo = str(record_values[cond_idx])[:10] if cond_idx is not None else ""
                            # Find matching case
                            for case in cond_meta["cases"]:
                                if self._evaluar_between(valor_campo, case["value"]):
                                    hash_field = case["hash_field"]
                                    hash_idx = original_schema.index(hash_field) if hash_field in original_schema else None
                                    valor_hash = record_values[hash_idx] if hash_idx is not None else "default"
                                    target_id = sharding.obtener_id_shard(valor_hash, case["total_workers"])
                                    target_queue = case["queues"][target_id]
                                    
                                    if target_queue not in records_by_queue:
                                        records_by_queue[target_queue] = (original_schema, [])
                                    records_by_queue[target_queue][1].append(record_values)
                                    break
                                    
                    for target_queue, (schema, q_records) in records_by_queue.items():
                        q_payload = {
                            "client_id": client_id,
                            "batches": [
                                {
                                    "header": {
                                        "schema": schema,
                                        "client_id": client_id,
                                        "count": len(q_records)
                                    },
                                    "payload": q_records
                                }
                            ]
                        }
                        target_queue.send(json.dumps(q_payload).encode("utf-8"))
                        
            else:
                # 2. ENVIAR A SHARDS (SINGLE RECORD OR EOF)
                for shard_meta in self.output_queues_sharded:
                    if es_eof:
                        for q in shard_meta["queues"].values():
                            q.send(mensaje)
                    else:
                        hash_fields = shard_meta.get("hash_fields", [])
                        hash_field = hash_fields[0] if hash_fields else shard_meta.get("hash_field")
                        valor_hash = payload.get(hash_field, "default")
                        target_id = sharding.obtener_id_shard(valor_hash, shard_meta["total_workers"])
                        shard_meta["queues"][target_id].send(mensaje)

                # 3. ENVIAR A CONDICIONALES (SINGLE RECORD OR EOF)
                for cond_meta in self.output_queues_conditional:
                    if es_eof:
                        for case in cond_meta["cases"]:
                            for q in case["queues"].values():
                                q.send(mensaje)
                    else:
                        valor_campo = str(payload.get(cond_meta["condition_field"], ""))[:10]
                        for case in cond_meta["cases"]:
                            if self._evaluar_between(valor_campo, case["value"]):
                                valor_hash = payload.get(case["hash_field"], "default")
                                target_id = sharding.obtener_id_shard(valor_hash, case["total_workers"])
                                case["queues"][target_id].send(mensaje)
                                break

        except Exception as e:
            logger.error(f"[Router] Error crítico en el ruteo: {e}", exc_info=True)

    def _evaluar_between(self, valor: str, rango: str) -> bool:
        limites = [l.strip() for l in rango.split(",")]
        return limites[0] <= valor <= limites[1]

    def stop_consuming(self):
        for iq in self.input_queues.values(): iq.stop_consuming()

    def close(self):
        for iq in self.input_queues.values(): iq.close()
        for oq in self.output_queues_direct: oq.close()
        for shard_meta in self.output_queues_sharded:
            for sq in shard_meta["queues"].values(): sq.close()
        for cond_meta in self.output_queues_conditional:
            for case in cond_meta["cases"]:
                for q in case["queues"].values(): q.close()