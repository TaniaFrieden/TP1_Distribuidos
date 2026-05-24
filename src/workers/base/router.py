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
                    total = item.get("total_workers")
                    shard_queues = {
                        i: middleware.MessageMiddlewareQueueRabbitMQ(
                            self.config.mom_host, f"{prefix}_{i}"
                        )
                        for i in range(1, total + 1)
                    }
                    self.output_queues_sharded.append({
                        "prefix": prefix,
                        "total_workers": total,
                        "hash_field": item.get("hash_field"),
                        "queues": shard_queues
                    })

    def enviar(self, mensaje: bytes, payload: dict = None):
        try:
            if mensaje is None:
                return

            if payload is None:
                try:
                    payload = json.loads(mensaje.decode('utf-8'))
                except:
                    payload = {}

            es_eof = payload.get("EOF", False)

            logger.info(f"[ROUTER DEBUG] Enviando {len(mensaje)} bytes a {len(self.output_queues_direct)} colas directas.")

            # 1. ENVIAR A COLAS SIMPLES
            for q in self.output_queues_direct:
                q.send(mensaje)

            # 2. ENVIAR A SHARDS
            for shard_meta in self.output_queues_sharded:
                if es_eof:
                    for q in shard_meta["queues"].values():
                        q.send(mensaje)
                else:
                    valor_hash = payload.get(shard_meta["hash_field"], "default")
                    logger.info(f"[DEBUG ROUTER] Campo buscado: '{shard_meta['hash_field']}' | Payload recibido: {payload}")
                    target_id = sharding.obtener_id_shard(valor_hash, shard_meta["total_workers"])
                    shard_meta["queues"][target_id].send(mensaje)

            # 3. ENVIAR A CONDICIONALES
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