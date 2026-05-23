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
                prefix = item.get("queue_shard_prefix", item.get("shard_prefix"))
                total = item.get("total_workers")
                shard_queues = {
                    i: middleware.MessageMiddlewareQueueRabbitMQ(self.config.mom_host, f"{prefix}_{i}") 
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
            for q in self.output_queues_direct:
                q.send(mensaje)
                
            is_broadcast = payload is None
            for shard_meta in self.output_queues_sharded:
                if is_broadcast:
                    for q in shard_meta["queues"].values():
                        q.send(mensaje)
                else:
                    valor_hash = payload.get(shard_meta["hash_field"], "default")
                    target_id = sharding.obtener_id_shard(valor_hash, shard_meta["total_workers"])
                    shard_meta["queues"][target_id].send(mensaje)
        except Exception as e:
            logger.error(f"[Router] Error enviando mensaje: {e}", exc_info=True)

    def stop_consuming(self):
        for iq in self.input_queues.values(): iq.stop_consuming()

    def close(self):
        for iq in self.input_queues.values(): iq.close()
        for oq in self.output_queues_direct: oq.close()
        for shard_meta in self.output_queues_sharded:
            for sq in shard_meta["queues"].values(): sq.close()