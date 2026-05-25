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


    def enviar(self, mensaje: bytes, payload: dict = None):
        logger.info(f"[ROUTER DEBUG] Intentando enviar {len(mensaje)} bytes a {len(self.output_queues_direct)} colas directas.")
        try:
            if mensaje is None:
                return

            # Parseo on-demand (seguro)
            if payload is None:
                try:
                    payload = json.loads(mensaje.decode('utf-8'))
                except:
                    payload = {}
            
            # Protección: Asegurar que las listas existan antes de medir su len()
            c_direct = self.output_queues_direct if self.output_queues_direct is not None else []
            c_sharded = self.output_queues_sharded if self.output_queues_sharded is not None else []
            
            # Log seguro
            logger.info(f"[ROUTER DEBUG] Enviando {len(mensaje)} bytes a {len(c_direct)} colas directas.")
            
          
            # Parseo on-demand solo si es necesario para el sharding
            if payload is None:
                try:
                    payload = json.loads(mensaje.decode('utf-8'))
                except:
                    payload = {}
            
            # 1. ENVIAR A COLAS SIMPLES (Una vez a cada una)
            for q in self.output_queues_direct:
                q.send(mensaje)
            
            # 2. ENVIAR A SHARDS (Una vez al shard calculado)
            for shard_meta in self.output_queues_sharded:
                # Comprobamos si es un EOF para saber si requiere broadcast
                es_eof = payload.get("EOF", False)
                
                if es_eof:
                    # El EOF sí debe llegar a TODOS para cerrar el pipeline
                    for q in shard_meta["queues"].values():
                        q.send(mensaje)
                else:
                    hash_fields = shard_meta.get("hash_fields", [])
                    valor_hash = "|".join(str(payload.get(f, "")) for f in hash_fields) if hash_fields else "default"
                    logger.info(f"[DEBUG ROUTER] hash_fields={hash_fields} | valor_hash={valor_hash}")
                    target_id = sharding.obtener_id_shard(valor_hash, shard_meta["total_workers"])
                    
                    # Accedemos directo al diccionario de colas del shard
                    shard_meta["queues"][target_id].send(mensaje)
                    
        except Exception as e:
            logger.error(f"[Router] Error crítico en el ruteo: {e}", exc_info=True)

    def stop_consuming(self):
        for iq in self.input_queues.values(): iq.stop_consuming()

    def close(self):
        for iq in self.input_queues.values(): iq.close()
        for oq in self.output_queues_direct: oq.close()
        for shard_meta in self.output_queues_sharded:
            for sq in shard_meta["queues"].values(): sq.close()