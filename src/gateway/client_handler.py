import socket
import uuid
import json
import logging
from common import message_protocol, middleware, sharding
from config import GatewayConfig

logger = logging.getLogger(__name__)

class ClientHandler:
    FIRST_ELEMENT = 0

    def __init__(self, config: GatewayConfig, state):
        self.config = config
        self.state = state

    def atender(self, client_socket):
        client_id = None
        
        colas_tx = [middleware.MessageMiddlewareQueueRabbitMQ(self.config.mom_host, q) for q in self.config.output_queues]
        colas_bancos = {}
        
        if self.config.bank_queue_config:
            prefix = self.config.bank_queue_config.get("queue_shard_prefix")
            total_workers = self.config.bank_queue_config.get("total_workers", self.config.DEFAULT_WORKERS)
            for i in range(1, total_workers + 1):
                colas_bancos[i] = middleware.MessageMiddlewareQueueRabbitMQ(self.config.mom_host, f"{prefix}_{i}")
        
        try:
            while True:
                msg_type, payload = message_protocol.external.recv_msg(client_socket)
                
                if msg_type == message_protocol.external.MsgType.LOTE_TRANSACCIONES:
                    header = payload["header"]
                    msg_client_id = header["client_id"]
                    schema = header["schema"]
                    records = payload["payload"]

                    if client_id is None:
                        client_id = msg_client_id
                        self.state.registrar_cliente(client_id, client_socket)
                        logger.info(f"Cliente {client_id} conectado")

                    # Convert "From Bank" in-place if present
                    if "From Bank" in schema:
                        idx = schema.index("From Bank")
                        for record_values in records:
                            val = record_values[idx]
                            if isinstance(val, str) and val.isdigit():
                                record_values[idx] = int(val)
                            elif isinstance(val, int):
                                record_values[idx] = val

                    # Construct internal batch message
                    internal_msg = {
                        "client_id": client_id,
                        "batches": [
                            {
                                "header": header,
                                "payload": records
                            }
                        ]
                    }
                    msg_bytes = json.dumps(internal_msg).encode("utf-8")
                    for q in colas_tx:
                        q.send(msg_bytes)
                    
                    _, lock, _ = self.state.obtener_cliente(client_id)
                    if lock:
                        with lock:
                            message_protocol.external.send_msg(client_socket, message_protocol.external.MsgType.ACK)
                            
                elif msg_type == message_protocol.external.MsgType.LOTE_BANCOS:
                    header = payload["header"]
                    msg_client_id = header["client_id"]
                    schema = header["schema"]
                    records = payload["payload"]

                    if client_id is None:
                        client_id = msg_client_id
                        self.state.registrar_cliente(client_id, client_socket)
                        logger.info(f"Cliente {client_id} conectado")

                    if not self.config.bank_queue_config:
                        _, lock, _ = self.state.obtener_cliente(client_id)
                        if lock:
                            with lock:
                                message_protocol.external.send_msg(client_socket, message_protocol.external.MsgType.ACK)
                        continue

                    hash_field = self.config.bank_queue_config.get("hash_field", "Bank ID")
                    total_workers = self.config.bank_queue_config.get("total_workers", 1)

                    if hash_field in schema:
                        hash_idx = schema.index(hash_field)
                    else:
                        hash_idx = None

                    # Group records by shard_id
                    records_by_shard = {}
                    for record_values in records:
                        bank_val = record_values[hash_idx] if hash_idx is not None else "default"
                        shard_id = sharding.obtener_id_shard(bank_val, total_workers)
                        if shard_id not in records_by_shard:
                            records_by_shard[shard_id] = []
                        records_by_shard[shard_id].append(record_values)

                    # Send a batch to each shard
                    for shard_id, shard_records in records_by_shard.items():
                        shard_batch = {
                            "client_id": client_id,
                            "batches": [
                                {
                                    "header": {
                                        "schema": schema,
                                        "client_id": client_id,
                                        "count": len(shard_records)
                                    },
                                    "payload": shard_records
                                }
                            ]
                        }
                        colas_bancos[shard_id].send(json.dumps(shard_batch).encode("utf-8"))
                    
                    _, lock, _ = self.state.obtener_cliente(client_id)
                    if lock:
                        with lock:
                            message_protocol.external.send_msg(client_socket, message_protocol.external.MsgType.ACK)
                            
                elif msg_type == message_protocol.external.MsgType.END_OF_RECODS:
                    if client_id is None:
                        client_id = payload or str(uuid.uuid4())
                        self.state.registrar_cliente(client_id, client_socket)
                        logger.info(f"Cliente {client_id} conectado (END_OF_RECODS)")
                        
                    eof_msg = json.dumps({"client_id": client_id, "EOF": True}).encode("utf-8")
                    for q in colas_tx:
                        q.send(eof_msg)
                    for q in colas_bancos.values():
                        q.send(eof_msg)
                    logger.info(f"EOF enviado para {client_id}")
                    break
        except socket.error:
            logger.warning(f"Cliente {client_id or 'Desconocido'} desconectado abruptamente")
        except Exception as e:
            logger.error(f"Error con cliente {client_id or 'Desconocido'}: {e}", exc_info=True)
        finally:
            for q in colas_tx:
                q.close()
            for q in colas_bancos.values():
                q.close()