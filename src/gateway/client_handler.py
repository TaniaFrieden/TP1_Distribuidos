import socket
import uuid
import json
import logging
from common import message_protocol, middleware
from config import GatewayConfig
from utils import ShardHasher

logger = logging.getLogger(__name__)

class ClientHandler:
    FIRST_ELEMENT = 0

    def __init__(self, config: GatewayConfig, state):
        self.config = config
        self.state = state

    def atender(self, client_socket):
        client_id = str(uuid.uuid4())
        self.state.registrar_cliente(client_id, client_socket)
        logger.info(f"Cliente {client_id} conectado")
        
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
                    # El payload ya viene como una lista de objetos JSON o el gateway 
                    # simplemente los reenvía.
                    for record_str in payload: # record_str es el string recibido
                        try:
                            # 1. Convertir string a diccionario
                            record_dict = json.loads(record_str)
                            
                            # 2. Ahora sí podemos asignar (el diccionario sí permite esto)
                            record_dict["client_id"] = client_id
                            
                            # 3. Serializar de nuevo para la cola
                            msg_bytes = json.dumps(record_dict).encode("utf-8")
                            for q in colas_tx:
                                q.send(msg_bytes)
                        except json.JSONDecodeError:
                            logger.warning(f"Mensaje descartado por formato JSON inválido: {record_str}")
                    
                    _, lock, _ = self.state.obtener_cliente(client_id)
                    if lock:
                        with lock:
                            message_protocol.external.send_msg(client_socket, message_protocol.external.MsgType.ACK)
                            
                elif msg_type == message_protocol.external.MsgType.LOTE_BANCOS:
                    # Lo mismo aquí: el gateway solo busca la clave que le dijiste por ENV
                    hash_field = self.config.bank_queue_config.get("hash_field", "Bank ID")
                    total_workers = self.config.bank_queue_config.get("total_workers", 1)
                    
                    for record_str in payload:
                        try:
                            banco_dict = json.loads(record_str)
                            banco_dict["client_id"] = client_id
                            
                            bank_val = banco_dict.get(hash_field, "default")
                            shard_id = ShardHasher.obtener_id_shard(bank_val, total_workers)
                            
                            colas_bancos[shard_id].send(json.dumps(banco_dict).encode("utf-8"))
                        except json.JSONDecodeError:
                            logger.warning(f"Mensaje banco descartado: {record_str}")
                    
                    _, lock, _ = self.state.obtener_cliente(client_id)
                    if lock:
                        with lock:
                            message_protocol.external.send_msg(client_socket, message_protocol.external.MsgType.ACK)
                            
                elif msg_type == message_protocol.external.MsgType.END_OF_RECODS:
                    eof_msg = json.dumps({"client_id": client_id, "EOF": True}).encode("utf-8")
                    for q in colas_tx:
                        q.send(eof_msg)
                    for q in colas_bancos.values():
                        q.send(eof_msg)
                    logger.info(f"EOF enviado para {client_id}")
                    break
        except socket.error:
            logger.warning(f"Cliente {client_id} desconectado abruptamente")
        except Exception as e:
            logger.error(f"Error con cliente {client_id}: {e}", exc_info=True)
        finally:
            for q in colas_tx:
                q.close()
            for q in colas_bancos.values():
                q.close()