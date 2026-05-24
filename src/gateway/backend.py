import json
import re
import logging
from common import message_protocol, middleware
from config import GatewayConfig

logger = logging.getLogger(__name__)

class BackendListener:
    QUERY_PATTERN = r'q(\d+)'
    FIRST_GROUP = 1

    def __init__(self, config: GatewayConfig, state):
        self.config = config
        self.state = state

    def escuchar(self, cola_nombre: str):
        match = re.search(self.QUERY_PATTERN, cola_nombre)
        query_id = int(match.group(self.FIRST_GROUP)) if match else cola_nombre
        
        cola_entrada = middleware.MessageMiddlewareQueueRabbitMQ(self.config.mom_host, cola_nombre)
        cola_entrada.start_consuming(lambda body, ack, nack: self._procesar_respuesta(query_id, cola_nombre, body, ack, nack))

    def _procesar_respuesta(self, query_id, cola_nombre, body, ack, nack):
        if not self.state.servidor_corriendo:
            return nack()
        
        try:
            transaccion = json.loads(body.decode("utf-8"))
            client_id = transaccion.pop("client_id", None)
            logger.info(f"[RESULTADO FINAL RECIBIDO] -> {transaccion}")
            if not client_id:
                ack()
                return
            
            sock, lock, eof_status = self.state.obtener_cliente(client_id)
            if not sock or not lock or eof_status is None:
                ack()
                return
            
            es_eof = transaccion.pop("EOF", False) or transaccion.pop("eof", False)
            transaccion["eof"] = es_eof
            
            payload = {
                "query": query_id,
                "resultado": transaccion
            }
            payload_str = json.dumps(payload)
            
            with lock:
                message_protocol.external.send_msg(
                    sock,
                    message_protocol.external.MsgType.REPORTE,
                    payload_str
                )
            
            if es_eof:
                logger.info(f"EOF enviado para {cola_nombre} a {client_id}")
                eof_status.add(cola_nombre)
                if len(eof_status) == self.config.num_queries:
                    logger.info(f"Todas las queries finalizadas para {client_id}")
                    with lock:
                        message_protocol.external.send_msg(
                            sock,
                            message_protocol.external.MsgType.END_OF_RECODS
                        )
                    self.state.remover_cliente(client_id)
            ack()
        except json.JSONDecodeError:
            logger.error("JSON invalido")
            ack()
        except Exception as e:
            logger.error(f"Error procesando respuesta: {e}", exc_info=True)
            nack()