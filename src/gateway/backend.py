import json
import re
from common.logger import obtener_logger
from common import message_protocol, middleware
from config import GatewayConfig

logger = obtener_logger(__name__)

class BackendListener:
    QUERY_PATTERN = r'q(\d+)'
    FIRST_GROUP = 1

    LOTE_Q4 = 500

    def __init__(self, config: GatewayConfig, state):
        self.config = config
        self.state = state
        self._processed_hashes = {}  # {client_id: set(request_ids)}
        self._q4_cuentas = {}  # {client_id: set of (bank, account)}

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
            if not client_id:
                ack()
                return
            
            sock, lock, eof_status = self.state.obtener_cliente(client_id)
            if not sock or not lock or eof_status is None:
                ack()
                return
            
            es_eof = transaccion.pop("EOF", False) or transaccion.pop("eof", False)
            
            if es_eof:
                columns_hint = None
                if query_id == 4:
                    self._enviar_cuentas_q4(client_id, sock, lock)
                    columns_hint = ["Bank", "Account"]

                payload = {
                    "query": query_id,
                    "resultado": {"eof": True},
                }
                if columns_hint:
                    payload["columns"] = columns_hint
                payload_str = json.dumps(payload)
                with lock:
                    message_protocol.external.send_msg(
                        sock,
                        message_protocol.external.MsgType.REPORTE,
                        payload_str
                    )
                logger.info(f"EOF enviado para {cola_nombre} a {client_id}")
                eof_status.add(cola_nombre)
                if len(eof_status) == self.config.num_queries:
                    logger.info(f"Todas las queries finalizadas para {client_id}")
                    with lock:
                        message_protocol.external.send_msg(
                            sock,
                            message_protocol.external.MsgType.END_OF_RECODS
                        )
                    self._processed_hashes.pop(client_id, None)
                    self._q4_cuentas.pop(client_id, None)
                    self.state.remover_cliente(client_id)
                ack()
                return

            if "batches" in transaccion:
                request_id = transaccion.get("request_id")
                if request_id:
                    if client_id not in self._processed_hashes:
                        self._processed_hashes[client_id] = set()
                    if request_id in self._processed_hashes[client_id]:
                        logger.info(f"Ignorando mensaje duplicado request_id={request_id} en {cola_nombre} para {client_id}")
                        ack()
                        return
                    self._processed_hashes[client_id].add(request_id)

                if query_id == 4:
                    self._acumular_cuentas_q4(client_id, transaccion["batches"])
                    ack()
                    return

                for batch in transaccion["batches"]:
                    header = batch["header"]
                    schema = header["schema"]
                    records = batch["payload"]

                    logger.info(f"Resultado recibido para {cola_nombre} a {client_id} con {len(records)} registros.")
                    resultado_lista = [
                        {**dict(zip(schema, record_values)), "eof": False}
                        for record_values in records
                    ]
                    payload = {
                        "query": query_id,
                        "resultado": resultado_lista
                    }
                    payload_str = json.dumps(payload)
                    with lock:
                        message_protocol.external.send_msg(
                            sock,
                            message_protocol.external.MsgType.REPORTE,
                            payload_str
                        )
            else:
                transaccion["eof"] = False
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
            ack()
        except json.JSONDecodeError:
            logger.error("JSON invalido")
            ack()
        except (BrokenPipeError, ConnectionResetError, OSError) as e:
            logger.warning(f"Cliente {client_id} desconectado, descartando resultado: {e}")
            self._processed_hashes.pop(client_id, None)
            self._q4_cuentas.pop(client_id, None)
            self.state.remover_cliente(client_id)
            ack()
        except Exception as e:
            logger.error(f"Error procesando respuesta: {e}", exc_info=True)
            nack()

    def _acumular_cuentas_q4(self, client_id: str, batches: list):
        cuentas = self._q4_cuentas.setdefault(client_id, set())
        for batch in batches:
            schema = batch["header"]["schema"]
            records = batch["payload"]
            try:
                fb_idx = schema.index("From Bank")
                fa_idx = schema.index("From Account")
                tb_idx = schema.index("To Bank")
                ta_idx = schema.index("To Account")
            except ValueError as e:
                logger.warning(f"[Q4] Schema inesperado {schema}: {e}")
                continue
            for r in records:
                cuentas.add((str(r[fb_idx]), str(r[fa_idx])))
                cuentas.add((str(r[tb_idx]), str(r[ta_idx])))
        logger.info(f"[Q4] Cuentas únicas acumuladas para {client_id}: {len(cuentas)}")

    def _enviar_cuentas_q4(self, client_id: str, sock, lock):
        cuentas = self._q4_cuentas.pop(client_id, set())
        registros = sorted(cuentas)
        logger.info(f"[Q4] Enviando {len(registros)} cuentas únicas a {client_id}")
        for i in range(0, max(1, len(registros)), self.LOTE_Q4):
            lote = registros[i:i + self.LOTE_Q4]
            if not lote:
                break
            resultado_lista = [{"Bank": b, "Account": a, "eof": False} for b, a in lote]
            payload_str = json.dumps({"query": 4, "resultado": resultado_lista})
            with lock:
                message_protocol.external.send_msg(sock, message_protocol.external.MsgType.REPORTE, payload_str)