import socket
import uuid
import json
import gc
import ctypes
from common.logger import obtener_logger
from common import message_protocol, middleware, sharding
from config import GatewayConfig

logger = obtener_logger(__name__)


class ClientHandler:
    FIRST_ELEMENT = 0

    def __init__(self, config: GatewayConfig, state):
        self.config = config
        self.state = state

    def atender(self, client_socket):
        client_id = self._leer_hello(client_socket)
        if not client_id:
            client_socket.close()
            return

        estado = self.state.cargar_estado_cliente(client_id)
        datos_ya_enviados = estado.get("datos_enviados", False)
        queries_ya_entregadas = set(estado.get("queries_entregadas", []))

        logger.info(f"Cliente {client_id} conectado (datos_enviados={datos_ya_enviados}, queries_entregadas={queries_ya_entregadas})")

        self.state.registrar_cliente(client_id, client_socket)

        # Refleja en eof_status las queries ya entregadas en sesiones anteriores
        if queries_ya_entregadas:
            _, _, eof_status = self.state.obtener_cliente(client_id)
            if eof_status is not None:
                eof_status.update(queries_ya_entregadas)

        self._enviar_config(client_socket, client_id, datos_ya_enviados)

        if datos_ya_enviados:
            self._modo_solo_resultados(client_id, client_socket)
        else:
            self._modo_normal(client_id, client_socket)

    def _leer_hello(self, client_socket):
        """Lee el mensaje HELLO del cliente y retorna el client_id (nuevo o existente)."""
        try:
            msg_type, payload = message_protocol.external.recv_msg(client_socket)
            if msg_type == message_protocol.external.MsgType.HELLO:
                data = json.loads(payload)
                cid = data.get("client_id", "").strip()
                if cid:
                    logger.info(f"Cliente reconectando con ID existente: {cid}")
                    return cid
                nuevo_id = str(uuid.uuid4())
                logger.info(f"Nuevo cliente, asignando ID: {nuevo_id}")
                return nuevo_id
        except Exception as e:
            logger.error(f"Error leyendo HELLO del cliente: {e}")
        return None

    def _obtener_lista_queries(self):
        queries = []
        for q in self.config.input_queues:
            try:
                queries.append(int(q.split('_')[0][1:]))
            except (IndexError, ValueError):
                pass
        queries.sort()
        return queries

    def _enviar_config(self, client_socket, client_id, omitir_envio):
        try:
            config_payload = json.dumps({
                "queries": self._obtener_lista_queries(),
                "client_id": client_id,
                "omitir_envio": omitir_envio,
            })
            message_protocol.external.send_msg(
                client_socket, message_protocol.external.MsgType.CONFIG_QUERIES, config_payload
            )
        except Exception as e:
            logger.error(f"Error enviando CONFIG_QUERIES a {client_id}: {e}")

    def _modo_solo_resultados(self, client_id, client_socket):
        """
        El cliente ya envió todos sus datos en una sesión anterior.
        Solo esperamos a que BackendListener termine de entregar los resultados
        y el cliente cierre la conexión.
        """
        logger.info(f"Cliente {client_id} en modo solo-resultados")
        try:
            while True:
                message_protocol.external.recv_msg(client_socket)
        except Exception:
            pass
        finally:
            try:
                client_socket.close()
            except Exception:
                pass
            gc.collect()
            try:
                ctypes.cdll.LoadLibrary("libc.so.6").malloc_trim(0)
            except Exception:
                pass

    def _modo_normal(self, client_id, client_socket):
        """Recibe lotes del cliente, los publica en RabbitMQ y espera el END_OF_RECORDS."""
        colas_tx = [middleware.MessageMiddlewareQueueRabbitMQ(self.config.mom_host, q) for q in self.config.output_queues]
        colas_bancos = {}

        if self.config.bank_queue_config:
            prefix = self.config.bank_queue_config.get("queue_shard_prefix")
            total_workers = self.config.bank_queue_config.get("total_workers", self.config.DEFAULT_WORKERS)
            for i in range(1, total_workers + 1):
                colas_bancos[i] = middleware.MessageMiddlewareQueueRabbitMQ(self.config.mom_host, f"{prefix}_{i}")

        eof_enviado = False
        try:
            while True:
                msg_type, payload = message_protocol.external.recv_msg(client_socket)

                if msg_type == message_protocol.external.MsgType.LOTE_TRANSACCIONES:
                    self._reenviar_lote_tx(client_id, client_socket, payload, colas_tx)

                elif msg_type == message_protocol.external.MsgType.LOTE_BANCOS:
                    self._reenviar_lote_bancos(client_id, client_socket, payload, colas_bancos)

                elif msg_type == message_protocol.external.MsgType.END_OF_RECODS:
                    eof_msg = json.dumps({"client_id": client_id, "EOF": True}).encode("utf-8")
                    for q in colas_tx:
                        q.send(eof_msg)
                    for q in colas_bancos.values():
                        q.send(eof_msg)
                    eof_enviado = True
                    logger.info(f"EOF enviado para {client_id}")

                    # Persiste que los datos fueron enviados al sistema
                    estado = self.state.cargar_estado_cliente(client_id)
                    estado["datos_enviados"] = True
                    self.state.guardar_estado_cliente(client_id, estado)
                    break

        except socket.error:
            logger.warning(f"Cliente {client_id} desconectado abruptamente")
        except Exception as e:
            logger.error(f"Error con cliente {client_id}: {e}", exc_info=True)
        finally:
            if not eof_enviado:
                self._enviar_disconnect(client_id, colas_tx, colas_bancos)
                self.state.remover_cliente(client_id)
            for q in colas_tx:
                q.close()
            for q in colas_bancos.values():
                q.close()
            gc.collect()
            try:
                ctypes.cdll.LoadLibrary("libc.so.6").malloc_trim(0)
            except Exception:
                pass

    def _reenviar_lote_tx(self, client_id, client_socket, payload, colas_tx):
        header = payload["header"]
        schema = self._deduplicar_schema(header["schema"])
        header["schema"] = schema
        records = payload["payload"]

        internal_msg = json.dumps({
            "client_id": client_id,
            "request_id": str(uuid.uuid4()),
            "batches": [{"header": header, "payload": records}]
        }).encode("utf-8")
        for q in colas_tx:
            q.send(internal_msg)

        _, lock, _ = self.state.obtener_cliente(client_id)
        if lock:
            with lock:
                message_protocol.external.send_msg(client_socket, message_protocol.external.MsgType.ACK)

    def _reenviar_lote_bancos(self, client_id, client_socket, payload, colas_bancos):
        if not self.config.bank_queue_config:
            _, lock, _ = self.state.obtener_cliente(client_id)
            if lock:
                with lock:
                    message_protocol.external.send_msg(client_socket, message_protocol.external.MsgType.ACK)
            return

        header = payload["header"]
        schema = self._deduplicar_schema(header["schema"])
        header["schema"] = schema
        records = payload["payload"]

        hash_field = self.config.bank_queue_config.get("hash_field", "Bank ID")
        total_workers = self.config.bank_queue_config.get("total_workers", 1)
        hash_idx = schema.index(hash_field) if hash_field in schema else None

        records_by_shard = {}
        for record_values in records:
            bank_val = record_values[hash_idx] if hash_idx is not None else "default"
            shard_id = sharding.obtener_id_shard(bank_val, total_workers)
            records_by_shard.setdefault(shard_id, []).append(record_values)

        for shard_id, shard_records in records_by_shard.items():
            shard_batch = json.dumps({
                "client_id": client_id,
                "request_id": str(uuid.uuid4()),
                "batches": [{
                    "header": {"schema": schema, "client_id": client_id, "count": len(shard_records)},
                    "payload": shard_records
                }]
            }).encode("utf-8")
            colas_bancos[shard_id].send(shard_batch)

        _, lock, _ = self.state.obtener_cliente(client_id)
        if lock:
            with lock:
                message_protocol.external.send_msg(client_socket, message_protocol.external.MsgType.ACK)

    @staticmethod
    def _deduplicar_schema(schema):
        limpio = []
        counts = {}
        for col in schema:
            if col in counts:
                counts[col] += 1
                limpio.append(f"{col}.{counts[col]}")
            else:
                counts[col] = 0
                limpio.append(col)
        return limpio

    def _enviar_disconnect(self, client_id, colas_tx, colas_bancos):
        disconnect_msg = json.dumps({"client_id": client_id, "CLIENT_DISCONNECT": True}).encode("utf-8")
        logger.info(f"Enviando CLIENT_DISCONNECT para {client_id}")
        for q in colas_tx:
            try:
                q.send(disconnect_msg)
            except Exception as e:
                logger.warning(f"No se pudo enviar CLIENT_DISCONNECT a cola tx: {e}")
        for q in colas_bancos.values():
            try:
                q.send(disconnect_msg)
            except Exception as e:
                logger.warning(f"No se pudo enviar CLIENT_DISCONNECT a cola bancos: {e}")
