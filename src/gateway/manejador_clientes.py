import socket
import uuid
import json
import gc
import ctypes
import threading
from queue import Queue
from common.logger import obtener_logger
from common.crash_hook import CrashHook
from common import message_protocol, middleware, sharding
from common import crash_points as CP
from common.constantes_protocolo import (
    CABECERA, ESQUEMA, PAYLOAD, ID_CLIENTE, ID_SOLICITUD, LOTES, CANTIDAD,
    FIN_DE_ARCHIVO, DESCONEXION_CLIENTE, ID_SESION,
    CONF_PREFIJO_SHARD, CONF_TOTAL_WORKERS, CONF_CAMPO_HASH,
)
from config import GatewayConfig
from constantes import TAMANIO_BUFFER_PUBLICACION

logger = obtener_logger(__name__)


class ManejadorClientes:

    def __init__(self, config: GatewayConfig, estado):
        self.config = config
        self.estado = estado
        self._hook = CrashHook()

    def atender(self, client_socket, client_id):
        estado_cliente = self.estado.cargar_estado_cliente(client_id)
        datos_ya_enviados = estado_cliente.get("datos_enviados", False)
        queries_ya_entregadas = set(estado_cliente.get("queries_entregadas", []))

        logger.info(f"Cliente {client_id} conectado (datos_enviados={datos_ya_enviados}, queries_entregadas={queries_ya_entregadas})")

        self.estado.registrar_cliente(client_id, client_socket)

        _, lock, eof_status = self.estado.obtener_cliente(client_id)
        with lock:
            if queries_ya_entregadas and eof_status is not None:
                eof_status.update(queries_ya_entregadas)
            self._enviar_config(client_socket, client_id, datos_ya_enviados)

        if datos_ya_enviados:
            sesion_guardada = estado_cliente.get("session_id")
            if sesion_guardada:
                self.estado.registrar_sesion(client_id, sesion_guardada)
            if len(queries_ya_entregadas) >= len(self._obtener_lista_queries()):
                logger.info(f"Cliente {client_id}: todas las queries ya entregadas, enviando FIN_DE_REGISTROS directo")
                self.estado.esperar_socket_resultados(client_id, timeout=30)
                results_sock, results_lock, _ = self.estado.obtener_socket_resultados(client_id)
                if results_sock:
                    try:
                        message_protocol.external.enviar_mensaje(
                            results_sock, message_protocol.external.TipoMensaje.FIN_DE_REGISTROS
                        )
                    except Exception as e:
                        logger.warning(f"Error enviando FIN_DE_REGISTROS a {client_id}: {e}")
                self.estado.limpiar_estado_cliente(client_id)
                self.estado.remover_cliente(client_id)
                client_socket.close()
                return
            client_socket.close()
        else:
            self._modo_normal(client_id, client_socket)

    def leer_acks_resultados(self, client_id, results_socket):
        try:
            while True:
                tipo_mensaje, payload = message_protocol.external.recibir_mensaje(results_socket)
                if tipo_mensaje == message_protocol.external.TipoMensaje.ACK_RESULTADO:
                    data = json.loads(payload)
                    self.estado.notificar_ack(client_id, data.get("batch_id"))
        except Exception:
            pass
        finally:
            self.estado.cancelar_acks_cliente(client_id)
            try:
                results_socket.close()
            except Exception:
                pass
            gc.collect()
            try:
                ctypes.cdll.LoadLibrary("libc.so.6").malloc_trim(0)
            except Exception:
                pass

    # --- Modo normal: recibir datos y publicar a RabbitMQ ---

    def _modo_normal(self, client_id, client_socket):
        colas_tx = [middleware.MessageMiddlewareQueueRabbitMQ(self.config.mom_host, q) for q in self.config.output_queues]
        colas_bancos = {}

        if self.config.bank_queue_config:
            prefix = self.config.bank_queue_config.get(CONF_PREFIJO_SHARD)
            total_workers = self.config.bank_queue_config.get(CONF_TOTAL_WORKERS, self.config.DEFAULT_WORKERS)
            for i in range(1, total_workers + 1):
                colas_bancos[i] = middleware.MessageMiddlewareQueueRabbitMQ(self.config.mom_host, f"{prefix}_{i}")

        estado_previo = self.estado.cargar_estado_cliente(client_id)
        if estado_previo.get("conectado"):
            sesion_vieja = estado_previo.get("session_id")
            logger.info(f"Cliente {client_id} reconectando — enviando DISCONNECT (sesión {sesion_vieja})")
            self._enviar_disconnect(client_id, colas_tx, colas_bancos, sesion_vieja)

        self._hook.verificar(CP.GW_BEFORE_PERSIST_CONNECTED, f"pre-conectado {client_id}")

        session_id = str(uuid.uuid4())[:8]
        self.estado.registrar_sesion(client_id, session_id)
        self.estado.actualizar_estado_cliente(client_id, {"conectado": True, "session_id": session_id})
        logger.info(f"Cliente {client_id} sesión {session_id} iniciada")

        self._hook.verificar(CP.GW_AFTER_PERSIST_CONNECTED, f"post-conectado {client_id}")

        buffer = Queue(maxsize=TAMANIO_BUFFER_PUBLICACION)
        error_publicacion = threading.Event()
        hilo_pub = threading.Thread(
            target=self._hilo_publicador,
            args=(buffer, colas_tx, colas_bancos, client_id, error_publicacion),
            daemon=True,
            name=f"publisher-{client_id[:8]}",
        )
        hilo_pub.start()

        eof_enviado = False
        try:
            while True:
                if error_publicacion.is_set():
                    raise RuntimeError("Hilo publicador falló")

                tipo_mensaje, payload = message_protocol.external.recibir_mensaje(client_socket)

                if tipo_mensaje == message_protocol.external.TipoMensaje.LOTE_TRANSACCIONES:
                    self._encolar_lote_tx(client_id, payload, colas_tx, buffer)

                elif tipo_mensaje == message_protocol.external.TipoMensaje.LOTE_BANCOS:
                    self._encolar_lote_bancos(client_id, payload, colas_bancos, buffer)

                elif tipo_mensaje == message_protocol.external.TipoMensaje.FIN_DE_REGISTROS:
                    if client_id is None:
                        client_id = payload or str(uuid.uuid4())
                        self.estado.registrar_cliente(client_id, client_socket)

                    eof_msg = json.dumps({ID_CLIENTE: client_id, FIN_DE_ARCHIVO: True}).encode("utf-8")
                    buffer.put(("eof", eof_msg))
                    buffer.put(None)
                    hilo_pub.join()
                    eof_enviado = True
                    logger.info(f"EOF enviado para {client_id}")

                    self._hook.verificar(CP.GW_BEFORE_PERSIST_DATOS_ENVIADOS, f"pre-datos_enviados {client_id}")
                    self.estado.actualizar_estado_cliente(client_id, {"datos_enviados": True, "session_id": session_id})
                    self._hook.verificar(CP.GW_AFTER_PERSIST_DATOS_ENVIADOS, f"post-datos_enviados {client_id}")
                    break

        except socket.error:
            logger.warning(f"Cliente {client_id} desconectado abruptamente")
        except Exception as e:
            logger.error(f"Error con cliente {client_id}: {e}", exc_info=True)
        finally:
            if not eof_enviado:
                sesion_actual = self.estado.obtener_sesion(client_id)
                disconnect_msg = json.dumps({
                    ID_CLIENTE: client_id,
                    DESCONEXION_CLIENTE: True,
                    ID_SESION: sesion_actual,
                }).encode("utf-8") if sesion_actual else None
                if disconnect_msg:
                    buffer.put(("eof", disconnect_msg))
                buffer.put(None)
                hilo_pub.join(timeout=10)
                self.estado.remover_cliente(client_id)

        try:
            client_socket.close()
        except Exception:
            pass

    # --- Publicador a RabbitMQ ---

    def _hilo_publicador(self, buffer, colas_tx, colas_bancos, client_id, error_event):
        try:
            while True:
                item = buffer.get()
                if item is None:
                    break
                tipo, datos = item
                if tipo == "tx":
                    for q in colas_tx:
                        q.send(datos[q.queue_name])
                elif tipo == "bancos":
                    for shard_id, msg in datos.items():
                        colas_bancos[shard_id].send(msg)
                elif tipo == "eof":
                    for q in colas_tx:
                        q.send(datos)
                    for q in colas_bancos.values():
                        q.send(datos)
                elif tipo == "disconnect":
                    for q in colas_tx:
                        q.send(datos[0])
                    for q in colas_bancos.values():
                        q.send(datos[0])
        except Exception as e:
            logger.error(f"Error en hilo publicador para {client_id}: {e}", exc_info=True)
            error_event.set()
        finally:
            for q in colas_tx:
                q.close()
            for q in colas_bancos.values():
                q.close()

    # --- Preparación de lotes ---

    def _encolar_lote_tx(self, client_id, payload, colas_tx, buffer):
        header = payload[CABECERA]
        schema = self._deduplicar_schema(header[ESQUEMA])
        header[ESQUEMA] = schema
        records = payload[PAYLOAD]

        msgs = {}
        for q in colas_tx:
            req_id = self.estado.generar_request_id(client_id, q.queue_name)
            msgs[q.queue_name] = json.dumps({
                ID_CLIENTE: client_id,
                ID_SOLICITUD: req_id,
                LOTES: [{CABECERA: header, PAYLOAD: records}],
            }).encode("utf-8")
        buffer.put(("tx", msgs))

        self._hook.verificar(CP.GW_UPSTREAM_BEFORE_ACK, f"upstream {client_id}")

    def _encolar_lote_bancos(self, client_id, payload, colas_bancos, buffer):
        if not self.config.bank_queue_config:
            return

        header = payload[CABECERA]
        schema = self._deduplicar_schema(header[ESQUEMA])
        header[ESQUEMA] = schema
        records = payload[PAYLOAD]

        hash_field = self.config.bank_queue_config.get(CONF_CAMPO_HASH, "Bank ID")
        total_workers = self.config.bank_queue_config.get(CONF_TOTAL_WORKERS, 1)
        hash_idx = schema.index(hash_field) if hash_field in schema else None

        records_by_shard = {}
        for record_values in records:
            bank_val = record_values[hash_idx] if hash_idx is not None else "default"
            shard_id = sharding.obtener_id_shard(bank_val, total_workers)
            records_by_shard.setdefault(shard_id, []).append(record_values)

        msgs = {}
        for shard_id, shard_records in records_by_shard.items():
            req_id = self.estado.generar_request_id(client_id, colas_bancos[shard_id].queue_name)
            msgs[shard_id] = json.dumps({
                ID_CLIENTE: client_id,
                ID_SOLICITUD: req_id,
                LOTES: [{
                    CABECERA: {ESQUEMA: schema, ID_CLIENTE: client_id, CANTIDAD: len(shard_records)},
                    PAYLOAD: shard_records,
                }],
            }).encode("utf-8")
        buffer.put(("bancos", msgs))

        self._hook.verificar(CP.GW_UPSTREAM_BEFORE_ACK, f"upstream {client_id}")

    # --- Helpers ---

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
            message_protocol.external.enviar_mensaje(
                client_socket, message_protocol.external.TipoMensaje.CONFIG_QUERIES, config_payload
            )
        except Exception as e:
            logger.error(f"Error enviando CONFIG_QUERIES a {client_id}: {e}")

    def _enviar_disconnect(self, client_id, colas_tx, colas_bancos, session_id=None):
        payload = {ID_CLIENTE: client_id, DESCONEXION_CLIENTE: True}
        if session_id:
            payload[ID_SESION] = session_id
        disconnect_msg = json.dumps(payload).encode("utf-8")
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
