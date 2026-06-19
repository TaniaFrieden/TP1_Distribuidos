import json
import re
import threading
from queue import Queue
from common.logger import obtener_logger
from common import message_protocol, middleware
from config import GatewayConfig

logger = obtener_logger(__name__)


class BackendListener:
    QUERY_PATTERN = r'q(\d+)'
    FIRST_GROUP = 1
    LOTE_Q4 = 500
    TIMEOUT_RECONEXION = 120

    def __init__(self, config: GatewayConfig, state):
        self.config = config
        self.state = state
        self._processed_hashes = {}  # {client_id: set(request_ids)}
        self._q4_cuentas = {}        # {client_id: set of (bank, account)}
        self._eof_counts = {}        # {client_id: {cola_nombre: eofs_recibidos}}
        self._lock = threading.Lock()
        self._client_queues = {}     # {client_id: Queue}
        self._client_workers = {}    # {client_id: Thread}
        self._queue_lock = threading.Lock()

    def escuchar(self, cola_nombre: str):
        match = re.search(self.QUERY_PATTERN, cola_nombre)
        query_id = int(match.group(self.FIRST_GROUP)) if match else cola_nombre

        cola_entrada = middleware.MessageMiddlewareQueueRabbitMQ(self.config.mom_host, cola_nombre)
        cola_entrada.start_consuming(
            lambda body, ack, nack: self._encolar(query_id, cola_nombre, body, ack, nack)
        )

    # --- Cola serial por cliente ---

    def _encolar(self, query_id, cola_nombre, body, ack, nack):
        """
        Extrae client_id y encola el mensaje en la cola serial del cliente.
        El callback de pika retorna inmediatamente sin bloquearse.
        """
        if not self.state.servidor_corriendo:
            return nack()

        try:
            client_id = json.loads(body).get("client_id")
        except Exception:
            client_id = None

        if not client_id:
            ack()
            return

        self._obtener_o_crear_worker(client_id).put((query_id, cola_nombre, body, ack, nack))

    def _obtener_o_crear_worker(self, client_id: str) -> Queue:
        with self._queue_lock:
            if client_id not in self._client_queues:
                q = Queue()
                self._client_queues[client_id] = q
                t = threading.Thread(
                    target=self._worker_loop,
                    args=(q,),
                    daemon=True,
                    name=f"backend-{client_id[:8]}"
                )
                self._client_workers[client_id] = t
                t.start()
            return self._client_queues[client_id]

    def _worker_loop(self, q: Queue):
        """Procesa mensajes de un cliente en orden, sin bloquear el hilo de pika."""
        while True:
            item = q.get()
            if item is None:  # centinela de parada
                break
            query_id, cola_nombre, body, ack, nack = item
            self._procesar_respuesta(query_id, cola_nombre, body, ack, nack)

    def _detener_worker(self, client_id: str):
        """Envía el centinela de parada al worker y limpia los registros."""
        with self._queue_lock:
            q = self._client_queues.pop(client_id, None)
            self._client_workers.pop(client_id, None)
        if q:
            q.put(None)

    # --- Helpers de estado ---

    def _obtener_socket_o_esperar(self, client_id, ack, nack):
        """
        Retorna (sock, lock, eof_status) si el cliente está conectado.
        Si no está pero tiene estado persistido, espera hasta TIMEOUT_RECONEXION segundos.
        Se ejecuta en el hilo worker del cliente, no en el hilo de pika.
        """
        sock, lock, eof_status = self.state.obtener_cliente(client_id)
        if sock:
            return sock, lock, eof_status

        if self.state.tiene_estado_persistido(client_id):
            logger.info(f"Cliente {client_id} no conectado, esperando reconexión (hasta {self.TIMEOUT_RECONEXION}s)...")
            self.state.esperar_cliente(client_id, timeout=self.TIMEOUT_RECONEXION)
            sock, lock, eof_status = self.state.obtener_cliente(client_id)
            if sock:
                return sock, lock, eof_status
            logger.warning(f"Timeout esperando reconexión de {client_id}, devolviendo mensaje a la cola")
            nack()
            return None, None, None

        ack()
        return None, None, None

    def _cargar_q4_si_necesario(self, client_id):
        with self._lock:
            if client_id not in self._q4_cuentas:
                estado = self.state.cargar_estado_cliente(client_id)
                cuentas_lista = estado.get("q4_cuentas", [])
                self._q4_cuentas[client_id] = set(tuple(par) for par in cuentas_lista)

    def _persistir_estado(self, client_id):
        """Guarda q4_cuentas, queries_entregadas y eof_counts_recibidos en disco."""
        with self._lock:
            q4_snapshot = set(self._q4_cuentas.get(client_id, set()))
            eof_counts_snapshot = dict(self._eof_counts[client_id]) if client_id in self._eof_counts else None

        _, _, eof_status = self.state.obtener_cliente(client_id)
        queries_entregadas = list(eof_status) if eof_status else []

        estado = self.state.cargar_estado_cliente(client_id)
        estado["q4_cuentas"] = [list(par) for par in q4_snapshot]
        estado["queries_entregadas"] = queries_entregadas
        if eof_counts_snapshot is not None:
            estado["eof_counts_recibidos"] = eof_counts_snapshot
        self.state.guardar_estado_cliente(client_id, estado)

    # --- Procesamiento principal ---

    def _procesar_respuesta(self, query_id, cola_nombre, body, ack, nack):
        if not self.state.servidor_corriendo:
            return nack()

        try:
            transaccion = json.loads(body.decode("utf-8"))
            client_id = transaccion.pop("client_id", None)
            if not client_id:
                ack()
                return

            # Saltar queries que ya fueron entregadas en una sesión anterior
            estado_persistido = self.state.cargar_estado_cliente(client_id)
            if cola_nombre in set(estado_persistido.get("queries_entregadas", [])):
                logger.info(f"Query {cola_nombre} ya entregada a {client_id}, descartando")
                ack()
                return

            sock, lock, eof_status = self._obtener_socket_o_esperar(client_id, ack, nack)
            if not sock:
                return

            if query_id == 4:
                self._cargar_q4_si_necesario(client_id)

            es_eof = transaccion.pop("EOF", False) or transaccion.pop("eof", False)

            if es_eof:
                self._procesar_eof(client_id, query_id, cola_nombre, sock, lock, eof_status, ack)
                return

            if "batches" in transaccion:
                request_id = transaccion.get("request_id")
                if request_id:
                    with self._lock:
                        if client_id not in self._processed_hashes:
                            self._processed_hashes[client_id] = set()
                        if request_id in self._processed_hashes[client_id]:
                            logger.info(f"Ignorando duplicado request_id={request_id} para {client_id}")
                            ack()
                            return
                        self._processed_hashes[client_id].add(request_id)

                if query_id == 4:
                    self._acumular_cuentas_q4(client_id, transaccion["batches"])
                    self._persistir_estado(client_id)
                    ack()
                    return

                for batch in transaccion["batches"]:
                    header = batch["header"]
                    schema = header["schema"]
                    records = batch["payload"]
                    resultado_lista = [
                        {**dict(zip(schema, record_values)), "eof": False}
                        for record_values in records
                    ]
                    payload = {"query": query_id, "resultado": resultado_lista}
                    with lock:
                        message_protocol.external.send_msg(
                            sock, message_protocol.external.MsgType.REPORTE, json.dumps(payload)
                        )
            else:
                transaccion["eof"] = False
                payload = {"query": query_id, "resultado": transaccion}
                with lock:
                    message_protocol.external.send_msg(
                        sock, message_protocol.external.MsgType.REPORTE, json.dumps(payload)
                    )
            ack()

        except json.JSONDecodeError:
            logger.error("JSON invalido")
            ack()
        except (BrokenPipeError, ConnectionResetError, OSError) as e:
            logger.warning(f"Cliente {client_id} desconectado al enviar resultado: {e}")
            with self._lock:
                self._processed_hashes.pop(client_id, None)
            self.state.remover_cliente(client_id)
            nack()
        except Exception as e:
            logger.error(f"Error procesando respuesta: {e}", exc_info=True)
            nack()

    def _procesar_eof(self, client_id, query_id, cola_nombre, sock, lock, eof_status, ack):
        esperados = self.config.eofs_esperados.get(cola_nombre, 1)

        # Cargar counts persistidos si es la primera vez en esta sesión
        with self._lock:
            if client_id not in self._eof_counts:
                estado = self.state.cargar_estado_cliente(client_id)
                self._eof_counts[client_id] = dict(estado.get("eof_counts_recibidos", {}))
            counts = self._eof_counts[client_id]
            counts[cola_nombre] = counts.get(cola_nombre, 0) + 1
            recibidos = counts[cola_nombre]

        self._persistir_estado(client_id)

        if recibidos < esperados:
            logger.info(f"EOF parcial {recibidos}/{esperados} para {cola_nombre} ({client_id})")
            ack()
            return

        columns_hint = None
        if query_id == 4:
            self._enviar_cuentas_q4(client_id, sock, lock)
            columns_hint = ["Bank", "Account"]

        payload = {"query": query_id, "resultado": {"eof": True}}
        if columns_hint:
            payload["columns"] = columns_hint
        with lock:
            message_protocol.external.send_msg(
                sock, message_protocol.external.MsgType.REPORTE, json.dumps(payload)
            )
        logger.info(f"EOF completo enviado para {cola_nombre} a {client_id} ({recibidos}/{esperados})")
        eof_status.add(cola_nombre)

        self._persistir_estado(client_id)

        if len(eof_status) == self.config.num_queries:
            logger.info(f"Todas las queries finalizadas para {client_id}")
            with lock:
                message_protocol.external.send_msg(
                    sock, message_protocol.external.MsgType.END_OF_RECODS
                )
            with self._lock:
                self._processed_hashes.pop(client_id, None)
                self._q4_cuentas.pop(client_id, None)
                self._eof_counts.pop(client_id, None)
            self.state.limpiar_estado_cliente(client_id)
            self.state.remover_cliente(client_id)
            self._detener_worker(client_id)

        ack()

    # --- Q4 ---

    def _acumular_cuentas_q4(self, client_id: str, batches: list):
        with self._lock:
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
        with self._lock:
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
