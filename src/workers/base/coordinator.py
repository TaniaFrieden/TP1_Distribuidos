import json
import threading
import logging
from common import middleware
from common.persistencia import PersistidorEstado

logger = logging.getLogger(__name__)

class DistributedCoordinator:
    """Maneja la barrera de sincronización global para los EOFs."""
    def __init__(self, config, on_sync_complete_cb, on_barrier_complete_cb=None):
        self.config = config
        self.on_sync_complete = on_sync_complete_cb
        self.on_barrier_complete = on_barrier_complete_cb
        
        self._coordinaciones_eof = {}
        self._eofs_locales_recibidos = {}
        self._mensajes_en_vuelo = {}
        self._clientes_flusheados = set()
        self._flush_en_progreso = set()  # flush iniciado pero datos aún no publicados downstream
        self._originadores_reconocidos = {}
        self._local_eof_completed = set()
        self._clientes_finalizados = set()
        self._barreras_pendientes = []
        self._flush_completados = {}  # {client_id: originator_id} — persisted, cleared on BARRIER_COMPLETE
        self._worker_finished_pendientes = []  # [(client_id, originator)] to resend on recovery

        nombre_nodo = f"coordinator_{config.node_prefix}_{config.node_id}"
        self._persistidor = PersistidorEstado(nombre_nodo)
        self._persistencia_lock = threading.Lock()
        self._recuperar_estado_coordinacion()
        
        if config.total_workers > 1:
            self._tiene_cola_sharded = any(
                q.endswith(f"_{config.node_id}") or f"_{config.node_id}_" in q or f"_{config.node_id}" in q
                for q in config.input_queues
            )
        else:
            self._tiene_cola_sharded = True
        
        self._coordinacion_lock = threading.Lock()
        self._vuelo_lock = threading.Lock()
        self._vuelo_cv = threading.Condition(self._vuelo_lock)
        self._control_send_lock = threading.Lock()

        self.control_exchange = middleware.FanoutExchangeRabbitMQ(
            config.mom_host, f"control_{config.node_prefix}_exchange"
        )
        self.control_queue = middleware.FanoutQueueRabbitMQ(
            config.mom_host, f"control_{config.node_prefix}_queue_{config.node_id}",
            self.control_exchange.exchange_name
        )

        self._cierre_evento = threading.Event()
        self._rebroadcast_thread = threading.Thread(
            target=self._rebroadcast_loop,
            name=f"Coordinator-Rebroadcast-{config.node_id}",
            daemon=True
        )
        self._rebroadcast_thread.start()

    def _recuperar_estado_coordinacion(self):
        estado = self._persistidor.cargar()
        coordinaciones = estado.get("coordinaciones_eof", {})

        eofs_locales = estado.get("eofs_locales_recibidos", {})
        for client_id, colas in eofs_locales.items():
            self._eofs_locales_recibidos[client_id] = set(colas)

        for client_id, datos in coordinaciones.items():
            mensaje_original = None
            if datos.get("mensaje_payload"):
                mensaje_original = json.dumps(datos["mensaje_payload"]).encode('utf-8')

            workers_confirmados = set(datos.get("workers_confirmados", []))
            self._coordinaciones_eof[client_id] = {
                "workers": workers_confirmados,
                "mensaje_original": mensaje_original
            }
            self._originadores_reconocidos[client_id] = self.config.node_id
            self._local_eof_completed.add(client_id)
            logger.info(
                f"[Coordinator] Recuperando coordinación para {client_id}: "
                f"{len(workers_confirmados)}/{self.config.total_workers} confirmados."
            )

            if len(workers_confirmados) >= self.config.total_workers:
                logger.info(f"[Coordinator] La barrera para {client_id} ya estaba completa. Encolando para enviar BARRIER_COMPLETE.")
                self._barreras_pendientes.append((client_id, mensaje_original))

        # Recover clientes_finalizados so late WORKER_FINISHED messages get a BARRIER_COMPLETE response
        for client_id in estado.get("clientes_finalizados", []):
            self._clientes_finalizados.add(client_id)
            logger.info(f"[Coordinator] Recuperando cliente finalizado: {client_id}.")

        # Recover workers that flushed but never confirmed WORKER_FINISHED
        flush_completados = estado.get("flush_completados", {})
        for client_id, originator in flush_completados.items():
            self._clientes_flusheados.add(client_id)
            self._local_eof_completed.add(client_id)
            self._originadores_reconocidos[client_id] = originator
            self._worker_finished_pendientes.append((client_id, originator))
            logger.info(
                f"[Coordinator] Recuperando flush pendiente para {client_id}: "
                f"ya flusheado, reenviando WORKER_FINISHED al originador {originator}."
            )

    def procesar_barreras_recuperadas(self):
        for cid, msg in self._barreras_pendientes:
            with self._coordinacion_lock:
                if cid in self._coordinaciones_eof:
                    del self._coordinaciones_eof[cid]
                    self._persistir_coordinacion()
                self._clientes_flusheados.discard(cid)
                self._flush_en_progreso.discard(cid)
                self._originadores_reconocidos.pop(cid, None)
                self._local_eof_completed.discard(cid)
                self._clientes_finalizados.add(cid)
                self._enviar_control({
                    "type": "BARRIER_COMPLETE",
                    "client_id": cid
                })
            self.on_sync_complete(cid, msg)
        self._barreras_pendientes.clear()

        for cid, originator in self._worker_finished_pendientes:
            logger.info(f"[Coordinator] Reenviando WORKER_FINISHED pendiente para {cid} al originador {originator}.")
            self._enviar_control({
                "type": "WORKER_FINISHED",
                "client_id": cid,
                "originator": originator,
                "worker_id": self.config.node_id
            })
        self._worker_finished_pendientes.clear()

    def _persistir_coordinacion(self):
        with self._persistencia_lock:
            coordinaciones_serial = {}
            for client_id, datos in self._coordinaciones_eof.items():
                mensaje_payload = None
                if datos.get("mensaje_original"):
                    try:
                        mensaje_payload = json.loads(datos["mensaje_original"].decode('utf-8'))
                    except Exception:
                        pass
                coordinaciones_serial[client_id] = {
                    "workers_confirmados": list(datos["workers"]),
                    "mensaje_payload": mensaje_payload
                }

            eofs_locales_serial = {
                client_id: list(colas)
                for client_id, colas in self._eofs_locales_recibidos.items()
            }

            self._persistidor.guardar({
                "coordinaciones_eof": coordinaciones_serial,
                "eofs_locales_recibidos": eofs_locales_serial,
                "flush_completados": dict(self._flush_completados),
                "clientes_finalizados": list(self._clientes_finalizados),
            })

    def registrar_vuelo(self, client_id):
        with self._vuelo_lock:
            self._mensajes_en_vuelo[client_id] = self._mensajes_en_vuelo.get(client_id, 0) + 1

    def descontar_vuelo(self, client_id):
        with self._vuelo_lock:
            if client_id in self._mensajes_en_vuelo:
                self._mensajes_en_vuelo[client_id] -= 1
                if self._mensajes_en_vuelo[client_id] <= 0:
                    del self._mensajes_en_vuelo[client_id]
                self._vuelo_cv.notify_all()

    def _esperar_vuelo_cero(self, client_id):
        with self._vuelo_lock:
            while self._mensajes_en_vuelo.get(client_id, 0) > 0:
                self._vuelo_cv.wait()

    def registrar_eof_local(self, client_id, queue_name, total_esperados) -> bool:
        with self._coordinacion_lock:
            if client_id not in self._eofs_locales_recibidos:
                self._eofs_locales_recibidos[client_id] = set()
            self._eofs_locales_recibidos[client_id].add(queue_name)
            self._persistir_coordinacion()
            return len(self._eofs_locales_recibidos[client_id]) == total_esperados

    def limpiar_eof_local(self, client_id):
        with self._coordinacion_lock:
            if client_id in self._eofs_locales_recibidos:
                del self._eofs_locales_recibidos[client_id]
                self._persistir_coordinacion()

    def _ejecutar_flush_y_notificar(self, client_id: str, originator: str):
        logger.info(f"[Coordinator] EOF local y de control recibidos para {client_id}. Esperando vuelos a cero antes de flush.")
        self._esperar_vuelo_cero(client_id)

        with self._coordinacion_lock:
            if client_id in self._clientes_flusheados:
                # Los datos ya se publicaron downstream: es seguro reconfirmar WORKER_FINISHED.
                debe_flushear = False
            elif client_id in self._flush_en_progreso:
                # Otro hilo está publicando los datos AHORA mismo. No mandamos WORKER_FINISHED
                # todavía: hacerlo dejaría que el originador reenvíe el EOF downstream y este
                # adelante a los datos que aún viajan (carrera que pierde un shard completo).
                # El rebroadcast del originador reintentará y entrará por la rama
                # _clientes_flusheados una vez que la publicación termine.
                logger.info(f"[Coordinator] Flush en progreso para client_id={client_id}. Postergando WORKER_FINISHED.")
                return
            else:
                debe_flushear = True
                self._flush_en_progreso.add(client_id)

        if debe_flushear:
            logger.info(f"[Coordinator] Vuelos en cero para client_id={client_id}. Flusheando datos locales.")
            # Publicar los datos ANTES de marcar flusheado/persistir: WORKER_FINISHED solo debe
            # emitirse cuando la salida ya está en la cola downstream.
            self.on_sync_complete(client_id, None)
            with self._coordinacion_lock:
                self._flush_en_progreso.discard(client_id)
                self._clientes_flusheados.add(client_id)
                self._flush_completados[client_id] = originator
                self._persistir_coordinacion()
        else:
            logger.info(f"[Coordinator] Ya flusheado para client_id={client_id}. Skip.")

        import os
        if os.environ.get("CRASH_BEFORE_FINISHED_CONFIRMATION") == "true":
            base_dir = os.path.dirname(self._persistidor.directory)
            bandera = os.path.join(base_dir, f"{self.config.node_prefix}_{self.config.node_id}_crash_before_finished_done")
            if not os.path.exists(bandera):
                open(bandera, "w").close()
                logger.warning(f"[Coordinator] CRASH_BEFORE_FINISHED_CONFIRMATION activado — muriendo ANTES de enviar WORKER_FINISHED")
                os._exit(1)

        logger.info(f"[Coordinator] Flush completo. Enviando WORKER_FINISHED a originator {originator}.")
        self._enviar_control({
            "type": "WORKER_FINISHED",
            "client_id": client_id,
            "originator": originator,
            "worker_id": self.config.node_id
        })

    def limpiar_cliente(self, client_id: str):
        with self._coordinacion_lock:
            self._coordinaciones_eof.pop(client_id, None)
            self._originadores_reconocidos.pop(client_id, None)
            self._eofs_locales_recibidos.pop(client_id, None)
            self._clientes_flusheados.discard(client_id)
            self._flush_en_progreso.discard(client_id)
            self._clientes_finalizados.discard(client_id)
        with self._vuelo_lock:
            self._mensajes_en_vuelo.pop(client_id, None)
        self._persistir_coordinacion()

    def esta_eof_local_completo(self, client_id: str) -> bool:
        with self._coordinacion_lock:
            return client_id in self._local_eof_completed

    def iniciar_barrera(self, client_id: str, mensaje_original: bytes):
        ejecutar_flush_inmediato = False
        originator_para_flush = None
        
        with self._coordinacion_lock:
            self._local_eof_completed.add(client_id)
            originator = self._originadores_reconocidos.get(client_id)

            if originator is not None and self._tiene_cola_sharded:
                logger.info(f"[Coordinator] Barrera ya activa para {client_id} (originador: {originator}). Disparando flush diferido.")
                ejecutar_flush_inmediato = True
                originator_para_flush = originator
            else:
                self._originadores_reconocidos[client_id] = self.config.node_id
                self._coordinaciones_eof[client_id] = {
                    "workers": set(),
                    "mensaje_original": mensaje_original
                }
                self._persistir_coordinacion()

                logger.info(
                    f"[Coordinator] EOF local completo para client_id={client_id} (somos originador). Difundiendo control."
                )
                self._enviar_control({
                    "type": "EOF_RECEIVED",
                    "client_id": client_id,
                    "originator": self.config.node_id
                })

        if ejecutar_flush_inmediato:
            self._ejecutar_flush_y_notificar(client_id, originator_para_flush)

    def _enviar_control(self, msg_dict: dict):
        try:
            logger.info(
                f"[Coordinator] Enviando control {msg_dict.get('type')} para client_id={msg_dict.get('client_id')} desde worker {self.config.node_id}."
            )
            with self._control_send_lock:
                self.control_exchange.send(json.dumps(msg_dict).encode('utf-8'))
        except Exception as e:
            logger.error(f"[Coordinator] Error enviando control: {e}")

    def _process_control_message(self, message, ack, nack):
        try:
            msg_dict = json.loads(message.decode('utf-8'))
            msg_type = msg_dict.get("type")
            client_id = msg_dict.get("client_id")
            originator = msg_dict.get("originator")

            if msg_type == "EOF_RECEIVED":
                with self._coordinacion_lock:
                    ya_finalizado = client_id in self._clientes_finalizados or client_id in self._clientes_flusheados
                
                if ya_finalizado:
                    logger.info(f"[Coordinator] EOF_RECEIVED recibido para cliente {client_id} ya finalizado. Respondiendo WORKER_FINISHED de todas formas.")
                    self._enviar_control({
                        "type": "WORKER_FINISHED",
                        "client_id": client_id,
                        "originator": originator,
                        "worker_id": self.config.node_id
                    })
                    return

                with self._coordinacion_lock:
                    originador_actual = self._originadores_reconocidos.get(client_id)

                    if originador_actual is not None:
                        if not self._tiene_cola_sharded:
                            if originator != originador_actual:
                                logger.info(f"[Coordinator] Cola no sharded: actualizando originador de {originador_actual} a {originator}")
                                self._originadores_reconocidos[client_id] = originator
                                if originador_actual == self.config.node_id:
                                    self._coordinaciones_eof.pop(client_id, None)
                                    self._persistir_coordinacion()
                        else:
                            if originator < originador_actual:
                                logger.info(f"[Coordinator] Colisión de barrera: cediendo originador de {originador_actual} a {originator}")
                                self._originadores_reconocidos[client_id] = originator
                                if originador_actual == self.config.node_id:
                                    self._coordinaciones_eof.pop(client_id, None)
                                    self._persistir_coordinacion()
                            elif originator > originador_actual:
                                logger.info(f"[Coordinator] Colisión de barrera: {originator} reclamó, pero yo ({originador_actual}) tengo menor ID. Reenviando mi reclamo para desatascar.")
                                self._enviar_control({
                                    "type": "EOF_RECEIVED",
                                    "client_id": client_id,
                                    "originator": originador_actual
                                })
                                return
                    else:
                        self._originadores_reconocidos[client_id] = originator

                with self._coordinacion_lock:
                    local_completo = (client_id in self._local_eof_completed) or not self._tiene_cola_sharded
                    originator_final = self._originadores_reconocidos[client_id]

                if local_completo:
                    self._ejecutar_flush_y_notificar(client_id, originator_final)
                else:
                    logger.info(f"[Coordinator] EOF_RECEIVED para {client_id} (originator {originator_final}), pero el EOF local aún no está listo. Postergando flush.")

            elif msg_type == "WORKER_FINISHED" and originator == self.config.node_id:
                with self._coordinacion_lock:
                    ya_finalizado = client_id in self._clientes_finalizados
                
                if ya_finalizado:
                    self._enviar_control({
                        "type": "BARRIER_COMPLETE",
                        "client_id": client_id
                    })
                    return

                with self._coordinacion_lock:
                    if client_id in self._coordinaciones_eof:
                        self._coordinaciones_eof[client_id]["workers"].add(msg_dict.get("worker_id"))
                        logger.info(
                            f"[Coordinator] WORKER_FINISHED para client_id={client_id}. "
                            f"Confirmados: {len(self._coordinaciones_eof[client_id]['workers'])}/{self.config.total_workers}."
                        )
                        self._persistir_coordinacion()

                        if len(self._coordinaciones_eof[client_id]["workers"]) >= self.config.total_workers:
                            msg_original = self._coordinaciones_eof[client_id]["mensaje_original"]
                            del self._coordinaciones_eof[client_id]
                            self._persistir_coordinacion()

                            logger.info(f"[Coordinator] Barrera completa para client_id={client_id}. Difundiendo BARRIER_COMPLETE.")
                            self._enviar_control({
                                "type": "BARRIER_COMPLETE",
                                "client_id": client_id
                            })
                            self.on_sync_complete(client_id, msg_original)

            elif msg_type == "BARRIER_COMPLETE":
                with self._coordinacion_lock:
                    self._clientes_flusheados.discard(client_id)
                    self._flush_en_progreso.discard(client_id)
                    self._originadores_reconocidos.pop(client_id, None)
                    self._local_eof_completed.discard(client_id)
                    self._clientes_finalizados.add(client_id)
                    self._coordinaciones_eof.pop(client_id, None)
                    self._flush_completados.pop(client_id, None)
                    self._persistir_coordinacion()
                logger.info(f"[Coordinator] Barrera completa liberada globalmente para client_id={client_id}.")
                if self.on_barrier_complete:
                    self.on_barrier_complete(client_id)

        except Exception as e:
            logger.error(f"[Coordinator] Error en control: {e}", exc_info=True)
        finally:
            ack()

    def start_consuming(self):
        self.control_queue.start_consuming(self._process_control_message)

    def stop_consuming(self):
        self.control_queue.stop_consuming()

    def _rebroadcast_loop(self):
        while not self._cierre_evento.wait(2.0):
            with self._coordinacion_lock:
                clients_to_rebroadcast = list(self._coordinaciones_eof.keys())
            for client_id in clients_to_rebroadcast:
                logger.info(f"[Coordinator] Re-difundiendo EOF_RECEIVED para client_id={client_id} para despertar posibles workers reiniciados.")
                self._enviar_control({
                    "type": "EOF_RECEIVED",
                    "client_id": client_id,
                    "originator": self.config.node_id
                })

    def close(self):
        self._cierre_evento.set()
        self.control_queue.close()
        self.control_exchange.close()