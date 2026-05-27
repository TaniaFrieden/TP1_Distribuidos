import json
import threading
import logging
import time
from common import middleware

logger = logging.getLogger(__name__)

class DistributedCoordinator:
    """Maneja la barrera de sincronización global para los EOFs."""
    def __init__(self, config, on_sync_complete_cb):
        self.config = config
        self.on_sync_complete = on_sync_complete_cb
        
        self._coordinaciones_eof = {}
        self._eofs_locales_recibidos = {}
        self._mensajes_en_vuelo = {}
        self._clientes_flusheados = set()  
        self._originadores_reconocidos = {}  
        self._local_eof_completed = set()
        
        if config.total_workers > 1:
            self._tiene_cola_sharded = any(
                q.endswith(f"_{config.node_id}") or f"_{config.node_id}_" in q or f"_{config.node_id}" in q
                for q in config.input_queues
            )
        else:
            self._tiene_cola_sharded = True
        
        self._coordinacion_lock = threading.Lock()
        self._vuelo_lock = threading.Lock()
        
        # Canal de Control
        self.control_exchange = middleware.FanoutExchangeRabbitMQ(
            config.mom_host, f"control_{config.node_prefix}_exchange"
        )
        self.control_queue = middleware.FanoutQueueRabbitMQ(
            config.mom_host, f"control_{config.node_prefix}_queue_{config.node_id}",
            self.control_exchange.exchange_name
        )

    # --- Tracking de mensajes (Vuelo) ---
    def registrar_vuelo(self, client_id):
        with self._vuelo_lock:
            self._mensajes_en_vuelo[client_id] = self._mensajes_en_vuelo.get(client_id, 0) + 1

    def descontar_vuelo(self, client_id):
        with self._vuelo_lock:
            if client_id in self._mensajes_en_vuelo:
                self._mensajes_en_vuelo[client_id] -= 1
                if self._mensajes_en_vuelo[client_id] <= 0:
                    del self._mensajes_en_vuelo[client_id]

    def _esperar_vuelo_cero(self, client_id):
        confirmado_cero_desde = None
        while True:
            with self._vuelo_lock:
                vuelo = self._mensajes_en_vuelo.get(client_id, 0)
            
            if vuelo == 0:
                if confirmado_cero_desde is None:
                    confirmado_cero_desde = time.perf_counter()
                elif time.perf_counter() - confirmado_cero_desde >= 1.0:
                    # El contador de vuelos ha permanecido en cero durante 1.0 segundo de forma continua
                    break
            else:
                confirmado_cero_desde = None
                
            time.sleep(0.05)

    # --- Tracking Local de EOFs ---
    def registrar_eof_local(self, client_id, queue_name, total_esperados) -> bool:
        with self._coordinacion_lock:
            if client_id not in self._eofs_locales_recibidos:
                self._eofs_locales_recibidos[client_id] = set()
            self._eofs_locales_recibidos[client_id].add(queue_name)
            return len(self._eofs_locales_recibidos[client_id]) == total_esperados

    def limpiar_eof_local(self, client_id):
        with self._coordinacion_lock:
            if client_id in self._eofs_locales_recibidos:
                del self._eofs_locales_recibidos[client_id]

    # --- Ejecutar Flush y Notificar ---
    def _ejecutar_flush_y_notificar(self, client_id: str, originator: str):
        logger.info(f"[Coordinator] EOF local y de control recibidos para {client_id}. Esperando vuelos a cero antes de flush.")
        self._esperar_vuelo_cero(client_id)

        with self._coordinacion_lock:
            ya_flusheado = client_id in self._clientes_flusheados
            if not ya_flusheado:
                self._clientes_flusheados.add(client_id)

        if not ya_flusheado:
            logger.info(f"[Coordinator] Vuelos en cero para client_id={client_id}. Flusheando datos locales.")
            self.on_sync_complete(client_id, None)
        else:
            logger.info(f"[Coordinator] Ya flusheado para client_id={client_id}. Skip.")

        logger.info(f"[Coordinator] Flush completo. Enviando WORKER_FINISHED a originator {originator}.")
        self._enviar_control({
            "type": "WORKER_FINISHED",
            "client_id": client_id,
            "originator": originator,
            "worker_id": self.config.node_id
        })

    # --- Barrera Distribuida ---
    def iniciar_barrera(self, client_id: str, mensaje_original: bytes):
        ejecutar_flush_inmediato = False
        originator_para_flush = None
        
        with self._coordinacion_lock:
            self._local_eof_completed.add(client_id)
            originator = self._originadores_reconocidos.get(client_id)
            
            if originator is not None:
                # La barrera de control ya fue activada por otro worker.
                # Como nuestro EOF local ya está listo, podemos ejecutar el flush.
                logger.info(f"[Coordinator] Barrera ya activa para {client_id} (originador: {originator}). Disparando flush diferido.")
                ejecutar_flush_inmediato = True
                originator_para_flush = originator
            else:
                # Somos el primer worker en completar el EOF local, nos autodeclaramos originador
                self._originadores_reconocidos[client_id] = self.config.node_id
                self._coordinaciones_eof[client_id] = {
                    "workers": set(),
                    "mensaje_original": mensaje_original
                }
                
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
                # --- RESOLUCIÓN DE COLISIONES ---
                with self._coordinacion_lock:
                    originador_actual = self._originadores_reconocidos.get(client_id)
                    
                    if originador_actual is not None:
                        # Si hay un conflicto de originadores, gana el nodo con el ID menor
                        if originator < originador_actual:
                            logger.info(f"[Coordinator] Colisión de barrera: cediendo originador de {originador_actual} a {originator}")
                            self._originadores_reconocidos[client_id] = originator
                            if originador_actual == self.config.node_id:
                                # Yo era el originador perdedor, elimino mi estado de recolección
                                self._coordinaciones_eof.pop(client_id, None)
                        elif originator > originador_actual:
                            # Recibí un mensaje de un originador que perdió, lo ignoro
                            return
                    else:
                        # No había originador previo, acepto este
                        self._originadores_reconocidos[client_id] = originator

                # AHORA: Verificamos si ya completamos nuestro EOF local
                with self._coordinacion_lock:
                    local_completo = (client_id in self._local_eof_completed) or not self._tiene_cola_sharded
                    originator_final = self._originadores_reconocidos[client_id]

                if local_completo:
                    self._ejecutar_flush_y_notificar(client_id, originator_final)
                else:
                    logger.info(f"[Coordinator] EOF_RECEIVED para {client_id} (originator {originator_final}), pero el EOF local aún no está listo. Postergando flush.")

            elif msg_type == "WORKER_FINISHED" and originator == self.config.node_id:
                with self._coordinacion_lock:
                    if client_id in self._coordinaciones_eof:
                        self._coordinaciones_eof[client_id]["workers"].add(msg_dict.get("worker_id"))
                        logger.info(
                            f"[Coordinator] WORKER_FINISHED para client_id={client_id}. "
                            f"Confirmados: {len(self._coordinaciones_eof[client_id]['workers'])}/{self.config.total_workers}."
                        )
                        
                        if len(self._coordinaciones_eof[client_id]["workers"]) >= self.config.total_workers:
                            msg_original = self._coordinaciones_eof[client_id]["mensaje_original"]
                            del self._coordinaciones_eof[client_id]
                            
                            logger.info(f"[Coordinator] Barrera completa para client_id={client_id}. Difundiendo BARRIER_COMPLETE.")
                            self._enviar_control({
                                "type": "BARRIER_COMPLETE",
                                "client_id": client_id
                            })
                            # Reenviar el EOF final a la siguiente etapa
                            self.on_sync_complete(client_id, msg_original)

            elif msg_type == "BARRIER_COMPLETE":
                with self._coordinacion_lock:
                    self._clientes_flusheados.discard(client_id)
                    self._originadores_reconocidos.pop(client_id, None)
                    self._local_eof_completed.discard(client_id)
                logger.info(f"[Coordinator] Barrera completa liberada globalmente para client_id={client_id}.")

        except Exception as e:
            logger.error(f"[Coordinator] Error en control: {e}", exc_info=True)
        finally:
            ack()

    # --- Lifecycle ---
    def start_consuming(self):
        # NOTA: Asegúrate de pasar el callback con lambda o directo para soportar ack/nack según tu middleware
        self.control_queue.start_consuming(self._process_control_message)

    def stop_consuming(self):
        self.control_queue.stop_consuming()

    def close(self):
        self.control_queue.close()
        self.control_exchange.close()