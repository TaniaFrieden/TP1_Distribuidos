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
        while True:
            with self._vuelo_lock:
                if self._mensajes_en_vuelo.get(client_id, 0) == 0:
                    break
            time.sleep(0.1)

    # --- Tracking Local de EOFs ---
    def registrar_eof_local(self, client_id, queue_name, total_esperados) -> bool:
        """Devuelve True si se recibieron todos los EOFs locales esperados."""
        with self._coordinacion_lock:
            if client_id not in self._eofs_locales_recibidos:
                self._eofs_locales_recibidos[client_id] = set()
            self._eofs_locales_recibidos[client_id].add(queue_name)
            return len(self._eofs_locales_recibidos[client_id]) == total_esperados

    def limpiar_eof_local(self, client_id):
        with self._coordinacion_lock:
            if client_id in self._eofs_locales_recibidos:
                del self._eofs_locales_recibidos[client_id]

    # --- Barrera Distribuida ---
    def iniciar_barrera(self, client_id: str, mensaje_original: bytes):
        with self._coordinacion_lock:
            self._coordinaciones_eof[client_id] = {
                "workers": set(),
                "mensaje_original": mensaje_original
            }
        logger.info(
            f"[Coordinator] EOF local completo para client_id={client_id}. Difundiendo a {self.config.total_workers} workers del nodo {self.config.node_prefix}."
        )
        self._enviar_control({
            "type": "EOF_RECEIVED",
            "client_id": client_id,
            "originator": self.config.node_id
        })

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
                logger.info(
                    f"[Coordinator] EOF_RECEIVED recibido en worker {self.config.node_id} para client_id={client_id}. Esperando vuelos en cero."
                )
                self._esperar_vuelo_cero(client_id)
                logger.info(
                    f"[Coordinator] Vuelos en cero para client_id={client_id} en worker {self.config.node_id}. Confirmando WORKER_FINISHED."
                )
                self._enviar_control({
                    "type": "WORKER_FINISHED",
                    "client_id": client_id,
                    "originator": originator,
                    "worker_id": self.config.node_id
                })

            elif msg_type == "WORKER_FINISHED" and originator == self.config.node_id:
                with self._coordinacion_lock:
                    if client_id in self._coordinaciones_eof:
                        self._coordinaciones_eof[client_id]["workers"].add(msg_dict.get("worker_id"))
                        logger.info(
                            f"[Coordinator] WORKER_FINISHED recibido para client_id={client_id}. Confirmados: {len(self._coordinaciones_eof[client_id]['workers'])}/{self.config.total_workers}."
                        )
                        
                        if len(self._coordinaciones_eof[client_id]["workers"]) >= self.config.total_workers:
                            msg_original = self._coordinaciones_eof[client_id]["mensaje_original"]
                            del self._coordinaciones_eof[client_id]
                            
                            # Dispara el callback al completar
                            logger.info(f"[Coordinator] Barrera completa para client_id={client_id}. Liberando EOF hacia la siguiente cola.")
                            self.on_sync_complete(client_id, msg_original)
            if msg_type == "WORKER_FINISHED" and msg_dict.get("worker_id") == self.config.node_id:
                # Esto garantiza que cada worker vacíe sus datos en cuanto termina
                logger.info(f"[Coordinator] Ejecutando limpieza local para client_id={client_id}.")
                # Pasamos None como mensaje_original porque este worker solo necesita vaciar,
                # no necesariamente reenviar el mensaje original si no es el originator.
                self.on_sync_complete(client_id, None)              
        except Exception as e:
            logger.error(f"[Coordinator] Error en control: {e}", exc_info=True)
        finally:
            ack()

    # --- Lifecycle ---
    def start_consuming(self):
        self.control_queue.start_consuming(self._process_control_message)

    def stop_consuming(self):
        self.control_queue.stop_consuming()

    def close(self):
        self.control_queue.close()
        self.control_exchange.close()