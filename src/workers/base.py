import signal
import logging
import os
import threading
import json
import time # <-- Necesario para el sleep
from abc import ABC, abstractmethod
from common import middleware

logger = logging.getLogger(__name__)


class BaseWorker(ABC):
    """
    Worker base reutilizable para todos los nodos del pipeline.
    Maneja la conexión, el protocolo de red (JSON), los EOF,
    y la barrera de sincronización distribuida segura.
    """

    def __init__(self):
        logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
        self._cierre_solicitado = False
        self.mensajes_pendientes = 0
        self.condicion_pendiente = threading.Condition(threading.Lock())

        self._registrar_senales()

        mom_host         = os.getenv("MOM_HOST", "localhost")
        input_queue      = os.getenv("INPUT_QUEUE", "input_queue")
        control_exchange = os.getenv("CONTROL_EXCHANGE", "control_exchange_default")
        node_prefix      = os.getenv("NODE_PREFIX", "node")
        
        self.node_id       = int(os.getenv("ID", "0"))
        self.total_workers = int(os.getenv("TOTAL_WORKERS", "1"))
        
        self._coordinaciones_eof = {}
        self._coordinacion_lock = threading.Lock()
        
        # --- NUEVO: Rastreo de mensajes en procesamiento por cliente ---
        # Mantiene un conteo de mensajes actualmente en el método _callback_interno
        self._mensajes_en_vuelo = {} 
        self._vuelo_lock = threading.Lock()
        # ---------------------------------------------------------------
        
        output_queue     = os.getenv("OUTPUT_QUEUE", "output_queue2")

        logger.info(f"[{self.__class__.__name__}] Conectando al middleware…")
        
        self.input_queue      = middleware.MessageMiddlewareQueueRabbitMQ(mom_host, input_queue)
        self.control_exchange = middleware.FanoutExchangeRabbitMQ(mom_host, control_exchange)
        self.control_queue    = middleware.FanoutQueueRabbitMQ(mom_host, f"{node_prefix}_{self.node_id}", control_exchange)
        self.output_queue     = middleware.MessageMiddlewareQueueRabbitMQ(mom_host, output_queue)

    # ------------------------------------------------------------------
    # Coordinación Distribuida (Control)
    # ------------------------------------------------------------------

    def _enviar_control(self, msg_dict: dict):
        try:
            self.control_exchange.send(json.dumps(msg_dict).encode('utf-8'))
        except Exception as e:
            logger.error(f"[BaseWorker] Error enviando control: {e}")

    def coordinar_eof(self, client_id: str, mensaje_original: bytes):
        with self._coordinacion_lock:
            self._coordinaciones_eof[client_id] = {
                "workers": set(),
                "mensaje_original": mensaje_original
            }
        
        self._enviar_control({
            "type": "EOF_RECEIVED",
            "client_id": client_id,
            "originator": self.node_id
        })

    def _process_control_message(self, message, ack, nack):
        try:
            msg_dict = json.loads(message.decode('utf-8'))
            msg_type = msg_dict.get("type")
            client_id = msg_dict.get("client_id")
            originator = msg_dict.get("originator")

            if msg_type == "EOF_RECEIVED":
                # --- NUEVO: Lógica de espera para secundarios ---
                if originator != self.node_id:
                    logger.info(f"[{self.__class__.__name__}] Aviso EOF recibido por control para cliente {client_id}. Validando memoria...")
                    
                    # Esperamos activamente hasta que no queden mensajes en procesamiento para este cliente
                    while True:
                        with self._vuelo_lock:
                            en_vuelo = self._mensajes_en_vuelo.get(client_id, 0)
                        
                        if en_vuelo == 0:
                            break
                        
                        logger.info(f"[{self.__class__.__name__}] Drenando memoria: Aún procesando {en_vuelo} mensajes de {client_id}. Esperando...")
                        time.sleep(0.1) # Pequeña pausa para no quemar CPU

                    logger.info(f"[{self.__class__.__name__}] Memoria limpia. Enviando WORKER_FINISHED al originator {originator}.")
                # ------------------------------------------------

                self._enviar_control({
                    "type": "WORKER_FINISHED",
                    "client_id": client_id,
                    "originator": originator,
                    "worker_id": self.node_id
                })

            elif msg_type == "WORKER_FINISHED":
                if originator == self.node_id:
                    with self._coordinacion_lock:
                        if client_id in self._coordinaciones_eof:
                            self._coordinaciones_eof[client_id]["workers"].add(msg_dict.get("worker_id"))
                            
                            # --- NUEVO: Log solicitado cuando un secundario confirma ---
                            logger.info(f"[{self.__class__.__name__}] Confirmación WORKER_FINISHED recibida del nodo {msg_dict.get('worker_id')}.")
                            # -----------------------------------------------------------

                            if len(self._coordinaciones_eof[client_id]["workers"]) >= self.total_workers:
                                msg_original = self._coordinaciones_eof[client_id]["mensaje_original"]
                                
                                # Disparamos el hook para subclases que acumulan estado
                                self.al_completar_cliente(client_id)
                                
                                logger.info(f"[{self.__class__.__name__}] Grupo sincronizado (Nodos: {len(self._coordinaciones_eof[client_id]['workers'])}) para {client_id}. Despachando EOF.")
                                self._enviar(msg_original)
                                del self._coordinaciones_eof[client_id]
                                
        except Exception as e:
            logger.error(f"[BaseWorker] Error en procesamiento de control: {e}")
        finally:
            ack()

    # ------------------------------------------------------------------
    # Señales del SO y Ciclo de vida
    # ------------------------------------------------------------------

    def _registrar_senales(self):
        signal.signal(signal.SIGTERM, self._manejar_senal_cierre)
        signal.signal(signal.SIGINT, self._manejar_senal_cierre)

    def _manejar_senal_cierre(self, num_senal, frame):
        nombre_senal = signal.Signals(num_senal).name
        logger.info(f"[BaseWorker] Señal {nombre_senal} recibida. Iniciando cierre graceful…")
        self._cierre_solicitado = True
        condicion = getattr(self, "condicion_pendiente", None)
        if condicion is not None:
            with condicion:
                condicion.notify_all()
        self.input_queue.stop_consuming()
        self.control_queue.stop_consuming()

    def iniciar(self):
        logger.info(f"[{self.__class__.__name__}] Arrancando worker…")
        try:
            control_thread = threading.Thread(
                target=self.control_queue.start_consuming,
                args=(self._process_control_message,),
            )
            control_thread.start()
            self.input_queue.start_consuming(self._callback_interno)
            control_thread.join()
        except Exception as e:
            if not self._cierre_solicitado:
                logger.error(f"[{self.__class__.__name__}] Error inesperado: {e}", exc_info=True)
                raise
        finally:
            self._cerrar()
        logger.info(f"[{self.__class__.__name__}] Terminado.")

    def _cerrar(self):
        try:
            self.al_cerrar()
        except Exception as e:
            logger.warning(f"[BaseWorker] Error en al_cerrar(): {e}")

        try:
            self.input_queue.close()
            self.control_queue.close()
            self.control_exchange.close()
            self.output_queue.close()
        except Exception as e:
            logger.warning(f"[BaseWorker] Error al cerrar middleware: {e}")

    # ------------------------------------------------------------------
    # Protocolo Interno (La Magia)
    # ------------------------------------------------------------------

    def _callback_interno(self, mensaje, ack, nack):
        """Intercepta el mensaje JSON, extrae metadatos y delega el negocio a la subclase."""
        if self._cierre_solicitado:
            nack()
            return

        try:
            mensaje_str = mensaje.decode('utf-8')
            
            try:
                transaccion = json.loads(mensaje_str)
            except json.JSONDecodeError:
                logger.warning(f"[{self.__class__.__name__}] Mensaje omitido (No es JSON válido): {mensaje_str[:30]}...")
                ack()
                return
            
            client_id = transaccion.get("client_id")
            
            if not client_id:
                logger.warning(f"[{self.__class__.__name__}] Mensaje omitido (Falta client_id en JSON): {mensaje_str[:30]}...")
                ack()
                return

            # --- NUEVO: Registrar que estamos procesando un mensaje para este cliente ---
            if not transaccion.get("EOF"):
                with self._vuelo_lock:
                    self._mensajes_en_vuelo[client_id] = self._mensajes_en_vuelo.get(client_id, 0) + 1
            # ----------------------------------------------------------------------------

            if transaccion.get("EOF"):
                logger.info(f"[{self.__class__.__name__}] EOF principal interceptado para cliente {client_id}. Iniciando barrera...")
                self.coordinar_eof(client_id, mensaje)
                ack()
            else:
                try:
                    # Envolvemos las funciones originales de ack/nack para descontar el contador
                    # independientemente de cómo termine la subclase su trabajo.
                    def ack_wrapper():
                        self._descontar_vuelo(client_id)
                        ack()
                        
                    def nack_wrapper():
                        self._descontar_vuelo(client_id)
                        nack()

                    self.procesar_payload(client_id, mensaje_str, mensaje, ack_wrapper, nack_wrapper)
                except Exception as e:
                    logger.error(f"Error interno en procesar_payload: {e}")
                    self._descontar_vuelo(client_id)
                    nack()

        except Exception as e:
            logger.error(f"[{self.__class__.__name__}] Error procesando mensaje general: {e}", exc_info=True)
            try:
                nack()
            except Exception:
                pass

    def _descontar_vuelo(self, client_id):
        """Disminuye el contador de mensajes en proceso de forma segura."""
        with self._vuelo_lock:
            if client_id in self._mensajes_en_vuelo:
                self._mensajes_en_vuelo[client_id] -= 1
                if self._mensajes_en_vuelo[client_id] <= 0:
                    del self._mensajes_en_vuelo[client_id]

    def _enviar(self, mensaje: bytes):
        try:
            self.output_queue.send(mensaje)
        except Exception as e:
            logger.error(f"[BaseWorker] Error enviando mensaje: {e}", exc_info=True)

    # ------------------------------------------------------------------
    # API para Subclases
    # ------------------------------------------------------------------

    @abstractmethod
    def procesar_payload(self, client_id: str, payload: str, mensaje_original: bytes, ack, nack):
        """
        Lógica de negocio del worker. Recibe el string JSON limpio.
        """

    @abstractmethod
    def al_cerrar(self):
        """Se ejecuta justo antes de cerrar la conexión."""

    def al_completar_cliente(self, client_id: str):
        """
        Hook Opcional. 
        Se dispara automáticamente cuando el grupo sincroniza un EOF.
        """
        pass