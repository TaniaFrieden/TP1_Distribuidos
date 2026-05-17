import signal
import logging
import os
import threading
from abc import ABC, abstractmethod
from common import middleware

logger = logging.getLogger(__name__)


class BaseWorker(ABC):
    """
    Worker base reutilizable para todos los nodos del pipeline.

    Responsabilidades:
    - conectarse al middleware de mensajería (RabbitMQ) al iniciar.
    - crear conexion con la cola de entrada
    - crear conexion con la cola de control
    - iniciar middleware para consumir mensajes de la cola de entrada, delegando en `procesar_mensaje` la lógica de negocio.
    - el base worker debe saber a donde enviar los mensajes, tanto a exchanges de colas o a exchanges de sharding
      dependiendo de la configuracion dada por variables de entorno, pero la logica de negocio de cada worker no deberia preocuparse por eso.
    - Consumir mensajes en loop llamando a `procesar_mensaje` por cada uno.
    - Capturar SIGTERM / SIGINT y detener el consumo limpiamente sin perder
      mensajes en tránsito (espera a que el mensaje actual termine antes de salir).

    Subclases deben implementar:
    - `procesar_mensaje(mensaje, ack, nack)`: lógica de negocio del worker.
    - `al_cerrar()`: lógica de limpieza extra antes de cerrar (flush de estado).
    """

# Modificaciones internas dentro de BaseWorker en common/worker_base/base.py

    def __init__(self):
        logging.basicConfig(level=logging.INFO)
        self._cierre_solicitado = False
        self.mensajes_pendientes = 0
        self.condicion_pendiente = threading.Condition(threading.Lock())

        self._registrar_senales()

        mom_host         = os.getenv("MOM_HOST", "localhost")
        input_queue      = os.getenv("INPUT_QUEUE", "input_queue")
        control_exchange = os.getenv("CONTROL_EXCHANGE", "control_exchange_default")
        node_prefix      = os.getenv("NODE_PREFIX", "node")
        
        # 👇 GUARDAMOS ESTOS EN LA INSTANCIA PARA USARLOS downstream 👇
        self.node_id       = int(os.getenv("ID", "0"))
        self.total_workers = int(os.getenv("TOTAL_WORKERS", "1")) # Cuántas réplicas tiene este grupo
        
        # Estructura para registrar los votos de fin de procesamiento: { client_id: { "workers": set(), "msg": bytes } }
        self._coordinaciones_eof = {}
        self._coordinacion_lock = threading.Lock()
        
        output_queue     = os.getenv("OUTPUT_QUEUE", "output_queue2")

        logging.info(f"[{self.__class__.__name__}] Conectando al middleware…")
        logging.info(f"{mom_host=}, {input_queue=}, {control_exchange=}, {node_prefix=}, self.node_id={self.node_id}, self.total_workers={self.total_workers}")

        self.input_queue      = middleware.MessageMiddlewareQueueRabbitMQ(mom_host, input_queue)
        self.control_exchange = middleware.FanoutExchangeRabbitMQ(mom_host, control_exchange)
        self.control_queue    = middleware.FanoutQueueRabbitMQ(mom_host, f"{node_prefix}_{self.node_id}", control_exchange)
        self.output_queue     = middleware.MessageMiddlewareQueueRabbitMQ(mom_host, output_queue)

    # 👇 NUEVO MÉTODO PARA ENVIAR MENSAJES DE CONTROL AL GRUPO 👇
    def _enviar_control(self, msg_dict: dict):
        import json
        try:
            self.control_exchange.send(json.dumps(msg_dict).encode('utf-8'))
        except Exception as e:
            logger.error(f"[BaseWorker] Error enviando control: {e}")

    # 👇 LOGICA PARA INICIAR LA COORDINACIÓN DESDE LA SUBCLASE 👇
    def coordinar_eof(self, client_id: str, mensaje_original: bytes):
        """Método público que llamará el FilterWorker al detectar un EOF."""
        with self._coordinacion_lock:
            self._coordinaciones_eof[client_id] = {
                "workers": set(),
                "mensaje_original": mensaje_original
            }
        
        # Le avisamos a todo el grupo (Fanout) que inició el fin de este cliente
        self._enviar_control({
            "type": "EOF_RECEIVED",
            "client_id": client_id,
            "originator": self.node_id
        })

    # 👇 EL HILO DE CONTROL GESTIONA LA BARRERA AQUÍ 👇
    def _process_control_message(self, message, ack, nack):
        import json
        try:
            msg_dict = json.loads(message.decode('utf-8'))
            msg_type = msg_dict.get("type")
            client_id = msg_dict.get("client_id")
            originator = msg_dict.get("originator")

            if msg_type == "EOF_RECEIVED":
                # Al recibir la notificación del grupo, este worker avisa que ya está liberado
                self._enviar_control({
                    "type": "WORKER_FINISHED",
                    "client_id": client_id,
                    "originator": originator,
                    "worker_id": self.node_id
                })

            elif msg_type == "WORKER_FINISHED":
                # Si yo soy el dueño/originador de este proceso de EOF, cuento el voto
                if originator == self.node_id:
                    with self._coordinacion_lock:
                        if client_id in self._coordinaciones_eof:
                            self._coordinaciones_eof[client_id]["workers"].add(msg_dict.get("worker_id"))
                            
                            # Si todas las réplicas del grupo confirmaron terminación
                            if len(self._coordinaciones_eof[client_id]["workers"]) >= self.total_workers:
                                msg_original = self._coordinaciones_eof[client_id]["mensaje_original"]
                                logger.info(f"[{self.__class__.__name__}] Grupo sincronizado para {client_id}. Despachando EOF río abajo.")
                                self._enviar(msg_original)
                                del self._coordinaciones_eof[client_id]
                                
        except Exception as e:
            logger.error(f"[BaseWorker] Error en procesamiento de control: {e}")
        finally:
            ack()
    # ------------------------------------------------------------------
    # Señales del SO
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

    # ------------------------------------------------------------------
    # Ciclo de vida principal
    # ------------------------------------------------------------------

    def iniciar(self):
        """Punto de entrada del worker. Conecta, consume y cierra."""
        logger.info(f"[{self.__class__.__name__}] Arrancando worker…")

        try:
            logger.info(f"[{self.__class__.__name__}] listo. Comenzando consumo.")
            control_thread = threading.Thread(
                target=self.control_queue.start_consuming,
                args=(self._process_control_message,),
            )
            control_thread.start()
            self.input_queue.start_consuming(self._callback_interno)
            control_thread.join()

        except Exception as e:
            if self._cierre_solicitado:
                logger.info(f"[{self.__class__.__name__}] Consumo detenido por cierre graceful.")
            else:
                logger.error(f"[{self.__class__.__name__}] Error inesperado: {e}", exc_info=True)
                raise
        finally:
            self._cerrar()

        logger.info(f"[{self.__class__.__name__}] Terminado.")

    def _cerrar(self):
        """Ejecuta limpieza de negocio y cierra la conexión."""
        try:
            self.al_cerrar()
        except Exception as e:
            logger.warning(f"[BaseWorker] Error en al_cerrar(): {e}")

        try:
            self.input_queue.close()
            self.control_queue.close()
            self.control_exchange.close()
            self.output_queue.close()
            logger.info(f"[{self.__class__.__name__}] Conexión cerrada.")
        except Exception as e:
            logger.warning(f"[BaseWorker] Error al cerrar middleware: {e}")

    # ------------------------------------------------------------------
    # Callback interno
    # ------------------------------------------------------------------

    def _callback_interno(self, mensaje, ack, nack):
        if self._cierre_solicitado:
            nack()
            return

        try:
            self.procesar_mensaje(mensaje, ack, nack)
        except Exception as e:
            logger.error(
                f"[{self.__class__.__name__}] Error procesando mensaje: {e}",
                exc_info=True,
            )
            try:
                nack()
            except Exception:
                pass


    def _enviar(self, mensaje: bytes):
        """
        Enviar mensaje al siguiente componente del pipeline.

        El base worker se encarga de decidir a dónde enviar el mensaje
        dependiendo de la configuración (exchange de sharding o exchange de colas).
        """
        try:
            self.output_queue.send(mensaje)
            logger.debug(f"[BaseWorker] Mensaje enviado al siguiente componente.")
        except Exception as e:
            logger.error(f"[BaseWorker] Error enviando mensaje: {e}", exc_info=True)

    # ------------------------------------------------------------------
    # API para subclases
    # ------------------------------------------------------------------

    @abstractmethod
    def procesar_mensaje(self, mensaje: bytes, ack, nack):
        """
        Lógica de negocio del worker.

        Parámetros
        ----------
        mensaje : bytes
            Cuerpo del mensaje tal como lo entregó el middleware.
        ack : callable
            Llámalo cuando el mensaje fue procesado exitosamente.
        nack : callable
            Llámalo si el mensaje debe volver a la cola (requeue=True).

        La subclase es responsable de llamar a ack() o nack() exactamente
        una vez por invocación.
        """

    @abstractmethod
    def al_cerrar(self):
        """
        Se ejecuta justo antes de cerrar la conexión.

        Útil para workers con estado que necesiten hacer
        flush de resultados parciales antes de apagarse.
        """