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
        node_id          = int(os.getenv("ID", "0"))

        logging.info(f"[{self.__class__.__name__}] Conectando al middleware…")
        logging.info(f"{mom_host=}, {input_queue=}, {control_exchange=}, {node_prefix=}, {node_id=}")

        self.input_queue      = middleware.MessageMiddlewareQueueRabbitMQ(mom_host, input_queue)
        self.control_exchange = middleware.FanoutExchangeRabbitMQ(mom_host, control_exchange)
        self.control_queue    = middleware.FanoutQueueRabbitMQ(mom_host, f"{node_prefix}_{node_id}", control_exchange)

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

    def _process_control_message(self, message, ack, nack):
        logging.info(f"[{self.__class__.__name__}] Mensaje de control recibido: {message}")
        ack()

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