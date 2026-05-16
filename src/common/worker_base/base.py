import signal
import logging
import os
import threading
from abc import ABC, abstractmethod
from common import middleware

logger = logging.getLogger(__name__)

ID = int(os.getenv("ID"))
MOM_HOST = os.getenv("MOM_HOST", "localhost")
NODE_PREFIX = os.getenv("NODE_PREFIX", "node")
INPUT_QUEUE = os.getenv("INPUT_QUEUE", "input_queue")
CONTROL_EXCHANGE = os.getenv("CONTROL_EXCHANGE", "control_exchange")
NUM_SIBLINGS = int(os.getenv("NUM_SIBLINGS", 1))

class BaseWorker(ABC):
    """
    Worker base reutilizable para todos los nodos del pipeline.

    Responsabilidades:
    - conectarse al middleware de mensajería (RabbitMQ) al iniciar.
    - crear conexion con la cola de entrada
    - crear conexion con la cola de control
    - iniciar middleware para consumir mensajes de la cola de entrada, delegando en `procesar_mensaje` la lógica de negocio.
      las implementaciones deben pasar la input_queue, control_queue y control_exchange al constructor de BaseWorker

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
        
        self._cierre_solicitado = False
        self._registrar_senales()

        self.input_queue = middleware.MessageMiddlewareQueueRabbitMQ(MOM_HOST, INPUT_QUEUE)
        self.control_exchange = middleware.FanoutExchangeRabbitMQ(MOM_HOST, CONTROL_EXCHANGE)
        self.control_queue = middleware.MessageMiddlewareQueueRabbitMQ(MOM_HOST, f"{NODE_PREFIX}_{ID}", CONTROL_EXCHANGE)

        # Condition para sincronizar el flush con el procesamiento de datos.
        # El thread de control espera a que no haya mensajes de datos en vuelo
        # antes de hacer flush, evitando que un dato llegue despues de la señal
        # de control del mismo cliente
        self.mensajes_pendientes = 0
        self.condicion_pendiente = threading.Condition(threading.Lock())

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
        with self.condicion_pendiente:
            self.condicion_pendiente.notify_all() 
        self.input_queue.stop_consuming()
        self.control_queue.stop_consuming()

    # ------------------------------------------------------------------
    # Ciclo de vida principal
    # ------------------------------------------------------------------

    def iniciar(self):
        """Punto de entrada del worker. Conecta, consume y cierra."""
        logger.info(f"[{self.__class__.__name__}] Arrancando worker…")

        try:
            logger.info(f"[{self.__class__.__name__}]  listo. Comenzando consumo.")
            control_thread = threading.Thread(
                target=self.control_queue.start_consuming, 
                args=(self._process_control_message,),
            )
            control_thread.start()
            self.input_queue.start_consuming(self._callback_interno)
            control_thread.join()
        
        except Exception as e:
            if self._cierre_solicitado:
                # stop_consuming() puede lanzar excepciones en algunos casos; es esperado.
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

        if self._middleware is not None:
            try:
                self._middleware.close()
                logger.info(f"[{self.__class__.__name__}] Conexión cerrada.")
            except Exception as e:
                logger.warning(f"[BaseWorker] Error al cerrar middleware: {e}")



    # ------------------------------------------------------------------
    # Callback interno
    # ------------------------------------------------------------------

    def _callback_interno(self, mensaje, ack, nack):
        """
        Invocado por el middleware por cada mensaje.

        Si ya se pidió cierre, hace nack (requeue=True) para no perder el
        mensaje y deja que el loop termine. En caso normal delega en
        procesar_mensaje() y maneja excepciones para que un error en un mensaje
        no tire el worker entero.
        """
        if self._cierre_solicitado:
            # No procesar más mensajes nuevos; devolverlos a la cola.
            nack()
            return

        try:
            self.procesar_mensaje(mensaje, ack, nack)
        except Exception as e:
            logger.error(
                f"[{self.__class__.__name__}] Error procesando mensaje: {e}",
                exc_info=True,
            )
            # Nack con requeue para no perder el mensaje.
            try:
                nack()
            except Exception:
                pass

    def _process_control_message(self, message, ack, nack):
        # fields = message_protocol.internal.deserialize(message)
        # client_id = fields[0]
        logging.info(f"[{self.__class__.__name__}] Mensaje de control recibido: {message}")
        # logging.info(f"Worker {self.__class__.__name__}: process control message for client {client_id}")
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

    def al_cerrar(self):
        """
        Se ejecuta justo antes de cerrar la conexión.

        Útil para workers con estado que necesiten hacer
        flush de resultados parciales antes de apagarse.
        """
