import signal
import logging
from abc import ABC, abstractmethod

logger = logging.getLogger(__name__)


class BaseWorker(ABC):
    """
    Worker base reutilizable para todos los nodos del pipeline.

    Responsabilidades:
    - Conectarse al middleware al arrancar.
    - Consumir mensajes en loop llamando a `procesar_mensaje` por cada uno.
    - Capturar SIGTERM / SIGINT y detener el consumo limpiamente sin perder
      mensajes en tránsito (espera a que el mensaje actual termine antes de salir).

    Subclases deben implementar:
    - `procesar_mensaje(mensaje, ack, nack)`: lógica de negocio del worker.
    - `inicializar_middleware()`: crear y retornar la instancia de MessageMiddleware
      adecuada (cola, exchange, etc.).
    - `al_cerrar()`: lógica de limpieza extra antes de cerrar (flush de estado).
    """

    def __init__(self):
        self._middleware = None
        self._cierre_solicitado = False
        self._registrar_senales()

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
        # Le pedimos al middleware que deje de entregar nuevos mensajes.
        # El mensaje que se está procesando en este momento terminará normalmente.
        if self._middleware is not None:
            try:
                self._middleware.stop_consuming()
            except Exception as e:
                logger.warning(f"[BaseWorker] Error al detener consumo: {e}")

    # ------------------------------------------------------------------
    # Ciclo de vida principal
    # ------------------------------------------------------------------

    def iniciar(self):
        """Punto de entrada del worker. Conecta, consume y cierra."""
        logger.info(f"[{self.__class__.__name__}] Arrancando…")

        try:
            self._middleware = self.inicializar_middleware()
            logger.info(f"[{self.__class__.__name__}] Middleware listo. Comenzando consumo.")
            self._middleware.start_consuming(self._callback_interno)
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
    # Callback interno (wrappea el de la subclase)
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

    # ------------------------------------------------------------------
    # API para subclases
    # ------------------------------------------------------------------

    @abstractmethod
    def inicializar_middleware(self):
        """
        Crea y retorna la instancia de MessageMiddleware que este worker usará.
        """

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
