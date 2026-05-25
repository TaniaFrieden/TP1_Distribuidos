import signal
import logging
import threading
import json
from abc import ABC, abstractmethod

try:
    from config import WorkerConfig          # Docker runtime (archivos copiados al root)
    from router import MessageRouter
    from coordinator import DistributedCoordinator
except ImportError:
    from workers.base.config import WorkerConfig   # entorno de tests
    from workers.base.router import MessageRouter
    from workers.base.coordinator import DistributedCoordinator

logger = logging.getLogger(__name__)

class BaseWorker(ABC):
    """
    Worker base para nodos del pipeline.
    Orquesta el Router (I/O) y el Coordinador (Sincronización).
    """
    def __init__(self):
        self._cierre_solicitado = False
        self.condicion_pendiente = threading.Condition(threading.Lock())

        # Instanciamos los componentes (Composición)
        self.config = WorkerConfig()
        self.router = MessageRouter(self.config)
        self.coordinator = DistributedCoordinator(
            self.config, 
            on_sync_complete_cb=self._al_completar_sincronizacion_global
        )

        self._registrar_senales()

    def _registrar_senales(self):
        signal.signal(signal.SIGTERM, self._manejar_senal_cierre)
        signal.signal(signal.SIGINT, self._manejar_senal_cierre)

    def _manejar_senal_cierre(self, num_senal, frame):
        logger.info(f"[{self.__class__.__name__}] Señal recibida. Cierre graceful…")
        self._cierre_solicitado = True
        
        with self.condicion_pendiente:
            self.condicion_pendiente.notify_all()
                
        self.router.stop_consuming()
        self.coordinator.stop_consuming()

    def iniciar(self):
        logger.info(f"[{self.__class__.__name__}] Arrancando worker…")
        try:
            input_threads = []
            
            for nombre_cola, iq in self.router.input_queues.items():
                t = threading.Thread(
                    target=iq.start_consuming,
                    args=(lambda msg, ack, nack, q=nombre_cola: self._callback_interno(q, msg, ack, nack),)
                )
                t.start()
                input_threads.append(t)

            control_thread = threading.Thread(target=self.coordinator.start_consuming)
            control_thread.start()

            control_thread.join()
            for t in input_threads:
                t.join()
                
        except Exception as e:
            if not self._cierre_solicitado:
                logger.error(f"Error inesperado: {e}", exc_info=True)
                raise
        finally:
            self._cerrar()

    def _cerrar(self):
        try:
            self.al_cerrar()
        except Exception as e:
            logger.warning(f"Error en al_cerrar(): {e}")
        
        self.router.close()
        self.coordinator.close()

    def _callback_interno(self, queue_name: str, mensaje, ack, nack):
        if self._cierre_solicitado:
            return nack()

        try:
            mensaje_json = json.loads(mensaje.decode('utf-8'))
            client_id = mensaje_json.get("client_id")
            
            if not client_id:
                return ack()

            if mensaje_json.get("EOF"):
                logger.info(f"[{self.__class__.__name__}] EOF interceptado en la cola {queue_name}. Esperando a {len(self.router.input_queues)} colas locales.")
                
                # Le preguntamos al coordinador si ya completamos los EOF locales
                termino_local = self.coordinator.registrar_eof_local(
                    client_id, queue_name, len(self.router.input_queues)
                )

                if termino_local:
                    logger.info(f"[{self.__class__.__name__}] Todos los EOFs locales recibidos. Iniciando barrera distribuida.")
                    self.al_completar_eof_local(client_id)
                    self.coordinator.iniciar_barrera(client_id, mensaje)
                    self.coordinator.limpiar_eof_local(client_id)
                ack()
            else:
                self.coordinator.registrar_vuelo(client_id)
                    
                def ack_wrapper():
                    self.coordinator.descontar_vuelo(client_id)
                    ack()
                        
                def nack_wrapper():
                    self.coordinator.descontar_vuelo(client_id)
                    nack()

                self.procesar_payload(queue_name, client_id, mensaje_json, mensaje, ack_wrapper, nack_wrapper)

        except json.JSONDecodeError:
            logger.warning("Mensaje no JSON omitido.")
            ack()
        except Exception as e:
            logger.error(f"Error procesando mensaje: {e}", exc_info=True)
            nack()

    def _enviar(self, mensaje: bytes, payload: dict = None):
        """Expone el ruteador de forma sencilla hacia las subclases."""
        self.router.enviar(mensaje, payload)

    def _al_completar_sincronizacion_global(self, client_id: str, mensaje_original: bytes):
        if mensaje_original is None:
            # Flush local — todos los workers
            logger.info(f"[{self.__class__.__name__}] Flusheando datos para client_id={client_id}.")
            self.al_completar_cliente(client_id)
        else:
            # Solo el originator llega acá — reenvía el EOF
            logger.info(f"[{self.__class__.__name__}] Barrera completa, reenviando EOF para client_id={client_id}.")
            self._enviar(mensaje_original)

    # ------------------------------------------------------------------
    # API Exclusiva para Subclases
    # ------------------------------------------------------------------

    @abstractmethod
    def procesar_payload(self, queue_name: str, client_id: str, payload: dict, mensaje_original: bytes, ack, nack):
        pass

    @abstractmethod
    def al_cerrar(self):
        pass

    def al_completar_eof_local(self, client_id: str):
        pass

    def al_completar_cliente(self, client_id: str):
        pass