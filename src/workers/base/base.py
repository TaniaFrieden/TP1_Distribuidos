import signal
import logging
import threading
import json
import time
from abc import ABC, abstractmethod
from common import middleware

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
        self._heartbeat_stop_event = threading.Event()
        self._heartbeat_thread = None
        self._eofs_pendientes_ack = {}  # {client_id: [ack_callbacks]}

        self.config = WorkerConfig()
        self._heartbeat_queue_name = f"heartbeat.{self.config.node_prefix}"
        self._heartbeat_instance_id = f"{self.config.node_id:02d}"
        self.router = MessageRouter(self.config)
        self.coordinator = DistributedCoordinator(
            self.config, 
            on_sync_complete_cb=self._al_completar_sincronizacion_global,
            on_barrier_complete_cb=self._al_completar_barrera
        )

        logger.info(
            f"[{self.__class__.__name__}] Inicializando worker: "
            f"etapa={self.config.node_prefix}, id={self.config.node_id}, "
            f"total_workers={self.config.total_workers}, input_queues={self.config.input_queues}, "
            f"output_queues={self.config.output_queues}"
        )

        self._registrar_senales()

    def _registrar_senales(self):
        signal.signal(signal.SIGTERM, self._manejar_senal_cierre)
        signal.signal(signal.SIGINT, self._manejar_senal_cierre)

    def _manejar_senal_cierre(self, num_senal, frame):
        logger.info(f"[{self.__class__.__name__}] Señal recibida. Cierre graceful…")
        self._cierre_solicitado = True
        self._heartbeat_stop_event.set()
        
        with self.condicion_pendiente:
            self.condicion_pendiente.notify_all()
                
        self.router.stop_consuming()
        self.coordinator.stop_consuming()

    def _iniciar_heartbeat(self):
        if self.config.heartbeat_interval_seconds <= 0:
            logger.info(
                f"[{self.__class__.__name__}] Heartbeat deshabilitado "
                f"(intervalo={self.config.heartbeat_interval_seconds})."
            )
            return

        self._heartbeat_stop_event.clear()
        self._heartbeat_thread = threading.Thread(
            target=self._heartbeat_loop,
            name=f"{self.__class__.__name__}-heartbeat",
            daemon=True,
        )
        self._heartbeat_thread.start()

    def _heartbeat_loop(self):
        heartbeat_queue = None

        try:
            while not self._heartbeat_stop_event.is_set():
                try:
                    if heartbeat_queue is None:
                        heartbeat_queue = middleware.MessageMiddlewareQueueRabbitMQ(
                            self.config.mom_host,
                            self._heartbeat_queue_name,
                        )

                    payload = {
                        "etapa": self.config.node_prefix,
                        "instancia": self._heartbeat_instance_id,
                        "timestamp": time.time(),
                    }
                    heartbeat_queue.send(json.dumps(payload).encode("utf-8"))
                except Exception as e:
                    if heartbeat_queue is not None:
                        try:
                            heartbeat_queue.close()
                        except Exception:
                            pass
                        heartbeat_queue = None

                    if not self._cierre_solicitado:
                        logger.warning(
                            f"[{self.__class__.__name__}] Error enviando heartbeat: {e}",
                            exc_info=True,
                        )

                if self._heartbeat_stop_event.wait(self.config.heartbeat_interval_seconds):
                    break
        finally:
            if heartbeat_queue is not None:
                try:
                    heartbeat_queue.close()
                except Exception as e:
                    logger.warning(
                        f"[{self.__class__.__name__}] Error cerrando heartbeat: {e}"
                    )

    def iniciar(self):
        logger.info(f"[{self.__class__.__name__}] Arrancando worker…")
        try:
            input_threads = []
            self._iniciar_heartbeat()
            
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
        self._heartbeat_stop_event.set()

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

            if mensaje_json.get("CLIENT_DISCONNECT"):
                logger.info(f"[{self.__class__.__name__}] CLIENT_DISCONNECT para {client_id}. Limpiando estado.")
                self._eofs_pendientes_ack.pop(client_id, None)
                self.al_desconectar_cliente(client_id)
                self.coordinator.limpiar_cliente(client_id)
                self._enviar(mensaje, mensaje_json)
                return ack()

            if mensaje_json.get("EOF"):
                if self.interceptar_eof(queue_name, client_id, mensaje_json, mensaje):
                    return ack()

                logger.info(f"[{self.__class__.__name__}] EOF interceptado en la cola {queue_name}. Esperando a {len(self.router.input_queues)} colas locales.")
                
                if client_id not in self._eofs_pendientes_ack:
                    self._eofs_pendientes_ack[client_id] = []
                self._eofs_pendientes_ack[client_id].append(ack)

                termino_local = self.coordinator.registrar_eof_local(
                    client_id, queue_name, len(self.router.input_queues)
                )

                if termino_local:
                    logger.info(f"[{self.__class__.__name__}] Todos los EOFs locales recibidos. Iniciando barrera distribuida.")
                    self.al_completar_eof_local(client_id)
                    self.coordinator.iniciar_barrera(client_id, mensaje)
                    self.coordinator.limpiar_eof_local(client_id)
            else:
                self.coordinator.registrar_vuelo(client_id)
                    
                def ack_wrapper():
                    self.coordinator.descontar_vuelo(client_id)
                    ack()
                        
                def nack_wrapper():
                    self.coordinator.descontar_vuelo(client_id)
                    nack()

                try:
                    self.procesar_payload(queue_name, client_id, mensaje_json, mensaje, ack_wrapper, nack_wrapper)
                except Exception as e:
                    self.coordinator.descontar_vuelo(client_id)
                    raise e

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
            logger.info(f"[{self.__class__.__name__}] Flusheando datos para client_id={client_id}.")
            self.al_completar_cliente(client_id)
        else:
            logger.info(f"[{self.__class__.__name__}] Barrera completa, reenviando EOF para client_id={client_id}.")
            try:
                self._enviar(mensaje_original)
            except Exception as e:
                logger.warning(f"[{self.__class__.__name__}] Error al reenviar EOF al downstream (puede que el downstream ya esté cerrado o la conexión se haya reseteado): {e}")

    def _al_completar_barrera(self, client_id: str):
        logger.info(f"[{self.__class__.__name__}] Barrera completada. Confirmando ACKs de EOFs acumulados para {client_id}.")
        acks = self._eofs_pendientes_ack.pop(client_id, [])
        for ack_cb in acks:
            try:
                ack_cb()
            except Exception as e:
                logger.warning(f"[{self.__class__.__name__}] Error al ejecutar ACK diferido: {e}")

    def interceptar_eof(self, queue_name: str, client_id: str, payload: dict, mensaje_original: bytes) -> bool:
        """
        Permite a las subclases manejar su propia lógica de EOF por cola.
        Si retorna True, la clase base no ejecutará el flujo de coordinación de EOF genérico.
        """
        return False

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

    def al_desconectar_cliente(self, client_id: str):
        pass
