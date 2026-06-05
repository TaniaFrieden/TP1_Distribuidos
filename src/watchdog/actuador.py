import json
import logging
import os

import docker
import docker.errors

from common.middleware.middleware_rabbitmq import MessageMiddlewareQueueRabbitMQ

logger = logging.getLogger(__name__)


class Actuador:
    """
    Consume la cola 'caidas' y reinicia el container correspondiente via Docker SDK.

    El nombre del container se deriva directamente del payload:
        {etapa}_{instancia}  →  e.g. "q5_filter_period_02"
    que coincide con el container_name que asigna generar_compose.py.
    """

    def __init__(self):
        self._mom_host = os.getenv("MOM_HOST", "localhost")
        self._caidas_queue_name = os.getenv("CAIDAS_QUEUE", "caidas")
        self._queue: MessageMiddlewareQueueRabbitMQ | None = None
        self._docker = docker.from_env()

    # ------------------------------------------------------------------
    # Ciclo de vida
    # ------------------------------------------------------------------

    def start(self):
        """Bloquea consumiendo la cola 'caidas' hasta que se llame stop()."""
        self._queue = MessageMiddlewareQueueRabbitMQ(self._mom_host, self._caidas_queue_name)
        logger.info(f"[Actuador] Escuchando cola '{self._caidas_queue_name}'.")
        self._queue.start_consuming(self._on_caida)

    def stop(self):
        if self._queue is not None:
            try:
                self._queue.stop_consuming()
            except Exception:
                pass

    # ------------------------------------------------------------------
    # Lógica de reinicio
    # ------------------------------------------------------------------

    def _on_caida(self, msg: bytes, ack, nack):
        try:
            evento = json.loads(msg.decode("utf-8"))
            etapa = evento["etapa"]
            instancia = evento["instancia"]
        except (json.JSONDecodeError, KeyError) as e:
            logger.warning(f"[Actuador] Mensaje de caída malformado: {e}")
            ack()
            return

        container_name = f"{etapa}_{instancia}"
        try:
            self._reiniciar(container_name)
            ack()
        except docker.errors.NotFound:
            logger.warning(f"[Actuador] Container '{container_name}' no encontrado — ignorando.")
            ack()
        except Exception as e:
            logger.error(f"[Actuador] Error reiniciando '{container_name}': {e}", exc_info=True)
            nack()

    def _reiniciar(self, container_name: str):
        container = self._docker.containers.get(container_name)
        logger.info(f"[Actuador] Reiniciando container '{container_name}'...")
        container.restart()
        logger.info(f"[Actuador] Container '{container_name}' reiniciado exitosamente.")
