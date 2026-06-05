import json
import logging
import threading
import time

from common.middleware.middleware_rabbitmq import MessageMiddlewareQueueRabbitMQ

logger = logging.getLogger(__name__)


class HeartbeatDetector:
    """
    Consume colas heartbeat.<etapa> y detecta caídas por ausencia de heartbeat.

    Al detectar una caída publica {"etapa": ..., "instancia": ...} en la cola
    de caidas para que el Actuador lo procese independientemente.
    """

    def __init__(self, config):
        self._config = config
        self._lock = threading.Lock()
        self._last_seen: dict[tuple, float] = {}       # {(etapa, instancia): timestamp}
        self._stop_event = threading.Event()
        self._consumer_queues: list[MessageMiddlewareQueueRabbitMQ] = []
        self._caidas_queue: MessageMiddlewareQueueRabbitMQ | None = None

    # ------------------------------------------------------------------
    # Ciclo de vida
    # ------------------------------------------------------------------

    def start(self):
        for stage in self._config.stages:
            t = threading.Thread(
                target=self._consume_stage,
                args=(stage,),
                daemon=True,
                name=f"detector-{stage}",
            )
            t.start()

        checker = threading.Thread(
            target=self._check_loop,
            daemon=True,
            name="detector-checker",
        )
        checker.start()
        logger.info(
            f"[Detector] Iniciado. Etapas monitoreadas: {self._config.stages}. "
            f"Timeout: {self._config.timeout_seconds:.1f}s"
        )

    def stop(self):
        self._stop_event.set()
        with self._lock:
            queues = list(self._consumer_queues)
        for q in queues:
            try:
                q.stop_consuming()
            except Exception:
                pass

    # ------------------------------------------------------------------
    # Consumidor por etapa
    # ------------------------------------------------------------------

    def _consume_stage(self, stage: str):
        queue_name = f"heartbeat.{stage}"
        try:
            q = MessageMiddlewareQueueRabbitMQ(self._config.mom_host, queue_name)
            with self._lock:
                self._consumer_queues.append(q)
            logger.info(f"[Detector] Escuchando {queue_name}")
            q.start_consuming(self._on_heartbeat)
        except Exception as e:
            if not self._stop_event.is_set():
                logger.error(f"[Detector] Error consumiendo {queue_name}: {e}", exc_info=True)

    def _on_heartbeat(self, msg: bytes, ack, nack):
        try:
            payload = json.loads(msg.decode("utf-8"))
            etapa = payload["etapa"]
            instancia = payload["instancia"]
            ts = payload.get("timestamp", time.time())
            with self._lock:
                self._last_seen[(etapa, instancia)] = ts
            ack()
        except Exception as e:
            logger.warning(f"[Detector] Heartbeat malformado: {e}")
            ack()

    # ------------------------------------------------------------------
    # Hilo de revisión periódica
    # ------------------------------------------------------------------

    def _check_loop(self):
        while not self._stop_event.wait(self._config.check_interval_seconds):
            now = time.time()
            with self._lock:
                snapshot = dict(self._last_seen)

            caidas = [
                (etapa, instancia)
                for (etapa, instancia), last_ts in snapshot.items()
                if now - last_ts > self._config.timeout_seconds
            ]

            for etapa, instancia in caidas:
                elapsed = now - snapshot[(etapa, instancia)]
                logger.warning(
                    f"[Detector] Caída detectada: {etapa}/{instancia} "
                    f"(último heartbeat hace {elapsed:.1f}s)"
                )
                self._publicar_caida(etapa, instancia)

    def _publicar_caida(self, etapa: str, instancia: str):
        try:
            if self._caidas_queue is None:
                self._caidas_queue = MessageMiddlewareQueueRabbitMQ(
                    self._config.mom_host,
                    self._config.caidas_queue,
                )

            evento = {"etapa": etapa, "instancia": instancia}
            self._caidas_queue.send(json.dumps(evento).encode("utf-8"))

            # Sacar del tracking para no publicar múltiples veces la misma caída
            with self._lock:
                self._last_seen.pop((etapa, instancia), None)

            logger.info(f"[Detector] Evento de caída publicado: {etapa}/{instancia}")
        except Exception as e:
            logger.error(
                f"[Detector] Error publicando caída de {etapa}/{instancia}: {e}",
                exc_info=True,
            )
