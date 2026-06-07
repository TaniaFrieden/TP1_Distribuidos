import json
import logging
import signal
import threading

from common.logging_setup import setup_logging
from common.middleware.middleware_rabbitmq import MessageMiddlewareQueueRabbitMQ
from config import WatchdogConfig
from detector import HeartbeatDetector
from ring_election import RingElection

logger = logging.getLogger(__name__)


def _publicar_caidas_watchdogs(config: WatchdogConfig, dead_ids: list[int]):
    if not dead_ids:
        return
    try:
        q = MessageMiddlewareQueueRabbitMQ(config.mom_host, config.caidas_queue)
        for wd_id in dead_ids:
            evento = {"etapa": "watchdog", "instancia": str(wd_id)}
            q.send(json.dumps(evento).encode())
            logger.info(f"[Watchdog] Caída publicada para reinicio: watchdog_{wd_id}")
    except Exception as e:
        logger.error(f"[Watchdog] Error publicando caídas de watchdogs: {e}", exc_info=True)


def main():
    setup_logging("watchdog")
    config = WatchdogConfig()

    if not config.stages:
        logger.warning("[Watchdog] WATCHDOG_STAGES está vacío — no hay nada que monitorear.")

    current_detector: HeartbeatDetector | None = None
    detector_running = False
    detector_lock = threading.Lock()

    def on_become_leader(dead_watchdog_ids: list[int]):
        """Activa el detector de caídas de workers y publica los watchdogs caídos detectados.

        Crea una instancia nueva del detector en cada mandato para evitar que
        _stop_event quede seteado de un mandato anterior.
        """
        nonlocal detector_running, current_detector
        with detector_lock:
            if not detector_running:
                detector_running = True
                current_detector = HeartbeatDetector(config)
                current_detector.start()
                logger.info(f"[Watchdog-{config.watchdog_id}] Soy el líder. Detector de caídas activo.")
        _publicar_caidas_watchdogs(config, dead_watchdog_ids)

    def on_lose_leader():
        """Detiene el detector de caídas al ceder el liderazgo."""
        nonlocal detector_running, current_detector
        with detector_lock:
            if detector_running:
                detector_running = False
                if current_detector is not None:
                    current_detector.stop()
                    current_detector = None
                logger.info(f"[Watchdog-{config.watchdog_id}] Perdí el liderazgo. Detector detenido.")

    def on_standby_dead(node_id: int):
        logger.warning(f"[Watchdog-{config.watchdog_id}] Standby watchdog_{node_id} caído — publicando para reinicio.")
        _publicar_caidas_watchdogs(config, [node_id])

    election = RingElection(config, on_become_leader, on_lose_leader, on_standby_dead)
    stop_event = threading.Event()

    def handle_signal(signum, frame):
        logger.info(f"[Watchdog-{config.watchdog_id}] Señal recibida. Cerrando...")
        election.stop()
        with detector_lock:
            if current_detector is not None:
                current_detector.stop()
        stop_event.set()

    signal.signal(signal.SIGTERM, handle_signal)
    signal.signal(signal.SIGINT, handle_signal)

    logger.info(f"[Watchdog-{config.watchdog_id}] Iniciando. Anillo de {config.num_watchdogs} nodos.")
    election.start()

    stop_event.wait()
    logger.info(f"[Watchdog-{config.watchdog_id}] Apagado completo.")


if __name__ == "__main__":
    main()
