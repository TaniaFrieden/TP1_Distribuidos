import logging
import signal
import threading

from common.logging_setup import setup_logging
from config import WatchdogConfig
from detector import HeartbeatDetector
from ring_election import RingElection

logger = logging.getLogger(__name__)


def main():
    setup_logging("watchdog")
    config = WatchdogConfig()

    if not config.stages:
        logger.warning("[Watchdog] WATCHDOG_STAGES está vacío — no hay nada que monitorear.")

    detector = HeartbeatDetector(config)
    detector_running = False
    detector_lock = threading.Lock()

    def on_become_leader():
        nonlocal detector_running
        with detector_lock:
            if not detector_running:
                detector_running = True
                detector.start()
                logger.info(f"[Watchdog-{config.watchdog_id}] Soy el líder. Detector de caídas activo.")

    def on_lose_leader():
        nonlocal detector_running
        with detector_lock:
            if detector_running:
                detector_running = False
                detector.stop()
                logger.info(f"[Watchdog-{config.watchdog_id}] Perdí el liderazgo. Detector detenido.")

    election = RingElection(config, on_become_leader, on_lose_leader)
    stop_event = threading.Event()

    def handle_signal(signum, frame):
        logger.info(f"[Watchdog-{config.watchdog_id}] Señal recibida. Cerrando...")
        election.stop()
        detector.stop()
        stop_event.set()

    signal.signal(signal.SIGTERM, handle_signal)
    signal.signal(signal.SIGINT, handle_signal)

    logger.info(f"[Watchdog-{config.watchdog_id}] Iniciando. Anillo de {config.num_watchdogs} nodos.")
    election.start()

    stop_event.wait()
    logger.info(f"[Watchdog-{config.watchdog_id}] Apagado completo.")


if __name__ == "__main__":
    main()
