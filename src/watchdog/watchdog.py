import logging
import signal
import threading

from common.logging_setup import setup_logging
from config import WatchdogConfig
from detector import HeartbeatDetector

logger = logging.getLogger(__name__)


def main():
    setup_logging("watchdog")
    config = WatchdogConfig()

    if not config.stages:
        logger.warning("[Watchdog] WATCHDOG_STAGES está vacío — no hay nada que monitorear.")

    detector = HeartbeatDetector(config)
    stop_event = threading.Event()

    def handle_signal(signum, frame):
        logger.info("[Watchdog] Señal recibida. Cerrando...")
        detector.stop()
        stop_event.set()

    signal.signal(signal.SIGTERM, handle_signal)
    signal.signal(signal.SIGINT, handle_signal)

    logger.info("[Watchdog] Iniciando detector de caídas.")
    detector.start()

    stop_event.wait()
    logger.info("[Watchdog] Apagado completo.")


if __name__ == "__main__":
    main()
