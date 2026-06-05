import logging
import signal

from common.logging_setup import setup_logging
from actuador import Actuador

logger = logging.getLogger(__name__)


def main():
    setup_logging("actuador")

    actuador = Actuador()

    def handle_signal(signum, frame):
        logger.info("[Actuador] Señal recibida. Cerrando...")
        actuador.stop()

    signal.signal(signal.SIGTERM, handle_signal)
    signal.signal(signal.SIGINT, handle_signal)

    logger.info("[Actuador] Iniciando.")
    actuador.start()
    logger.info("[Actuador] Apagado completo.")


if __name__ == "__main__":
    main()
