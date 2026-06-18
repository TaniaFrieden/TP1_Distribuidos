import signal

from common.logger import Logger, obtener_logger
from actuador import Actuador

logger = obtener_logger(__name__)


def main():
    Logger.configurar("actuador")

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
