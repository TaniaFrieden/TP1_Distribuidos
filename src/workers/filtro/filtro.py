from base.worker_base import WorkerBase
from common.logger import Logger, obtener_logger
from common.constantes_protocolo import LOTES
from common.message_protocol.internal import ParseadorMensajes
from config_filtro import ConfigFiltro
from procesador_lotes import ProcesadorLotes
from reglas import FabricaReglas

logger = obtener_logger(__name__)


class WorkerFiltro(WorkerBase):
    def __init__(self):
        super().__init__()
        self.config = ConfigFiltro()
        self.regla = FabricaReglas.crear(
            self.config.operador_str,
            self.config.campo_objetivo,
            self.config.valor_objetivo_crudo,
        )
        self.procesador = ProcesadorLotes(self.regla)
        logger.info(
            f"[WorkerFiltro] Iniciado con regla para campo '{self.config.campo_objetivo}'"
        )

    def procesar_payload(self, nombre_cola, client_id, payload, mensaje_original, ack, nack):
        if LOTES in payload:
            resultado = self.procesador.procesar_payload(payload)
            if resultado:
                self._enviar(ParseadorMensajes.serializar(resultado), payload=resultado)
        elif self.regla.coincide(payload):
            self._enviar(mensaje_original, payload=payload)
        ack()

    def al_cerrar(self):
        logger.info("[WorkerFiltro] Apagado.")


def main():
    Logger.configurar("filter")
    WorkerFiltro().iniciar()


if __name__ == "__main__":
    main()
