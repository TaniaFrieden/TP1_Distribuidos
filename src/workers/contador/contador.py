from base.worker_base import WorkerBase
from common.logger import Logger, obtener_logger
from common.constantes_protocolo import ID_CLIENTE, LOTES, CABECERA, ESQUEMA, CANTIDAD, PAYLOAD, ID_SOLICITUD
from common.message_protocol.internal import ParseadorMensajes
from estado import EstadoConteo
from base.coordinacion.hooks import crear_hook_crash_despues_persistir, crear_hook_crash_despues_flush

logger = obtener_logger(__name__)


def _calcular_cantidad(payload: dict) -> int:
    if LOTES in payload:
        return sum(
            int(lote[CABECERA].get(CANTIDAD, len(lote[PAYLOAD])))
            for lote in payload[LOTES]
        )
    return 1


class WorkerContador(WorkerBase):

    def __init__(self):
        super().__init__()
        self.estado = EstadoConteo(self.configuracion.id_nodo)
        self._hook_post_persistir = crear_hook_crash_despues_persistir()
        self._hook_post_flush = crear_hook_crash_despues_flush()

    def procesar_payload(self, nombre_cola, client_id, payload, mensaje_original, ack, nack):
        try:
            request_id = payload.get(ID_SOLICITUD)
            cantidad = _calcular_cantidad(payload)
            ya_procesado = self.estado.incrementar(client_id, cantidad, request_id)

            if ya_procesado:
                logger.info(f"Duplicado detectado por persistencia: request_id={request_id}")
                ack()
                return

            if self._hook_post_persistir:
                self._hook_post_persistir()

            ack()
        except Exception as e:
            logger.error(f"Error contando mensaje: {e}", exc_info=True)
            nack()

    def al_completar_cliente(self, client_id: str):
        if self.estado.ya_completado(client_id):
            logger.info(f"Flush ya completado para {client_id}, omitiendo re-emisión.")
            return

        conteo = self.estado.obtener_y_limpiar(client_id)
        resultado = {
            ID_CLIENTE: client_id,
            LOTES: [{
                CABECERA: {
                    ESQUEMA: [CANTIDAD],
                    ID_CLIENTE: client_id,
                    CANTIDAD: 1,
                },
                PAYLOAD: [[conteo]],
            }],
        }
        self._enviar(ParseadorMensajes.serializar(resultado), payload=resultado)
        logger.info(f"Conteo emitido para {client_id}: {conteo} transacciones.")

        if self._hook_post_flush:
            self._hook_post_flush()

        self.estado.marcar_completado(client_id)

    def al_desconectar_cliente(self, client_id: str):
        self.estado.descartar(client_id)
        logger.info(f"Estado descartado para {client_id}.")

    def al_cerrar(self):
        logger.info("[WorkerContador] Apagado.")


def main():
    Logger.configurar("counter")
    WorkerContador().iniciar()


if __name__ == "__main__":
    main()
