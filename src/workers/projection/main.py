"""
ProjectionWorker
================
Recorta cada mensaje de datos a los campos indicados y lo reenvía.
Los mensajes EOF se propagan sin modificación.

Variables de entorno:
    RABBITMQ_HOST   host de RabbitMQ          (default: "rabbitmq")
    COLA_ENTRADA    cola de entrada            (requerida)
    COLA_SALIDA     cola de salida             (requerida)
    CAMPOS          campos a conservar, CSV    (requerida)
                    Ej: "from_id,to_id,amount_paid"

Uso en Q1:
    CAMPOS=from_id,to_id,amount_paid
"""

import os
import logging
from common.worker_base.base import BaseWorker
from common.middleware.middleware_rabbitmq import DirectQueueRabbitMQ
from common.message_protocol import internal as protocol

logger = logging.getLogger(__name__)


class ProjectionWorker(BaseWorker):

    def __init__(self):
        super().__init__()
        self._host         = os.environ.get("RABBITMQ_HOST", "rabbitmq")
        self._cola_entrada = os.environ["COLA_ENTRADA"]
        self._cola_salida  = os.environ["COLA_SALIDA"]
        campos_str         = os.environ["CAMPOS"]
        self._campos       = [c.strip() for c in campos_str.split(",") if c.strip()]
        self._salida       = None
        logger.info(f"[ProjectionWorker] campos={self._campos}")

    def inicializar_middleware(self):
        self._salida = DirectQueueRabbitMQ(self._host, self._cola_salida)
        return DirectQueueRabbitMQ(self._host, self._cola_entrada)

    def procesar_mensaje(self, mensaje: bytes, ack, nack):
        payload = protocol.deserialize(mensaje)

        if protocol.is_eof(payload):
            self._salida.send(mensaje)  # propagar EOF sin tocar
            logger.info("[ProjectionWorker] EOF propagado.")
            ack()
            return

        # Siempre conservar client_id para que los workers downstream
        # puedan identificar el cliente y propagar el EOF correctamente.
        proyectado = {"client_id": protocol.get_client_id(payload)}
        for campo in self._campos:
            if campo in payload:
                proyectado[campo] = payload[campo]

        self._salida.send(protocol.serialize(proyectado))
        ack()

    def al_cerrar(self):
        if self._salida:
            try:
                self._salida.close()
            except Exception as e:
                logger.warning(f"[ProjectionWorker] Error al cerrar salida: {e}")


def main():
    logging.basicConfig(level=logging.INFO)
    ProjectionWorker().iniciar()


if __name__ == "__main__":
    main()