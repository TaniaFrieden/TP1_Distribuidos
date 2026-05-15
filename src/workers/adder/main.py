"""
AdderWorker
===========
Worker intermedio para escalar horizontalmente el pipeline.

Cuando hay N instancias de un worker de filtrado/proyección publicando
en la misma cola, el AdderWorker recibe sus resultados parciales,
los reenvía a la cola del Aggregator final, y propaga el EOF solo
cuando TODOS los workers upstream ya mandaron su EOF.

Esto resuelve el problema de EOF en sistemas distribuidos:
si hay 3 instancias del FiltroUSD, el Aggregator recibirá 3 EOFs
pero solo debe emitir el resultado cuando llegaron los 3.

Variables de entorno:
    RABBITMQ_HOST       host de RabbitMQ                   (default: "rabbitmq")
    COLA_ENTRADA        cola de entrada                    (requerida)
    COLA_SALIDA         cola de salida al aggregator       (requerida)
    N_PRODUCTORES       cantidad de workers upstream       (requerida)

Con un solo nodo (N_PRODUCTORES=1) es transparente: pasa todo directo.
Cuando escalen, solo cambian N_PRODUCTORES en el docker-compose.
"""

import os
import logging
from common.worker_base.base import BaseWorker
from common.middleware.middleware_rabbitmq import DirectQueueRabbitMQ
from common.message_protocol import internal as protocol

logger = logging.getLogger(__name__)


class AdderWorker(BaseWorker):

    def __init__(self):
        super().__init__()
        self._host          = os.environ.get("RABBITMQ_HOST", "rabbitmq")
        self._cola_entrada  = os.environ["COLA_ENTRADA"]
        self._cola_salida   = os.environ["COLA_SALIDA"]
        self._n_productores = int(os.environ["N_PRODUCTORES"])
        self._salida        = None

        # Contador de EOFs recibidos por client_id
        # Cuando llega a N_PRODUCTORES, se propaga el EOF
        self._eofs_recibidos: dict = {}

        logger.info(
            f"[AdderWorker] n_productores={self._n_productores} "
            f"{self._cola_entrada} -> {self._cola_salida}"
        )

    # ------------------------------------------------------------------
    # BaseWorker API
    # ------------------------------------------------------------------

    def inicializar_middleware(self):
        self._salida = DirectQueueRabbitMQ(self._host, self._cola_salida)
        return DirectQueueRabbitMQ(self._host, self._cola_entrada)

    def procesar_mensaje(self, mensaje: bytes, ack, nack):
        payload = protocol.deserialize(mensaje)
        client_id = protocol.get_client_id(payload)

        if protocol.is_eof(payload):
            self._eofs_recibidos[client_id] = self._eofs_recibidos.get(client_id, 0) + 1
            recibidos = self._eofs_recibidos[client_id]
            logger.info(
                f"[AdderWorker] EOF {recibidos}/{self._n_productores} "
                f"client_id={client_id}"
            )

            if recibidos >= self._n_productores:
                # Todos los upstream terminaron → propagar EOF al aggregator
                self._salida.send(protocol.make_eof(client_id))
                del self._eofs_recibidos[client_id]
                logger.info(
                    f"[AdderWorker] EOF propagado al aggregator client_id={client_id}."
                )
            ack()
            return

        # Mensaje de datos: reenviar sin modificación
        self._salida.send(mensaje)
        ack()

    def al_cerrar(self):
        if self._salida:
            try:
                self._salida.close()
            except Exception as e:
                logger.warning(f"[AdderWorker] Error al cerrar salida: {e}")


def main():
    logging.basicConfig(level=logging.INFO)
    AdderWorker().iniciar()


if __name__ == "__main__":
    main()