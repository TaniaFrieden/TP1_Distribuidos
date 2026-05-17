"""
AggregatorWorker
================
Acumula resultados de los workers upstream y, al recibir el EOF,
emite el resultado final hacia el gateway.

La función de agregación es configurable via MODO:

    collect   → acumula todos los registros como lista           (Q1, Q3, Q4)
    count     → cuenta los registros, emite el total             (Q5)
    max       → emite el registro con mayor valor en CAMPO_MAX   (Q2)

Variables de entorno:
    RABBITMQ_HOST   host de RabbitMQ                   (default: "rabbitmq")
    COLA_ENTRADA    cola de entrada                    (requerida)
    COLA_SALIDA     cola de salida hacia el gateway    (requerida)
    QUERY_ID        identificador de la query          (requerida)
    MODO            collect | count | max              (default: "collect")
    CAMPO_MAX       campo a maximizar (solo modo max)  (requerida si MODO=max)

Protocolo de salida:
    El gateway espera en deserialize_result_message una lista de dos elementos:
        [client_id, resultado]
    Seguido de un EOF {"client_id": ...} para cerrar el stream.
"""

import os
import logging
from base import BaseWorker
from common.middleware.middleware_rabbitmq import DirectQueueRabbitMQ
from common.message_protocol import internal as protocol

logger = logging.getLogger(__name__)

MODOS = ("collect", "count", "max")


class AggregatorWorker(BaseWorker):

    def __init__(self):
        super().__init__()
        self._host         = os.environ.get("RABBITMQ_HOST", "rabbitmq")
        self._cola_entrada = os.environ["COLA_ENTRADA"]
        self._cola_salida  = os.environ["COLA_SALIDA"]
        self._query_id     = os.environ["QUERY_ID"]
        self._modo         = os.environ.get("MODO", "collect")
        self._campo_max    = os.environ.get("CAMPO_MAX", "")
        self._salida       = None

        if self._modo not in MODOS:
            raise ValueError(f"MODO invalido: '{self._modo}'. Validos: {MODOS}")
        if self._modo == "max" and not self._campo_max:
            raise ValueError("MODO=max requiere definir CAMPO_MAX")

        # Estado por client_id — preparado para múltiples clientes
        self._estado: dict = {}

        logger.info(
            f"[AggregatorWorker] query={self._query_id} modo={self._modo}"
            + (f" campo_max={self._campo_max}" if self._modo == "max" else "")
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
            logger.info(
                f"[AggregatorWorker] EOF client_id={client_id}. Emitiendo resultado."
            )
            self._emitir_resultado(client_id)
            ack()
            return

        self._acumular(client_id, payload)
        ack()

    def al_cerrar(self):
        if self._salida:
            try:
                self._salida.close()
            except Exception as e:
                logger.warning(f"[AggregatorWorker] Error al cerrar salida: {e}")

    # ------------------------------------------------------------------
    # Lógica de agregación
    # ------------------------------------------------------------------

    def _estado_inicial(self):
        if self._modo == "collect":
            return []
        if self._modo == "count":
            return 0
        if self._modo == "max":
            return None

    def _acumular(self, client_id: int, payload: dict):
        if client_id not in self._estado:
            self._estado[client_id] = self._estado_inicial()

        if self._modo == "collect":
            self._estado[client_id].append(payload)

        elif self._modo == "count":
            self._estado[client_id] += 1

        elif self._modo == "max":
            actual = self._estado[client_id]
            if actual is None or payload[self._campo_max] > actual[self._campo_max]:
                self._estado[client_id] = payload

    def _resultado_final(self, client_id: int):
        estado = self._estado.pop(client_id, self._estado_inicial())
        if self._modo == "collect":
            return estado
        if self._modo == "count":
            return estado
        if self._modo == "max":
            return [estado] if estado else []

    # ------------------------------------------------------------------
    # Emisión hacia el gateway
    # ------------------------------------------------------------------

    def _emitir_resultado(self, client_id: int):
        resultado = self._resultado_final(client_id)

        # El gateway espera [client_id, resultado] según deserialize_result_message
        self._salida.send(protocol.serialize([client_id, resultado]))
        logger.info(f"[AggregatorWorker] Resultado emitido client_id={client_id}.")

        # EOF para que el gateway sepa que esta query terminó
        self._salida.send(protocol.make_eof(client_id))
        logger.info(f"[AggregatorWorker] EOF propagado client_id={client_id}.")


def main():
    logging.basicConfig(level=logging.INFO)
    AggregatorWorker().iniciar()


if __name__ == "__main__":
    main()