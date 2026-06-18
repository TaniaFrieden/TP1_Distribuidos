import os

from base import WorkerBase
from common.logger import Logger, obtener_logger
from common.persistencia import TAMANIO_BATCH_PERSISTENCIA
from common.constantes_protocolo import ID_SOLICITUD
from config_contador import ConfigContador
from acumulador_grupos import AcumuladorGrupos
from procesador_lotes import ProcesadorLotes
from persistencia_contador import PersistenciaContador, BASE_DIR
from emisor_resultados import EmisorResultados

logger = obtener_logger(__name__)


class ContadorDistintoWorker(WorkerBase):
    """
    Worker genérico de agrupación con conteo de valores distintos.

    Agrupa registros por GROUP_FIELDS, acumula un set de valores distintos de
    VALUE_FIELDS y, al recibir EOF de un cliente, emite los grupos que cumplen
    la condición definida por EXPECTED_COUNT y COMPARISON_OPERATOR.

    Variables de entorno:
      GROUP_FIELDS          campos por los que agrupar (CSV)
      GROUP_OUTPUT_FIELDS   nombres de salida para los campos de grupo (CSV)
      VALUE_FIELDS          campos cuyos valores se acumulan como set distinto (CSV)
      VALUE_OUTPUT_FIELDS   nombres de salida para los campos de valor (CSV, solo en explode)
      EXPECTED_COUNT        umbral para la condición (default: 5)
      COMPARISON_OPERATOR   "eq" | "gt" | "gte" (default: eq)
      EMIT_MODE             "explode" | "aggregate" (default: aggregate)
      COUNT_OUTPUT_FIELD    nombre del campo de conteo en modo aggregate (default: Amount Transactions)
    """

    TAMANIO_LOTE_GUARDADO = TAMANIO_BATCH_PERSISTENCIA

    def __init__(self):
        super().__init__()
        self.config = ConfigContador()
        self.acumulador = AcumuladorGrupos()
        prefijo = f"gdc_{self.configuracion.prefijo_nodo}_{self.configuracion.id_nodo}"
        self.persistencia = PersistenciaContador(prefijo, BASE_DIR)
        self.procesador = ProcesadorLotes(self.acumulador, self.config.campos_grupo, self.config.campos_valor)
        self.emisor = EmisorResultados(self.config, self._enviar)
        self._recuperar_estado()
        logger.info(
            f"[ContadorDistinto] group={self.config.campos_grupo} value={self.config.campos_valor} "
            f"expected={self.config.conteo_esperado} operator={self.config.operador} mode={self.config.modo_emision}"
        )

    def _recuperar_estado(self):
        """Restaura el estado de todos los clientes desde disco al iniciar."""
        datos = self.persistencia.recuperar_todos()
        for client_id, (grupos, vistos) in datos.items():
            with self.acumulador.lock:
                self.acumulador.restaurar(client_id, grupos, vistos)

    def procesar_payload(self, queue_name: str, client_id: str, payload: dict,
                         mensaje_original: bytes, ack, nack):
        acks_a_liberar = []
        try:
            with self.acumulador.lock:
                request_id = payload.get(ID_SOLICITUD)

                if request_id and self.acumulador.ya_visto(client_id, request_id):
                    logger.warning(f"[ContadorDistinto] Duplicado ignorado: request_id={request_id} client_id={client_id}")
                    acks_a_liberar = [ack]
                else:
                    self.procesador.procesar_payload(payload, client_id)
                    if request_id:
                        self.acumulador.marcar_visto(client_id, request_id)
                    self.acumulador.registrar_ack(client_id, ack)

                    if self.acumulador.total_acks_pendientes() >= self.TAMANIO_LOTE_GUARDADO:
                        clientes = self.acumulador.clientes_con_acks()
                        for cid in clientes:
                            self.persistencia.guardar(
                                cid,
                                self.acumulador.snapshot_grupos(cid),
                                self.acumulador.snapshot_vistos(cid),
                            )
                        for cid in clientes:
                            acks_a_liberar.extend(self.acumulador.extraer_acks(cid))

        except Exception as e:
            logger.error(f"Error procesando payload: {e}", exc_info=True)
            nack()
            return

        for fn in acks_a_liberar:
            fn()

    def al_completar_eof_local(self, client_id: str):
        """Libera los acks del último lote parcial antes de que el coordinador
        espere vuelos=0. Si esperáramos a al_completar_cliente habría deadlock."""
        acks_a_liberar = []
        with self.acumulador.lock:
            self.persistencia.guardar(
                client_id,
                self.acumulador.snapshot_grupos(client_id),
                self.acumulador.snapshot_vistos(client_id),
            )
            acks_a_liberar = self.acumulador.extraer_acks(client_id)
        for fn in acks_a_liberar:
            fn()

    def al_completar_cliente(self, client_id: str):
        """Emite resultados y limpia el estado del cliente al recibir EOF completo."""
        with self.acumulador.lock:
            self.persistencia.guardar(
                client_id,
                self.acumulador.snapshot_grupos(client_id),
                self.acumulador.snapshot_vistos(client_id),
            )
            grupos = self.acumulador.extraer_grupos(client_id)

        logger.info(f"[ContadorDistinto] grupos totales para client_id={client_id}: {len(grupos)}")
        top = sorted(grupos.items(), key=lambda x: len(x[1]), reverse=True)[:5]
        for clave_grupo, conjunto_valores in top:
            logger.info(f"[ContadorDistinto] grupo {clave_grupo}: {len(conjunto_valores)} valores distintos")

        enviados = self.emisor.emitir(client_id, grupos)
        logger.info(f"[ContadorDistinto] Flush completo para client_id={client_id}. Registros emitidos: {enviados}.")

        if os.environ.get("CRASH_AFTER_FLUSH") == "true":
            bandera = os.path.join(BASE_DIR, "crash_flush_done")
            if not os.path.exists(bandera):
                open(bandera, "w").close()
                logger.warning("[ContadorDistinto] CRASH_AFTER_FLUSH — muriendo después del envío, antes de barrier_completada")
                os._exit(1)

        self.persistencia.marcar_barrera_completada(client_id)
        self.persistencia.borrar(client_id)

    def al_desconectar_cliente(self, client_id: str):
        """Descarta el estado del cliente sin emitir resultados."""
        acks_a_liberar = []
        with self.acumulador.lock:
            acks_a_liberar = self.acumulador.extraer_acks(client_id)
            self.acumulador.extraer_grupos(client_id)
        self.persistencia.borrar(client_id)
        for fn in acks_a_liberar:
            fn()
        logger.info(f"[ContadorDistinto] Estado descartado para client_id={client_id}.")

    def al_cerrar(self):
        logger.info("[ContadorDistinto] Apagado.")


def main():
    Logger.configurar("contador_distinto")
    ContadorDistintoWorker().iniciar()


if __name__ == "__main__":
    main()
