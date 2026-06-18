import ctypes
import gc
from common.logger import obtener_logger

logger = obtener_logger(__name__)


class ManejadorCoordinacionEof:
    def __init__(self, total_colas_entrada, enviar_fn, interceptar_eof_fn,
                 al_completar_eof_local_fn, al_completar_cliente_fn, nombre_clase):
        self.coordinador = None
        self._total_colas = total_colas_entrada
        self._enviar = enviar_fn
        self._interceptar_eof = interceptar_eof_fn
        self._al_completar_eof_local = al_completar_eof_local_fn
        self._al_completar_cliente = al_completar_cliente_fn
        self._nombre_clase = nombre_clase
        self._eofs_pendientes_ack = {}

    def procesar_eof(self, nombre_cola, client_id, mensaje_json, mensaje, ack):
        if self._interceptar_eof(nombre_cola, client_id, mensaje_json, mensaje):
            return ack()

        logger.info(
            f"[{self._nombre_clase}] EOF en cola {nombre_cola}. "
            f"Esperando a {self._total_colas} colas locales."
        )

        if client_id not in self._eofs_pendientes_ack:
            self._eofs_pendientes_ack[client_id] = []
        self._eofs_pendientes_ack[client_id].append(ack)

        termino_local = self.coordinador.registrar_eof_local(
            client_id, nombre_cola, self._total_colas
        )

        if termino_local:
            logger.info(
                f"[{self._nombre_clase}] Todos los EOFs locales recibidos. "
                f"Iniciando barrera distribuida."
            )
            self._al_completar_eof_local(client_id)
            self.coordinador.iniciar_barrera(client_id, mensaje)
            self.coordinador.limpiar_eof_local(client_id)

    def al_completar_sincronizacion(self, client_id, mensaje_original):
        if mensaje_original is None:
            logger.info(
                f"[{self._nombre_clase}] Flusheando datos "
                f"para client_id={client_id}."
            )
            self._al_completar_cliente(client_id)
            try:
                gc.collect()
                ctypes.cdll.LoadLibrary("libc.so.6").malloc_trim(0)
            except Exception:
                pass
        else:
            logger.info(
                f"[{self._nombre_clase}] Barrera completa, "
                f"reenviando EOF para client_id={client_id}."
            )
            try:
                self._enviar(mensaje_original)
            except Exception as e:
                logger.warning(
                    f"[{self._nombre_clase}] Error al reenviar EOF "
                    f"al downstream: {e}"
                )

    def al_completar_barrera(self, client_id):
        logger.info(
            f"[{self._nombre_clase}] Barrera completada. Confirmando "
            f"ACKs de EOFs acumulados para {client_id}."
        )
        acks = self._eofs_pendientes_ack.pop(client_id, [])
        for ack_cb in acks:
            try:
                ack_cb()
            except Exception as e:
                logger.warning(
                    f"[{self._nombre_clase}] Error al ejecutar "
                    f"ACK diferido: {e}"
                )

    def limpiar_cliente(self, client_id):
        self._eofs_pendientes_ack.pop(client_id, None)
