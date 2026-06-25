import json
import logging
import os
import sys
import time
import threading
from common.conexion_tcp import ConexionTCP
from common.message_protocol.external import TipoMensaje
from common.logger import Logger
from config import SERVER_HOST, SERVER_PORT, TRANSACTIONS_FILE, ACCOUNTS_FILE, OUTPUT_DIR
from constantes import (
    INTENTOS_RECONEXION, ESPERA_RECONEXION_SEG,
    RESULTADO_COMPLETADO, RESULTADO_REINTENTAR, RESULTADO_FALLO_CONEXION,
)
from persistencia import PersistenciaCliente
from enviador import Enviador
from receptor import Receptor


ARCHIVOS_A_ENVIAR = [
    (TRANSACTIONS_FILE, TipoMensaje.LOTE_TRANSACCIONES),
    (ACCOUNTS_FILE, TipoMensaje.LOTE_BANCOS),
]

CLIENT_ID_SUFFIX = os.environ.get("CLIENT_ID_SUFFIX", "")


class Cliente:
    """Orquesta el envío de datos y la recepción de resultados."""

    def __init__(self):
        self._host = SERVER_HOST
        self._puerto = SERVER_PORT
        self._persistencia = PersistenciaCliente(OUTPUT_DIR)
        self._client_id = self._persistencia.cargar_o_generar_id(CLIENT_ID_SUFFIX)

    def ejecutar(self):
        inicio = time.perf_counter()
        intentos = INTENTOS_RECONEXION
        while intentos > 0:
            intentos -= 1
            resultado = self._ejecutar_sesion(inicio)
            if resultado == RESULTADO_COMPLETADO:
                return
            if resultado == RESULTADO_FALLO_CONEXION:
                intentos = INTENTOS_RECONEXION
            logging.info(
                f"Reconectando en {ESPERA_RECONEXION_SEG}s ({intentos} intentos restantes)..."
            )
            time.sleep(ESPERA_RECONEXION_SEG)

    def _ejecutar_sesion(self, inicio):
        data_conn = ConexionTCP(self._host, self._puerto)
        if not data_conn.conectar():
            return RESULTADO_REINTENTAR

        try:
            queries, omitir_envio = self._handshake(data_conn)
        except Exception as e:
            logging.error(f"Error en handshake: {e}")
            data_conn.cerrar()
            return RESULTADO_REINTENTAR

        results_conn = ConexionTCP(self._host, self._puerto)
        if not results_conn.conectar():
            data_conn.cerrar()
            return RESULTADO_REINTENTAR

        try:
            payload = json.dumps({"client_id": self._client_id})
            results_conn.enviar(TipoMensaje.HELLO_RESULTS, payload)
        except Exception as e:
            logging.error(f"Error conectando socket de resultados: {e}")
            data_conn.cerrar()
            results_conn.cerrar()
            return RESULTADO_REINTENTAR

        shutdown = threading.Event()
        completado = threading.Event()

        receptor = Receptor(
            results_conn, queries, inicio, self._client_id,
            completado, self._persistencia,
        )
        hilo_receptor = threading.Thread(target=receptor.escuchar, daemon=True)
        hilo_receptor.start()

        if not omitir_envio:
            send_lock = threading.Lock()
            enviador = Enviador(data_conn, self._client_id, send_lock, shutdown)
            enviador.enviar_archivos(ARCHIVOS_A_ENVIAR)

            if shutdown.is_set():
                logging.warning("Envío interrumpido, reconectando...")
                data_conn.cerrar()
                results_conn.cerrar()
                hilo_receptor.join(timeout=2)
                return RESULTADO_FALLO_CONEXION

            try:
                with send_lock:
                    data_conn.enviar(TipoMensaje.FIN_DE_REGISTROS, self._client_id)
                self._persistencia.marcar_envio_completo(self._client_id)
            except (BrokenPipeError, ConnectionResetError, OSError) as e:
                logging.warning(f"Error enviando fin de registros: {e}")
                data_conn.cerrar()
                results_conn.cerrar()
                hilo_receptor.join(timeout=2)
                return RESULTADO_FALLO_CONEXION
        else:
            logging.info("Datos ya enviados en sesión anterior, esperando resultados...")

        data_conn.cerrar()
        hilo_receptor.join()
        results_conn.cerrar()
        return RESULTADO_COMPLETADO if completado.is_set() else RESULTADO_FALLO_CONEXION

    def _handshake(self, conexion):
        payload = json.dumps({"client_id": self._client_id})
        conexion.enviar(TipoMensaje.HELLO, payload)

        tipo, respuesta = conexion.recibir()
        if tipo != TipoMensaje.CONFIG_QUERIES:
            raise Exception("Respuesta inesperada del gateway")

        config = json.loads(respuesta)
        queries = config.get("queries", [])
        omitir_envio = config.get("omitir_envio", False)
        logging.info(
            f"Conectado. ID: {self._client_id} | Queries: {queries} | Omitir envío: {omitir_envio}"
        )
        return queries, omitir_envio


def main():
    Logger.configurar("client")
    cliente = Cliente()
    cliente.ejecutar()
    return 0


if __name__ == "__main__":
    sys.exit(main())
