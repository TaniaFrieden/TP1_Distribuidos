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
from common.progreso import Progreso
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
        self._client_id = self._persistencia.cargar_id(CLIENT_ID_SUFFIX)

    def ejecutar(self):
        if not Enviador.validar_archivos_estatico(ARCHIVOS_A_ENVIAR):
            sys.exit(1)
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
        progreso = Progreso()

        receptor = Receptor(
            results_conn, queries, inicio, self._client_id,
            completado, self._persistencia, progreso,
        )
        hilo_receptor = threading.Thread(target=receptor.escuchar, daemon=True)
        hilo_receptor.start()

        if not omitir_envio:
            send_lock = threading.Lock()
            enviador = Enviador(data_conn, self._client_id, send_lock, shutdown, progreso)
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
            pass

        data_conn.cerrar()
        hilo_receptor.join()
        results_conn.cerrar()
        return RESULTADO_COMPLETADO if completado.is_set() else RESULTADO_FALLO_CONEXION

    def _handshake(self, conexion):
        hello_data = {}
        if self._client_id:
            hello_data["client_id"] = self._client_id
        conexion.enviar(TipoMensaje.HELLO, json.dumps(hello_data))

        tipo, respuesta = conexion.recibir()
        if tipo != TipoMensaje.CONFIG_QUERIES:
            raise Exception("Respuesta inesperada del gateway")

        config = json.loads(respuesta)
        assigned_id = config.get("client_id", "")
        if assigned_id and assigned_id != self._client_id:
            self._client_id = assigned_id
            self._persistencia.guardar_id(self._client_id, CLIENT_ID_SUFFIX)

        queries = config.get("queries", [])
        omitir_envio = config.get("omitir_envio", False)
        logging.info(f"Conectado al gateway. ID: {self._client_id} | Queries: {queries}")
        if omitir_envio:
            logging.info("El gateway confirmó que los datos ya fueron recibidos, esperando resultados...")
        return queries, omitir_envio


def main():
    Logger.configurar("client")
    try:
        cliente = Cliente()
        cliente.ejecutar()
    except KeyboardInterrupt:
        logging.info("Cliente interrumpido por el usuario.")
        return 130
    return 0


if __name__ == "__main__":
    sys.exit(main())
