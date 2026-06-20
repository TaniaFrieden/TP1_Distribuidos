import signal
import socket
import threading
import logging
import sys
import uuid
import time
import json
from common import message_protocol
from common.constantes_protocolo import ID_CLIENTE
from common.logger import Logger
from config import SERVER_HOST, SERVER_PORT, TRANSACTIONS_FILE, ACCOUNTS_FILE
from receiver import escuchar_respuesta
from sender import enviar_archivo

def main():
    Logger.configurar("client")
    inicio_cliente = time.perf_counter()

    sock = _conectar_socket()
    if not sock:
        return 1

    client_id = None
    try:
        tipo_mensaje, payload = message_protocol.external.recibir_mensaje(sock)
        if tipo_mensaje == message_protocol.external.TipoMensaje.CONFIG_QUERIES:
            config_data = json.loads(payload)
            queries = config_data.get("queries", [])
            client_id = config_data.get(ID_CLIENTE)
            logging.info(f"Conectado al Gateway")
            logging.info(f"ID Cliente: {client_id}")
            logging.info(f"Queries: {queries}")
        else:
            logging.warning("No se recibió la configuración esperada del gateway. Usando valores vacíos.")
            queries = []
    except Exception as e:
        logging.error(f"Error recibiendo configuración del gateway: {e}")
        queries = []

    if not client_id:
        client_id = str(uuid.uuid4())
        logging.warning(f"No se recibió ID del gateway. Generando fallback local: {client_id}")

    socket_lock = threading.Lock()
    shutdown_event = threading.Event()
    hilo_receptor, hilos_envio = _iniciar_hilos(sock, socket_lock, client_id, queries, inicio_cliente, shutdown_event)

    _esperar_envios(hilos_envio)

    if not shutdown_event.is_set():
        _enviar_fin_registros(sock, socket_lock, client_id)

    _finalizar_conexion(hilo_receptor, sock)
    return 0

def _conectar_socket():
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        sock.connect((SERVER_HOST, SERVER_PORT))
        return sock
    except Exception as e:
        logging.error(f"No se pudo conectar al servidor: {e}")
        return None

def _iniciar_hilos(sock, lock, client_id, queries, inicio_envio, shutdown_event):
    hilo_receptor = threading.Thread(
        target=escuchar_respuesta,
        args=(sock, queries, inicio_envio, client_id),
        daemon=True
    )

    hilo_transacciones = threading.Thread(
        target=enviar_archivo,
        args=(TRANSACTIONS_FILE, message_protocol.external.TipoMensaje.LOTE_TRANSACCIONES, sock, lock, client_id, shutdown_event)
    )

    hilo_bancos = threading.Thread(
        target=enviar_archivo,
        args=(ACCOUNTS_FILE, message_protocol.external.TipoMensaje.LOTE_BANCOS, sock, lock, client_id, shutdown_event)
    )

    hilo_receptor.start()
    hilo_transacciones.start()
    hilo_bancos.start()

    return hilo_receptor, [hilo_transacciones, hilo_bancos]

def _esperar_envios(hilos_envio):
    for hilo in hilos_envio:
        hilo.join()

def _enviar_fin_registros(sock, lock, client_id):
    with lock:
        message_protocol.external.enviar_mensaje(
            sock,
            message_protocol.external.TipoMensaje.FIN_DE_REGISTROS,
            client_id
        )

def _finalizar_conexion(hilo_receptor, sock):
    hilo_receptor.join()
    try:
        sock.close()
    except Exception:
        pass

if __name__ == "__main__":
    sys.exit(main())