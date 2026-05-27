import socket
import threading
import logging
import sys
import uuid
import time
from common import message_protocol
from common.logging_setup import setup_logging
from config import SERVER_HOST, SERVER_PORT, TRANSACTIONS_FILE, ACCOUNTS_FILE
from receiver import escuchar_respuesta
from sender import enviar_archivo

LOG_FORMAT = "%(levelname)s: %(message)s"

def main():
    setup_logging("client")
    inicio_cliente = time.perf_counter()
    
    sock = _conectar_socket()
    if not sock:
        return 1

    # Leer la configuración de queries enviada por el gateway al conectar
    try:
        msg_type, payload = message_protocol.external.recv_msg(sock)
        if msg_type == message_protocol.external.MsgType.CONFIG_QUERIES:
            queries = json.loads(payload)
            logging.info(f"Queries configuradas en el sistema: {queries}")
        else:
            logging.warning("No se recibió la configuración de queries esperada del gateway. Usando lista vacía.")
            queries = []
    except Exception as e:
        logging.error(f"Error recibiendo configuración de queries: {e}")
        queries = []

    client_id = str(uuid.uuid4())
    logging.info(f"Cliente iniciado con ID: {client_id}")

    socket_lock = threading.Lock()
    hilo_receptor, hilos_envio = _iniciar_hilos(sock, socket_lock, client_id, queries, inicio_cliente)
    
    _esperar_envios(hilos_envio)
    _enviar_fin_registros(sock, socket_lock, client_id)
    _finalizar_conexion(hilo_receptor, sock)
    logging.info(f"Cliente finalizado en {time.perf_counter() - inicio_cliente:.3f} s")
    
    return 0

## --------------------
## Funciones auxiliares
## --------------------

def _conectar_socket():
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        sock.connect((SERVER_HOST, SERVER_PORT))
        logging.info(f"Conectado a {SERVER_HOST}:{SERVER_PORT}")
        return sock
    except Exception as e:
        logging.error(f"No se pudo conectar al servidor: {e}")
        return None

import json
def _iniciar_hilos(sock, lock, client_id, queries, inicio_envio):
    hilo_receptor = threading.Thread(
        target=escuchar_respuesta,
        args=(sock, queries, inicio_envio),
        daemon=True
    )

    hilo_transacciones = threading.Thread(
        target=enviar_archivo,
        args=(TRANSACTIONS_FILE, message_protocol.external.MsgType.LOTE_TRANSACCIONES, sock, lock, client_id)
    )

    hilo_bancos = threading.Thread(
        target=enviar_archivo,
        args=(ACCOUNTS_FILE, message_protocol.external.MsgType.LOTE_BANCOS, sock, lock, client_id)
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
        message_protocol.external.send_msg(
            sock,
            message_protocol.external.MsgType.END_OF_RECODS,
            client_id
        )
    logging.info("Señal global de END_OF_RECODS enviada.")

def _finalizar_conexion(hilo_receptor, sock):
    hilo_receptor.join()
    sock.close()
    logging.info("Proceso terminado.")

if __name__ == "__main__":
    sys.exit(main())