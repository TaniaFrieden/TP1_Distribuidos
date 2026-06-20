import socket
import threading
import logging
import sys
import uuid
import time
import json
import os
from common import message_protocol
from common.logger import Logger
from config import SERVER_HOST, SERVER_PORT, TRANSACTIONS_FILE, ACCOUNTS_FILE, OUTPUT_DIR
from receiver import escuchar_respuesta
from sender import enviar_archivo

CLIENT_ID_SUFFIX = os.getenv('CLIENT_ID_SUFFIX', '')
CLIENT_ID_FILE = f"client_id_{CLIENT_ID_SUFFIX}.txt" if CLIENT_ID_SUFFIX else "client_id.txt"
ENVIO_COMPLETO_FILE = "envio_completo.txt"
INTENTOS_RECONEXION = 20
ESPERA_RECONEXION_SEG = 3


def main():
    Logger.configurar("client")
    inicio_cliente = time.perf_counter()

    client_id = _cargar_o_generar_client_id()

    for intento in range(INTENTOS_RECONEXION):
        if intento > 0:
            logging.info(f"Reconectando en {ESPERA_RECONEXION_SEG}s (intento {intento + 1}/{INTENTOS_RECONEXION})...")
            time.sleep(ESPERA_RECONEXION_SEG)

        resultado = _ejecutar_sesion(client_id, inicio_cliente)
        if resultado == "completado":
            break
        if resultado == "reintentar":
            continue
        break

    return 0


def _ejecutar_sesion(client_id, inicio_cliente):
    sock = _conectar_socket()
    if not sock:
        return "reintentar"

    try:
        # Handshake: el cliente se presenta primero
        hello_payload = json.dumps({"client_id": client_id})
        message_protocol.external.send_msg(sock, message_protocol.external.MsgType.HELLO, hello_payload)

        msg_type, payload = message_protocol.external.recv_msg(sock)
        if msg_type != message_protocol.external.MsgType.CONFIG_QUERIES:
            logging.warning("Respuesta inesperada del gateway en handshake")
            return "reintentar"

        config_data = json.loads(payload)
        queries = config_data.get("queries", [])
        omitir_envio = config_data.get("omitir_envio", False)
        logging.info(f"Conectado. ID: {client_id} | Queries: {queries} | Omitir envío: {omitir_envio}")

    except Exception as e:
        logging.error(f"Error en handshake con gateway: {e}")
        _cerrar_socket(sock)
        return "reintentar"

    socket_lock = threading.Lock()
    shutdown_event = threading.Event()
    evento_completado = threading.Event()

    hilo_receptor = threading.Thread(
        target=escuchar_respuesta,
        args=(sock, queries, inicio_cliente, client_id, evento_completado, socket_lock),
        daemon=True
    )
    hilo_receptor.start()

    ya_enviado = omitir_envio or _envio_ya_completado(client_id)

    if not ya_enviado:
        hilos_envio = _iniciar_hilos_envio(sock, socket_lock, client_id, shutdown_event)
        _esperar_envios(hilos_envio)

        if shutdown_event.is_set():
            logging.warning("Envío interrumpido por error de red, reconectando...")
            _cerrar_socket(sock)
            hilo_receptor.join(timeout=2)
            return "reintentar"

        try:
            _enviar_fin_registros(sock, socket_lock, client_id)
            _marcar_envio_completo(client_id)
        except (BrokenPipeError, ConnectionResetError, OSError) as e:
            logging.warning(f"Error enviando fin de registros: {e}, reconectando...")
            _cerrar_socket(sock)
            hilo_receptor.join(timeout=2)
            return "reintentar"
    else:
        logging.info("Datos ya enviados en sesión anterior, esperando resultados...")

    hilo_receptor.join()
    _cerrar_socket(sock)
    return "completado" if evento_completado.is_set() else "reintentar"


# --- Persistencia del client_id y estado de envío ---

def _cargar_o_generar_client_id():
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    id_path = os.path.join(OUTPUT_DIR, CLIENT_ID_FILE)
    if os.path.exists(id_path):
        with open(id_path, "r") as f:
            cid = f.read().strip()
        if cid:
            logging.info(f"Usando client_id persistido: {cid}")
            return cid
    cid = str(uuid.uuid4())
    with open(id_path, "w") as f:
        f.write(cid)
    logging.info(f"Nuevo client_id generado: {cid}")
    return cid


def _envio_ya_completado(client_id):
    path = os.path.join(OUTPUT_DIR, client_id, ENVIO_COMPLETO_FILE)
    return os.path.exists(path)


def _marcar_envio_completo(client_id):
    directorio = os.path.join(OUTPUT_DIR, client_id)
    os.makedirs(directorio, exist_ok=True)
    with open(os.path.join(directorio, ENVIO_COMPLETO_FILE), "w") as f:
        f.write("1")


# --- Helpers de red y envío ---

def _conectar_socket():
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        sock.connect((SERVER_HOST, SERVER_PORT))
        return sock
    except Exception as e:
        logging.error(f"No se pudo conectar al servidor: {e}")
        sock.close()
        return None


def _iniciar_hilos_envio(sock, lock, client_id, shutdown_event):
    hilo_tx = threading.Thread(
        target=enviar_archivo,
        args=(TRANSACTIONS_FILE, message_protocol.external.MsgType.LOTE_TRANSACCIONES,
              sock, lock, client_id, shutdown_event)
    )
    hilo_bancos = threading.Thread(
        target=enviar_archivo,
        args=(ACCOUNTS_FILE, message_protocol.external.MsgType.LOTE_BANCOS,
              sock, lock, client_id, shutdown_event)
    )
    hilo_tx.start()
    hilo_bancos.start()
    return [hilo_tx, hilo_bancos]


def _esperar_envios(hilos):
    for h in hilos:
        h.join()


def _enviar_fin_registros(sock, lock, client_id):
    with lock:
        message_protocol.external.send_msg(
            sock, message_protocol.external.MsgType.END_OF_RECODS, client_id
        )


def _cerrar_socket(sock):
    try:
        sock.close()
    except Exception:
        pass


if __name__ == "__main__":
    sys.exit(main())
