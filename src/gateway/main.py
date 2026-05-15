import json
import logging
import multiprocessing
import os
import signal
import socket
from typing import cast

import message_handler
from common import message_protocol

SERVER_HOST = os.environ["SERVER_HOST"]
SERVER_PORT = int(os.environ["SERVER_PORT"])


def _is_usd_record(record):
    """Acepta variantes de nombre de campo para moneda."""
    currency = (
        record.get("payment_currency")
        or record.get("Payment Currency")
        or record.get("Receiving Currency")
        or record.get("currency")
    )
    if not isinstance(currency, str):
        return False

    normalized = currency.strip().lower().replace("_", " ")
    return normalized in {"usd", "us dollar", "us dollars", "dolar estadounidense"}

def handle_client_request(client_socket, handler):
    """Recibe lotes de un cliente y responde con un reporte de confirmación."""
    usd_records = []

    try:
        while True:
            msg_type, payload = message_protocol.external.recv_msg(client_socket)

            if msg_type == message_protocol.external.MsgType.LOTE:
                if not isinstance(payload, list):
                    continue

                lote = cast(list, payload)
                for record in lote:
                    serialized_message = handler.serialize_data_message(record)
                    logging.info(f"LOTE: {serialized_message}")
                    if isinstance(record, dict) and _is_usd_record(record):
                        usd_records.append(record)

                message_protocol.external.send_msg(
                    client_socket,
                    message_protocol.external.MsgType.ACK,
                )
                continue

            if msg_type == message_protocol.external.MsgType.END_OF_RECODS:
                serialized_message = handler.serialize_eof_message(payload)
                logging.info(f"EOF: {serialized_message}")
                reporte = json.dumps(usd_records, ensure_ascii=False)
                message_protocol.external.send_msg(
                    client_socket,
                    message_protocol.external.MsgType.REPORTE,
                    reporte,
                )
                message_protocol.external.recv_msg(client_socket)
                return
    except socket.error:
        logging.error("Se perdió la conexión con el cliente")
    except Exception as exc:
        logging.error(exc)


def handle_sigterm(server_socket, client_list, sigterm_received):
    logging.info("Recibida señal de terminación, iniciando cierre graceful...")
    sigterm_received.value = 1

    try:
        server_socket.shutdown(socket.SHUT_RDWR)
    except Exception:
        pass

    try:
        server_socket.close()
    except Exception:
        pass

    for [_, client_socket] in client_list:
        try:
            client_socket.shutdown(socket.SHUT_RDWR)
        except Exception:
            pass
        try:
            client_socket.close()
        except Exception:
            pass


def main():
    logging.basicConfig(level=logging.INFO)

    with multiprocessing.Manager() as manager:
        client_list = manager.list()
        sigterm_received = manager.Value("c_short", 0)

        with multiprocessing.Pool(processes=os.process_cpu_count()) as processes_pool:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server_socket:
                server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
                server_socket.bind((SERVER_HOST, SERVER_PORT))
                server_socket.listen()
                logging.info(f"Gateway escuchando en {SERVER_HOST}:{SERVER_PORT}")

                def signal_handler(signum, frame):
                    handle_sigterm(server_socket, client_list, sigterm_received)

                signal.signal(signal.SIGTERM, signal_handler)
                signal.signal(signal.SIGINT, signal_handler)

                while True:
                    try:
                        client_socket, _ = server_socket.accept()
                        logging.info("Nuevo cliente conectado")
                        handler = message_handler.MessageHandler()
                        client_list.append([handler, client_socket])
                        processes_pool.apply_async(
                            handle_client_request,
                            (client_socket, handler),
                        )
                    except socket.error:
                        if sigterm_received.value == 0:
                            logging.error("Se perdió la conexión con un cliente")
                            return 1
                        logging.info("Cerrando servidor...")
                        break
                    except Exception as exc:
                        logging.error(exc)
                        return 2

            logging.info("Esperando a que finalicen los procesos...")
            processes_pool.terminate()
            processes_pool.join()

    logging.info("Gateway cerrado correctamente")
    return 0


if __name__ == "__main__":
    main()