import json
import logging
import multiprocessing
import os
import signal
import socket
import threading
from typing import cast

import message_handler
from common import message_protocol, middleware

logging.basicConfig(level=logging.INFO)

SERVER_HOST = os.environ["SERVER_HOST"]
SERVER_PORT = int(os.environ["SERVER_PORT"])
COLA_ENTRADA = os.getenv("INPUT_QUEUE", "raw_data")
COLA_SALIDA = os.getenv("OUTPUT_QUEUE", "filtered_data")
MOM_HOST = os.getenv("MOM_HOST", "localhost")


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


def _listen_input_queue(client_socket, input_queue):
    """Hilo secundario: Consume de RabbitMQ de forma bloqueante y envía al cliente."""
    
    def on_message(body, ack, nack):
        try:
            logging.info(f"Recibido mensaje desde {COLA_ENTRADA}. Enviando reporte al cliente...")
            message_protocol.external.send_msg(
                client_socket,
                message_protocol.external.MsgType.REPORTE,
                body,
            )
            ack()  # Si se envió bien por el socket, confirmamos a RabbitMQ
        except socket.error:
            logging.error("Error de socket enviando reporte. Se hará nack para encolar de nuevo.")
            nack()
        except Exception as exc:
            logging.error(f"Error procesando mensaje de entrada: {exc}")
            nack()

    try:
        logging.info("Hilo de escucha de COLA_ENTRADA iniciado.")
        # Esto bloquea el hilo hasta que se llame a stop_consuming() desde afuera
        input_queue.start_consuming(on_message)
    except Exception as exc:
        logging.error(f"Error en el hilo de lectura de la cola de entrada: {exc}")
    finally:
        logging.info("Hilo de escucha de COLA_ENTRADA finalizado.")


def handle_client_request(client_socket, handler, sigterm_received):
    """Hilo principal del cliente: Recibe lotes del socket y los manda a COLA_SALIDA."""
    usd_records = []
    
    q1_queue = middleware.MessageMiddlewareQueueRabbitMQ(MOM_HOST, COLA_SALIDA)
    # Instanciamos la cola de entrada aquí para tener control sobre ella y poder detenerla
    input_queue = middleware.MessageMiddlewareQueueRabbitMQ(MOM_HOST, COLA_ENTRADA)

    # Iniciar el hilo que escucha de RabbitMQ de forma asíncrona
    back_thread = threading.Thread(
        target=_listen_input_queue, 
        args=(client_socket, input_queue),
        daemon=True
    )
    back_thread.start()

    try:
        while sigterm_received.value == 0:
            msg_type, payload = message_protocol.external.recv_msg(client_socket)

            if msg_type == message_protocol.external.MsgType.LOTE:
                if not isinstance(payload, list):
                    continue

                lote = cast(list, payload)
                for record in lote:
                    serialized_message = handler.serialize_data_message(record)
                    logging.info(f"LOTE: {serialized_message}")
                    q1_queue.send(serialized_message)
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
                
                # Avisamos al backend que se terminaron los registros
                q1_queue.send(serialized_message)
                
                # Reporte local opcional
                reporte_local = json.dumps(usd_records, ensure_ascii=False)
                message_protocol.external.send_msg(
                    client_socket,
                    message_protocol.external.MsgType.REPORTE,
                    reporte_local,
                )
                
                # Esperamos un ACK final del cliente
                message_protocol.external.recv_msg(client_socket)
                return
                
    except socket.error:
        logging.error("Se perdió la conexión con el cliente en el hilo emisor.")
    except Exception as exc:
        logging.error(f"Error general en handle_client_request: {exc}")
    finally:
        # 1. Cerramos la cola de salida local
        q1_queue.close()
        
        # 2. Frenamos el ciclo de consumo bloqueante de Pika (thread-safe)
        logging.info("Deteniendo consumo de la cola de entrada...")
        input_queue.stop_consuming()
        
        # 3. Esperamos a que el hilo secundario caiga limpiamente
        back_thread.join(timeout=2.0)
        
        # 4. Cerramos la conexión de la cola de entrada
        try:
            # Asumo que tienes un método close() heredado de RabbitMQBase
            input_queue.close() 
        except Exception:
            pass


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
                            (client_socket, handler, sigterm_received),
                        )
                    except socket.error:
                        if sigterm_received.value == 0:
                            logging.error("Se perdió la conexión con un cliente (Server Socket)")
                            return 1
                        logging.info("Cerrando servidor principal...")
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