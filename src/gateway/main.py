import os
import logging
import socket
import signal
import multiprocessing
from typing import cast
import message_handler
from common import middleware, message_protocol

SERVER_HOST = os.environ["SERVER_HOST"]
SERVER_PORT = int(os.environ["SERVER_PORT"])

MOM_HOST = os.environ["MOM_HOST"]
INPUT_QUEUE = os.environ["INPUT_QUEUE"]
#OUTPUT_QUEUE = os.environ["OUTPUT_QUEUE"]


def handle_client_request(client_socket, message_handler):
    #output_queue = middleware.MessageMiddlewareQueueRabbitMQ(MOM_HOST, OUTPUT_QUEUE)

    try:
        while True:
            msg_type, payload = message_protocol.external.recv_msg(client_socket)

            if msg_type == message_protocol.external.MsgType.LOTE:
                if not isinstance(payload, list):
                    continue
                lote = cast(list, payload)
                for record in lote:
                    serialized_message = message_handler.serialize_data_message(record)
                    #output_queue.send(serialized_message)
                    print(f"LOTE: {serialized_message}")
                message_protocol.external.send_msg(
                    client_socket, message_protocol.external.MsgType.ACK
                )

            if msg_type == message_protocol.external.MsgType.END_OF_RECODS:
                serialized_message = message_handler.serialize_eof_message(payload)
                print(f"EOF: {serialized_message}")
                #output_queue.send(serialized_message)
                message_protocol.external.send_msg(
                    client_socket,
                    message_protocol.external.MsgType.REPORTE,
                    "Procesamiento completado",
                )
                message_protocol.external.recv_msg(client_socket)
                return
    except socket.error:
        logging.error("The connection with the server was lost")
    except Exception as e:
        logging.error(e)
    #finally:
        #output_queue.close()


def handle_client_response(client_list):
    # input_queue = middleware.MessageMiddlewareQueueRabbitMQ(MOM_HOST, INPUT_QUEUE)

    def _consume_result(message, ack, nack):
        client_index = 0
        try:
            for [message_handler_instance, client_socket] in client_list:
                
                # deserialized_message = (
                #     message_handler_instance.deserialize_result_message(message)
                # )
                deserialized_message = "termino"

                if not deserialized_message:
                    client_index += 1
                    continue

                message_protocol.external.send_msg(
                    client_socket,
                    message_protocol.external.MsgType.REPORTE,
                    deserialized_message,
                )
                message_protocol.external.recv_msg(client_socket)
                break
            client_list.pop(client_index)
            ack()
        except socket.error:
            logging.error("The connection with the server was lost")
            client_list.pop(client_index)
            ack()
        except Exception as e:
            logging.error(e)
            nack()
            #input_queue.stop_consuming()

    # input_queue.start_consuming(_consume_result)
    # input_queue.close()


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
            processes_pool.apply_async(handle_client_response, (client_list,))

            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server_socket:
                server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
                logging.info("Listening to connections")
                server_socket.bind((SERVER_HOST, SERVER_PORT))
                server_socket.listen()
                
                def signal_handler(signum, frame):
                    handle_sigterm(server_socket, client_list, sigterm_received)

                signal.signal(signal.SIGTERM, signal_handler)
                signal.signal(signal.SIGINT, signal_handler)
                
                while True:
                    try:
                        client_socket, _ = server_socket.accept()

                        logging.info("A new client has connected")
                        message_handler_instance = message_handler.MessageHandler()
                        client_list.append([message_handler_instance, client_socket])
                        processes_pool.apply_async(
                            handle_client_request,
                            (client_socket, message_handler_instance),
                        )
                    except socket.error:
                        if sigterm_received.value == 0:
                            logging.error("The connection with the client was lost")
                            return 1
                        else:
                            logging.info("Cerrando servidor...")
                            break
                    except Exception as e:
                        logging.error(e)
                        return 2
            
            logging.info("Esperando a que finalicen los procesos...")
            processes_pool.terminate()
            processes_pool.join()
    
    logging.info("Gateway cerrado correctamente")
    return 0


if __name__ == "__main__":
    main()
