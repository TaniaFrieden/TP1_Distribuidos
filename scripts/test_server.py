#!/usr/bin/env python3
import os
import socket
import logging
from common import message_protocol

HOST = os.environ.get("SERVER_HOST", "0.0.0.0")
PORT = int(os.environ.get("SERVER_PORT", "5678"))

logging.basicConfig(level=logging.INFO, format="[TEST-SERVER] %(message)s")


def run():
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        s.bind((HOST, PORT))
        s.listen(1)
        logging.info(f"Escuchando en {HOST}:{PORT}")

        conn, addr = s.accept()
        with conn:
            logging.info(f"Conexión desde {addr}")
            while True:
                try:
                    msg_type, payload = message_protocol.external.recv_msg(conn)
                except Exception as e:
                    logging.info(f"Lectura terminada: {e}")
                    break

                logging.info(f"Tipo: {msg_type} -> {type(payload)}")

                if msg_type == message_protocol.external.MsgType.LOTE:
                    logging.info(f"Batch recibido: {len(payload)} registros")
                    # imprimir un ejemplo
                    if len(payload) > 0:
                        logging.info(f"Primer registro: {payload[0]}")
                    # enviar ACK
                    message_protocol.external.send_msg(conn, message_protocol.external.MsgType.ACK)

                elif msg_type == message_protocol.external.MsgType.END_OF_RECODS:
                    logging.info("END_OF_RECODS recibido")
                    message_protocol.external.send_msg(conn, message_protocol.external.MsgType.ACK)
                    
                    # Enviar un REPORTE simulado al cliente
                    reporte = "Procesamiento completado exitosamente"
                    logging.info(f"Enviando REPORTE: {reporte}")
                    message_protocol.external.send_msg(
                        conn, 
                        message_protocol.external.MsgType.REPORTE,
                        reporte
                    )
                    break

                else:
                    logging.info(f"Mensaje recibido: {payload}")
                    message_protocol.external.send_msg(conn, message_protocol.external.MsgType.ACK)


if __name__ == '__main__':
    run()
