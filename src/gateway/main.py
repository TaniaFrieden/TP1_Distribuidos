import os
import socket
import threading
import logging
import uuid
import signal
import sys

from common import message_protocol, middleware

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")

SERVER_HOST = os.environ.get("SERVER_HOST", "0.0.0.0")
SERVER_PORT = int(os.environ.get("SERVER_PORT", "12345"))
COLA_ENTRADA = os.getenv("INPUT_QUEUE", "filtered_data2")
COLA_SALIDA = os.getenv("OUTPUT_QUEUE", "raw_data2")
MOM_HOST = os.getenv("MOM_HOST", "localhost")

clientes_conectados = {}
servidor_corriendo = True

def escuchar_respuestas_backend():
    """
    Lee resultados crudos del backend.
    Espera el formato: "client_id|contenido" o "client_id|EOF"
    """
    cola_entrada = middleware.MessageMiddlewareQueueRabbitMQ(MOM_HOST, COLA_ENTRADA)
    
    def on_message(body, ack, nack):
        try:
            # 1. Decodificamos el string crudo
            mensaje_str = body.decode("utf-8")
            
            # 2. Separamos el ID del cliente del resto del mensaje
            # Usamos split("|", 1) para partirlo solo en la primera coincidencia
            partes = mensaje_str.split("|", 1)
            
            if len(partes) != 2:
                logging.warning(f"Mensaje descartado. Formato inválido desde backend: {mensaje_str}")
                ack()
                return
                
            client_id, contenido = partes
            
            # 3. Ruteamos al socket correspondiente
            if client_id in clientes_conectados:
                client_socket = clientes_conectados[client_id]
                
                if contenido == "EOF":
                    message_protocol.external.send_msg(client_socket, message_protocol.external.MsgType.END_OF_RECODS)
                    logging.info(f"Enviado EOF final al cliente {client_id}")
                else:
                    # Le mandamos el contenido del reporte directo
                    message_protocol.external.send_msg(client_socket, message_protocol.external.MsgType.REPORTE, contenido)
                    
            ack()
        except Exception as e:
            logging.error(f"Error ruteando respuesta del backend: {e}")
            nack()

    logging.info("Gateway: Escuchando reportes del backend en texto plano...")
    try:
        cola_entrada.start_consuming(on_message)
    except Exception:
        pass


def atender_cliente(client_socket):
    """
    Recibe las líneas de CSV, les concatena el client_id con un '|' y las manda.
    """
    client_id = str(uuid.uuid4())
    clientes_conectados[client_id] = client_socket
    logging.info(f"Cliente {client_id} conectado.")
    
    cola_salida = middleware.MessageMiddlewareQueueRabbitMQ(MOM_HOST, COLA_SALIDA)

    try:
        while True:
            msg_type, payload = message_protocol.external.recv_msg(client_socket)

            if msg_type == message_protocol.external.MsgType.LOTE:
                # 1. Iterar sobre las filas (ahora son strings de texto)
                for record in payload:
                    # 2. Inyección del client_id pegándolo al principio
                    mensaje_crudo = f"{client_id}|{record}"
                    cola_salida.send(mensaje_crudo.encode("utf-8"))

                # 3. Confirmar lote
                message_protocol.external.send_msg(client_socket, message_protocol.external.MsgType.ACK)

            elif msg_type == message_protocol.external.MsgType.END_OF_RECODS:
                # 1. Le avisamos al filtro mandando el ID y la palabra EOF
                mensaje_eof = f"{client_id}|EOF"
                cola_salida.send(mensaje_eof.encode("utf-8"))
                logging.info(f"Cliente {client_id} terminó de enviar datos.")
                break

    except socket.error:
        logging.warning(f"Cliente {client_id} se desconectó bruscamente.")
    except Exception as e:
        logging.error(f"Error procesando cliente {client_id}: {e}")
    finally:
        cola_salida.close()


def main():
    global servidor_corriendo

    hilo_respuestas = threading.Thread(target=escuchar_respuestas_backend, daemon=True)
    hilo_respuestas.start()

    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server_socket.bind((SERVER_HOST, SERVER_PORT))
    server_socket.listen()
    
    logging.info(f"Gateway crudo listo en {SERVER_HOST}:{SERVER_PORT}")

    def cerrar_graceful(sig, frame):
        logging.info("Apagando Gateway...")
        global servidor_corriendo
        servidor_corriendo = False
        server_socket.close()
        sys.exit(0)
        
    signal.signal(signal.SIGINT, cerrar_graceful)
    signal.signal(signal.SIGTERM, cerrar_graceful)

    try:
        while servidor_corriendo:
            client_sock, addr = server_socket.accept()
            hilo_cliente = threading.Thread(target=atender_cliente, args=(client_sock,), daemon=True)
            hilo_cliente.start()
    except Exception:
        pass
    finally:
        server_socket.close()

if __name__ == "__main__":
    main()