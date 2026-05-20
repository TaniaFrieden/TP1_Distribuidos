import os
import socket
import threading
import logging
import uuid
import signal
import json
import sys

from common import message_protocol, middleware

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")

SERVER_HOST = os.environ.get("SERVER_HOST", "0.0.0.0")
SERVER_PORT = int(os.environ.get("SERVER_PORT", "12345"))
MOM_HOST = os.getenv("MOM_HOST", "localhost")

# Colas de entrada al sistema (Backend -> RabbitMQ)
OUTPUT_QUEUE = os.getenv("OUTPUT_QUEUE", "raw_data")

# Constantes de negocio
NUM_QUERIES = 2
CSV_HEADERS_TX = [
    "Timestamp", "From Bank", "Account", "To Bank", "Account.1", 
    "Amount Received", "Receiving Currency", "Amount Paid", 
    "Payment Currency", "Payment Format", "Is Laundering"
]

# Control de estado de clientes
clientes_conectados = {}
clientes_locks = {}
clientes_eof_status = {}  # Set para rastrear qué queries terminaron por cada cliente
servidor_corriendo = True

def escuchar_respuestas_backend(query_id):
    """Hilo que escucha una cola específica de resultados y envía el JSON al cliente."""
    cola_nombre = f"q{query_id}_results"
    cola_entrada = middleware.MessageMiddlewareQueueRabbitMQ(MOM_HOST, cola_nombre)

    def callback(body, ack, nack):
        try:
            mensaje_str = body.decode('utf-8')
            transaccion = json.loads(mensaje_str)
            client_id = transaccion.get("client_id")

            if client_id in clientes_conectados:
                sock = clientes_conectados[client_id]
                lock = clientes_locks.get(client_id)
                eof_status = clientes_eof_status.get(client_id)

                if not lock or eof_status is None:
                    ack()
                    return

                # Limpiamos metadatos internos antes de enviar al cliente
                transaccion.pop("client_id", None)
                es_eof = transaccion.pop("EOF", False) or transaccion.pop("eof", False)

                # Armamos el JSON según el protocolo del cliente
                payload = {
                    "query": query_id,
                    "resultado": {"eof": True} if es_eof else transaccion
                }
                
                payload_str = json.dumps(payload)

                with lock:
                    message_protocol.external.send_msg(sock, message_protocol.external.MsgType.REPORTE, payload_str)

                # Si es el final de esta query, actualizamos el estado
                if es_eof:
                    logging.info(f"[GATEWAY -> CLIENTE] EOF de query {query_id} enviado a {client_id}")
                    eof_status.add(query_id)
                    
                    # Si ya terminaron todas las queries, mandamos el fin global
                    if len(eof_status) == NUM_QUERIES:
                        logging.info(f"Todas las queries finalizadas para {client_id}. Enviando EOF global y cerrando sesión.")
                        with lock:
                            message_protocol.external.send_msg(sock, message_protocol.external.MsgType.END_OF_RECODS)
                        
                        # Limpieza de memoria
                        del clientes_conectados[client_id]
                        del clientes_locks[client_id]
                        del clientes_eof_status[client_id]
            else:
                logging.warning(f"Mensaje para cliente desconectado (ignorado): {client_id}")

            ack()

        except json.JSONDecodeError:
            logging.error("Llegó un mensaje a la cola que no es un JSON válido.")
            ack()
        except Exception as e:
            logging.error(f"Error procesando respuesta del backend: {e}", exc_info=True)
            nack()

    logging.info(f"Gateway escuchando resultados en la cola: {cola_nombre}")
    cola_entrada.start_consuming(callback)


def atender_cliente(client_socket):
    """Recibe lotes, inyecta client_id y los rutea a la cola correspondiente."""
    client_id = str(uuid.uuid4())
    clientes_conectados[client_id] = client_socket
    clientes_locks[client_id] = threading.Lock()
    clientes_eof_status[client_id] = set()
    
    logging.info(f"Cliente {client_id} conectado. Iniciando recepción...")
    
    cola_tx = middleware.MessageMiddlewareQueueRabbitMQ(MOM_HOST, OUTPUT_QUEUE)

    try:
        while True:
            msg_type, payload = message_protocol.external.recv_msg(client_socket)

            # --- MANEJO DE TRANSACCIONES ---
            if msg_type == message_protocol.external.MsgType.LOTE_TRANSACCIONES :
                for record in payload:
                    valores = record.split(',')
                    if valores[0] == "Timestamp": continue  # Omitir cabecera original
                    
                    if len(valores) == len(CSV_HEADERS_TX):
                        transaccion_dict = dict(zip(CSV_HEADERS_TX, valores))
                        transaccion_dict["client_id"] = client_id
                        cola_tx.send(json.dumps(transaccion_dict).encode("utf-8"))
                        
                with clientes_locks.get(client_id, threading.Lock()):
                    message_protocol.external.send_msg(client_socket, message_protocol.external.MsgType.ACK)

                    
                with clientes_locks.get(client_id, threading.Lock()):
                    message_protocol.external.send_msg(client_socket, message_protocol.external.MsgType.ACK)

            # --- FIN DE REGISTROS DEL CLIENTE ---
            elif msg_type == message_protocol.external.MsgType.END_OF_RECODS:
                mensaje_eof = json.dumps({"client_id": client_id, "EOF": True}).encode("utf-8")
                # Se envía el EOF a las colas para que los workers sepan que no hay más datos
                cola_tx.send(mensaje_eof)
                logging.info(f"[CLIENTE -> GATEWAY] {client_id} terminó de enviar datos de subida.")
                break

    except socket.error:
        logging.warning(f"Cliente {client_id} se desconectó bruscamente.")
    except Exception as e:
        logging.error(f"Error procesando cliente {client_id}: {e}", exc_info=True)
    finally:
        cola_tx.close()


def main():
    global servidor_corriendo

    # Levantamos 5 hilos, uno para cada cola de resultados de query
    hilos_queries = []
    for q_id in range(1, NUM_QUERIES + 1):
        t = threading.Thread(target=escuchar_respuestas_backend, args=(q_id,), daemon=True)
        t.start()
        hilos_queries.append(t)

    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server_socket.bind((SERVER_HOST, SERVER_PORT))
    server_socket.listen()
    
    logging.info(f"Gateway listo y escuchando en {SERVER_HOST}:{SERVER_PORT}")

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