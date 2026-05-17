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
COLA_ENTRADA = os.getenv("INPUT_QUEUE", "filtered_data2")
COLA_SALIDA = os.getenv("OUTPUT_QUEUE", "raw_data2")
MOM_HOST = os.getenv("MOM_HOST", "localhost")

clientes_conectados = {}
clientes_locks = {}  # <-- NUEVO: Candados para evitar colisiones al escribir en el socket
servidor_corriendo = True

CSV_HEADERS = [
    "Timestamp", "From Bank", "Account", "To Bank", "Account.1", 
    "Amount Received", "Receiving Currency", "Amount Paid", 
    "Payment Currency", "Payment Format", "Is Laundering"
]

def escuchar_respuestas_backend():
    """Hilo que lee de RabbitMQ y le manda los resultados al cliente."""
    cola_entrada = middleware.MessageMiddlewareQueueRabbitMQ(MOM_HOST, COLA_ENTRADA)

    # --- CAMBIO ACÁ: Usamos la firma (body, ack, nack) que espera tu middleware ---
    def callback(body, ack, nack):
        try:
            mensaje_str = body.decode('utf-8')
            transaccion = json.loads(mensaje_str)
            client_id = transaccion.get("client_id")

            if client_id in clientes_conectados:
                sock = clientes_conectados[client_id]
                lock = clientes_locks.get(client_id)

                # Si por algún motivo perdimos el lock, lo ignoramos
                if not lock:
                    ack()
                    return

                if transaccion.get("EOF"):
                    logging.info(f"[GATEWAY -> CLIENTE] Enviando EOF final a {client_id}")
                    with lock:
                        message_protocol.external.send_msg(sock, message_protocol.external.MsgType.END_OF_RECODS)
                    
                    # Limpieza de memoria
                    del clientes_conectados[client_id]
                    del clientes_locks[client_id]
                
                else:
                    valores_csv = [str(transaccion.get(col, "")) for col in CSV_HEADERS]
                    fila_texto_plano = ",".join(valores_csv)

                    logging.info(f"[GATEWAY -> CLIENTE] Ruteando TX filtrada: {fila_texto_plano[:50]}...")
                    
                    with lock:
                        message_protocol.external.send_msg(sock, message_protocol.external.MsgType.REPORTE, fila_texto_plano)
            else:
                logging.warning(f"Mensaje para cliente desconectado: {client_id}")

            # Confirmamos a RabbitMQ que ya procesamos el mensaje exitosamente
            ack()

        except json.JSONDecodeError:
            logging.error("Llegó un mensaje a la cola que no es un JSON válido.")
            # Si es basura irrecuperable, le damos ACK para sacarlo de la cola y que no trabe
            ack() 
        except Exception as e:
            logging.error(f"Error procesando respuesta del backend: {e}", exc_info=True)
            # Si es un error del sistema, le damos NACK para que se reencole
            nack() 

    logging.info("Gateway escuchando respuestas de los workers (Backend -> Cliente)...")
    cola_entrada.start_consuming(callback)
def atender_cliente(client_socket):
    """
    Recibe las líneas de CSV, las parsea a diccionario, 
    inyecta el client_id y manda todo a RabbitMQ.
    """
    client_id = str(uuid.uuid4())
    clientes_conectados[client_id] = client_socket
    clientes_locks[client_id] = threading.Lock() # Creamos el lock para este cliente específico
    
    logging.info(f"Cliente {client_id} conectado. Iniciando recepción...")
    
    # 1. ENVIAR ENCABEZADOS AL CONECTARSE
    try:
        encabezados_str = ",".join(CSV_HEADERS)
        with clientes_locks[client_id]:
            message_protocol.external.send_msg(client_socket, message_protocol.external.MsgType.REPORTE, encabezados_str)
        logging.info(f"[GATEWAY -> CLIENTE] Encabezados CSV enviados a {client_id}")
    except Exception as e:
        logging.error(f"Error al enviar encabezados: {e}")

    cola_salida = middleware.MessageMiddlewareQueueRabbitMQ(MOM_HOST, COLA_SALIDA)

    try:
        while True:
            msg_type, payload = message_protocol.external.recv_msg(client_socket)

            if msg_type == message_protocol.external.MsgType.LOTE:
                logging.info(f"[CLIENTE -> GATEWAY] Recibido lote de {len(payload)} líneas de {client_id}")
                
                for record in payload:
                    valores = record.split(',')
                    
                    # 2. SALTEAR LA FILA DE TÍTULOS
                    if valores[0] == "Timestamp":
                        continue

                    if len(valores) == len(CSV_HEADERS):
                        transaccion_dict = dict(zip(CSV_HEADERS, valores))
                        transaccion_dict["client_id"] = client_id
                        
                        mensaje_json = json.dumps(transaccion_dict)
                        cola_salida.send(mensaje_json.encode("utf-8"))
                        # logging.info(f"[GATEWAY -> RABBITMQ] Enviado a cola: {valores[2]}") # Opcional si querés loguear envío a cola
                    else:
                        logging.warning(f"Fila descartada por formato incorrecto: {record}")

                # Confirmar lote usando el lock
                with clientes_locks[client_id]:
                    message_protocol.external.send_msg(client_socket, message_protocol.external.MsgType.ACK)

            elif msg_type == message_protocol.external.MsgType.END_OF_RECODS:
                mensaje_eof = json.dumps({"client_id": client_id, "EOF": True})
                cola_salida.send(mensaje_eof.encode("utf-8"))
                logging.info(f"[CLIENTE -> GATEWAY] {client_id} terminó de enviar datos (EOF principal).")
                break

    except socket.error:
        logging.warning(f"Cliente {client_id} se desconectó bruscamente.")
    except Exception as e:
        logging.error(f"Error procesando cliente {client_id}: {e}", exc_info=True)
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