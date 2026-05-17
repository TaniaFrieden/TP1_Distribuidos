import os
import socket
import threading
import logging
from common import message_protocol

INPUT_FILE = os.environ["INPUT_FILE"]
OUTPUT_FILE = os.environ["OUTPUT_FILE"]
SERVER_HOST = os.environ["SERVER_HOST"]
SERVER_PORT = int(os.environ["SERVER_PORT"])
LOTE = int(os.environ["BATCH_SIZE"])

def escuchar_respuesta(sock):
    """Hilo paralelo: recibe los reportes y los guarda en texto plano."""
    logging.info("Hilo receptor activo: Esperando reportes...")
    
    with open(OUTPUT_FILE, "w", newline="", encoding="utf-8") as f_out:
        while True:
            try:
                msg_type, payload = message_protocol.external.recv_msg(sock)
                
                if msg_type == message_protocol.external.MsgType.REPORTE:
                    f_out.write(str(payload) + "\n")
                    f_out.flush() 
                    
                elif msg_type == message_protocol.external.MsgType.END_OF_RECODS:
                    logging.info("Fin de registros recibido. Cerrando archivo.")
                    break
                    
                elif msg_type == message_protocol.external.MsgType.ACK:
                    logging.info("ACK recibido del servidor.")
                    continue
                    
            except Exception as e:
                logging.error(f"Error recibiendo respuesta: {e}")
                break

def main():
    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")

    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        sock.connect((SERVER_HOST, SERVER_PORT))
        logging.info(f"Conectado a {SERVER_HOST}:{SERVER_PORT}")
    except Exception as e:
        logging.error(f"No se pudo conectar al servidor: {e}")
        return 1

    hilo_receptor = threading.Thread(target=escuchar_respuesta, args=(sock,), daemon=True)
    hilo_receptor.start()

    logging.info(f"Leyendo y enviando transacciones crudas desde {INPUT_FILE}")
    try:
        with open(INPUT_FILE, "r", encoding="utf-8") as f:
            registros_crudos = [linea.strip() for linea in f if linea.strip()]
            
            for i in range(0, len(registros_crudos), LOTE):
                lote = registros_crudos[i:i + LOTE]
                logging.info(f"Enviando lote de {len(lote)} líneas de texto plano...")
                message_protocol.external.send_msg(
                    sock,
                    message_protocol.external.MsgType.LOTE,
                    lote
                )
            
        message_protocol.external.send_msg(
            sock,
            message_protocol.external.MsgType.END_OF_RECODS
        )
        logging.info("Señal de END_OF_RECODS enviada.")

    except FileNotFoundError:
        logging.error(f"No se encontró el archivo: {INPUT_FILE}")
    except Exception as e:
        logging.error(f"Error al enviar datos: {e}")

    hilo_receptor.join()
    sock.close()
    logging.info("Proceso terminado.")

if __name__ == "__main__":
    exit(main())