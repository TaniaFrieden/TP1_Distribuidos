import logging
from common import message_protocol
from config import LOTE_SIZE

def enviar_archivo(filepath, tipo_mensaje, sock, lock):
    logging.info(f"Iniciando envío desde {filepath}")
    try:
        registros = _leer_registros(filepath)
        _enviar_lotes(registros, tipo_mensaje, sock, lock)
        logging.info(f"Envío de {filepath} finalizado.")
    except FileNotFoundError:
        logging.error(f"No se encontró el archivo: {filepath}")
    except Exception as e:
        logging.error(f"Error al enviar datos de {filepath}: {e}")

## --------------------
## Funciones auxiliares
## --------------------

def _leer_registros(filepath):
    with open(filepath, "r", encoding="utf-8") as f:
        return [linea.strip() for linea in f if linea.strip()]

def _enviar_lotes(registros, tipo_mensaje, sock, lock):
    for i in range(0, len(registros), LOTE_SIZE):
        lote = registros[i:i + LOTE_SIZE]
        
        with lock:
            message_protocol.external.send_msg(sock, tipo_mensaje, lote)