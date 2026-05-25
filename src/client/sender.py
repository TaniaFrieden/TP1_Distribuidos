import logging
import json
from common import message_protocol
from config import LOTE_SIZE

def enviar_archivo(filepath, tipo_mensaje, sock, lock):
    logging.info(f"Iniciando envío dinámico desde {filepath}")
    try:
        # Leemos el archivo y extraemos headers dinámicamente
        registros = _leer_registros_dinamico(filepath)
        _enviar_lotes(registros, tipo_mensaje, sock, lock)
        logging.info(f"Envío de {filepath} finalizado.")
    except Exception as e:
        logging.error(f"Error al procesar {filepath}: {e}")

## --------------------
## Funciones auxiliares
## --------------------    
def _leer_registros_dinamico(filepath):
    with open(filepath, "r", encoding="utf-8") as f:
        linea_headers = f.readline().strip()
        if not linea_headers:
            return
        headers = linea_headers.split(",")
        for linea in f:
            if linea.strip():
                valores = linea.strip().split(",")
                yield json.dumps(dict(zip(headers, valores)))

def _enviar_lotes(registros, tipo_mensaje, sock, lock):
    lote = []
    for registro in registros:
        lote.append(registro)
        if len(lote) >= LOTE_SIZE:
            with lock:
                message_protocol.external.send_msg(sock, tipo_mensaje, lote)
            lote = []
    if lote:
        with lock:
            message_protocol.external.send_msg(sock, tipo_mensaje, lote)