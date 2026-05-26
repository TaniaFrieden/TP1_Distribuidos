import logging
import json
from common import message_protocol
from config import LOTE_SIZE

def enviar_archivo(filepath, tipo_mensaje, sock, lock, shutdown_event=None):
    logging.info(f"Iniciando envío dinámico desde {filepath}")
    try:
        registros = _leer_registros_dinamico(filepath)
        _enviar_lotes(registros, tipo_mensaje, sock, lock, shutdown_event)
        if shutdown_event and shutdown_event.is_set():
            logging.info(f"Envío de {filepath} interrumpido por cierre.")
        else:
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

def _enviar_lotes(registros, tipo_mensaje, sock, lock, shutdown_event=None):
    lote = []
    for registro in registros:
        if shutdown_event and shutdown_event.is_set():
            return
        lote.append(registro)
        if len(lote) >= LOTE_SIZE:
            with lock:
                message_protocol.external.send_msg(sock, tipo_mensaje, lote)
            lote = []
    if lote and not (shutdown_event and shutdown_event.is_set()):
        with lock:
            message_protocol.external.send_msg(sock, tipo_mensaje, lote)