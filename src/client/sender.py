import logging
import json
from common import message_protocol
from config import LOTE_SIZE


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

def enviar_archivo(filepath, tipo_mensaje, sock, lock, client_id, shutdown_event=None):
    logging.info(f"Iniciando envío dinámico desde {filepath} para client_id {client_id}")
    try:
        headers, registros = _leer_registros_dinamico(filepath)
        if headers is not None:
            _enviar_lotes(headers, registros, tipo_mensaje, sock, lock, client_id, shutdown_event)
            if shutdown_event and shutdown_event.is_set():
                logging.info(f"Envío de {filepath} interrumpido por cierre.")
            else:
                logging.info(f"Envío de {filepath} finalizado.")
    except Exception as e:
        logging.error(f"Error al procesar {filepath}: {e}")

def _enviar_lotes(headers, registros, tipo_mensaje, sock, lock, client_id, shutdown_event=None):
    lote = []
    for registro in registros:
        if shutdown_event and shutdown_event.is_set():
            return
        lote.append(registro)
        if len(lote) >= LOTE_SIZE:
            with lock:
                message_protocol.external.send_msg(sock, tipo_mensaje, headers, client_id, lote)
            lote = []
    if lote and not (shutdown_event and shutdown_event.is_set()):
        with lock:
            message_protocol.external.send_msg(sock, tipo_mensaje, headers, client_id, lote)