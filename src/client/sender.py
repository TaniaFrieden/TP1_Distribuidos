import logging
import json
from common import message_protocol
from config import LOTE_SIZE

def enviar_archivo(filepath, tipo_mensaje, sock, lock, client_id):
    logging.info(f"Iniciando envío dinámico desde {filepath} para client_id {client_id}")
    try:
        # Leemos el archivo y extraemos headers dinámicamente
        headers, registros = _leer_registros_dinamico(filepath)
        if headers is not None:
            _enviar_lotes(headers, registros, tipo_mensaje, sock, lock, client_id)
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
            return None, []
        headers = linea_headers.split(",")
        
    def iterar_filas():
        with open(filepath, "r", encoding="utf-8") as f:
            f.readline() # saltar encabezado
            for linea in f:
                if linea.strip():
                    valores = linea.strip().split(",")
                    yield valores
                    
    return headers, iterar_filas()

def _enviar_lotes(headers, registros, tipo_mensaje, sock, lock, client_id):
    lote = []
    for registro in registros:
        lote.append(registro)
        if len(lote) >= LOTE_SIZE:
            with lock:
                message_protocol.external.send_msg(sock, tipo_mensaje, headers, client_id, lote)
            lote = []
    if lote:
        with lock:
            message_protocol.external.send_msg(sock, tipo_mensaje, headers, client_id, lote)