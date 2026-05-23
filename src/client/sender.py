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
    registros_json = []
    with open(filepath, "r", encoding="utf-8") as f:
        # Leemos la primera línea para obtener los headers dinámicamente
        linea_headers = f.readline().strip()
        if not linea_headers:
            return []
            
        headers = linea_headers.split(",")
        
        # Procesamos el resto de las líneas
        for linea in f:
            if linea.strip():
                valores = linea.strip().split(",")
                # Creamos el diccionario usando los headers leídos hace un instante
                diccionario = dict(zip(headers, valores))
                registros_json.append(json.dumps(diccionario))
    return registros_json

def _enviar_lotes(registros, tipo_mensaje, sock, lock):
    for i in range(0, len(registros), LOTE_SIZE):
        lote = registros[i:i + LOTE_SIZE]
        with lock:
            message_protocol.external.send_msg(sock, tipo_mensaje, lote)