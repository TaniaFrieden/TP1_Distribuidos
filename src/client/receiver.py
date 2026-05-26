import os
import json
import logging
import time
from common import message_protocol
from config import OUTPUT_DIR

KEY_QUERY = 'query'
KEY_RESULT = 'resultado'
KEY_EOF = 'eof'

OUTPUT_FILE_NAME = "output_{q_id}.csv"

def escuchar_respuesta(sock, start_time=None):
    logging.info("Hilo receptor activo: Esperando reportes...")
    if start_time is None:
        start_time = time.monotonic()
    archivos_salida = {}
    cabeceras_escritas = {}

    os.makedirs(OUTPUT_DIR, exist_ok=True)

    try:
        while True:
            try:
                msg_type, payload = message_protocol.external.recv_msg(sock)
            except Exception as e:
                logging.error(f"Error de red recibiendo mensaje: {e}")
                break

            if msg_type == message_protocol.external.MsgType.REPORTE:
                _procesar_resultado(payload, archivos_salida, cabeceras_escritas, start_time)

            elif msg_type == message_protocol.external.MsgType.END_OF_RECODS:
                elapsed = time.monotonic() - start_time
                logging.info(f"[TIMER] Todas las queries completadas en {elapsed:.2f}s")
                break

    finally:
        for f in archivos_salida.values():
            f.close()

def _procesar_resultado(payload, archivos, cabeceras, start_time):
    try:
        data = json.loads(payload) if isinstance(payload, str) else payload
    except json.JSONDecodeError:
        return

    q_id = data.get(KEY_QUERY)
    resultado = data.get(KEY_RESULT)

    if q_id is None:
        return

    if q_id not in archivos:
        path = os.path.join(OUTPUT_DIR, OUTPUT_FILE_NAME.format(q_id=q_id))
        archivos[q_id] = open(path, "w", encoding="utf-8")
        cabeceras[q_id] = False

    es_mensaje_final = _es_eof(resultado)

    if isinstance(resultado, dict) and not (len(resultado) == 1 and es_mensaje_final):
        _escribir_cabecera(q_id, resultado, archivos, cabeceras)
        _escribir_datos(q_id, resultado, archivos)

    if es_mensaje_final:
        elapsed = time.monotonic() - start_time
        logging.info(f"[TIMER] Query {q_id} completada en {elapsed:.2f}s")
        _cerrar_archivo(q_id, archivos)

## --- Funciones auxiliares mantenidas ---
def _es_eof(resultado):
    return isinstance(resultado, dict) and resultado.get(KEY_EOF) is True

def _escribir_cabecera(q_id, resultado, archivos, cabeceras):
    if not cabeceras[q_id]:
        claves = [str(k) for k in resultado.keys() if k != KEY_EOF]
        archivos[q_id].write(",".join(claves) + "\n")
        cabeceras[q_id] = True

def _escribir_datos(q_id, resultado, archivos):
    valores = [str(v) for k, v in resultado.items() if k != KEY_EOF]
    archivos[q_id].write(",".join(valores) + "\n")
    archivos[q_id].flush()

def _cerrar_archivo(q_id, archivos):
    logging.info(f"EOF recibido para query {q_id}")
    if q_id in archivos:
        archivos[q_id].close()
        del archivos[q_id]