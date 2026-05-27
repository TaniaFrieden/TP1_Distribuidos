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

def escuchar_respuesta(sock, queries, inicio_envio):
    logging.info("Hilo receptor activo: Esperando reportes...")
    # Ahora archivos y cabeceras comienzan vacíos
    archivos_salida = {}
    cabeceras_escritas = {}
    tiempos_inicio = {q_id: inicio_envio for q_id in queries}
    
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    
    try:
        while True: # Cambiado a True, el corte se maneja cuando ya no hay archivos activos o EOF global
            try:
                msg_type, payload = message_protocol.external.recv_msg(sock)
            except Exception as e:
                logging.error(f"Error de red recibiendo mensaje: {e}")
                break
            
            if msg_type == message_protocol.external.MsgType.REPORTE:
                _procesar_resultado(payload, archivos_salida, cabeceras_escritas, tiempos_inicio, inicio_envio)
            
            elif msg_type == message_protocol.external.MsgType.END_OF_RECODS:
                break
                
    finally:
        for f in archivos_salida.values():
            f.close()

def _procesar_resultado(payload, archivos, cabeceras, tiempos_inicio, inicio_envio):
    try:
        data = json.loads(payload) if isinstance(payload, str) else payload
    except json.JSONDecodeError:
        return

    q_id = data.get(KEY_QUERY)
    resultado = data.get(KEY_RESULT)
    
    if q_id is None:
        return

    if q_id not in tiempos_inicio:
        tiempos_inicio[q_id] = inicio_envio
        logging.info(f"[QUERY {q_id}] Inicio de recepción de resultados.")

    # Lógica dinámica: Crear archivo si es la primera vez que vemos este q_id
    if q_id not in archivos:
        path = os.path.join(OUTPUT_DIR, OUTPUT_FILE_NAME.format(q_id=q_id))
        archivos[q_id] = open(path, "w", encoding="utf-8")
        cabeceras[q_id] = False

    # resultado puede ser un dict (registro único / EOF) o una lista de dicts (batch)
    items = resultado if isinstance(resultado, list) else [resultado]

    for item in items:
        es_mensaje_final = _es_eof(item)

        if isinstance(item, dict) and not (len(item) == 1 and es_mensaje_final):
            _escribir_cabecera(q_id, item, archivos, cabeceras)
            _escribir_datos(q_id, item, archivos)

        if es_mensaje_final:
            _cerrar_archivo(q_id, archivos)
            inicio_query = tiempos_inicio.pop(q_id, None)
            if inicio_query is not None:
                logging.info(f"[QUERY {q_id}] Finalizada en {time.perf_counter() - inicio_query:.3f} s")
            else:
                logging.info(f"[QUERY {q_id}] EOF recibido sin inicio registrado")
            break  # EOF cierra el archivo; no procesar más ítems del batch

## --- Funciones auxiliares mantenidas ---
def _es_eof(resultado):
    return isinstance(resultado, dict) and resultado.get(KEY_EOF) is True

def _escribir_cabecera(q_id, resultado, archivos, cabeceras):
    if not cabeceras[q_id]:
        claves = []
        for k in resultado.keys():
            if k == KEY_EOF:
                continue
            if k == "Account.1":
                claves.append("Account")
            else:
                claves.append(str(k))
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