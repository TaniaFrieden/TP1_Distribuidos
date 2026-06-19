import os
import json
import logging
import time
from common import message_protocol
from config import OUTPUT_DIR

KEY_QUERY = 'query'
KEY_RESULT = 'resultado'
KEY_EOF = 'eof'

OUTPUT_FILE_NAME = "q{q_id}_solucion.csv"
QUERIES_COMPLETADAS_FILE = "queries_completadas.json"


def escuchar_respuesta(sock, queries, inicio_envio, client_id, evento_completado=None):
    output_path = os.path.join(OUTPUT_DIR, client_id)
    os.makedirs(output_path, exist_ok=True)

    queries_terminadas = _cargar_queries_completadas(output_path)

    archivos_salida = {}
    cabeceras_escritas = {}
    tiempos_inicio = {q_id: inicio_envio for q_id in queries if q_id not in queries_terminadas}

    try:
        while True:
            try:
                msg_type, payload = message_protocol.external.recv_msg(sock)
            except Exception as e:
                logging.error(f"Error de red recibiendo mensaje: {e}")
                break

            if msg_type == message_protocol.external.MsgType.REPORTE:
                _procesar_resultado(
                    payload, archivos_salida, cabeceras_escritas,
                    tiempos_inicio, inicio_envio, output_path, queries_terminadas
                )

            elif msg_type == message_protocol.external.MsgType.END_OF_RECODS:
                elapsed = time.perf_counter() - inicio_envio
                logging.info(f"Todas las queries completadas en {elapsed:.2f}s")
                if evento_completado:
                    evento_completado.set()
                break

    finally:
        for f in archivos_salida.values():
            f.close()


def _cargar_queries_completadas(output_path):
    path = os.path.join(output_path, QUERIES_COMPLETADAS_FILE)
    if not os.path.exists(path):
        return set()
    try:
        with open(path, "r", encoding="utf-8") as f:
            return set(json.load(f))
    except Exception:
        return set()


def _guardar_queries_completadas(output_path, completadas):
    path = os.path.join(output_path, QUERIES_COMPLETADAS_FILE)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(list(completadas), f)


def _procesar_resultado(payload, archivos, cabeceras, tiempos_inicio, inicio_envio, output_path, queries_terminadas):
    try:
        data = json.loads(payload) if isinstance(payload, str) else payload
    except json.JSONDecodeError:
        return

    q_id = data.get(KEY_QUERY)
    resultado = data.get(KEY_RESULT)
    columns_hint = data.get("columns")

    if q_id is None or q_id in queries_terminadas:
        return

    if q_id not in tiempos_inicio:
        tiempos_inicio[q_id] = inicio_envio

    if q_id not in archivos:
        path = os.path.join(output_path, OUTPUT_FILE_NAME.format(q_id=q_id))
        # Si el archivo ya tiene datos de una sesión anterior, agregar en lugar de truncar
        if os.path.exists(path) and os.path.getsize(path) > 0:
            archivos[q_id] = open(path, "a", encoding="utf-8")
            cabeceras[q_id] = True  # la cabecera ya está escrita
        else:
            archivos[q_id] = open(path, "w", encoding="utf-8")
            cabeceras[q_id] = False

    items = resultado if isinstance(resultado, list) else [resultado]

    for item in items:
        es_mensaje_final = _es_eof(item)

        if isinstance(item, dict) and not (len(item) == 1 and es_mensaje_final):
            _escribir_cabecera(q_id, item, archivos, cabeceras)
            _escribir_datos(q_id, item, archivos, cabeceras)

        if es_mensaje_final:
            if columns_hint and q_id in archivos and not cabeceras.get(q_id):
                archivos[q_id].write(",".join(columns_hint) + "\n")
            _cerrar_archivo(q_id, archivos)
            inicio_query = tiempos_inicio.pop(q_id, None)
            if inicio_query is not None:
                logging.info(f"[QUERY {q_id}] Finalizada en {time.perf_counter() - inicio_query:.3f} s")
            else:
                logging.info(f"[QUERY {q_id}] EOF recibido")
            queries_terminadas.add(q_id)
            _guardar_queries_completadas(output_path, queries_terminadas)
            break


def _es_eof(resultado):
    return isinstance(resultado, dict) and resultado.get(KEY_EOF) is True


def _escribir_cabecera(q_id, resultado, archivos, cabeceras):
    if not cabeceras[q_id]:
        claves = [k for k in resultado.keys() if str(k).lower() != 'eof']
        cabeceras[q_id] = claves
        claves_cabecera = ["Account" if k == "Account.1" else str(k) for k in claves]
        archivos[q_id].write(",".join(claves_cabecera) + "\n")


def _escribir_datos(q_id, resultado, archivos, cabeceras):
    claves = cabeceras[q_id]
    valores = [str(resultado.get(k, '')) for k in claves]
    archivos[q_id].write(",".join(valores) + "\n")
    archivos[q_id].flush()


def _cerrar_archivo(q_id, archivos):
    logging.info(f"Resultados de Query {q_id} recibidos por completo.")
    if q_id in archivos:
        archivos[q_id].close()
        del archivos[q_id]
