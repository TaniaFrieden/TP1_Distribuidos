import os
import glob
import json
import logging
import sys
import time
from common import message_protocol
from common.constantes_protocolo import CLAVE_QUERY, CLAVE_RESULTADO, CLAVE_EOF_REPORTE, CLAVE_COLUMNAS
from config import OUTPUT_DIR

_SPINNER = ["⠋", "⠙", "⠹", "⠸", "⠼", "⠴", "⠦", "⠧", "⠇", "⠏"]
_spinner_idx = 0
_filas_por_query = {}
_valor_query = {}
_queries_completas = set()
_progreso_habilitado = os.environ.get("PROGRESS_BAR", "1") != "0"
_client_tag = os.environ.get("CLIENT_ID_SUFFIX", "")

OUTPUT_FILE_NAME = "q{q_id}_solucion.csv"
QUERIES_COMPLETADAS_FILE = "queries_completadas.json"
BATCH_IDS_FILE = "batch_ids_q{q_id}.json"


def escuchar_respuesta(sock, queries, inicio_envio, client_id, evento_completado=None, write_lock=None, ack_pendiente=None):
    global _filas_por_query, _valor_query, _queries_completas, _spinner_idx
    _filas_por_query = {}
    _valor_query = {}
    _queries_completas = set()
    _spinner_idx = 0

    output_path = os.path.join(OUTPUT_DIR, client_id)
    os.makedirs(output_path, exist_ok=True)

    queries_terminadas = _cargar_queries_completadas(output_path)
    batch_ids_vistos = _cargar_batch_ids_vistos(output_path)

    archivos_salida = {}
    cabeceras_escritas = {}
    tiempos_inicio = {q_id: inicio_envio for q_id in queries if q_id not in queries_terminadas}

    try:
        while True:
            try:
                tipo_mensaje, payload = message_protocol.external.recibir_mensaje(sock)
            except Exception as e:
                logging.error(f"Error de red recibiendo mensaje: {e}")
                break

            if tipo_mensaje == message_protocol.external.TipoMensaje.REPORTE:
                batch_id = _procesar_resultado(
                    payload, archivos_salida, cabeceras_escritas,
                    tiempos_inicio, inicio_envio, output_path, queries_terminadas,
                    batch_ids_vistos
                )
                if batch_id:
                    _enviar_ack(sock, batch_id, write_lock, ack_pendiente)
            elif tipo_mensaje == message_protocol.external.TipoMensaje.FIN_DE_REGISTROS:
                elapsed = time.perf_counter() - inicio_envio
                _mostrar_progreso_downstream()
                sys.stderr.write("\n")
                sys.stderr.flush()
                logging.info(f"Todas las queries completadas en {elapsed:.2f}s")
                if evento_completado:
                    evento_completado.set()
                break

    finally:
        for f in archivos_salida.values():
            f.close()


def _enviar_ack(sock, batch_id, write_lock=None, ack_pendiente=None):
    try:
        ack_payload = json.dumps({"batch_id": batch_id})
        if ack_pendiente:
            ack_pendiente.clear()
        if write_lock:
            with write_lock:
                message_protocol.external.enviar_mensaje(
                    sock, message_protocol.external.TipoMensaje.ACK_RESULTADO, ack_payload
                )
        else:
            message_protocol.external.enviar_mensaje(
                sock, message_protocol.external.TipoMensaje.ACK_RESULTADO, ack_payload
            )
    except Exception as e:
        logging.warning(f"No se pudo enviar ACK_RESULTADO al gateway: {e}")
    finally:
        if ack_pendiente:
            ack_pendiente.set()


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


def _cargar_batch_ids_vistos(output_path):
    """Carga los batch_ids ya procesados por query desde disco."""
    batch_ids = {}
    for path in glob.glob(os.path.join(output_path, "batch_ids_q*.json")):
        try:
            basename = os.path.basename(path)
            q_id = int(basename.replace("batch_ids_q", "").replace(".json", ""))
            with open(path, "r", encoding="utf-8") as f:
                batch_ids[q_id] = set(json.load(f))
        except Exception:
            pass
    return batch_ids


def _guardar_batch_ids_vistos(output_path, q_id, batch_ids_set):
    path = os.path.join(output_path, BATCH_IDS_FILE.format(q_id=q_id))
    with open(path, "w", encoding="utf-8") as f:
        json.dump(list(batch_ids_set), f)


def _limpiar_batch_ids_vistos(output_path, q_id):
    path = os.path.join(output_path, BATCH_IDS_FILE.format(q_id=q_id))
    try:
        os.remove(path)
    except FileNotFoundError:
        pass


def _procesar_resultado(payload, archivos, cabeceras, tiempos_inicio, inicio_envio, output_path, queries_terminadas, batch_ids_vistos=None):
    """Procesa un REPORTE. Retorna el batch_id para que el caller envíe el ACK, o None si no hay que ACKear."""
    try:
        data = json.loads(payload) if isinstance(payload, str) else payload
    except json.JSONDecodeError:
        return None

    batch_id = data.get("batch_id")
    q_id = data.get(CLAVE_QUERY)
    resultado = data.get(CLAVE_RESULTADO)
    columns_hint = data.get(CLAVE_COLUMNAS)

    if q_id is None or q_id in queries_terminadas:
        return batch_id  # ACKear igual para que el gateway no quede bloqueado

    # Saltar batches ya procesados (re-entregas tras crash del gateway)
    if batch_id and batch_ids_vistos is not None:
        if batch_id in batch_ids_vistos.get(q_id, set()):
            return batch_id

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
    datos_escritos = False

    for item in items:
        es_mensaje_final = _es_eof(item)

        if isinstance(item, dict) and not (len(item) == 1 and es_mensaje_final):
            _escribir_cabecera(q_id, item, archivos, cabeceras)
            _escribir_datos(q_id, item, archivos, cabeceras)
            _filas_por_query[q_id] = _filas_por_query.get(q_id, 0) + 1
            campos_dato = {k: v for k, v in item.items() if k != "eof"}
            if len(campos_dato) == 1:
                _valor_query[q_id] = str(list(campos_dato.values())[0])
            datos_escritos = True

        if es_mensaje_final:
            if columns_hint and q_id in archivos and not cabeceras.get(q_id):
                archivos[q_id].write(",".join(columns_hint) + "\n")
            _cerrar_archivo(q_id, archivos)
            if batch_ids_vistos is not None and q_id in batch_ids_vistos:
                del batch_ids_vistos[q_id]
                _limpiar_batch_ids_vistos(output_path, q_id)
            inicio_query = tiempos_inicio.pop(q_id, None)
            if inicio_query is not None:
                logging.info(f"[QUERY {q_id}] Finalizada en {time.perf_counter() - inicio_query:.3f} s")
            else:
                logging.info(f"[QUERY {q_id}] EOF recibido")
            queries_terminadas.add(q_id)
            _queries_completas.add(q_id)
            _filas_por_query.setdefault(q_id, 0)
            _mostrar_progreso_downstream()
            _guardar_queries_completadas(output_path, queries_terminadas)
            break

    # Marcar batch como procesado después de escribir los datos
    if datos_escritos and batch_id and batch_ids_vistos is not None:
        batch_ids_vistos.setdefault(q_id, set()).add(batch_id)
        _guardar_batch_ids_vistos(output_path, q_id, batch_ids_vistos[q_id])

    if datos_escritos:
        _mostrar_progreso_downstream()

    return batch_id


def _ancho_terminal():
    try:
        return os.get_terminal_size(sys.stderr.fileno()).columns
    except (OSError, ValueError):
        return 120

def _mostrar_progreso_downstream():
    if not _progreso_habilitado:
        return
    global _spinner_idx
    _spinner_idx = (_spinner_idx + 1) % len(_SPINNER)
    partes = []
    for q_id in sorted(_filas_por_query.keys()):
        filas = _filas_por_query[q_id]
        if q_id in _valor_query:
            display = f"={_valor_query[q_id]}"
        else:
            display = f"{filas:,}"
        if q_id in _queries_completas:
            partes.append(f"Q{q_id}: ✔ {display}")
        else:
            partes.append(f"Q{q_id}: {_SPINNER[_spinner_idx]} {display}")
    prefix = f"[C{_client_tag}] " if _client_tag else ""
    linea = f"\r  {prefix}Recibiendo: {' | '.join(partes)}"
    ancho_term = _ancho_terminal()
    if len(linea) > ancho_term:
        linea = linea[:ancho_term]
    sys.stderr.write(f"\033[2K{linea}")
    sys.stderr.flush()


def _es_eof(resultado):
    return isinstance(resultado, dict) and resultado.get(CLAVE_EOF_REPORTE) is True


def _escribir_cabecera(q_id, resultado, archivos, cabeceras):
    if cabeceras[q_id] is False:
        claves = [k for k in resultado.keys() if str(k).lower() != 'eof']
        cabeceras[q_id] = claves
        archivos[q_id].write(",".join(str(k) for k in claves) + "\n")
    elif cabeceras[q_id] is True:
        # Archivo existente: aprender columnas del primer registro sin escribir cabecera
        claves = [k for k in resultado.keys() if str(k).lower() != 'eof']
        cabeceras[q_id] = claves


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
