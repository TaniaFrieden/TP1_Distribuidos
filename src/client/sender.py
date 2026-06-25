import logging
import os
import sys
from common import message_protocol
from config import LOTE_SIZE


def _contar_lineas(filepath):
    count = 0
    with open(filepath, "rb") as f:
        for _ in f:
            count += 1
    return count - 1  # descontar header


def _leer_headers_y_registros(f):
    linea_headers = f.readline().strip()
    if not linea_headers:
        return None, iter([])
    headers = linea_headers.split(",")
    registros = (linea.strip().split(",") for linea in f if linea.strip())
    return headers, registros


_progreso_lock = __import__('threading').Lock()
_progreso_estado = {}
_progreso_habilitado = os.environ.get("PROGRESS_BAR", "1") != "0"
_client_tag = os.environ.get("CLIENT_ID_SUFFIX", "")

def _ancho_terminal():
    try:
        return os.get_terminal_size(sys.stderr.fileno()).columns
    except (OSError, ValueError):
        return 120

def _mostrar_progreso(nombre, enviados, total):
    if not _progreso_habilitado:
        return
    with _progreso_lock:
        _progreso_estado[nombre] = (enviados, total)
        prefix = f"[C{_client_tag}] " if _client_tag else ""
        n_archivos = len(_progreso_estado)
        # Subir N líneas, limpiar y redibujar
        if n_archivos > 1:
            sys.stderr.write(f"\033[{n_archivos}A")
        for n, (e, t) in sorted(_progreso_estado.items()):
            pct = e / t * 100 if t > 0 else 100
            ancho = 25
            lleno = int(ancho * e / t) if t > 0 else ancho
            barra = "█" * lleno + "░" * (ancho - lleno)
            sys.stderr.write(f"\033[2K  {prefix}{n}: {barra} {pct:5.1f}%\n")
        sys.stderr.flush()


def enviar_archivo(filepath, tipo_mensaje, sock, lock, client_id, shutdown_event=None, ack_pendiente=None):
    nombre = os.path.basename(filepath)
    total = _contar_lineas(filepath)
    logging.info(f"Enviando {filepath} ({total:,} registros)...")
    try:
        with open(filepath, "r", encoding="utf-8") as f:
            headers, registros = _leer_headers_y_registros(f)
            if headers is not None:
                _enviar_lotes(headers, registros, tipo_mensaje, sock, lock, client_id, shutdown_event, ack_pendiente, nombre, total)
        if shutdown_event and shutdown_event.is_set():
            logging.info(f"Envío de {filepath} interrumpido.")
        else:
            logging.info(f"Envío de {filepath} completado.")
    except FileNotFoundError:
        logging.error(f"Archivo no encontrado: {filepath}")
        raise
    except (BrokenPipeError, ConnectionResetError, OSError) as e:
        logging.error(f"Error de red al procesar {filepath}: {e}")
        if shutdown_event:
            shutdown_event.set()
    except Exception as e:
        logging.error(f"Error al procesar {filepath}: {e}")

def _enviar_con_prioridad_ack(sock, lock, ack_pendiente, tipo_mensaje, headers, client_id, lote):
    while True:
        if ack_pendiente:
            ack_pendiente.wait()
        with lock:
            if ack_pendiente and not ack_pendiente.is_set():
                continue
            message_protocol.external.enviar_mensaje(sock, tipo_mensaje, headers, client_id, lote)
            return

def _enviar_lotes(headers, registros, tipo_mensaje, sock, lock, client_id, shutdown_event=None, ack_pendiente=None, nombre="", total=0):
    lote = []
    enviados = 0
    for registro in registros:
        if shutdown_event and shutdown_event.is_set():
            return
        lote.append(registro)
        if len(lote) >= LOTE_SIZE:
            _enviar_con_prioridad_ack(sock, lock, ack_pendiente, tipo_mensaje, headers, client_id, lote)
            enviados += len(lote)
            if total > 0:
                _mostrar_progreso(nombre, enviados, total)
            lote = []
    if lote and not (shutdown_event and shutdown_event.is_set()):
        _enviar_con_prioridad_ack(sock, lock, ack_pendiente, tipo_mensaje, headers, client_id, lote)
        enviados += len(lote)
        if total > 0:
            _mostrar_progreso(nombre, enviados, total)