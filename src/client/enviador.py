import os
import logging
import threading
from common.message_protocol.external import TipoMensaje
from config import LOTE_SIZE
class Enviador:
    """Envía archivos CSV al gateway en lotes, en paralelo."""

    def __init__(self, conexion, client_id, lock_escritura, shutdown, progreso):
        self._conexion = conexion
        self._client_id = client_id
        self._lock = lock_escritura
        self._shutdown = shutdown
        self._progreso = progreso

    @staticmethod
    def validar_archivos_estatico(archivos_con_tipo):
        faltantes = [ruta for ruta, _ in archivos_con_tipo if not os.path.isfile(ruta)]
        if faltantes:
            for ruta in faltantes:
                logging.error(f"Archivo no encontrado: {ruta}")
            return False
        return True

    def enviar_archivos(self, archivos_con_tipo):
        """Envía múltiples archivos en paralelo. Cada elemento es (ruta, tipo_mensaje)."""
        hilos = []
        for ruta, tipo_mensaje in archivos_con_tipo:
            hilo = threading.Thread(target=self._enviar_archivo, args=(ruta, tipo_mensaje))
            hilo.start()
            hilos.append(hilo)
        for h in hilos:
            h.join()

    def _enviar_archivo(self, ruta, tipo_mensaje):
        nombre = os.path.basename(ruta)
        try:
            total = self._contar_lineas(ruta)
        except FileNotFoundError:
            logging.error(f"Archivo no encontrado: {ruta}")
            self._shutdown.set()
            return
        logging.info(f"Enviando {ruta} ({total:,} registros)...")
        try:
            with open(ruta, "r", encoding="utf-8") as f:
                headers, registros = self._leer_headers_y_registros(f)
                if headers is not None:
                    self._enviar_lotes(headers, registros, tipo_mensaje, nombre, total)
            if self._shutdown.is_set():
                logging.info(f"Envío de {ruta} interrumpido.")
            else:
                logging.info(f"Envío de {ruta} completado.")
        except (BrokenPipeError, ConnectionResetError, OSError) as e:
            logging.error(f"Error de red al procesar {ruta}: {e}")
            self._shutdown.set()
        except Exception as e:
            logging.error(f"Error al procesar {ruta}: {e}")

    def _enviar_lotes(self, headers, registros, tipo_mensaje, nombre, total):
        lote = []
        enviados = 0
        for registro in registros:
            if self._shutdown.is_set():
                return
            lote.append(registro)
            if len(lote) >= LOTE_SIZE:
                self._enviar_con_prioridad_ack(tipo_mensaje, headers, lote)
                enviados += len(lote)
                if total > 0:
                    self._progreso.actualizar_envio(nombre, enviados, total)
                lote = []
        if lote and not self._shutdown.is_set():
            self._enviar_con_prioridad_ack(tipo_mensaje, headers, lote)
            enviados += len(lote)
            if total > 0:
                self._progreso.actualizar_envio(nombre, enviados, total)

    def _enviar_con_prioridad_ack(self, tipo_mensaje, headers, lote):
        with self._lock:
            self._conexion.enviar(tipo_mensaje, headers, self._client_id, lote)

    @staticmethod
    def _contar_lineas(ruta):
        count = 0
        with open(ruta, "rb") as f:
            for _ in f:
                count += 1
        return count - 1

    @staticmethod
    def _leer_headers_y_registros(f):
        linea_headers = f.readline().strip()
        if not linea_headers:
            return None, iter([])
        headers = linea_headers.split(",")
        registros = (linea.strip().split(",") for linea in f if linea.strip())
        return headers, registros
