import os
import glob
import json
import logging
from constantes import (
    ARCHIVO_ENVIO_COMPLETO,
    ARCHIVO_QUERIES_COMPLETADAS,
    ARCHIVO_BATCH_IDS,
)


class PersistenciaCliente:
    """Maneja la persistencia del estado del cliente en disco."""

    def __init__(self, directorio_salida):
        self._directorio = directorio_salida

    def cargar_id(self, sufijo=""):
        env_id = os.environ.get("CLIENT_ID")
        if env_id:
            return env_id

        nombre_archivo = f"client_id_{sufijo}.txt" if sufijo else "client_id.txt"
        ruta = os.path.join(self._directorio, nombre_archivo)

        if os.path.exists(ruta):
            with open(ruta, "r") as f:
                cid = f.read().strip()
            if cid:
                return cid
        return None

    def guardar_id(self, client_id, sufijo=""):
        nombre_archivo = f"client_id_{sufijo}.txt" if sufijo else "client_id.txt"
        os.makedirs(self._directorio, exist_ok=True)
        ruta = os.path.join(self._directorio, nombre_archivo)
        with open(ruta, "w") as f:
            f.write(client_id)

    def directorio_cliente(self, client_id):
        directorio = os.path.join(self._directorio, client_id)
        os.makedirs(directorio, exist_ok=True)
        return directorio

    def marcar_envio_completo(self, client_id):
        directorio = self.directorio_cliente(client_id)
        with open(os.path.join(directorio, ARCHIVO_ENVIO_COMPLETO), "w") as f:
            f.write("1")

    def cargar_queries_completadas(self, client_id):
        ruta = os.path.join(self.directorio_cliente(client_id), ARCHIVO_QUERIES_COMPLETADAS)
        if not os.path.exists(ruta):
            return set()
        try:
            with open(ruta, "r", encoding="utf-8") as f:
                return set(json.load(f))
        except Exception:
            return set()

    def guardar_queries_completadas(self, client_id, completadas):
        ruta = os.path.join(self.directorio_cliente(client_id), ARCHIVO_QUERIES_COMPLETADAS)
        with open(ruta, "w", encoding="utf-8") as f:
            json.dump(list(completadas), f)

    def cargar_batch_ids(self, client_id):
        directorio = self.directorio_cliente(client_id)
        batch_ids = {}
        for ruta in glob.glob(os.path.join(directorio, "batch_ids_q*.json")):
            try:
                nombre = os.path.basename(ruta)
                q_id = int(nombre.replace("batch_ids_q", "").replace(".json", ""))
                with open(ruta, "r", encoding="utf-8") as f:
                    batch_ids[q_id] = set(json.load(f))
            except Exception:
                pass
        return batch_ids

    def guardar_batch_ids(self, client_id, q_id, batch_ids_set):
        ruta = os.path.join(
            self.directorio_cliente(client_id),
            ARCHIVO_BATCH_IDS.format(q_id=q_id),
        )
        with open(ruta, "w", encoding="utf-8") as f:
            json.dump(list(batch_ids_set), f)

    def limpiar_batch_ids(self, client_id, q_id):
        ruta = os.path.join(
            self.directorio_cliente(client_id),
            ARCHIVO_BATCH_IDS.format(q_id=q_id),
        )
        try:
            os.remove(ruta)
        except FileNotFoundError:
            pass
