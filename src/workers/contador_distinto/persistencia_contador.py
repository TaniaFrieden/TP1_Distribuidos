import json
import os

from common.persistencia import PersistidorAppendOnly, VOLUMEN_DIR
from common.logger import obtener_logger
from base.constantes import CLAVE_BARRERA_COMPLETADA

logger = obtener_logger(__name__)

BASE_DIR = VOLUMEN_DIR

CLAVE_OPS = "o"
CLAVE_IDS = "i"


class PersistenciaContador:

    def __init__(self, prefijo_nodo: str, base_dir: str = BASE_DIR):
        self._prefijo = prefijo_nodo
        self._base_dir = base_dir

    def _nombre_archivo(self, client_id: str) -> str:
        return f"{self._prefijo}_cliente_{client_id}"

    def _persistidor(self, client_id: str) -> PersistidorAppendOnly:
        return PersistidorAppendOnly(self._nombre_archivo(client_id), base_dir=self._base_dir)

    def appendear(self, client_id: str, ops: list, ids: list):
        if not ops and not ids:
            return
        entrada = {
            CLAVE_OPS: [[list(g), list(v)] for g, v in ops],
            CLAVE_IDS: ids,
        }
        self._persistidor(client_id).appendear(entrada)

    def recuperar_todos(self) -> dict[str, tuple[dict, set]]:
        if not os.path.exists(self._base_dir):
            logger.info(f"[PersistenciaContador] Directorio {self._base_dir} no existe. Arrancando limpio.")
            return {}

        prefijo_cliente = self._prefijo + "_cliente_"
        archivos = [f[:-6] for f in os.listdir(self._base_dir)
                     if f.startswith(prefijo_cliente) and f.endswith('.jsonl')]
        if not archivos:
            logger.info("[PersistenciaContador] Sin estado previo en disco. Arrancando limpio.")
            return {}

        resultado = {}
        for nombre in archivos:
            client_id = nombre[len(prefijo_cliente):]
            entradas = self._persistidor(client_id).recuperar()

            barrera = any(e.get(CLAVE_BARRERA_COMPLETADA, False) for e in entradas)
            if barrera:
                logger.info(f"[PersistenciaContador] Barrera completada para client_id={client_id}. Omitiendo.")
                continue

            grupos: dict[tuple, set] = {}
            vistos: set = set()
            for entrada in entradas:
                for g, v in entrada.get(CLAVE_OPS, []):
                    clave_grupo = tuple(g)
                    grupos.setdefault(clave_grupo, set()).add(tuple(v))
                vistos.update(entrada.get(CLAVE_IDS, []))

            resultado[client_id] = (grupos, vistos)
            logger.info(
                f"[PersistenciaContador] Recuperado client_id={client_id}: "
                f"grupos={len(grupos)}, vistos={len(vistos)}"
            )

        return resultado

    def borrar(self, client_id: str):
        self._persistidor(client_id).borrar()

    def marcar_barrera_completada(self, client_id: str):
        self._persistidor(client_id).appendear({CLAVE_BARRERA_COMPLETADA: True})

    def esta_barrera_completada(self, client_id: str) -> bool:
        entradas = self._persistidor(client_id).recuperar()
        return any(e.get(CLAVE_BARRERA_COMPLETADA, False) for e in entradas)
