import os

from constantes import CLAVE_SCATTER, CLAVE_TXNS
from common.persistencia import PersistidorAppendOnly, VOLUMEN_DIR
from common.logger import obtener_logger
from base.constantes import CLAVE_BARRERA_COMPLETADA

logger = obtener_logger(__name__)

BASE_DIR = VOLUMEN_DIR

CLAVE_IDS = "i"


class PersistenciaJoiner:
    def __init__(self, prefijo_nodo: str, base_dir: str = BASE_DIR):
        self._prefijo = prefijo_nodo
        self._base_dir = base_dir

    def _nombre_archivo(self, client_id: str) -> str:
        return f"{self._prefijo}_cliente_{client_id}"

    def _persistidor(self, client_id: str) -> PersistidorAppendOnly:
        return PersistidorAppendOnly(self._nombre_archivo(client_id), base_dir=self._base_dir)

    def appendear(self, client_id: str, aristas: list, txns: list, ids: list):
        if not aristas and not txns and not ids:
            return
        entrada = {
            CLAVE_SCATTER: [[k, list(v)] for k, v in aristas],
            CLAVE_TXNS: [[k, list(v)] for k, v in txns],
            CLAVE_IDS: ids,
        }
        self._persistidor(client_id).appendear(entrada)

    def recuperar_todos(self) -> dict[str, tuple[dict, dict, set]]:
        if not os.path.exists(self._base_dir):
            logger.info(f"[PersistenciaJoiner] Directorio {self._base_dir} no existe. Arrancando limpio.")
            return {}

        prefijo_cliente = self._prefijo + "_cliente_"
        archivos = [f[:-6] for f in os.listdir(self._base_dir)
                     if f.startswith(prefijo_cliente) and f.endswith('.jsonl')]
        if not archivos:
            logger.info("[PersistenciaJoiner] Sin estado previo en disco. Arrancando limpio.")
            return {}

        resultado = {}
        for nombre in archivos:
            client_id = nombre[len(prefijo_cliente):]
            entradas = self._persistidor(client_id).recuperar()

            barrera = any(e.get(CLAVE_BARRERA_COMPLETADA, False) for e in entradas)
            if barrera:
                logger.info(f"[PersistenciaJoiner] Barrera completada para client_id={client_id}. Omitiendo.")
                continue

            scatter: dict[str, list] = {}
            txns: dict[str, set] = {}
            vistos: set = set()
            for entrada in entradas:
                for k, v in entrada.get(CLAVE_SCATTER, []):
                    scatter.setdefault(k, []).append(tuple(v))
                for k, v in entrada.get(CLAVE_TXNS, []):
                    txns.setdefault(k, set()).add(tuple(v))
                vistos.update(entrada.get(CLAVE_IDS, []))

            resultado[client_id] = (scatter, txns, vistos)
            logger.info(
                f"[PersistenciaJoiner] Recuperado client_id={client_id}: "
                f"scatter_keys={len(scatter)}, txns_keys={len(txns)}, vistos={len(vistos)}"
            )

        return resultado

    def borrar(self, client_id: str):
        self._persistidor(client_id).borrar()

    def marcar_barrera_completada(self, client_id: str):
        self._persistidor(client_id).appendear({CLAVE_BARRERA_COMPLETADA: True})

    def esta_barrera_completada(self, client_id: str) -> bool:
        entradas = self._persistidor(client_id).recuperar()
        return any(e.get(CLAVE_BARRERA_COMPLETADA, False) for e in entradas)
