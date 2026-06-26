import json
import os

from common.persistencia import PersistidorEstado, VOLUMEN_DIR
from common.logger import obtener_logger
from common.constantes_protocolo import ID_CLIENTE
from base.constantes import CLAVE_BARRERA_COMPLETADA, CLAVE_IDS_PROCESADOS
from constantes import CLAVE_GRUPOS

logger = obtener_logger(__name__)

BASE_DIR = VOLUMEN_DIR


class PersistenciaContador:

    def __init__(self, prefijo_nodo: str, base_dir: str = BASE_DIR):
        self._prefijo = prefijo_nodo
        self._base_dir = base_dir

    def _nombre_carpeta(self, client_id: str) -> str:
        return f"{self._prefijo}_cliente_{client_id}"

    def guardar(self, client_id: str, grupos: dict, vistos: set):
        grupos_serial = {}
        for clave_grupo, conjunto_valores in grupos.items():
            k = json.dumps(list(clave_grupo))
            grupos_serial[k] = [list(v) for v in conjunto_valores]

        PersistidorEstado(self._nombre_carpeta(client_id), base_dir=self._base_dir).guardar({
            ID_CLIENTE: client_id,
            CLAVE_GRUPOS: grupos_serial,
            CLAVE_IDS_PROCESADOS: list(vistos),
        })

    def recuperar_todos(self) -> dict[str, tuple[dict, set]]:
        if not os.path.exists(self._base_dir):
            logger.info(f"[PersistenciaContador] Directorio {self._base_dir} no existe. Arrancando limpio.")
            return {}

        prefijo_cliente = self._prefijo + "_cliente_"
        archivos = [f[:-5] for f in os.listdir(self._base_dir)
                     if f.startswith(prefijo_cliente) and f.endswith('.json')]
        if not archivos:
            logger.info("[PersistenciaContador] Sin estado previo en disco. Arrancando limpio.")
            return {}

        resultado = {}
        for nombre in archivos:
            client_id = nombre[len(prefijo_cliente):]
            persistidor = PersistidorEstado(nombre, base_dir=self._base_dir)
            estado = persistidor.cargar()

            if not estado:
                continue

            if estado.get(CLAVE_BARRERA_COMPLETADA, False):
                logger.info(f"[PersistenciaContador] Barrera completada detectada para client_id={client_id}. Omitiendo recuperación.")
                continue

            grupos_serial = estado.get(CLAVE_GRUPOS, {})
            grupos = {}
            for k, vlist in grupos_serial.items():
                clave_grupo = tuple(json.loads(k))
                grupos[clave_grupo] = set(tuple(v) for v in vlist)

            vistos = set(estado.get(CLAVE_IDS_PROCESADOS, []))
            resultado[client_id] = (grupos, vistos)

            logger.info(
                f"[PersistenciaContador] Recuperado estado para client_id={client_id}: "
                f"grupos={len(grupos)}, vistos={len(vistos)}"
            )

        return resultado

    def borrar(self, client_id: str):
        PersistidorEstado(self._nombre_carpeta(client_id), base_dir=self._base_dir).borrar()

    def marcar_barrera_completada(self, client_id: str):
        PersistidorEstado(self._nombre_carpeta(client_id), base_dir=self._base_dir).guardar({CLAVE_BARRERA_COMPLETADA: True})

    def esta_barrera_completada(self, client_id: str) -> bool:
        estado = PersistidorEstado(self._nombre_carpeta(client_id), base_dir=self._base_dir).cargar()
        return estado.get(CLAVE_BARRERA_COMPLETADA, False)
