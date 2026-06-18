import os
from common.logger import obtener_logger
from common.persistencia import PersistidorEstado, VOLUMEN_DIR
from constantes import PREFIJO_COUNTER, CLAVE_CONTEO, CLAVE_BARRERA_COMPLETADA

logger = obtener_logger(__name__)


class PersistenciaConteo:

    def __init__(self, id_nodo: int):
        self._id_nodo = id_nodo

    def _nombre_nodo(self, client_id: str) -> str:
        return f"{PREFIJO_COUNTER}_{self._id_nodo}_{client_id}"

    def recuperar_conteos(self) -> dict[str, int]:
        conteos: dict[str, int] = {}

        if not os.path.exists(VOLUMEN_DIR):
            logger.info(f"Directorio {VOLUMEN_DIR} no existe. Arrancando limpio.")
            return conteos

        prefijo = f"{PREFIJO_COUNTER}_{self._id_nodo}_"
        carpetas = [c for c in os.listdir(VOLUMEN_DIR) if c.startswith(prefijo)]

        if not carpetas:
            logger.info(f"Sin estado previo en disco (prefijo={prefijo}).")
            return conteos

        for carpeta in carpetas:
            client_id = carpeta[len(prefijo):]
            persistidor = PersistidorEstado(carpeta, base_dir=VOLUMEN_DIR)
            estado = persistidor.cargar()

            if not estado:
                logger.warning(f"Carpeta {carpeta} encontrada pero estado vacío o corrupto.")
                continue

            if estado.get(CLAVE_BARRERA_COMPLETADA, False):
                persistidor.borrar()
                logger.info(f"Barrera completada para client_id={client_id}. Limpiando remanente.")
                continue

            conteos[client_id] = estado.get(CLAVE_CONTEO, 0)
            logger.info(f"Recuperado estado para client_id={client_id}: count={conteos[client_id]}")

        return conteos

    def guardar(self, client_id: str, conteo: int):
        PersistidorEstado(
            self._nombre_nodo(client_id), base_dir=VOLUMEN_DIR
        ).guardar({CLAVE_CONTEO: conteo})

    def borrar(self, client_id: str):
        PersistidorEstado(
            self._nombre_nodo(client_id), base_dir=VOLUMEN_DIR
        ).borrar()

    def marcar_completado(self, client_id: str):
        nombre = self._nombre_nodo(client_id)
        persistidor = PersistidorEstado(nombre, base_dir=VOLUMEN_DIR)
        persistidor.guardar({CLAVE_BARRERA_COMPLETADA: True})
        persistidor.borrar()
