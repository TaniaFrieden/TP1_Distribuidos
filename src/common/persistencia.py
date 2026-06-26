import os
import json
from common.logger import obtener_logger
import tempfile

logger = obtener_logger(__name__)

VOLUMEN_DIR = "/app/volumen"
TAMANIO_BATCH_PERSISTENCIA = 50
TAMANIO_BATCH_EMISION = 1000

class PersistidorAppendOnly:
    def __init__(self, node_name: str, base_dir: str = VOLUMEN_DIR):
        self.node_name = node_name
        self.directory = base_dir
        self.filepath = os.path.join(base_dir, f"{node_name}.jsonl")
        os.makedirs(self.directory, mode=0o755, exist_ok=True)

    def appendear(self, entrada: dict) -> bool:
        try:
            linea = json.dumps(entrada, ensure_ascii=False).encode("utf-8") + b"\n"
            fd = os.open(self.filepath, os.O_WRONLY | os.O_CREAT | os.O_APPEND, 0o644)
            try:
                os.write(fd, linea)
                os.fsync(fd)
            finally:
                os.close(fd)
            return True
        except Exception as e:
            logger.error(f"[AppendOnly] Error appendeando para {self.node_name}: {e}", exc_info=True)
            return False

    def recuperar(self) -> list[dict]:
        if not os.path.exists(self.filepath):
            return []
        entradas = []
        with open(self.filepath, "r", encoding="utf-8") as f:
            for linea in f:
                linea = linea.strip()
                if linea:
                    try:
                        entradas.append(json.loads(linea))
                    except json.JSONDecodeError:
                        logger.warning(f"[AppendOnly] Línea corrupta ignorada en {self.filepath}")
        return entradas

    def borrar(self) -> bool:
        try:
            if os.path.exists(self.filepath):
                os.remove(self.filepath)
            return True
        except Exception as e:
            logger.error(f"[AppendOnly] Error borrando {self.filepath}: {e}")
            return False


class PersistidorEstado:
    def __init__(self, node_name: str, base_dir: str = VOLUMEN_DIR):
        self.node_name = node_name
        self.directory = base_dir
        self.filepath = os.path.join(base_dir, f"{node_name}.json")
        self._inicializar_directorio()

    def _inicializar_directorio(self):
        try:
            old_umask = os.umask(0o022)
            try:
                os.makedirs(self.directory, mode=0o755, exist_ok=True)
            finally:
                os.umask(old_umask)
        except Exception as e:
            logger.error(f"[Persistencia] Error creando directorio {self.directory}: {e}")

    def guardar(self, estado: dict) -> bool:
        temp_file = None
        try:
            old_umask = os.umask(0o022)
            try:
                fd, temp_path = tempfile.mkstemp(dir=self.directory, prefix="temp_estado_", suffix=".json")
            finally:
                os.umask(old_umask)
            temp_file = os.fdopen(fd, 'w', encoding='utf-8')

            json.dump(estado, temp_file, ensure_ascii=False)
            temp_file.flush()
            os.fsync(fd)
            temp_file.close()
            temp_file = None

            os.replace(temp_path, self.filepath)
            return True
        except Exception as e:
            logger.error(f"[Persistencia] Error guardando estado para {self.node_name}: {e}", exc_info=True)
            if temp_file:
                try:
                    temp_file.close()
                except:
                    pass
            try:
                if 'temp_path' in locals() and os.path.exists(temp_path):
                    os.remove(temp_path)
            except:
                pass
            return False

    def cargar(self) -> dict:
        if not os.path.exists(self.filepath):
            logger.debug(f"[Persistencia] No se encontró estado anterior para {self.node_name}. Iniciando limpio.")
            return {}

        try:
            with open(self.filepath, 'r', encoding='utf-8') as f:
                return json.load(f)
        except json.JSONDecodeError as e:
            logger.error(f"[Persistencia] Archivo de estado corrupto {self.filepath}: {e}")
            raise RuntimeError(f"Archivo de estado corrupto: {self.filepath}") from e
        except Exception as e:
            logger.error(f"[Persistencia] Error inesperado al leer archivo de estado {self.filepath}: {e}")
            raise

    def borrar(self) -> bool:
        try:
            if os.path.exists(self.filepath):
                os.remove(self.filepath)
            return True
        except Exception as e:
            logger.error(f"[Persistencia] Error al borrar el archivo de estado {self.filepath}: {e}")
        return False
