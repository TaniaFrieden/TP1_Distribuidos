import os
from common.logger import obtener_logger

logger = obtener_logger(__name__)

VOLUMEN_DIR = os.getenv("CRASH_HOOK_VOLUMEN", "/app/volumen")


class CrashHook:
    def __init__(self, volumen_dir=VOLUMEN_DIR):
        self._volumen_dir = volumen_dir
        raw = os.environ.get("CRASH_HOOK", "")
        self._hooks = set(h.strip() for h in raw.split(",") if h.strip())

    def verificar(self, env_var, descripcion=""):
        if env_var not in self._hooks:
            return
        crashes_dir = os.path.join(self._volumen_dir, "crashes")
        bandera = os.path.join(crashes_dir, env_var)
        if os.path.exists(bandera):
            return
        os.makedirs(crashes_dir, exist_ok=True)
        with open(bandera, "w") as f:
            f.write("1")
        logger.warning(f"CRASH HOOK: {env_var} ({descripcion})")
        os._exit(1)
