import os
from common.logger import obtener_logger
from common.persistencia import VOLUMEN_DIR
from constantes import ENV_CRASH_DESPUES_PERSISTIR, BANDERA_CRASH_PERSISTIR
from base.coordinacion.hooks import crear_hook_crash_despues_flush

__all__ = ["crear_hook_crash_despues_persistir", "crear_hook_crash_despues_flush"]


logger = obtener_logger(__name__)


def crear_hook_crash_despues_persistir():
    if os.environ.get(ENV_CRASH_DESPUES_PERSISTIR) != "true":
        return None

    bandera = os.path.join(VOLUMEN_DIR, BANDERA_CRASH_PERSISTIR)

    def hook():
        if not os.path.exists(bandera):
            open(bandera, "w").close()
            logger.warning("CRASH_AFTER_PERSIST activado — muriendo antes del ack()")
            os._exit(1)

    return hook
