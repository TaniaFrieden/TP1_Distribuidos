import os
from common.logger import obtener_logger
from common.persistencia import VOLUMEN_DIR

logger = obtener_logger(__name__)

HOOK_PRE_FINISHED = "pre_finished"


def crear_hook_crash_pre_finished(prefijo_nodo, id_nodo):
    if os.environ.get("CRASH_BEFORE_FINISHED_CONFIRMATION") != "true":
        return None
    bandera = os.path.join(
        VOLUMEN_DIR,
        f"{prefijo_nodo}_{id_nodo}_crash_before_finished_done",
    )

    def hook():
        if not os.path.exists(bandera):
            open(bandera, "w").close()
            logger.warning(
                "CRASH_BEFORE_FINISHED_CONFIRMATION activado "
                "— muriendo ANTES de enviar WORKER_FINALIZADO"
            )
            os._exit(1)

    return hook
