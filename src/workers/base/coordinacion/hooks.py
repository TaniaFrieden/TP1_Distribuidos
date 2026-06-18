import os
from common.logger import obtener_logger
from common.persistencia import VOLUMEN_DIR

logger = obtener_logger(__name__)

HOOK_PRE_FINISHED = "pre_finished"
HOOK_POST_FLUSH = "post_flush"

ENV_CRASH_ANTES_FINISHED = "CRASH_BEFORE_FINISHED_CONFIRMATION"
ENV_CRASH_DESPUES_FLUSH = "CRASH_AFTER_FLUSH"
ENV_CRASH_DESPUES_PERSISTIR = "CRASH_AFTER_PERSIST"
ENV_CRASH_PRE_BARRERA = "CRASH_PRE_BARRERA"

BANDERA_CRASH_FLUSH = "crash_flush_done"
BANDERA_CRASH_PERSISTIR = "crash_once_done"


def crear_hook_crash_pre_finished(prefijo_nodo, id_nodo):
    if os.environ.get(ENV_CRASH_ANTES_FINISHED) != "true":
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


def crear_hook_crash_despues_flush():
    if os.environ.get(ENV_CRASH_DESPUES_FLUSH) != "true":
        return None
    bandera = os.path.join(VOLUMEN_DIR, BANDERA_CRASH_FLUSH)

    def hook():
        if not os.path.exists(bandera):
            open(bandera, "w").close()
            logger.warning(
                "CRASH_AFTER_FLUSH — muriendo después del envío, "
                "antes de barrier_completada"
            )
            os._exit(1)

    return hook


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


def crear_hook_crash_pre_barrera(prefijo_nodo: str):
    if os.environ.get(ENV_CRASH_PRE_BARRERA) != "true":
        return None
    bandera = os.path.join(VOLUMEN_DIR, f"{prefijo_nodo}_crash_pre_barrera_done")

    def hook():
        if not os.path.exists(bandera):
            open(bandera, "w").close()
            logger.warning("CRASH_PRE_BARRERA activado — muriendo antes de persistir flush_iniciado")
            os._exit(1)

    return hook
