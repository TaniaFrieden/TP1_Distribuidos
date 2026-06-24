from common.crash_hook import CrashHook
from common import crash_points as CP

HOOK_PRE_FINISHED = "pre_finished"
HOOK_POST_FLUSH = "post_flush"
HOOK_BEFORE_EOF_FORWARD = "before_eof_forward"
HOOK_BEFORE_DATA_ACK = "before_data_ack"

_hook = CrashHook()


def crear_hook_crash_pre_finished(prefijo_nodo, id_nodo):
    def hook():
        _hook.verificar(CP.BEFORE_FINISHED_CONFIRMATION, f"pre-finished {prefijo_nodo}_{id_nodo}")
    return hook


def crear_hook_crash_despues_flush():
    def hook():
        _hook.verificar(CP.AFTER_FLUSH, "post-flush")
    return hook


def crear_hook_crash_despues_persistir():
    def hook():
        _hook.verificar(CP.AFTER_PERSIST, "post-persist")
    return hook


def crear_hook_crash_pre_barrera(prefijo_nodo: str):
    def hook():
        _hook.verificar(CP.PRE_BARRERA, f"pre-barrera {prefijo_nodo}")
    return hook


def crear_hook_crash_before_eof_forward(prefijo_nodo, id_nodo):
    def hook():
        _hook.verificar(CP.BEFORE_EOF_FORWARD, f"before-eof-forward {prefijo_nodo}_{id_nodo}")
    return hook


def crear_hook_crash_before_data_ack(prefijo_nodo, id_nodo):
    def hook():
        _hook.verificar(CP.BEFORE_DATA_ACK, f"before-data-ack {prefijo_nodo}_{id_nodo}")
    return hook
