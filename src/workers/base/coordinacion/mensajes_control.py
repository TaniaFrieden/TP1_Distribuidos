from common.constantes_protocolo import ID_CLIENTE
from base.constantes import (
    TIPO_MENSAJE,
    TIPO_EOF_RECIBIDO,
    TIPO_WORKER_FINALIZADO,
    TIPO_BARRERA_COMPLETA,
    ORIGINADOR,
    ID_WORKER,
)



def msg_eof_recibido(client_id, id_nodo):
    return {
        TIPO_MENSAJE: TIPO_EOF_RECIBIDO,
        ID_CLIENTE: client_id,
        ORIGINADOR: id_nodo,
    }


def msg_worker_finalizado(client_id, originador, id_nodo):
    return {
        TIPO_MENSAJE: TIPO_WORKER_FINALIZADO,
        ID_CLIENTE: client_id,
        ORIGINADOR: originador,
        ID_WORKER: id_nodo,
    }


def msg_barrera_completa(client_id):
    return {
        TIPO_MENSAJE: TIPO_BARRERA_COMPLETA,
        ID_CLIENTE: client_id,
    }
