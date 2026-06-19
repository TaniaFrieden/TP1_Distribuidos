from asyncio import IncompleteReadError
import json
from common.logger import obtener_logger

from . import external_serializer


logger = obtener_logger(__name__)


class MsgType:
    ACK = 3
    END_OF_RECODS = 4
    REPORTE = 5
    LOTE_TRANSACCIONES = 6
    LOTE_BANCOS = 7
    CONFIG_QUERIES = 8
    HELLO = 9  # El cliente lo envía primero con su client_id para soportar reconexión

  
def _recv_sized(socket, size):
    """
    Receives exactly 'num_bytes' bytes through the provided socket.
    If no bytes are read from the socket IncompleteReadError is raised
    """
    buf = bytearray(size)
    pos = 0
    while pos < size:
        n = socket.recv_into(memoryview(buf)[pos:])
        if n == 0:
            raise IncompleteReadError(bytes(buf[:pos]), size)
        pos += n
    return bytes(buf)


def _recv_empty(socket):
    return None


def _recv_reporte(socket):
    reporte_size = external_serializer.deserialize_uint32(
        _recv_sized(socket, external_serializer.UINT32_SIZE)
    )
    
    # Capturamos los bytes brutos ANTES de decodificarlos
    raw_bytes = _recv_sized(socket, reporte_size)
    
    try:
        reporte = raw_bytes.decode("utf-8")
    except UnicodeDecodeError:
        logger.error("DEBUG CRÍTICO: Recibidos %s bytes que no son UTF-8: %s", reporte_size, raw_bytes.hex())
        raise
        
    return reporte

def _send_lote(socket, headers, client_id, lote):
    """Envía un lote de registros con un encabezado (schema, client_id, count)
       y un payload que consiste en un arreglo de arreglos de valores.
    """
    batch = {
        "header": {
            "schema": headers,
            "client_id": client_id,
            "count": len(lote)
        },
        "payload": lote
    }
    batch_bytes = json.dumps(batch).encode("utf-8")
    msg = external_serializer.serialize_uint32(len(batch_bytes)) + batch_bytes
    socket.sendall(msg)


def _recv_lote(socket):
    """Recibe un lote de registros con un encabezado y payload en formato compacto."""
    batch_size = external_serializer.deserialize_uint32(
        _recv_sized(socket, external_serializer.UINT32_SIZE)
    )
    batch_bytes = _recv_sized(socket, batch_size)
    return json.loads(batch_bytes.decode("utf-8"))


def _recv_end_of_records(socket):
    size = external_serializer.deserialize_uint32(
        _recv_sized(socket, external_serializer.UINT32_SIZE)
    )
    if size == 0:
        return None
    return _recv_sized(socket, size).decode("utf-8")


RECV_MSG_HANDLERS = {
    MsgType.ACK: _recv_empty,
    MsgType.END_OF_RECODS: _recv_end_of_records,
    MsgType.REPORTE: _recv_reporte,
    MsgType.LOTE_TRANSACCIONES: _recv_lote,
    MsgType.LOTE_BANCOS: _recv_lote,
    MsgType.CONFIG_QUERIES: _recv_reporte,
    MsgType.HELLO: _recv_reporte,
}


def recv_msg(socket):
    msg_type = external_serializer.deserialize_uint32(
        _recv_sized(socket, external_serializer.UINT32_SIZE)
    )
    msg_handler = RECV_MSG_HANDLERS[msg_type]
    return (msg_type, msg_handler(socket))


def _send_ack(socket):
    socket.sendall(external_serializer.serialize_uint32(MsgType.ACK))


def _send_end_of_records(socket, client_id=None):
    if client_id is not None:
        client_id_bytes = client_id.encode("utf-8")
        msg = external_serializer.serialize_uint32(len(client_id_bytes)) + client_id_bytes
    else:
        msg = external_serializer.serialize_uint32(0)
    socket.sendall(msg)


def _send_reporte(socket, reporte):
    """Envía un reporte como string."""
    reporte_bytes = reporte.encode("utf-8")
    msg = external_serializer.serialize_uint32(len(reporte_bytes))
    msg += reporte_bytes
    socket.sendall(msg)

def _send_empty(socket):
    pass

SEND_MSG_HANDLERS = {
    MsgType.ACK: _send_empty,
    MsgType.END_OF_RECODS: _send_end_of_records,
    MsgType.REPORTE: _send_reporte,
    MsgType.LOTE_TRANSACCIONES: _send_lote,
    MsgType.LOTE_BANCOS: _send_lote,
    MsgType.CONFIG_QUERIES: _send_reporte,
    MsgType.HELLO: _send_reporte,
}


def send_msg(socket, msg_type, *args):
    socket.sendall(external_serializer.serialize_uint32(msg_type))
    msg_handler = SEND_MSG_HANDLERS[msg_type]
    msg_handler(socket, *args)