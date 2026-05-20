from asyncio import IncompleteReadError
import json

from . import external_serializer


class MsgType:
    ACK = 3
    END_OF_RECODS = 4
    REPORTE = 5
    LOTE_TRANSACCIONES = 6   
    LOTE_CUENTAS = 7  

  
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
    """Recibe un reporte como string."""
    reporte_size = external_serializer.deserialize_uint32(
        _recv_sized(socket, external_serializer.UINT32_SIZE)
    )
    reporte = external_serializer.deserialize_string(_recv_sized(socket, reporte_size))
    return reporte

def _send_lote(socket, lote):
    """Envía un lote de registros como strings de texto plano.
       El MsgType ya se envió en la función send_msg.
    """
    msg = external_serializer.serialize_uint32(len(lote))
    
    for record in lote:
        record_bytes = str(record).encode("utf-8")
        msg += external_serializer.serialize_uint32(len(record_bytes))
        msg += record_bytes
    
    socket.sendall(msg)


def _recv_lote(socket):
    """Recibe un lote de registros como strings de texto plano."""
    lote_size = external_serializer.deserialize_uint32(
        _recv_sized(socket, external_serializer.UINT32_SIZE)
    )
    lote = []
    for _ in range(lote_size):
        record_size = external_serializer.deserialize_uint32(
            _recv_sized(socket, external_serializer.UINT32_SIZE)
        )
        record_bytes = _recv_sized(socket, record_size)
        
        record = record_bytes.decode("utf-8")
        lote.append(record)
        
    return lote


RECV_MSG_HANDLERS = {
    MsgType.ACK: _recv_empty,
    MsgType.END_OF_RECODS: _recv_empty,
    MsgType.REPORTE: _recv_reporte,
    MsgType.LOTE_TRANSACCIONES: _recv_lote,
    MsgType.LOTE_CUENTAS: _recv_lote,
}


def recv_msg(socket):
    msg_type = external_serializer.deserialize_uint32(
        _recv_sized(socket, external_serializer.UINT32_SIZE)
    )
    msg_handler = RECV_MSG_HANDLERS[msg_type]
    return (msg_type, msg_handler(socket))


def _send_ack(socket):
    socket.sendall(external_serializer.serialize_uint32(MsgType.ACK))


def _send_end_of_records(socket):
    socket.sendall(external_serializer.serialize_uint32(MsgType.END_OF_RECODS))


def _send_reporte(socket, reporte):
    """Envía un reporte como string."""
    msg = external_serializer.serialize_uint32(MsgType.REPORTE)
    msg += external_serializer.serialize_uint32(len(reporte))
    msg += external_serializer.serialize_string(reporte)
    socket.sendall(msg)


SEND_MSG_HANDLERS = {
    MsgType.ACK: _send_ack,
    MsgType.END_OF_RECODS: _send_end_of_records,
    MsgType.REPORTE: _send_reporte,
    MsgType.LOTE_TRANSACCIONES: _send_lote,
    MsgType.LOTE_CUENTAS: _send_lote,
}


def send_msg(socket, msg_type, *args):
    socket.sendall(external_serializer.serialize_uint32(msg_type))
    
    msg_handler = SEND_MSG_HANDLERS[msg_type]
    msg_handler(socket, *args)