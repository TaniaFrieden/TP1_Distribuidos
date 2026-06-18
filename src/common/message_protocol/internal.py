import json

class ParseadorMensajes:
    @staticmethod
    def deserializar(datos: str | bytes | dict) -> dict:
        if isinstance(datos, dict):
            return datos
        if isinstance(datos, bytes):
            datos = datos.decode("utf-8")
        return json.loads(datos)

    @staticmethod
    def serializar(payload: dict) -> bytes:
        return json.dumps(payload).encode("utf-8")

def serialize(payload: dict) -> bytes:
    return ParseadorMensajes.serializar(payload)

def deserialize(data: bytes) -> dict:
    return ParseadorMensajes.deserializar(data)

def is_eof(payload: dict) -> bool:
    return isinstance(payload, dict) and set(payload.keys()) == {"client_id"}

def is_data(payload: dict) -> bool:
    return isinstance(payload, dict) and not is_eof(payload)

def make_eof(client_id: int) -> bytes:
    return serialize({"client_id": int(client_id)})

def make_data(record: dict) -> bytes:
    return serialize(record)

def get_client_id(payload: dict) -> int:
    return int(payload["client_id"])

def make_client_disconnect(client_id: str) -> bytes:
    return serialize({"client_id": client_id, "CLIENT_DISCONNECT": True})

def is_client_disconnect(payload: dict) -> bool:
    return isinstance(payload, dict) and payload.get("CLIENT_DISCONNECT") is True