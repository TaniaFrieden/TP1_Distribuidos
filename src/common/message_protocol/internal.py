"""
message_protocol.internal
=========================
Protocolo de serialización para mensajes internos entre workers.

Formato en cola:
  - Mensaje de datos:
      {"client_id": 0, "from_id": "ABC123", "amount_paid": 12.5, ...}

  - Mensaje de EOF (control):
      {"client_id": 0}   ← generado por serialize_control / serialize_eof_message

Helpers para workers:
    serialize(payload)        → bytes
    deserialize(data)         → dict
    is_eof(payload)           → bool  (tiene solo "client_id", sin otros campos)
    make_eof(client_id)       → bytes
"""

import json


def serialize(payload: dict) -> bytes:
    return json.dumps(payload).encode("utf-8")


def deserialize(data: bytes) -> dict:
    return json.loads(data.decode("utf-8"))


def is_eof(payload: dict) -> bool:
    """
    Un mensaje es EOF si tiene exactamente la clave "client_id" y nada más.
    Esto es lo que produce serialize_control() del gateway.
    """
    return isinstance(payload, dict) and set(payload.keys()) == {"client_id"}


def is_data(payload: dict) -> bool:
    return isinstance(payload, dict) and not is_eof(payload)


def make_eof(client_id: int) -> bytes:
    return serialize({"client_id": int(client_id)})


def make_data(record: dict) -> bytes:
    return serialize(record)


def get_client_id(payload: dict) -> int:
    return int(payload["client_id"])