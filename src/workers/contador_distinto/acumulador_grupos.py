import threading


class AcumuladorGrupos:

    def __init__(self):
        self._grupos: dict[str, dict[tuple, set]] = {}
        self._vistos: dict[str, set] = {}
        self._pending_acks: dict[str, list] = {}
        self._buffer: dict[str, list] = {}
        self._buffer_ids: dict[str, list] = {}
        self._lock = threading.Lock()

    @property
    def lock(self) -> threading.Lock:
        return self._lock

    def agregar(self, client_id: str, clave_grupo: tuple, clave_valor: tuple):
        self._grupos.setdefault(client_id, {}).setdefault(clave_grupo, set()).add(clave_valor)
        self._buffer.setdefault(client_id, []).append((clave_grupo, clave_valor))

    def marcar_visto(self, client_id: str, request_id: str):
        self._vistos.setdefault(client_id, set()).add(request_id)
        self._buffer_ids.setdefault(client_id, []).append(request_id)

    def registrar_ack(self, client_id: str, ack):
        self._pending_acks.setdefault(client_id, []).append(ack)

    def restaurar(self, client_id: str, grupos: dict, vistos: set):
        self._grupos[client_id] = grupos
        self._vistos[client_id] = vistos

    def ya_visto(self, client_id: str, request_id: str) -> bool:
        return request_id in self._vistos.get(client_id, set())

    def extraer_buffer(self, client_id: str) -> tuple[list, list]:
        ops = self._buffer.pop(client_id, [])
        ids = self._buffer_ids.pop(client_id, [])
        return ops, ids

    def total_acks_pendientes(self) -> int:
        return sum(len(v) for v in self._pending_acks.values())

    def clientes_con_acks(self) -> list[str]:
        return list(self._pending_acks.keys())

    def extraer_acks(self, client_id: str) -> list:
        return self._pending_acks.pop(client_id, [])

    def extraer_grupos(self, client_id: str) -> dict:
        grupos = self._grupos.pop(client_id, {})
        self._vistos.pop(client_id, None)
        self._pending_acks.pop(client_id, None)
        self._buffer.pop(client_id, None)
        self._buffer_ids.pop(client_id, None)
        return grupos
