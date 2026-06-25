import threading


class AcumuladorJoiner:
    def __init__(self):
        self._scatter: dict[str, dict[str, list]] = {}
        self._txns: dict[str, dict[str, set]] = {}
        self._vistos: dict[str, set] = {}
        self._pending_acks: dict[str, list] = {}
        self._lock = threading.Lock()

    @property
    def lock(self) -> threading.Lock:
        return self._lock

    def agregar_arista(self, client_id: str, b_key: str, a_info: tuple):
        self._scatter.setdefault(client_id, {}).setdefault(b_key, []).append(a_info)

    def agregar_transaccion(self, client_id: str, b_key: str, c_info: tuple):
        self._txns.setdefault(client_id, {}).setdefault(b_key, set()).add(c_info)

    def marcar_visto(self, client_id: str, request_id: str):
        self._vistos.setdefault(client_id, set()).add(request_id)

    def registrar_ack(self, client_id: str, ack):
        self._pending_acks.setdefault(client_id, []).append(ack)

    def restaurar(self, client_id: str, scatter: dict, txns: dict, vistos: set):
        self._scatter[client_id] = scatter
        self._txns[client_id] = txns
        self._vistos[client_id] = vistos

    def ya_visto(self, client_id: str, request_id: str) -> bool:
        return request_id in self._vistos.get(client_id, set())

    def snapshot_scatter(self, client_id: str) -> dict:
        return self._scatter.get(client_id, {})

    def snapshot_txns(self, client_id: str) -> dict:
        return self._txns.get(client_id, {})

    def snapshot_vistos(self, client_id: str) -> set:
        return self._vistos.get(client_id, set())

    def total_acks_pendientes(self) -> int:
        return sum(len(v) for v in self._pending_acks.values())

    def clientes_con_acks(self) -> list[str]:
        return list(self._pending_acks.keys())

    def extraer_acks(self, client_id: str) -> list:
        return self._pending_acks.pop(client_id, [])

    def extraer_cliente(self, client_id: str) -> tuple[dict, dict]:
        scatter = self._scatter.pop(client_id, {})
        txns = self._txns.pop(client_id, {})
        self._vistos.pop(client_id, None)
        self._pending_acks.pop(client_id, None)
        return scatter, txns
