import threading
import uuid

class GatewayState:
    def __init__(self):
        self.clientes_conectados = {}
        self.clientes_locks = {}
        self.clientes_eof_status = {}
        self.request_counters = {}
        self.servidor_corriendo = True
        self.state_lock = threading.Lock()

    def generar_siguiente_id(self):
        return str(uuid.uuid4())

    def registrar_cliente(self, client_id, socket_cliente):
        with self.state_lock:
            self.clientes_conectados[client_id] = socket_cliente
            self.clientes_locks[client_id] = threading.Lock()
            self.clientes_eof_status[client_id] = set()

    def generar_request_id(self, client_id, query_key):
        with self.state_lock:
            key = (client_id, query_key)
            if key not in self.request_counters:
                self.request_counters[key] = 0
            self.request_counters[key] += 1
            seq = self.request_counters[key]
            return f"{client_id}:{query_key}:{seq}"

    def obtener_cliente(self, client_id):
        with self.state_lock:
            return (
                self.clientes_conectados.get(client_id),
                self.clientes_locks.get(client_id),
                self.clientes_eof_status.get(client_id)
            )

    def remover_cliente(self, client_id):
        with self.state_lock:
            self.clientes_conectados.pop(client_id, None)
            self.clientes_locks.pop(client_id, None)
            self.clientes_eof_status.pop(client_id, None)
            keys_to_remove = [k for k in self.request_counters if k[0] == client_id]
            for k in keys_to_remove:
                self.request_counters.pop(k, None)

    def detener_servidor(self):
        self.servidor_corriendo = False