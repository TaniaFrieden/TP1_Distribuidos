import threading

class GatewayState:
    def __init__(self):
        self.clientes_conectados = {}
        self.clientes_locks = {}
        self.clientes_eof_status = {}
        self.servidor_corriendo = True
        self.state_lock = threading.Lock()

    def registrar_cliente(self, client_id, socket_cliente):
        with self.state_lock:
            self.clientes_conectados[client_id] = socket_cliente
            self.clientes_locks[client_id] = threading.Lock()
            self.clientes_eof_status[client_id] = set()

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

    def detener_servidor(self):
        self.servidor_corriendo = False