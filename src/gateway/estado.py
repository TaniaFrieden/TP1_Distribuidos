import threading
from common.persistencia import PersistidorEstado
from constantes import VOLUMEN_GATEWAY


class EstadoGateway:
    def __init__(self):
        self._estado_locks = {}
        self.clientes_conectados = {}
        self.clientes_locks = {}
        self.clientes_eof_status = {}
        self.clientes_resultados = {}
        self.clientes_results_locks = {}
        self.request_counters = {}
        self.servidor_corriendo = True
        self.state_lock = threading.Lock()
        self._eventos_reconexion = {}
        self._eventos_socket_resultados = {}
        self._acks_pendientes = {}
        self._sesiones = {}
        self._contador_clientes = self._cargar_contador_clientes()

    def _cargar_contador_clientes(self):
        estado = PersistidorEstado("gateway_contador_clientes", VOLUMEN_GATEWAY).cargar()
        return estado.get("siguiente", 0)

    def _guardar_contador_clientes(self):
        PersistidorEstado("gateway_contador_clientes", VOLUMEN_GATEWAY).guardar(
            {"siguiente": self._contador_clientes}
        )

    def generar_siguiente_id(self):
        with self.state_lock:
            client_id = str(self._contador_clientes)
            self._contador_clientes += 1
            self._guardar_contador_clientes()
            return client_id

    def registrar_cliente(self, client_id, socket_cliente):
        with self.state_lock:
            self.clientes_conectados[client_id] = socket_cliente
            if client_id not in self.clientes_locks:
                self.clientes_locks[client_id] = threading.Lock()
            if client_id not in self.clientes_eof_status:
                self.clientes_eof_status[client_id] = set()
            if client_id not in self._eventos_reconexion:
                self._eventos_reconexion[client_id] = threading.Event()
            self._eventos_reconexion[client_id].set()

    def registrar_socket_resultados(self, client_id, sock):
        with self.state_lock:
            self.clientes_resultados[client_id] = sock
            if client_id not in self.clientes_results_locks:
                self.clientes_results_locks[client_id] = threading.Lock()
            if client_id not in self._eventos_socket_resultados:
                self._eventos_socket_resultados[client_id] = threading.Event()
            self._eventos_socket_resultados[client_id].set()

    def obtener_cliente(self, client_id):
        with self.state_lock:
            return (
                self.clientes_conectados.get(client_id),
                self.clientes_locks.get(client_id),
                self.clientes_eof_status.get(client_id),
            )

    def obtener_socket_resultados(self, client_id):
        with self.state_lock:
            return (
                self.clientes_resultados.get(client_id),
                self.clientes_results_locks.get(client_id),
                self.clientes_eof_status.get(client_id),
            )

    def remover_cliente(self, client_id):
        with self.state_lock:
            self.clientes_conectados.pop(client_id, None)
            self.clientes_locks.pop(client_id, None)
            self.clientes_eof_status.pop(client_id, None)
            self.clientes_resultados.pop(client_id, None)
            self.clientes_results_locks.pop(client_id, None)
            for k in [k for k in self.request_counters if k[0] == client_id]:
                self.request_counters.pop(k, None)
            if client_id in self._eventos_reconexion:
                self._eventos_reconexion[client_id].clear()
            if client_id in self._eventos_socket_resultados:
                self._eventos_socket_resultados[client_id].clear()

    def detener_servidor(self):
        self.servidor_corriendo = False

    def esperar_cliente(self, client_id, timeout=120):
        with self.state_lock:
            if client_id not in self._eventos_reconexion:
                self._eventos_reconexion[client_id] = threading.Event()
            evento = self._eventos_reconexion[client_id]
        return evento.wait(timeout=timeout)

    def esperar_socket_resultados(self, client_id, timeout=120):
        with self.state_lock:
            if client_id not in self._eventos_socket_resultados:
                self._eventos_socket_resultados[client_id] = threading.Event()
            evento = self._eventos_socket_resultados[client_id]
        return evento.wait(timeout=timeout)

    def registrar_sesion(self, client_id, session_id):
        with self.state_lock:
            self._sesiones[client_id] = session_id

    def obtener_sesion(self, client_id):
        with self.state_lock:
            return self._sesiones.get(client_id)

    def generar_request_id(self, client_id, query_key):
        with self.state_lock:
            key = (client_id, query_key)
            if key not in self.request_counters:
                self.request_counters[key] = 0
            self.request_counters[key] += 1
            session_id = self._sesiones.get(client_id, "")
            return f"{client_id}:{session_id}:{query_key}:{self.request_counters[key]}"

    def registrar_ack_esperado(self, client_id, batch_id):
        evento = threading.Event()
        with self.state_lock:
            if client_id not in self._acks_pendientes:
                self._acks_pendientes[client_id] = {}
            self._acks_pendientes[client_id][batch_id] = evento
        return evento

    def notificar_ack(self, client_id, batch_id):
        with self.state_lock:
            evento = self._acks_pendientes.get(client_id, {}).get(batch_id)
        if evento:
            evento.set()

    def cancelar_acks_cliente(self, client_id):
        with self.state_lock:
            pendientes = self._acks_pendientes.pop(client_id, {})
        for evento in pendientes.values():
            evento.set()

    def limpiar_ack(self, client_id, batch_id):
        with self.state_lock:
            self._acks_pendientes.get(client_id, {}).pop(batch_id, None)

    def _persistidor(self, client_id):
        return PersistidorEstado(f"gateway_resultados_{client_id}", VOLUMEN_GATEWAY)

    def _estado_lock(self, client_id):
        with self.state_lock:
            if client_id not in self._estado_locks:
                self._estado_locks[client_id] = threading.Lock()
            return self._estado_locks[client_id]

    def tiene_estado_persistido(self, client_id):
        with self._estado_lock(client_id):
            return bool(self._persistidor(client_id).cargar())

    def cargar_estado_cliente(self, client_id):
        with self._estado_lock(client_id):
            return self._persistidor(client_id).cargar()

    def guardar_estado_cliente(self, client_id, estado):
        with self._estado_lock(client_id):
            self._persistidor(client_id).guardar(estado)

    def actualizar_estado_cliente(self, client_id, actualizaciones):
        with self._estado_lock(client_id):
            estado = self._persistidor(client_id).cargar()
            estado.update(actualizaciones)
            self._persistidor(client_id).guardar(estado)

    def limpiar_estado_cliente(self, client_id):
        with self._estado_lock(client_id):
            self._persistidor(client_id).borrar()
        with self.state_lock:
            self._estado_locks.pop(client_id, None)
            self._eventos_reconexion.pop(client_id, None)
