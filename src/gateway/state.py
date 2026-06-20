import threading
import uuid
from common.persistencia import PersistidorEstado

GATEWAY_VOLUMEN_DIR = "/app/volumen"


class GatewayState:
    def __init__(self):
        self.clientes_conectados = {}
        self.clientes_locks = {}
        self.clientes_eof_status = {}
        self.servidor_corriendo = True
        self.state_lock = threading.Lock()
        self._eventos_reconexion = {}  # {client_id: threading.Event}
        self._acks_pendientes = {}     # {client_id: {batch_id: threading.Event}}

    def generar_siguiente_id(self):
        return str(uuid.uuid4())

    def registrar_cliente(self, client_id, socket_cliente):
        with self.state_lock:
            self.clientes_conectados[client_id] = socket_cliente
            if client_id not in self.clientes_locks:
                self.clientes_locks[client_id] = threading.Lock()
            if client_id not in self.clientes_eof_status:
                self.clientes_eof_status[client_id] = set()
            # Siempre crear y activar el evento: evita la race condition donde
            # registrar_cliente se llama antes de que esperar_cliente cree el evento.
            if client_id not in self._eventos_reconexion:
                self._eventos_reconexion[client_id] = threading.Event()
            self._eventos_reconexion[client_id].set()

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
            # Limpia el evento para el próximo ciclo pero no lo elimina,
            # así esperar_cliente puede reutilizarlo en la siguiente reconexión.
            if client_id in self._eventos_reconexion:
                self._eventos_reconexion[client_id].clear()

    def detener_servidor(self):
        self.servidor_corriendo = False

    def esperar_cliente(self, client_id, timeout=120):
        """Bloquea hasta que el cliente se reconecte o se agote el timeout. Retorna True si conectó."""
        with self.state_lock:
            if client_id not in self._eventos_reconexion:
                self._eventos_reconexion[client_id] = threading.Event()
            evento = self._eventos_reconexion[client_id]
        return evento.wait(timeout=timeout)

    # --- ACKs de resultados ---

    def registrar_ack_esperado(self, client_id, batch_id):
        """Registra que se espera un ACK_RESULTADO del cliente para este batch_id. Retorna el Event."""
        evento = threading.Event()
        with self.state_lock:
            if client_id not in self._acks_pendientes:
                self._acks_pendientes[client_id] = {}
            self._acks_pendientes[client_id][batch_id] = evento
        return evento

    def notificar_ack(self, client_id, batch_id):
        """El ClientHandler llama esto cuando recibe un ACK_RESULTADO del cliente."""
        with self.state_lock:
            evento = self._acks_pendientes.get(client_id, {}).get(batch_id)
        if evento:
            evento.set()

    def cancelar_acks_cliente(self, client_id):
        """Cancela todos los ACKs pendientes de un cliente (e.g. al desconectarse)."""
        with self.state_lock:
            pendientes = self._acks_pendientes.pop(client_id, {})
        for evento in pendientes.values():
            evento.set()  # desbloquea workers que estaban esperando

    def limpiar_ack(self, client_id, batch_id):
        with self.state_lock:
            self._acks_pendientes.get(client_id, {}).pop(batch_id, None)

    # --- Persistencia por cliente ---

    def _persistidor(self, client_id):
        return PersistidorEstado(f"gateway_resultados_{client_id}", GATEWAY_VOLUMEN_DIR)

    def tiene_estado_persistido(self, client_id):
        return bool(self._persistidor(client_id).cargar())

    def cargar_estado_cliente(self, client_id):
        return self._persistidor(client_id).cargar()

    def guardar_estado_cliente(self, client_id, estado):
        self._persistidor(client_id).guardar(estado)

    def limpiar_estado_cliente(self, client_id):
        self._persistidor(client_id).borrar()
        with self.state_lock:
            self._eventos_reconexion.pop(client_id, None)
