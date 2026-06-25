import socket
import logging
from common import message_protocol


class ConexionTCP:
    """Conexión TCP con el protocolo externo de mensajes."""

    def __init__(self, host, puerto):
        self._host = host
        self._puerto = puerto
        self._sock = None

    def conectar(self):
        self._sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            self._sock.connect((self._host, self._puerto))
            return True
        except Exception as e:
            logging.error(f"No se pudo conectar a {self._host}:{self._puerto}: {e}")
            self._sock.close()
            self._sock = None
            return False

    def cerrar(self):
        if self._sock:
            try:
                self._sock.close()
            except Exception:
                pass
            self._sock = None

    def enviar(self, tipo_mensaje, *args):
        message_protocol.external.enviar_mensaje(self._sock, tipo_mensaje, *args)

    def recibir(self):
        return message_protocol.external.recibir_mensaje(self._sock)
