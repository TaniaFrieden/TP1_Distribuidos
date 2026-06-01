
import os
import logging
import pika
from functools import wraps
from .middleware import (
    MessageMiddlewareCloseError,
    MessageMiddlewareDisconnectedError,
    MessageMiddlewareMessageError
)

from common import exceptions

logger = logging.getLogger(__name__)

def handle_pika_errors(action_name):
    """Decorador para atrapar excepciones de Pika sin repetir código."""
    def decorator(func):
        @wraps(func)
        def wrapper(self, *args, **kwargs):
            try:
                return func(self, *args, **kwargs)
            except exceptions.GracefulExitException:
                raise
            except pika.exceptions.AMQPConnectionError as e:
                self._cleanup_resources()
                raise MessageMiddlewareDisconnectedError(f"Conexión perdida al {action_name}") from e
            except pika.exceptions.AMQPChannelError as e:
                self._cleanup_resources()
                raise MessageMiddlewareMessageError(f"Error de canal al {action_name}") from e
            except Exception as e:
                self._cleanup_resources()
                raise MessageMiddlewareMessageError(f"Error interno inesperado al {action_name}") from e
        return wrapper
    return decorator


def handle_pika_send_errors(action_name):
    """Decorador para send(): reconecta y reintenta una vez si se pierde la conexión."""
    def decorator(func):
        @wraps(func)
        def wrapper(self, *args, **kwargs):
            try:
                return func(self, *args, **kwargs)
            except exceptions.GracefulExitException:
                raise
            except pika.exceptions.AMQPConnectionError as e:
                logger.warning(f"[RabbitMQ] Conexión perdida al {action_name}, reconectando...")
                try:
                    self._reconnect()
                    return func(self, *args, **kwargs)
                except Exception as reconnect_err:
                    self._cleanup_resources()
                    raise MessageMiddlewareDisconnectedError(
                        f"Conexión perdida al {action_name} y reconexión fallida"
                    ) from reconnect_err
            except pika.exceptions.AMQPChannelError as e:
                self._cleanup_resources()
                raise MessageMiddlewareMessageError(f"Error de canal al {action_name}") from e
            except Exception as e:
                self._cleanup_resources()
                raise MessageMiddlewareMessageError(f"Error interno inesperado al {action_name}") from e
        return wrapper
    return decorator


class RabbitMQBase:
    def __init__(self, host):
        self._host = host
        self._port = int(os.getenv("MOM_PORT", "5672"))
        self._user = os.getenv("MOM_USER", "guest")
        self._password = os.getenv("MOM_PASSWORD", "guest")
        self._vhost = os.getenv("MOM_VHOST", "/")
        self.connection = None
        self.channel = None
        self._connect()

    def _connect(self):
        self.connection = pika.BlockingConnection(
           pika.ConnectionParameters(
               host=self._host,
               port=self._port,
               virtual_host=self._vhost,
               credentials=pika.PlainCredentials(self._user, self._password),
               heartbeat=300,
               blocked_connection_timeout=600,
           )
        )
        self.channel = self.connection.channel()
        self.channel.confirm_delivery()

    def _setup(self):
        """Subclases sobreescriben para re-declarar colas/exchanges tras reconexión."""
        pass

    def _reconnect(self):
        logger.warning(f"[RabbitMQ] Reconectando a {self._host}...")
        self._cleanup_resources()
        self._connect()
        self._setup()
        logger.info("[RabbitMQ] Reconexión exitosa.")

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
        return False

    def _cleanup_resources(self):
        if self.channel and self.channel.is_open:
            try:
                self.channel.close()
            except Exception:
                pass
        if self.connection and self.connection.is_open:
            try:
                self.connection.close()
            except Exception:
                pass
        self.channel = None
        self.connection = None

    def close(self):
        try:
            self._cleanup_resources()
        except Exception as e:
            raise MessageMiddlewareCloseError("Error al cerrar la conexión") from e
