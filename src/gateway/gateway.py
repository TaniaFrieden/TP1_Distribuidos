import json
import socket
import threading
import signal
import sys
import time
from config import GatewayConfig
from estado import EstadoGateway
from receptor_resultados import ReceptorResultados
from manejador_clientes import ManejadorClientes
from common.logger import Logger, obtener_logger
from common import middleware, message_protocol

logger = obtener_logger(__name__)


def _emitir_latidos(config, evento_cierre):
    intervalo = float(config.heartbeat_interval_seconds)
    if intervalo <= 0:
        return

    cola = None
    while not evento_cierre.is_set():
        try:
            if cola is None:
                cola = middleware.MessageMiddlewareQueueRabbitMQ(config.mom_host, "heartbeat.gateway")
            payload = json.dumps({"etapa": "gateway", "instancia": "01", "timestamp": time.time()})
            cola.send(payload.encode("utf-8"))
        except Exception as e:
            logger.warning(f"[Gateway] Error enviando heartbeat: {e}")
            if cola is not None:
                try:
                    cola.close()
                except Exception:
                    pass
                cola = None
        evento_cierre.wait(intervalo)

    if cola is not None:
        try:
            cola.close()
        except Exception:
            pass


def _despachar_conexion(sock, manejador, estado):
    try:
        tipo_mensaje, payload = message_protocol.external.recibir_mensaje(sock)
    except Exception as e:
        logger.error(f"Error leyendo primer mensaje de conexión: {e}")
        sock.close()
        return

    if tipo_mensaje == message_protocol.external.TipoMensaje.HELLO:
        data = json.loads(payload)
        client_id = data.get("client_id", "").strip()
        if not client_id:
            logger.warning("HELLO sin client_id, cerrando conexión")
            sock.close()
            return
        manejador.atender(sock, client_id)

    elif tipo_mensaje == message_protocol.external.TipoMensaje.HELLO_RESULTS:
        data = json.loads(payload)
        client_id = data.get("client_id", "").strip()
        if not client_id:
            logger.warning("HELLO_RESULTS sin client_id, cerrando conexión")
            sock.close()
            return
        logger.info(f"Socket de resultados conectado para {client_id}")
        estado.registrar_socket_resultados(client_id, sock)
        manejador.leer_acks_resultados(client_id, sock)

    else:
        logger.warning(f"Tipo de mensaje inesperado en conexión: {tipo_mensaje}")
        sock.close()


def main():
    Logger.configurar("gateway")

    config = GatewayConfig()
    estado = EstadoGateway()
    receptor = ReceptorResultados(config, estado)
    manejador = ManejadorClientes(config, estado)

    for cola_nombre in config.input_queues:
        if cola_nombre:
            threading.Thread(
                target=receptor.escuchar, args=(cola_nombre,), daemon=True
            ).start()

    evento_cierre = threading.Event()
    threading.Thread(
        target=_emitir_latidos, args=(config, evento_cierre),
        daemon=True, name="gateway-heartbeat",
    ).start()

    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server_socket.bind((config.server_host, config.server_port))
    server_socket.listen()

    logger.info(f"Gateway listo en {config.server_host}:{config.server_port}")

    def cerrar_graceful(sig, frame):
        logger.info("Apagando Gateway...")
        evento_cierre.set()
        estado.detener_servidor()
        server_socket.close()
        sys.exit(0)

    signal.signal(signal.SIGINT, cerrar_graceful)
    signal.signal(signal.SIGTERM, cerrar_graceful)

    try:
        while estado.servidor_corriendo:
            client_sock, _ = server_socket.accept()
            threading.Thread(
                target=_despachar_conexion,
                args=(client_sock, manejador, estado),
                daemon=True,
            ).start()
    except Exception:
        pass
    finally:
        evento_cierre.set()
        server_socket.close()


if __name__ == "__main__":
    main()
