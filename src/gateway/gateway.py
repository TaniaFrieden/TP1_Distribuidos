import socket
import threading
import logging
import signal
import sys
from config import GatewayConfig
from state import GatewayState
from backend import BackendListener
from client_handler import ClientHandler
from common.logging_setup import setup_logging

logger = logging.getLogger(__name__)

def main():
    setup_logging("gateway")

    config = GatewayConfig()
    state = GatewayState()
    backend_listener = BackendListener(config, state)
    client_handler = ClientHandler(config, state)
    
    for cola_nombre in config.input_queues:
        if cola_nombre:
            t = threading.Thread(
                target=backend_listener.escuchar,
                args=(cola_nombre,),
                daemon=True
            )
            t.start()
    
    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server_socket.bind((config.server_host, config.server_port))
    server_socket.listen()
    
    logger.info(f"Gateway listo en {config.server_host}:{config.server_port}")
    
    def cerrar_graceful(sig, frame):
        logger.info("Apagando Gateway...")
        state.detener_servidor()
        server_socket.close()
        sys.exit(0)
        
    signal.signal(signal.SIGINT, cerrar_graceful)
    signal.signal(signal.SIGTERM, cerrar_graceful)
    
    try:
        while state.servidor_corriendo:
            client_sock, _ = server_socket.accept()
            hilo_cliente = threading.Thread(
                target=client_handler.atender,
                args=(client_sock,),
                daemon=True
            )
            hilo_cliente.start()
    except Exception:
        pass
    finally:
        server_socket.close()

if __name__ == "__main__":
    main()