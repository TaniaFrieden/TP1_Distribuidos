"""Test de independencia entre envío de datos y ACK de resultados.

Con la arquitectura de dos sockets TCP (uno para datos, otro para
resultados), el envío y la recepción son completamente independientes.
Estos tests verifican que el Enviador y el Receptor funcionan sin
ningún lock compartido ni mecanismo de prioridad.
"""

import sys
import os
import types
import threading
import time
import unittest
from unittest.mock import MagicMock

raiz = os.path.abspath(os.path.join(os.path.dirname(__file__), '../..'))
if os.path.join(raiz, 'src/client') not in sys.path:
    sys.path.insert(0, os.path.join(raiz, 'src/client'))
    sys.path.insert(0, os.path.join(raiz, 'src'))

mock_config = types.ModuleType('config')
mock_config.OUTPUT_DIR = "output"
mock_config.SERVER_HOST = "127.0.0.1"
mock_config.SERVER_PORT = 5678
mock_config.TRANSACTIONS_FILE = "tx.csv"
mock_config.ACCOUNTS_FILE = "acc.csv"
mock_config.LOTE_SIZE = 5
sys.modules['config'] = mock_config

from enviador import Enviador
from receptor import Receptor


class TestDosSocketsIndependientes(unittest.TestCase):

    def test_sender_envia_sin_bloqueo(self):
        """El sender envía todos los lotes sin pausarse."""
        lock = threading.Lock()
        shutdown = threading.Event()
        envios = []
        mock_conexion = MagicMock()
        mock_conexion.enviar.side_effect = lambda *a, **kw: envios.append(1)

        enviador = Enviador(mock_conexion, "client1", lock, shutdown)

        registros = [["a", "b"]] * 15
        headers = ["c1", "c2"]
        enviador._enviar_lotes(headers, iter(registros), "LOTE", "", 0)

        self.assertEqual(len(envios), 3)  # 15 / 5 = 3 lotes

    def test_receptor_envia_ack_sin_lock(self):
        """El receptor envía ACK directamente sin necesitar lock."""
        mock_conexion = MagicMock()
        mock_persistencia = MagicMock()
        mock_persistencia.directorio_cliente.return_value = "/tmp/test"
        mock_persistencia.cargar_queries_completadas.return_value = set()
        mock_persistencia.cargar_batch_ids.return_value = {}

        receptor = Receptor(
            conexion=mock_conexion,
            queries=[],
            inicio=0.0,
            client_id="test",
            evento_completado=threading.Event(),
            persistencia=mock_persistencia,
        )

        receptor._enviar_ack("batch123")
        mock_conexion.enviar.assert_called_once()

    def test_sender_y_receptor_en_paralelo(self):
        """Sender y receptor corren simultáneamente sin interferencia."""
        send_conn = MagicMock()
        recv_conn = MagicMock()
        lock = threading.Lock()
        shutdown = threading.Event()

        envios = []
        send_conn.enviar.side_effect = lambda *a, **kw: envios.append(time.monotonic())

        enviador = Enviador(send_conn, "client1", lock, shutdown)

        mock_persistencia = MagicMock()
        mock_persistencia.directorio_cliente.return_value = "/tmp/test"
        mock_persistencia.cargar_queries_completadas.return_value = set()
        mock_persistencia.cargar_batch_ids.return_value = {}

        receptor = Receptor(
            conexion=recv_conn,
            queries=[],
            inicio=0.0,
            client_id="test",
            evento_completado=threading.Event(),
            persistencia=mock_persistencia,
        )

        ack_done = threading.Event()
        sender_done = threading.Event()

        def run_sender():
            registros = [["x", "y"]] * 500
            enviador._enviar_lotes(["c1", "c2"], iter(registros), "LOTE", "", 0)
            sender_done.set()

        def run_ack():
            threading.Event().wait(timeout=0.01)
            receptor._enviar_ack("batch123")
            ack_done.set()

        t_sender = threading.Thread(target=run_sender, daemon=True)
        t_ack = threading.Thread(target=run_ack, daemon=True)

        inicio = time.monotonic()
        t_sender.start()
        t_ack.start()

        ack_ok = ack_done.wait(timeout=5.0)
        duracion = time.monotonic() - inicio

        t_sender.join(timeout=2)
        t_ack.join(timeout=2)

        self.assertTrue(ack_ok, "ACK no se completó")
        self.assertLess(duracion, 1.0, f"ACK tardó {duracion:.3f}s")
        self.assertTrue(sender_done.is_set(), "Sender no terminó")


if __name__ == "__main__":
    unittest.main()
