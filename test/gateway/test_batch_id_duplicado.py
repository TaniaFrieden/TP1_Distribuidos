"""Test del bug de batch_id duplicado.

Reproduce el bug donde dos lotes distintos (de mensajes RabbitMQ diferentes)
con contenido identico tras proyeccion generaban el mismo batch_id
(md5 del contenido). El receiver del cliente los descartaba como
re-entregas duplicadas, perdiendo registros legitimos.

Caso real: transacciones identicas en todos los campos proyectados
pero con Timestamps distintos (ej: misma cuenta, mismo monto, fechas
diferentes). Tras la proyeccion que elimina Timestamp, los lotes
quedan con contenido identico.
"""

import hashlib
import json
import os
import sys
import tempfile
import threading
import types
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

from receptor import Receptor


def _hacer_reporte(q_id, registros, batch_id):
    """Construye un payload de REPORTE como lo envia el gateway."""
    resultado = [{**r, "eof": False} for r in registros]
    return json.dumps({
        "query": q_id,
        "resultado": resultado,
        "batch_id": batch_id,
    })


def _crear_receptor(tmpdir):
    mock_persistencia = MagicMock()
    mock_persistencia.directorio_cliente.return_value = tmpdir
    mock_persistencia.cargar_queries_completadas.return_value = set()
    mock_persistencia.cargar_batch_ids.return_value = {}
    return Receptor(
        conexion=MagicMock(),
        queries=[],
        inicio=0.0,
        client_id="test",
        evento_completado=threading.Event(),
        persistencia=mock_persistencia,
        progreso=MagicMock(),
    )


class TestBatchIdDuplicado(unittest.TestCase):

    def setUp(self):
        self.tmpdir = tempfile.mkdtemp()
        self.receptor = _crear_receptor(self.tmpdir)

    def tearDown(self):
        for f in self.receptor._archivos.values():
            f.close()
        import shutil
        shutil.rmtree(self.tmpdir, ignore_errors=True)

    def _contar_lineas_datos(self, q_id):
        path = os.path.join(self.tmpdir, f"q{q_id}_solucion.csv")
        if not os.path.exists(path):
            return 0
        with open(path) as f:
            lineas = f.readlines()
        return len(lineas) - 1  # descontar header

    def test_lotes_identicos_con_mismo_batch_id_pierde_datos(self):
        """Demuestra el bug: dos lotes con el mismo batch_id (hash del
        contenido) causan que el segundo se descarte como duplicado."""
        registros = [
            {"From Bank": 27880, "Account": "824242750",
             "To Bank": 27880, "Account.1": "824242750",
             "Amount Paid": 18.23},
        ]

        contenido = json.dumps({"query": 1, "resultado": registros})
        batch_id_identico = hashlib.md5(contenido.encode()).hexdigest()

        reporte1 = _hacer_reporte(1, registros, batch_id_identico)
        reporte2 = _hacer_reporte(1, registros, batch_id_identico)

        self.receptor._procesar_resultado(reporte1)
        self.receptor._procesar_resultado(reporte2)

        escritas = self._contar_lineas_datos(1)
        self.assertEqual(escritas, 1,
                         f"Con batch_id duplicado deberia escribir solo 1 "
                         f"(el segundo se descarta), pero escribio {escritas}")

    def test_lotes_identicos_con_distinto_batch_id_no_pierde_datos(self):
        """Verifica el fix: dos lotes con contenido identico pero
        batch_ids distintos (derivados del request_id) se escriben ambos."""
        registros = [
            {"From Bank": 27880, "Account": "824242750",
             "To Bank": 27880, "Account.1": "824242750",
             "Amount Paid": 18.23},
        ]

        batch_id_1 = hashlib.md5(b"request_id_abc:0").hexdigest()[:16]
        batch_id_2 = hashlib.md5(b"request_id_xyz:0").hexdigest()[:16]

        reporte1 = _hacer_reporte(1, registros, batch_id_1)
        reporte2 = _hacer_reporte(1, registros, batch_id_2)

        self.receptor._procesar_resultado(reporte1)
        self.receptor._procesar_resultado(reporte2)

        escritas = self._contar_lineas_datos(1)
        self.assertEqual(escritas, 2,
                         f"Con batch_ids distintos deberia escribir 2, "
                         f"pero escribio {escritas}")

    def test_reentrega_real_con_mismo_batch_id_se_descarta(self):
        """Verifica que la deduplicacion sigue funcionando para
        re-entregas reales (mismo batch_id = mismo mensaje reenviado)."""
        registros = [
            {"From Bank": 100, "Account": "ABC",
             "To Bank": 200, "Account.1": "DEF",
             "Amount Paid": 5.0},
        ]

        batch_id = hashlib.md5(b"request_id_123:0").hexdigest()[:16]

        reporte = _hacer_reporte(1, registros, batch_id)

        self.receptor._procesar_resultado(reporte)
        self.receptor._procesar_resultado(reporte)

        escritas = self._contar_lineas_datos(1)
        self.assertEqual(escritas, 1,
                         f"Re-entrega con mismo batch_id deberia descartarse, "
                         f"pero escribio {escritas}")

    def test_tres_ocurrencias_identicas_todas_se_escriben(self):
        """Reproduce el caso exacto del bug: 3 transacciones identicas
        tras proyeccion (distintas fechas en el original) deben generar
        3 filas en el output."""
        registro = {
            "From Bank": 27880, "Account": "824242750",
            "To Bank": 27880, "Account.1": "824242750",
            "Amount Paid": 18.23,
        }

        for i in range(3):
            batch_id = hashlib.md5(f"msg_{i}:0".encode()).hexdigest()[:16]
            reporte = _hacer_reporte(1, [registro], batch_id)
            self.receptor._procesar_resultado(reporte)

        escritas = self._contar_lineas_datos(1)
        self.assertEqual(escritas, 3,
                         f"3 lotes con contenido identico pero batch_ids "
                         f"distintos deberian producir 3 filas, "
                         f"pero produjo {escritas}")


if __name__ == "__main__":
    unittest.main()
