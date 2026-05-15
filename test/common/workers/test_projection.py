"""
Tests para ProjectionWorker
============================
Cubren la proyección de campos, propagación de EOF y ciclo de vida.
"""
import json
import pytest
from unittest.mock import MagicMock, patch

from workers.projection.main import ProjectionWorker


# ------------------------------------------------------------------
# Fixture: worker con middleware mockeado
# ------------------------------------------------------------------

def _make_worker(campos="from_id,to_id,amount_paid", cola_entrada="entrada", cola_salida="salida"):
    env = {
        "RABBITMQ_HOST": "rabbitmq",
        "COLA_ENTRADA": cola_entrada,
        "COLA_SALIDA": cola_salida,
        "CAMPOS": campos,
    }
    middleware_entrada = MagicMock()
    middleware_salida = MagicMock()

    with patch.dict("os.environ", env):
        with patch(
            "workers.projection.main.DirectQueueRabbitMQ",
            side_effect=[middleware_salida, middleware_entrada],
        ):
            w = ProjectionWorker()
            w._middleware = w.inicializar_middleware()

    return w, middleware_salida


# ------------------------------------------------------------------
# Tests: proyección de campos
# ------------------------------------------------------------------

class TestProyeccion:

    def test_conserva_solo_los_campos_indicados(self):
        w, salida = _make_worker("from_id,to_id,amount_paid")
        mensaje = json.dumps({
            "client_id": 0,
            "from_id": "ABC",
            "to_id": "XYZ",
            "amount_paid": 12.5,
            "payment_currency": "USD",   # debe eliminarse
            "timestamp": "2023-09-01",   # debe eliminarse
        }).encode()

        w._callback_interno(mensaje, MagicMock(), MagicMock())

        enviado = json.loads(salida.send.call_args[0][0])
        assert set(enviado.keys()) == {"client_id", "from_id", "to_id", "amount_paid"}

    def test_siempre_conserva_client_id(self):
        """client_id debe estar aunque no figure en CAMPOS."""
        w, salida = _make_worker("from_id")
        mensaje = json.dumps({"client_id": 7, "from_id": "ABC", "amount_paid": 5}).encode()

        w._callback_interno(mensaje, MagicMock(), MagicMock())

        enviado = json.loads(salida.send.call_args[0][0])
        assert enviado["client_id"] == 7

    def test_campo_faltante_en_mensaje_se_omite_sin_error(self):
        """Si un campo de CAMPOS no existe en el mensaje, simplemente no se incluye."""
        w, salida = _make_worker("from_id,to_id,amount_paid")
        mensaje = json.dumps({"client_id": 0, "from_id": "ABC"}).encode()

        w._callback_interno(mensaje, MagicMock(), MagicMock())

        enviado = json.loads(salida.send.call_args[0][0])
        assert "to_id" not in enviado
        assert "amount_paid" not in enviado
        assert enviado["from_id"] == "ABC"

    def test_valores_se_conservan_correctamente(self):
        w, salida = _make_worker("from_id,amount_paid")
        mensaje = json.dumps({
            "client_id": 0, "from_id": "ABC123", "amount_paid": 49.99
        }).encode()

        w._callback_interno(mensaje, MagicMock(), MagicMock())

        enviado = json.loads(salida.send.call_args[0][0])
        assert enviado["from_id"] == "ABC123"
        assert enviado["amount_paid"] == 49.99

    def test_hace_ack_tras_proyectar(self):
        w, _ = _make_worker()
        ack = MagicMock()
        nack = MagicMock()
        mensaje = json.dumps({"client_id": 0, "from_id": "A", "to_id": "B", "amount_paid": 10}).encode()

        w._callback_interno(mensaje, ack, nack)

        ack.assert_called_once()
        nack.assert_not_called()


# ------------------------------------------------------------------
# Tests: propagación de EOF
# ------------------------------------------------------------------

class TestEOF:

    def test_eof_se_propaga_sin_modificacion(self):
        w, salida = _make_worker()
        eof = json.dumps({"client_id": 0}).encode()
        ack = MagicMock()

        w._callback_interno(eof, ack, MagicMock())

        salida.send.assert_called_once_with(eof)
        ack.assert_called_once()

    def test_eof_no_aplica_proyeccion(self):
        """El EOF no debe tocarse: no se le quitan ni agregan campos."""
        w, salida = _make_worker("from_id,to_id")
        eof = json.dumps({"client_id": 3}).encode()

        w._callback_interno(eof, MagicMock(), MagicMock())

        enviado = json.loads(salida.send.call_args[0][0])
        assert enviado == {"client_id": 3}


# ------------------------------------------------------------------
# Tests: ciclo de vida
# ------------------------------------------------------------------

class TestCicloDeVida:

    def test_al_cerrar_cierra_middleware_salida(self):
        w, salida = _make_worker()
        w.al_cerrar()
        salida.close.assert_called_once()

    def test_al_cerrar_sin_middleware_no_falla(self):
        env = {"COLA_ENTRADA": "e", "COLA_SALIDA": "s", "CAMPOS": "from_id"}
        with patch.dict("os.environ", env):
            with patch("workers.projection.main.DirectQueueRabbitMQ"):
                w = ProjectionWorker()
        w._salida = None
        w.al_cerrar()  # no debe lanzar