"""
Tests para AdderWorker
=======================
Cubren el reenvío de datos, la lógica de conteo de EOFs
y el comportamiento con múltiples productores y clientes.
"""
import json
import pytest
from unittest.mock import MagicMock, patch

from workers.adder.main import AdderWorker


# ------------------------------------------------------------------
# Fixture helpers
# ------------------------------------------------------------------

def _make_worker(n_productores=1):
    env = {
        "RABBITMQ_HOST": "rabbitmq",
        "COLA_ENTRADA": "entrada",
        "COLA_SALIDA": "salida",
        "N_PRODUCTORES": str(n_productores),
    }
    middleware_entrada = MagicMock()
    middleware_salida = MagicMock()

    with patch.dict("os.environ", env):
        with patch(
            "workers.adder.main.DirectQueueRabbitMQ",
            side_effect=[middleware_salida, middleware_entrada],
        ):
            w = AdderWorker()
            w._middleware = w.inicializar_middleware()

    return w, middleware_salida


def _dato(client_id=0, **kwargs):
    return json.dumps({"client_id": client_id, **kwargs}).encode()


def _eof(client_id=0):
    return json.dumps({"client_id": client_id}).encode()


# ------------------------------------------------------------------
# Tests: reenvío de mensajes de datos
# ------------------------------------------------------------------

class TestReenvio:

    def test_mensaje_de_datos_se_reenvía_sin_modificacion(self):
        w, salida = _make_worker()
        mensaje = _dato(from_id="ABC", amount_paid=30)

        w._callback_interno(mensaje, MagicMock(), MagicMock())

        salida.send.assert_called_once_with(mensaje)

    def test_multiples_datos_se_reenvian_todos(self):
        w, salida = _make_worker()
        mensajes = [_dato(amount_paid=i) for i in range(5)]

        for m in mensajes:
            w._callback_interno(m, MagicMock(), MagicMock())

        assert salida.send.call_count == 5

    def test_dato_hace_ack(self):
        w, _ = _make_worker()
        ack = MagicMock()
        w._callback_interno(_dato(), ack, MagicMock())
        ack.assert_called_once()


# ------------------------------------------------------------------
# Tests: lógica de EOF con un solo productor
# ------------------------------------------------------------------

class TestEOFUnProductor:

    def test_eof_se_propaga_inmediatamente_con_n1(self):
        w, salida = _make_worker(n_productores=1)
        w._callback_interno(_eof(), MagicMock(), MagicMock())

        salida.send.assert_called_once()
        enviado = json.loads(salida.send.call_args[0][0])
        assert enviado == {"client_id": 0}

    def test_eof_hace_ack(self):
        w, _ = _make_worker(n_productores=1)
        ack = MagicMock()
        w._callback_interno(_eof(), ack, MagicMock())
        ack.assert_called_once()


# ------------------------------------------------------------------
# Tests: lógica de EOF con múltiples productores
# ------------------------------------------------------------------

class TestEOFMultiplesProductores:

    def test_eof_no_se_propaga_hasta_recibir_todos(self):
        w, salida = _make_worker(n_productores=3)

        w._callback_interno(_eof(), MagicMock(), MagicMock())
        assert salida.send.call_count == 0  # todavía no

        w._callback_interno(_eof(), MagicMock(), MagicMock())
        assert salida.send.call_count == 0  # todavía no

    def test_eof_se_propaga_al_recibir_el_ultimo(self):
        w, salida = _make_worker(n_productores=3)

        for _ in range(3):
            w._callback_interno(_eof(), MagicMock(), MagicMock())

        salida.send.assert_called_once()
        enviado = json.loads(salida.send.call_args[0][0])
        assert enviado == {"client_id": 0}

    def test_contador_se_limpia_tras_propagar_eof(self):
        """Tras propagar el EOF, el contador se resetea para ese client_id."""
        w, _ = _make_worker(n_productores=2)

        w._callback_interno(_eof(), MagicMock(), MagicMock())
        w._callback_interno(_eof(), MagicMock(), MagicMock())

        assert 0 not in w._eofs_recibidos

    def test_datos_intercalados_con_eofs_se_procesan_correctamente(self):
        w, salida = _make_worker(n_productores=2)

        w._callback_interno(_dato(amount_paid=10), MagicMock(), MagicMock())
        w._callback_interno(_eof(), MagicMock(), MagicMock())          # EOF 1/2
        w._callback_interno(_dato(amount_paid=20), MagicMock(), MagicMock())
        w._callback_interno(_eof(), MagicMock(), MagicMock())          # EOF 2/2

        # 2 datos + 1 EOF propagado
        assert salida.send.call_count == 3
        eof_enviado = json.loads(salida.send.call_args_list[2][0][0])
        assert eof_enviado == {"client_id": 0}


# ------------------------------------------------------------------
# Tests: múltiples clientes simultáneos
# ------------------------------------------------------------------

class TestMultiplesClientes:

    def test_eofs_de_distintos_clientes_son_independientes(self):
        w, salida = _make_worker(n_productores=2)

        # cliente 0: 1 de 2 EOFs
        w._callback_interno(_eof(client_id=0), MagicMock(), MagicMock())
        assert salida.send.call_count == 0

        # cliente 1: 2 de 2 EOFs → debe propagarse solo el del cliente 1
        w._callback_interno(_eof(client_id=1), MagicMock(), MagicMock())
        w._callback_interno(_eof(client_id=1), MagicMock(), MagicMock())
        assert salida.send.call_count == 1
        enviado = json.loads(salida.send.call_args[0][0])
        assert enviado["client_id"] == 1

        # cliente 0: 2 de 2 EOFs → ahora sí se propaga
        w._callback_interno(_eof(client_id=0), MagicMock(), MagicMock())
        assert salida.send.call_count == 2

    def test_datos_de_distintos_clientes_se_reenvian_todos(self):
        w, salida = _make_worker(n_productores=1)

        w._callback_interno(_dato(client_id=0, amount_paid=10), MagicMock(), MagicMock())
        w._callback_interno(_dato(client_id=1, amount_paid=20), MagicMock(), MagicMock())

        assert salida.send.call_count == 2


# ------------------------------------------------------------------
# Tests: ciclo de vida
# ------------------------------------------------------------------

class TestCicloDeVida:

    def test_al_cerrar_cierra_middleware_salida(self):
        w, salida = _make_worker()
        w.al_cerrar()
        salida.close.assert_called_once()

    def test_al_cerrar_sin_middleware_no_falla(self):
        env = {"COLA_ENTRADA": "e", "COLA_SALIDA": "s", "N_PRODUCTORES": "1"}
        with patch.dict("os.environ", env):
            with patch("workers.adder.main.DirectQueueRabbitMQ"):
                w = AdderWorker()
        w._salida = None
        w.al_cerrar()  # no debe lanzar