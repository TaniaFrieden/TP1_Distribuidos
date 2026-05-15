"""
Tests para AggregatorWorker
============================
Cubren los tres modos (collect, count, max), manejo de EOF,
múltiples clientes y ciclo de vida.
"""
import json
import pytest
from unittest.mock import MagicMock, patch

from workers.aggregator.main import AggregatorWorker


# ------------------------------------------------------------------
# Fixture helpers
# ------------------------------------------------------------------

def _make_worker(modo="collect", campo_max="", query_id="1"):
    env = {
        "RABBITMQ_HOST": "rabbitmq",
        "COLA_ENTRADA": "entrada",
        "COLA_SALIDA": "reportes",
        "QUERY_ID": query_id,
        "MODO": modo,
        "CAMPO_MAX": campo_max,
    }
    middleware_entrada = MagicMock()
    middleware_salida = MagicMock()

    with patch.dict("os.environ", env):
        with patch(
            "workers.aggregator.main.DirectQueueRabbitMQ",
            side_effect=[middleware_salida, middleware_entrada],
        ):
            w = AggregatorWorker()
            w._middleware = w.inicializar_middleware()

    return w, middleware_salida


def _dato(client_id=0, **kwargs):
    # Un dato siempre tiene al menos un campo de negocio además de client_id.
    # Sin kwargs ponemos un campo dummy para no confundirse con un EOF {"client_id": x}.
    if not kwargs:
        kwargs = {"_dummy": True}
    return json.dumps({"client_id": client_id, **kwargs}).encode()


def _eof(client_id=0):
    return json.dumps({"client_id": client_id}).encode()


# ------------------------------------------------------------------
# Tests: modo collect (Q1, Q3, Q4)
# ------------------------------------------------------------------


class TestModoCollect:

    def test_acumula_mensajes_y_emite_lista_al_eof(self):
        w, salida = _make_worker("collect")

        w._callback_interno(_dato(from_id="A", to_id="B", amount_paid=10), MagicMock(), MagicMock())
        w._callback_interno(_dato(from_id="C", to_id="D", amount_paid=20), MagicMock(), MagicMock())
        w._callback_interno(_eof(), MagicMock(), MagicMock())

        resultado = json.loads(salida.send.call_args_list[0][0][0])
        assert resultado[0] == 0
        assert len(resultado[1]) == 2
        assert resultado[1][0]["from_id"] == "A"
        assert resultado[1][1]["from_id"] == "C"

    def test_emite_eof_despues_del_resultado(self):
        w, salida = _make_worker("collect")
        w._callback_interno(_eof(), MagicMock(), MagicMock())

        assert salida.send.call_count == 2
        eof_enviado = json.loads(salida.send.call_args_list[1][0][0])
        assert eof_enviado == {"client_id": 0}

    def test_sin_datos_emite_lista_vacia(self):
        w, salida = _make_worker("collect")
        w._callback_interno(_eof(), MagicMock(), MagicMock())

        resultado = json.loads(salida.send.call_args_list[0][0][0])
        assert resultado[1] == []

    def test_hace_ack_en_mensajes_de_datos(self):
        w, _ = _make_worker("collect")
        ack = MagicMock()
        w._callback_interno(_dato(amount_paid=5), ack, MagicMock())
        ack.assert_called_once()

    def test_hace_ack_en_eof(self):
        w, _ = _make_worker("collect")
        ack = MagicMock()
        w._callback_interno(_eof(), ack, MagicMock())
        ack.assert_called_once()


# ------------------------------------------------------------------
# Tests: modo count (Q5)
# ------------------------------------------------------------------


class TestModoCount:

    def test_cuenta_mensajes_correctamente(self):
        w, salida = _make_worker("count")

        for _ in range(5):
            w._callback_interno(_dato(amount_paid=1), MagicMock(), MagicMock())
        w._callback_interno(_eof(), MagicMock(), MagicMock())

        resultado = json.loads(salida.send.call_args_list[0][0][0])
        assert resultado[0] == 0
        assert resultado[1] == 5

    def test_sin_mensajes_cuenta_cero(self):
        w, salida = _make_worker("count")
        w._callback_interno(_eof(), MagicMock(), MagicMock())

        resultado = json.loads(salida.send.call_args_list[0][0][0])
        assert resultado[1] == 0


# ------------------------------------------------------------------
# Tests: modo max (Q2)
# ------------------------------------------------------------------


class TestModoMax:

    def test_emite_el_registro_con_mayor_valor(self):
        w, salida = _make_worker("max", campo_max="amount_paid")

        w._callback_interno(_dato(amount_paid=10, from_id="A"), MagicMock(), MagicMock())
        w._callback_interno(_dato(amount_paid=99, from_id="B"), MagicMock(), MagicMock())
        w._callback_interno(_dato(amount_paid=50, from_id="C"), MagicMock(), MagicMock())
        w._callback_interno(_eof(), MagicMock(), MagicMock())

        resultado = json.loads(salida.send.call_args_list[0][0][0])
        assert resultado[1][0]["from_id"] == "B"
        assert resultado[1][0]["amount_paid"] == 99

    def test_un_solo_registro_es_el_maximo(self):
        w, salida = _make_worker("max", campo_max="amount_paid")
        w._callback_interno(_dato(amount_paid=42, from_id="X"), MagicMock(), MagicMock())
        w._callback_interno(_eof(), MagicMock(), MagicMock())

        resultado = json.loads(salida.send.call_args_list[0][0][0])
        assert len(resultado[1]) == 1
        assert resultado[1][0]["amount_paid"] == 42

    def test_sin_datos_emite_lista_vacia(self):
        w, salida = _make_worker("max", campo_max="amount_paid")
        w._callback_interno(_eof(), MagicMock(), MagicMock())

        resultado = json.loads(salida.send.call_args_list[0][0][0])
        assert resultado[1] == []


# ------------------------------------------------------------------
# Tests: múltiples clientes simultáneos
# ------------------------------------------------------------------


class TestMultiplesClientes:

    def test_estado_separado_por_client_id(self):
        w, salida = _make_worker("collect")

        w._callback_interno(_dato(client_id=0, from_id="A"), MagicMock(), MagicMock())
        w._callback_interno(_dato(client_id=1, from_id="B"), MagicMock(), MagicMock())
        w._callback_interno(_dato(client_id=0, from_id="C"), MagicMock(), MagicMock())
        w._callback_interno(_eof(client_id=0), MagicMock(), MagicMock())

        resultado = json.loads(salida.send.call_args_list[0][0][0])
        assert resultado[0] == 0
        assert len(resultado[1]) == 2  # solo A y C, no B

    def test_estado_client1_no_afecta_client0(self):
        w, salida = _make_worker("count")

        for _ in range(3):
            w._callback_interno(_dato(client_id=0, amount_paid=1), MagicMock(), MagicMock())
        for _ in range(7):
            w._callback_interno(_dato(client_id=1, amount_paid=1), MagicMock(), MagicMock())

        w._callback_interno(_eof(client_id=0), MagicMock(), MagicMock())

        resultado = json.loads(salida.send.call_args_list[0][0][0])
        assert resultado[0] == 0
        assert resultado[1] == 3  # solo los del client 0

    def test_estado_se_limpia_tras_eof(self):
        w, _ = _make_worker("collect")
        w._callback_interno(_dato(client_id=0, from_id="A"), MagicMock(), MagicMock())
        w._callback_interno(_eof(client_id=0), MagicMock(), MagicMock())

        assert 0 not in w._estado


# ------------------------------------------------------------------
# Tests: validaciones de configuración
# ------------------------------------------------------------------


class TestConfiguracion:

    def test_modo_invalido_lanza_error(self):
        env = {
            "COLA_ENTRADA": "e", "COLA_SALIDA": "s",
            "QUERY_ID": "1", "MODO": "inventado",
        }
        with patch.dict("os.environ", env):
            with patch("workers.aggregator.main.DirectQueueRabbitMQ"):
                with pytest.raises(ValueError, match="MODO invalido"):
                    AggregatorWorker()

    def test_modo_max_sin_campo_max_lanza_error(self):
        env = {
            "COLA_ENTRADA": "e", "COLA_SALIDA": "s",
            "QUERY_ID": "1", "MODO": "max", "CAMPO_MAX": "",
        }
        with patch.dict("os.environ", env):
            with patch("workers.aggregator.main.DirectQueueRabbitMQ"):
                with pytest.raises(ValueError, match="CAMPO_MAX"):
                    AggregatorWorker()


# ------------------------------------------------------------------
# Tests: ciclo de vida
# ------------------------------------------------------------------


class TestCicloDeVida:

    def test_al_cerrar_cierra_middleware_salida(self):
        w, salida = _make_worker()
        w.al_cerrar()
        salida.close.assert_called_once()

    def test_al_cerrar_sin_middleware_no_falla(self):
        env = {
            "COLA_ENTRADA": "e", "COLA_SALIDA": "s",
            "QUERY_ID": "1", "MODO": "collect",
        }
        with patch.dict("os.environ", env):
            with patch("workers.aggregator.main.DirectQueueRabbitMQ"):
                w = AggregatorWorker()
        w._salida = None
        w.al_cerrar()  # no debe lanzar