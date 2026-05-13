"""
Tests para FilterWorker
========================
Cubren el parseo de condiciones, la lógica de filtrado y el ciclo de vida
del worker (integración con BaseWorker).

"""
import json
import pytest
from unittest.mock import MagicMock, patch
from datetime import datetime

from workers.filter.filter_worker import FilterWorker, parsear_filtros


# ------------------------------------------------------------------
# Tests: parsear_filtros
# ------------------------------------------------------------------

class TestParsearFiltros:

    def test_filtro_numerico_lt(self):
        condiciones = parsear_filtros("amount_paid:lt:50")
        assert len(condiciones) == 1
        assert condiciones[0] == {"campo": "amount_paid", "operador": "lt", "valor": 50}

    def test_filtro_float(self):
        condiciones = parsear_filtros("amount_paid:lt:49.99")
        assert condiciones[0]["valor"] == 49.99

    def test_filtro_fecha(self):
        condiciones = parsear_filtros("timestamp:gte:2023-09-01")
        assert condiciones[0]["valor"] == datetime(2023, 9, 1)

    def test_filtro_string_eq(self):
        condiciones = parsear_filtros("payment_format:eq:Wire")
        assert condiciones[0] == {"campo": "payment_format", "operador": "eq", "valor": "Wire"}

    def test_multiples_condiciones(self):
        condiciones = parsear_filtros("timestamp:gte:2023-09-01,timestamp:lte:2023-09-05")
        assert len(condiciones) == 2
        assert condiciones[0]["operador"] == "gte"
        assert condiciones[1]["operador"] == "lte"

    def test_filtros_vacio_retorna_lista_vacia(self):
        assert parsear_filtros("") == []
        assert parsear_filtros("   ") == []

    def test_operador_invalido_lanza_error(self):
        with pytest.raises(ValueError, match="Operador desconocido"):
            parsear_filtros("amount_paid:mayor:50")

    def test_formato_invalido_lanza_error(self):
        with pytest.raises(ValueError, match="Condición mal formada"):
            parsear_filtros("amount_paid_sin_operador")

    def test_valor_con_dos_puntos_en_timestamp(self):
        """Timestamps con hora contienen ':', el parseo no debe romperse."""
        condiciones = parsear_filtros("timestamp:gte:2023-09-01 10:30:00")
        assert condiciones[0]["valor"] == datetime(2023, 9, 1, 10, 30, 0)


# ------------------------------------------------------------------
# Fixture: worker con middleware mockeado
# ------------------------------------------------------------------

@pytest.fixture
def middleware_entrada():
    mock = MagicMock()
    mock.start_consuming = MagicMock()
    mock.stop_consuming = MagicMock()
    mock.close = MagicMock()
    return mock


@pytest.fixture
def middleware_salida():
    mock = MagicMock()
    mock.send = MagicMock()
    mock.close = MagicMock()
    return mock


@pytest.fixture
def worker(middleware_entrada, middleware_salida):
    """Crea un FilterWorker con middlewares mockeados."""
    env = {
        "RABBITMQ_HOST": "rabbitmq",
        "COLA_ENTRADA": "transacciones_usd",
        "COLA_SALIDA": "reporte_q1",
        "FILTROS": "amount_paid:lt:50",
    }
    with patch.dict("os.environ", env):
        with patch(
            "workers.filter.filter_worker.DirectQueueRabbitMQ",
            side_effect=[middleware_salida, middleware_entrada],
        ):
            w = FilterWorker()
            # inicializar_middleware() no se llama en el constructor sino en iniciar(),
            # así que lo llamamos manualmente para que _middleware_salida quede seteado
            w._middleware = w.inicializar_middleware()
    return w


# ------------------------------------------------------------------
# Tests: lógica de filtrado (_cumple_condiciones)
# ------------------------------------------------------------------

class TestCumpleCondiciones:

    def _worker_con_filtros(self, filtros_str):
        env = {
            "COLA_ENTRADA": "entrada",
            "COLA_SALIDA": "salida",
            "FILTROS": filtros_str,
        }
        with patch.dict("os.environ", env):
            with patch("workers.filter.filter_worker.DirectQueueRabbitMQ"):
                return FilterWorker()

    # Numéricos
    def test_lt_pasa_cuando_valor_menor(self):
        w = self._worker_con_filtros("amount_paid:lt:50")
        assert w._cumple_condiciones({"amount_paid": 30}) is True

    def test_lt_descarta_cuando_valor_igual(self):
        w = self._worker_con_filtros("amount_paid:lt:50")
        assert w._cumple_condiciones({"amount_paid": 50}) is False

    def test_lte_pasa_cuando_valor_igual(self):
        w = self._worker_con_filtros("amount_paid:lte:50")
        assert w._cumple_condiciones({"amount_paid": 50}) is True

    def test_gt_descarta_cuando_valor_menor(self):
        w = self._worker_con_filtros("amount_paid:gt:50")
        assert w._cumple_condiciones({"amount_paid": 30}) is False

    def test_gte_pasa_cuando_valor_igual(self):
        w = self._worker_con_filtros("amount_paid:gte:50")
        assert w._cumple_condiciones({"amount_paid": 50}) is True

    # Strings
    def test_eq_string_pasa(self):
        w = self._worker_con_filtros("payment_format:eq:Wire")
        assert w._cumple_condiciones({"payment_format": "Wire"}) is True

    def test_eq_string_descarta(self):
        w = self._worker_con_filtros("payment_format:eq:Wire")
        assert w._cumple_condiciones({"payment_format": "ACH"}) is False

    def test_neq_string_pasa(self):
        w = self._worker_con_filtros("payment_currency:neq:USD")
        assert w._cumple_condiciones({"payment_currency": "EUR"}) is True

    def test_contiene_pasa(self):
        w = self._worker_con_filtros("payment_format:contiene:Wire")
        assert w._cumple_condiciones({"payment_format": "Wire Transfer"}) is True

    def test_no_contiene_pasa(self):
        w = self._worker_con_filtros("payment_format:no_contiene:Wire")
        assert w._cumple_condiciones({"payment_format": "ACH"}) is True

    # Fechas
    def test_fecha_gte_pasa(self):
        w = self._worker_con_filtros("timestamp:gte:2023-09-01")
        assert w._cumple_condiciones({"timestamp": "2023-09-05"}) is True

    def test_fecha_gte_descarta(self):
        w = self._worker_con_filtros("timestamp:gte:2023-09-01")
        assert w._cumple_condiciones({"timestamp": "2023-08-31"}) is False

    def test_rango_de_fechas(self):
        w = self._worker_con_filtros("timestamp:gte:2023-09-01,timestamp:lte:2023-09-05")
        assert w._cumple_condiciones({"timestamp": "2023-09-03"}) is True
        assert w._cumple_condiciones({"timestamp": "2023-09-06"}) is False

    # Casos borde
    def test_campo_faltante_descarta(self):
        w = self._worker_con_filtros("amount_paid:lt:50")
        assert w._cumple_condiciones({"otro_campo": 10}) is False

    def test_sin_condiciones_pasa_todo(self):
        w = self._worker_con_filtros("")
        assert w._cumple_condiciones({"cualquier_campo": "cualquier_valor"}) is True

    def test_multiples_condiciones_and(self):
        """Todas las condiciones deben cumplirse (AND)."""
        w = self._worker_con_filtros("amount_paid:lt:50,payment_currency:eq:USD")
        assert w._cumple_condiciones({"amount_paid": 30, "payment_currency": "USD"}) is True
        assert w._cumple_condiciones({"amount_paid": 30, "payment_currency": "EUR"}) is False
        assert w._cumple_condiciones({"amount_paid": 60, "payment_currency": "USD"}) is False


# ------------------------------------------------------------------
# Tests: procesar_mensaje
# ------------------------------------------------------------------

class TestProcesarMensaje:

    def test_mensaje_que_pasa_se_publica_y_hace_ack(self, worker, middleware_salida):
        mensaje = json.dumps({"amount_paid": 30}).encode()
        ack = MagicMock()
        nack = MagicMock()

        worker._callback_interno(mensaje, ack, nack)

        middleware_salida.send.assert_called_once_with(mensaje)
        ack.assert_called_once()
        nack.assert_not_called()

    def test_mensaje_descartado_no_se_publica_pero_hace_ack(self, worker, middleware_salida):
        mensaje = json.dumps({"amount_paid": 100}).encode()
        ack = MagicMock()
        nack = MagicMock()

        worker._callback_interno(mensaje, ack, nack)

        middleware_salida.send.assert_not_called()
        ack.assert_called_once()
        nack.assert_not_called()

    def test_al_cerrar_cierra_middleware_salida(self, worker, middleware_salida):
        worker.al_cerrar()
        middleware_salida.close.assert_called_once()