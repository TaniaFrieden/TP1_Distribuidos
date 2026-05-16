import importlib
import json
import os
import sys
from unittest.mock import MagicMock, patch

import pytest

def _cargar_modulo_filter(filter_type: str):
    env = {
        "ID": "1",
        "NUM_SIBLINGS": "1",
        "MOM_HOST": "rabbitmq",
        "INPUT_QUEUE": "input_queue",
        "CONTROL_EXCHANGE": "control_exchange",
        "NODE_PREFIX": "node",
        "FILTER_TYPE": filter_type,
    }

    with patch.dict(os.environ, env, clear=False):
        base_module = "common.worker_base.base"
        filter_module = "workers.filter.main"

        if base_module in sys.modules:
            importlib.reload(sys.modules[base_module])
        else:
            importlib.import_module(base_module)

        if filter_module in sys.modules:
            return importlib.reload(sys.modules[filter_module])

        return importlib.import_module(filter_module)


def _crear_worker(filter_type: str):
    module = _cargar_modulo_filter(filter_type)

    input_queue = MagicMock()
    input_queue.start_consuming = MagicMock()
    input_queue.stop_consuming = MagicMock()
    input_queue.close = MagicMock()

    control_queue = MagicMock()
    control_queue.start_consuming = MagicMock()
    control_queue.stop_consuming = MagicMock()
    control_queue.close = MagicMock()

    control_exchange = MagicMock()
    control_exchange.close = MagicMock()

    with patch(
        "common.worker_base.base.middleware.MessageMiddlewareQueueRabbitMQ",
        side_effect=[input_queue, control_queue],
    ), patch(
        "common.worker_base.base.middleware.FanoutExchangeRabbitMQ",
        return_value=control_exchange,
    ):
        worker = module.FilterWorker()

    return worker, module, input_queue, control_queue, control_exchange


class TestInicializacion:

    def test_rechaza_filter_type_invalido(self):
        module = _cargar_modulo_filter("INVALIDO")

        with patch(
            "common.worker_base.base.middleware.MessageMiddlewareQueueRabbitMQ"
        ), patch(
            "common.worker_base.base.middleware.FanoutExchangeRabbitMQ"
        ):
            with pytest.raises(ValueError, match="no reconocido"):
                module.FilterWorker()


class TestProcesarMensaje:

    @pytest.mark.parametrize(
        "filter_type,payload,espera_envio",
        [
            ("USD", {"payment_currency": "USD"}, True),
            ("USD", {"payment_currency": "EUR"}, False),
            ("QUERY1", {"amount_paid": 30}, True),
            ("QUERY1", {"amount_paid": 60}, False),
        ],
    )
    def test_filtro_aplica_segun_tipo(self, filter_type, payload, espera_envio):
        worker, _, _, _, _ = _crear_worker(filter_type)
        ack = MagicMock()
        nack = MagicMock()
        mensaje = json.dumps(payload).encode("utf-8")

        with patch.object(worker, "_enviar", create=True) as enviar:
            worker.procesar_mensaje(mensaje, ack, nack)

        if espera_envio:
            enviar.assert_called_once_with(mensaje)
        else:
            enviar.assert_not_called()

        ack.assert_called_once()
        nack.assert_not_called()

    def test_json_invalido_se_descarta_con_ack(self):
        worker, _, _, _, _ = _crear_worker("USD")
        ack = MagicMock()
        nack = MagicMock()

        with patch.object(worker, "_enviar", create=True) as enviar:
            worker.procesar_mensaje(b"no-json", ack, nack)

        enviar.assert_not_called()
        ack.assert_called_once()
        nack.assert_not_called()

    def test_eof_se_acepta_sin_enviar(self):
        worker, _, _, _, _ = _crear_worker("USD")
        ack = MagicMock()
        nack = MagicMock()
        mensaje = json.dumps({"client_id": 7}).encode("utf-8")

        with patch.object(worker, "_enviar", create=True) as enviar:
            worker.procesar_mensaje(mensaje, ack, nack)

        enviar.assert_not_called()
        ack.assert_called_once()
        nack.assert_not_called()

    def test_al_cerrar_no_falla(self):
        worker, _, _, _, _ = _crear_worker("USD")
        worker.al_cerrar()