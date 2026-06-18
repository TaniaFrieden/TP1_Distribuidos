import json
from unittest.mock import MagicMock, patch

from workers.base.worker_base import WorkerBase


class WorkerDePrueba(WorkerBase):
    def procesar_payload(self, nombre_cola, client_id, payload, mensaje_original, ack, nack):
        ack()

    def al_cerrar(self):
        pass


def _crear_worker():
    env = {
        "MOM_HOST": "rabbitmq",
        "NODE_PREFIX": "q5_filter_period",
        "ID": "2",
        "TOTAL_WORKERS": "1",
        "INPUT_QUEUES": '["q_in"]',
        "OUTPUT_QUEUES": '["q_out"]',
        "HEARTBEAT_INTERVAL_SECONDS": "5",
    }

    with patch.dict("os.environ", env):
        with patch("workers.base.worker_base.EnrutadorMensajes"), \
             patch("workers.base.worker_base.CoordinadorDistribuido"), \
             patch("workers.base.worker_base.signal.signal"):
            return WorkerDePrueba()


def test_inicia_thread_heartbeat_como_daemon():
    worker = _crear_worker()

    with patch("workers.base.latido.threading.Thread") as mock_thread_cls:
        mock_thread = MagicMock()
        mock_thread_cls.return_value = mock_thread

        worker._latido.iniciar()

    mock_thread_cls.assert_called_once_with(
        target=worker._latido._bucle,
        name="WorkerDePrueba-heartbeat",
        daemon=True,
    )
    mock_thread.start.assert_called_once_with()


def test_heartbeat_publica_en_cola_de_etapa_con_payload_esperado():
    worker = _crear_worker()
    worker._latido._evento_cierre = MagicMock()
    worker._latido._evento_cierre.is_set.return_value = False
    worker._latido._evento_cierre.wait.return_value = True

    heartbeat_queue = MagicMock()

    with patch(
        "workers.base.latido.middleware.MessageMiddlewareQueueRabbitMQ",
        return_value=heartbeat_queue,
    ) as mock_queue_cls, patch("workers.base.latido.time.time", return_value=123.0):
        worker._latido._bucle()

    mock_queue_cls.assert_called_once_with("rabbitmq", "heartbeat.q5_filter_period")
    heartbeat_queue.send.assert_called_once()
    worker._latido._evento_cierre.wait.assert_called_once_with(5.0)
    heartbeat_queue.close.assert_called_once_with()

    payload = json.loads(heartbeat_queue.send.call_args[0][0].decode("utf-8"))
    assert payload == {
        "etapa": "q5_filter_period",
        "instancia": "02",
        "timestamp": 123.0,
    }
