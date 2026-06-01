import json
from unittest.mock import MagicMock, patch

from workers.base.base import BaseWorker


class WorkerDePrueba(BaseWorker):
    def procesar_payload(
        self, queue_name: str, client_id: str, payload: dict, mensaje_original: bytes, ack, nack
    ):
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
        with patch("workers.base.base.MessageRouter"), \
             patch("workers.base.base.DistributedCoordinator"), \
             patch("workers.base.base.signal.signal"):
            return WorkerDePrueba()


def test_inicia_thread_heartbeat_como_daemon():
    worker = _crear_worker()

    with patch("workers.base.base.threading.Thread") as mock_thread_cls:
        mock_thread = MagicMock()
        mock_thread_cls.return_value = mock_thread

        worker._iniciar_heartbeat()

    mock_thread_cls.assert_called_once_with(
        target=worker._heartbeat_loop,
        name="WorkerDePrueba-heartbeat",
        daemon=True,
    )
    mock_thread.start.assert_called_once_with()


def test_heartbeat_publica_en_cola_de_etapa_con_payload_esperado():
    worker = _crear_worker()
    worker._heartbeat_stop_event = MagicMock()
    worker._heartbeat_stop_event.is_set.return_value = False
    worker._heartbeat_stop_event.wait.return_value = True

    heartbeat_queue = MagicMock()

    with patch(
        "workers.base.base.middleware.MessageMiddlewareQueueRabbitMQ",
        return_value=heartbeat_queue,
    ) as mock_queue_cls, patch("workers.base.base.time.time", return_value=123.0):
        worker._heartbeat_loop()

    mock_queue_cls.assert_called_once_with("rabbitmq", "heartbeat.q5_filter_period")
    heartbeat_queue.send.assert_called_once()
    worker._heartbeat_stop_event.wait.assert_called_once_with(5.0)
    heartbeat_queue.close.assert_called_once_with()

    payload = json.loads(heartbeat_queue.send.call_args[0][0].decode("utf-8"))
    assert payload == {
        "etapa": "q5_filter_period",
        "instancia": "02",
        "timestamp": 123.0,
    }
