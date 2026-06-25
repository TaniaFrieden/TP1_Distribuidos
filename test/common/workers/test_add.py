import json
import pytest
from unittest.mock import MagicMock, patch


BASE_ENV = {
    "MOM_HOST": "rabbitmq",
    "NODE_PREFIX": "test_counter",
    "ID": "1",
    "TOTAL_WORKERS": "1",
    "INPUT_QUEUES": '["q_test_in"]',
    "OUTPUT_QUEUES": '["q_test_out"]',
    "HEARTBEAT_INTERVAL_SECONDS": "0",
}


@pytest.fixture
def worker(tmp_path):
    with patch.dict("os.environ", BASE_ENV), \
         patch("common.middleware.MessageMiddlewareQueueRabbitMQ"), \
         patch("common.middleware.FanoutQueueRabbitMQ"), \
         patch("common.middleware.FanoutExchangeRabbitMQ"), \
         patch("persistencia.VOLUMEN_DIR", str(tmp_path)):
        from contador import WorkerContador
        w = WorkerContador()
    w._enviar = MagicMock()
    return w


def _msg(payload: dict) -> bytes:
    return json.dumps(payload).encode("utf-8")


class TestContar:

    def test_mensaje_simple_incrementa_conteo(self, worker):
        payload = {"client_id": "c1", "request_id": "r1"}
        ack = MagicMock()

        worker.procesar_payload("q_in", "c1", payload, _msg(payload), ack, MagicMock())

        assert worker.estado._conteos["c1"] == 1
        ack.assert_called_once()

    def test_multiples_mensajes_acumulan_conteo(self, worker):
        for i in range(4):
            payload = {"client_id": "c1", "request_id": f"r{i}"}
            worker.procesar_payload("q_in", "c1", payload, _msg(payload), MagicMock(), MagicMock())

        assert worker.estado._conteos["c1"] == 4

    def test_batch_usa_count_del_header(self, worker):
        payload = {
            "client_id": "c1",
            "request_id": "r1",
            "batches": [{"header": {"count": 7, "schema": []}, "payload": []}],
        }

        worker.procesar_payload("q_in", "c1", payload, _msg(payload), MagicMock(), MagicMock())

        assert worker.estado._conteos["c1"] == 7

    def test_multiples_batches_se_suman(self, worker):
        payload = {
            "client_id": "c1",
            "request_id": "r1",
            "batches": [
                {"header": {"count": 3, "schema": []}, "payload": []},
                {"header": {"count": 5, "schema": []}, "payload": []},
            ],
        }

        worker.procesar_payload("q_in", "c1", payload, _msg(payload), MagicMock(), MagicMock())

        assert worker.estado._conteos["c1"] == 8

    def test_clientes_distintos_conteos_independientes(self, worker):
        for cid in ["c1", "c2"]:
            payload = {"client_id": cid, "request_id": f"r_{cid}"}
            worker.procesar_payload("q_in", cid, payload, _msg(payload), MagicMock(), MagicMock())

        assert worker.estado._conteos["c1"] == 1
        assert worker.estado._conteos["c2"] == 1

    def test_excepcion_llama_nack(self, worker):
        nack = MagicMock()
        ack = MagicMock()

        worker.procesar_payload("q_in", "c1", None, b"invalido", ack, nack)

        nack.assert_called_once()
        ack.assert_not_called()


class TestFlush:

    def test_al_completar_cliente_emite_el_conteo(self, worker):
        payload = {"client_id": "c1", "request_id": "r1"}
        worker.procesar_payload("q_in", "c1", payload, _msg(payload), MagicMock(), MagicMock())

        worker.al_completar_cliente("c1")

        worker._enviar.assert_called_once()
        emitido = json.loads(worker._enviar.call_args[0][0])
        assert emitido["client_id"] == "c1"
        assert emitido["batches"][0]["payload"] == [[1]]

    def test_al_completar_cliente_emite_conteo_acumulado(self, worker):
        for i in range(5):
            payload = {"client_id": "c1", "request_id": f"r{i}"}
            worker.procesar_payload("q_in", "c1", payload, _msg(payload), MagicMock(), MagicMock())

        worker.al_completar_cliente("c1")

        emitido = json.loads(worker._enviar.call_args[0][0])
        assert emitido["batches"][0]["payload"] == [[5]]

    def test_al_completar_cliente_limpia_estado_interno(self, worker):
        payload = {"client_id": "c1", "request_id": "r1"}
        worker.procesar_payload("q_in", "c1", payload, _msg(payload), MagicMock(), MagicMock())

        worker.al_completar_cliente("c1")

        assert "c1" not in worker.estado._conteos

    def test_schema_del_output_es_count(self, worker):
        payload = {"client_id": "c1", "request_id": "r1"}
        worker.procesar_payload("q_in", "c1", payload, _msg(payload), MagicMock(), MagicMock())

        worker.al_completar_cliente("c1")

        emitido = json.loads(worker._enviar.call_args[0][0])
        assert emitido["batches"][0]["header"]["schema"] == ["count"]


class TestCicloDeVida:

    def test_al_cerrar_no_falla(self, worker):
        worker.al_cerrar()

    def test_al_desconectar_cliente_limpia_estado(self, worker):
        payload = {"client_id": "c1", "request_id": "r1"}
        worker.procesar_payload("q_in", "c1", payload, _msg(payload), MagicMock(), MagicMock())

        worker.al_desconectar_cliente("c1")

        assert "c1" not in worker.estado._conteos


class TestToleranciaCaidasBug:

    def test_caida_despues_de_persistir_causa_duplicacion(self, tmp_path):
        import os
        from unittest.mock import patch, MagicMock
        import json
        from contador import WorkerContador

        payload = {"client_id": "c1", "request_id": "r_123"}
        msg_bytes = json.dumps(payload).encode("utf-8")

        env_con_crash = {
            **BASE_ENV,
            "CRASH_HOOK": "CRASH_AFTER_PERSIST",
        }

        class SimularCrashException(Exception):
            pass

        def simulacion_exit(code):
            raise SimularCrashException("El worker crasheó simuladamente antes del ack")

        with patch.dict("os.environ", env_con_crash), \
             patch("common.middleware.MessageMiddlewareQueueRabbitMQ"), \
             patch("common.middleware.FanoutQueueRabbitMQ"), \
             patch("common.middleware.FanoutExchangeRabbitMQ"), \
             patch("persistencia.VOLUMEN_DIR", str(tmp_path)), \
             patch("common.crash_hook.VOLUMEN_DIR", str(tmp_path)), \
             patch("common.dedup_filter.VOLUMEN_DIR", str(tmp_path)), \
             patch("os._exit", side_effect=simulacion_exit):

            w1 = WorkerContador()
            w1._enviar = MagicMock()

            ack_mock = MagicMock()
            nack_mock = MagicMock()

            w1.procesar_payload("q_in", "c1", payload, msg_bytes, ack_mock, nack_mock)

            ack_mock.assert_not_called()
            nack_mock.assert_called_once()

            assert w1.estado._conteos["c1"] == 1

        with patch.dict("os.environ", BASE_ENV), \
             patch("common.middleware.MessageMiddlewareQueueRabbitMQ"), \
             patch("common.middleware.FanoutQueueRabbitMQ"), \
             patch("common.middleware.FanoutExchangeRabbitMQ"), \
             patch("persistencia.VOLUMEN_DIR", str(tmp_path)), \
             patch("common.dedup_filter.VOLUMEN_DIR", str(tmp_path)):

            w2 = WorkerContador()
            w2._enviar = MagicMock()

            assert w2.estado._conteos["c1"] == 1
            assert not w2.filtro_dedup.es_duplicado("c1", "r_123")

            ack_mock2 = MagicMock()
            nack_mock2 = MagicMock()
            w2.procesar_payload("q_in", "c1", payload, msg_bytes, ack_mock2, nack_mock2)

            ack_mock2.assert_called_once()

            print(f"\n[DEMO BUG] Conteo final después del crash y reentrega: {w2.estado._conteos['c1']} (Esperado con fix: 1)")
            assert w2.estado._conteos["c1"] == 1
