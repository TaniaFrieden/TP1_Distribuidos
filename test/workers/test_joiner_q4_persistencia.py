"""
Tests de persistencia para WorkerJoinerQ4 (append-only JSONL)
"""
import json
import pytest
from unittest.mock import MagicMock, patch
from common.persistencia import PersistidorAppendOnly
from base.constantes import CLAVE_BARRERA_COMPLETADA
from workers.joiner_q4.constantes import CLAVE_SCATTER, CLAVE_TXNS


BASE_ENV = {
    "MOM_HOST": "rabbitmq",
    "NODE_PREFIX": "q4_joiner",
    "ID": "1",
    "TOTAL_WORKERS": "1",
    "INPUT_QUEUES": '["q4_scatter_edges_1", "q4_to_joiner_1"]',
    "OUTPUT_QUEUES": '["q4_paths_1"]',
    "HEARTBEAT_INTERVAL_SECONDS": "0",
}


def _nombre_nodo(client_id):
    return f"joiner_q4_1_cliente_{client_id}"


def _escribir_ops(tmp_path, client_id, scatter_items, txn_items, vistos, barrera=None):
    p = PersistidorAppendOnly(_nombre_nodo(client_id), base_dir=str(tmp_path))
    entrada = {
        CLAVE_SCATTER: [[k, list(v)] for k, vals in scatter_items.items() for v in vals],
        CLAVE_TXNS: [[k, list(v)] for k, vals in txn_items.items() for v in vals],
        "i": list(vistos),
    }
    p.appendear(entrada)
    if barrera is not None:
        p.appendear({CLAVE_BARRERA_COMPLETADA: barrera})


def _crear_worker(tmp_path, extra_env=None):
    import workers.joiner_q4.joiner_q4 as mod
    env = {**BASE_ENV, **(extra_env or {})}
    with patch.dict("os.environ", env), \
         patch("common.middleware.MessageMiddlewareQueueRabbitMQ"), \
         patch("common.middleware.FanoutQueueRabbitMQ"), \
         patch("common.middleware.FanoutExchangeRabbitMQ"), \
         patch.object(mod, "BASE_DIR", str(tmp_path)):
        w = mod.WorkerJoinerQ4()
    return w


class TestJoinerQ4Recovery:

    def test_carga_scatter_txns_y_vistos_desde_disco(self, tmp_path):
        scatter = {"10|acc1": [("20", "acc2"), ("30", "acc3")]}
        txns = {"10|acc1": [("40", "acc4")]}
        _escribir_ops(tmp_path, "c1", scatter, txns, {"r1"})
        w = _crear_worker(tmp_path)

        assert "10|acc1" in w.acumulador._scatter["c1"]
        assert ("20", "acc2") in w.acumulador._scatter["c1"]["10|acc1"]
        assert ("30", "acc3") in w.acumulador._scatter["c1"]["10|acc1"]
        assert ("40", "acc4") in w.acumulador._txns["c1"]["10|acc1"]
        assert w.acumulador._vistos["c1"] == {"r1"}

    def test_arranca_limpio_sin_estado_en_disco(self, tmp_path):
        w = _crear_worker(tmp_path)
        assert "c1" not in w.acumulador._scatter
        assert "c1" not in w.acumulador._txns

    def test_txns_se_recuperan_como_set(self, tmp_path):
        txns = {"bank|acc": [("b2", "a2"), ("b3", "a3")]}
        _escribir_ops(tmp_path, "c1", {}, txns, set())
        w = _crear_worker(tmp_path)
        assert isinstance(w.acumulador._txns["c1"]["bank|acc"], set)

    def test_scatter_se_recupera_como_lista_de_tuples(self, tmp_path):
        scatter = {"bank|acc": [("b2", "a2"), ("b3", "a3")]}
        _escribir_ops(tmp_path, "c1", scatter, {}, set())
        w = _crear_worker(tmp_path)
        elementos = w.acumulador._scatter["c1"]["bank|acc"]
        assert all(isinstance(e, tuple) for e in elementos)

    def test_multiples_clientes_se_recuperan_independientemente(self, tmp_path):
        _escribir_ops(tmp_path, "c1", {"k1|v1": [("a", "b")]}, {}, set())
        _escribir_ops(tmp_path, "c2", {}, {"k2|v2": [("c", "d")]}, {"x"})
        w = _crear_worker(tmp_path)
        assert "k1|v1" in w.acumulador._scatter["c1"]
        assert "k2|v2" in w.acumulador._txns["c2"]
        assert w.acumulador._vistos["c2"] == {"x"}


class TestJoinerQ4BarrierCompletada:

    def test_estado_con_barrier_completada_no_se_carga_en_memoria(self, tmp_path):
        scatter = {"10|acc1": [("20", "acc2")]}
        txns = {"10|acc1": [("30", "acc3")]}
        _escribir_ops(tmp_path, "c1", scatter, txns, {"r1"}, barrera=True)
        w = _crear_worker(tmp_path)
        assert "c1" not in w.acumulador._scatter
        assert "c1" not in w.acumulador._txns
        assert "c1" not in w.acumulador._vistos

    def test_estado_con_barrier_completada_se_mantiene_en_disco(self, tmp_path):
        _escribir_ops(tmp_path, "c1", {}, {}, set(), barrera=True)
        _crear_worker(tmp_path)
        filepath = tmp_path / f"{_nombre_nodo('c1')}.jsonl"
        assert filepath.exists()

    def test_estado_sin_barrier_completada_si_se_carga(self, tmp_path):
        scatter = {"10|acc1": [("20", "acc2")]}
        _escribir_ops(tmp_path, "c1", scatter, {}, set())
        w = _crear_worker(tmp_path)
        assert "c1" in w.acumulador._scatter


class TestJoinerQ4DedupPropio:

    def test_request_id_duplicado_no_agrega_a_scatter(self, tmp_path):
        scatter = {"10|acc1": [("20", "acc2")]}
        _escribir_ops(tmp_path, "c1", scatter, {}, {"req-dup"})
        w = _crear_worker(tmp_path)

        ack = MagicMock()
        nack = MagicMock()
        payload = {
            "client_id": "c1",
            "request_id": "req-dup",
            "batches": [{
                "header": {
                    "schema": ["from_bank", "from_account", "to_bank", "to_account"],
                    "client_id": "c1", "count": 1,
                },
                "payload": [["20", "acc2", "10", "acc1"]],
            }],
        }
        import workers.joiner_q4.joiner_q4 as mod
        with patch.object(mod, "BASE_DIR", str(tmp_path)):
            w.procesar_payload("q4_scatter_edges_1", "c1", payload, json.dumps(payload).encode(), ack, nack)

        assert len(w.acumulador._scatter["c1"]["10|acc1"]) == 1
        ack.assert_called_once()
        nack.assert_not_called()

    def test_txns_son_idempotentes_pero_vistos_igual_protege(self, tmp_path):
        txns = {"10|acc1": [("20", "acc2")]}
        _escribir_ops(tmp_path, "c1", {}, txns, {"req-dup"})
        w = _crear_worker(tmp_path)

        ack = MagicMock()
        payload = {
            "client_id": "c1",
            "request_id": "req-dup",
            "batches": [{
                "header": {
                    "schema": ["From Bank", "Account", "To Bank", "Account.1"],
                    "client_id": "c1", "count": 1,
                },
                "payload": [["10", "acc1", "30", "acc3"]],
            }],
        }
        import workers.joiner_q4.joiner_q4 as mod
        with patch.object(mod, "BASE_DIR", str(tmp_path)):
            w.procesar_payload("q4_to_joiner_1", "c1", payload, json.dumps(payload).encode(), ack, MagicMock())

        assert len(w.acumulador._txns["c1"]["10|acc1"]) == 1
        ack.assert_called_once()
