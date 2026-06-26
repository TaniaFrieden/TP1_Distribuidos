"""
Tests de persistencia para ContadorDistintoWorker (append-only JSONL)
"""
import json
import os
import pytest
from unittest.mock import MagicMock, patch
from common.persistencia import PersistidorAppendOnly
from base.constantes import CLAVE_BARRERA_COMPLETADA


BASE_ENV = {
    "MOM_HOST": "rabbitmq",
    "NODE_PREFIX": "q4_sumador",
    "ID": "1",
    "TOTAL_WORKERS": "1",
    "INPUT_QUEUES": '["q4_to_sumador_1"]',
    "OUTPUT_QUEUES": '["q4_scatter_edges_1"]',
    "HEARTBEAT_INTERVAL_SECONDS": "0",
    "GROUP_FIELDS": "From Bank,Account",
    "GROUP_OUTPUT_FIELDS": "from_bank,from_account",
    "VALUE_FIELDS": "To Bank,Account.1",
    "VALUE_OUTPUT_FIELDS": "to_bank,to_account",
    "EXPECTED_COUNT": "5",
    "COMPARISON_OPERATOR": "gt",
    "EMIT_MODE": "explode",
}


def _nombre_nodo(client_id):
    return f"gdc_q4_sumador_1_cliente_{client_id}"


def _escribir_ops(tmp_path, client_id, grupos, vistos, barrera=None):
    p = PersistidorAppendOnly(_nombre_nodo(client_id), base_dir=str(tmp_path))
    ops = []
    for gkey, vset in grupos.items():
        for v in vset:
            ops.append([list(gkey), list(v)])
    p.appendear({"o": ops, "i": list(vistos)})
    if barrera is not None:
        p.appendear({CLAVE_BARRERA_COMPLETADA: barrera})


def _crear_worker(tmp_path, extra_env=None):
    import workers.contador_distinto.contador_distinto as mod
    env = {**BASE_ENV, **(extra_env or {})}
    with patch.dict("os.environ", env), \
         patch("common.middleware.MessageMiddlewareQueueRabbitMQ"), \
         patch("common.middleware.FanoutQueueRabbitMQ"), \
         patch("common.middleware.FanoutExchangeRabbitMQ"), \
         patch.object(mod, "BASE_DIR", str(tmp_path)):
        w = mod.ContadorDistintoWorker()
    return w


class TestGDCRecovery:

    def test_carga_grupos_y_vistos_desde_disco(self, tmp_path):
        grupos = {("bank1", "acc1"): {("bank2", "acc2"), ("bank3", "acc3")}}
        _escribir_ops(tmp_path, "c1", grupos, {"r1"})
        w = _crear_worker(tmp_path)
        assert ("bank1", "acc1") in w.acumulador._grupos["c1"]
        assert ("bank2", "acc2") in w.acumulador._grupos["c1"][("bank1", "acc1")]
        assert w.acumulador._vistos["c1"] == {"r1"}

    def test_arranca_limpio_sin_estado_en_disco(self, tmp_path):
        w = _crear_worker(tmp_path)
        assert "c1" not in w.acumulador._grupos

    def test_multiples_clientes_se_recuperan_independientemente(self, tmp_path):
        _escribir_ops(tmp_path, "c1", {("b1", "a1"): {("b2", "a2")}}, set())
        _escribir_ops(tmp_path, "c2", {("b3", "a3"): {("b4", "a4")}}, {"x"})
        w = _crear_worker(tmp_path)
        assert ("b1", "a1") in w.acumulador._grupos["c1"]
        assert ("b3", "a3") in w.acumulador._grupos["c2"]
        assert w.acumulador._vistos["c2"] == {"x"}


class TestGDCBarrierCompletada:

    def test_estado_con_barrier_completada_no_se_carga_en_memoria(self, tmp_path):
        grupos = {("bank1", "acc1"): {("bank2", "acc2")}}
        _escribir_ops(tmp_path, "c1", grupos, {"r1"}, barrera=True)
        w = _crear_worker(tmp_path)
        assert "c1" not in w.acumulador._grupos
        assert "c1" not in w.acumulador._vistos

    def test_estado_con_barrier_completada_se_mantiene_en_disco(self, tmp_path):
        _escribir_ops(tmp_path, "c1", {}, set(), barrera=True)
        _crear_worker(tmp_path)
        filepath = tmp_path / f"{_nombre_nodo('c1')}.jsonl"
        assert filepath.exists()

    def test_estado_sin_barrier_completada_si_se_carga(self, tmp_path):
        _escribir_ops(tmp_path, "c1", {("b1", "a1"): {("b2", "a2")}}, set())
        w = _crear_worker(tmp_path)
        assert "c1" in w.acumulador._grupos


class TestGDCDedupPropio:

    def test_request_id_duplicado_no_agrega_al_grupo(self, tmp_path):
        grupos = {("bank1", "acc1"): {("bank2", "acc2")}}
        _escribir_ops(tmp_path, "c1", grupos, {"req-dup"})
        w = _crear_worker(tmp_path)

        ack = MagicMock()
        nack = MagicMock()
        payload = {
            "client_id": "c1",
            "request_id": "req-dup",
            "batches": [{
                "header": {"schema": ["From Bank", "Account", "To Bank", "Account.1"], "client_id": "c1", "count": 1},
                "payload": [["bank1", "acc1", "bank3", "acc3"]],
            }],
        }
        import workers.contador_distinto.contador_distinto as mod
        with patch.object(mod, "BASE_DIR", str(tmp_path)):
            w.procesar_payload("q4_to_sumador_1", "c1", payload, json.dumps(payload).encode(), ack, nack)

        assert len(w.acumulador._grupos["c1"][("bank1", "acc1")]) == 1
        ack.assert_called_once()
        nack.assert_not_called()

    def test_request_id_nuevo_agrega_al_grupo_y_persiste(self, tmp_path):
        grupos = {("bank1", "acc1"): {("bank2", "acc2")}}
        _escribir_ops(tmp_path, "c1", grupos, {"req-viejo"})
        w = _crear_worker(tmp_path)

        payload = {
            "client_id": "c1",
            "request_id": "req-nuevo",
            "batches": [{
                "header": {"schema": ["From Bank", "Account", "To Bank", "Account.1"], "client_id": "c1", "count": 1},
                "payload": [["bank1", "acc1", "bank3", "acc3"]],
            }],
        }
        import workers.contador_distinto.contador_distinto as mod
        with patch.object(mod, "BASE_DIR", str(tmp_path)):
            w.procesar_payload("q4_to_sumador_1", "c1", payload, json.dumps(payload).encode(), MagicMock(), MagicMock())

        assert ("bank3", "acc3") in w.acumulador._grupos["c1"][("bank1", "acc1")]
        assert "req-nuevo" in w.acumulador._vistos["c1"]
