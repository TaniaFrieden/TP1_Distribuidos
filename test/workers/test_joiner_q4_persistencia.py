"""
Tests de persistencia para JoinerQ4Worker
==========================================
Cubren:
  - Caso 1: recovery de scatter, txns y vistos desde disco al reiniciarse
  - Caso 4: dedup propio (_vistos) en ventana crash-antes-de-ack (crítico para
            _scatter que es no-idempotente: list.append vs set.add)
  - Caso 8: barrier_completada previene re-flush tras caída en al_completar_cliente
"""
import json
import os
import pytest
from unittest.mock import MagicMock, patch
from common.persistencia import PersistidorEstado


BASE_ENV = {
    "MOM_HOST": "rabbitmq",
    "NODE_PREFIX": "q4_joiner",
    "ID": "1",
    "TOTAL_WORKERS": "1",
    "INPUT_QUEUES": '["q4_scatter_edges_1", "q4_to_joiner_1"]',
    "OUTPUT_QUEUES": '["q4_paths_1"]',
    "HEARTBEAT_INTERVAL_SECONDS": "0",
}

# node_id=1 → nombre = "joiner_q4_1_{client_id}"
def _nombre_nodo(client_id):
    return f"joiner_q4_1_{client_id}"


def _escribir_estado(tmp_path, client_id, estado):
    PersistidorEstado(_nombre_nodo(client_id), base_dir=str(tmp_path)).guardar(estado)


def _crear_worker(tmp_path, extra_env=None):
    import workers.joiner_q4.joiner_q4 as mod
    env = {**BASE_ENV, **(extra_env or {})}
    with patch.dict("os.environ", env), \
         patch("common.middleware.MessageMiddlewareQueueRabbitMQ"), \
         patch("common.middleware.FanoutQueueRabbitMQ"), \
         patch("common.middleware.FanoutExchangeRabbitMQ"), \
         patch.object(mod, "BASE_DIR", str(tmp_path)):
        w = mod.JoinerQ4Worker()
    return w


# ──────────────────────────────────────────────────────────────────
# Caso 1 — Recovery de estado desde disco
# ──────────────────────────────────────────────────────────────────

class TestJoinerQ4Recovery:

    def test_carga_scatter_txns_y_vistos_desde_disco(self, tmp_path):
        estado = {
            "scatter": {"10|acc1": [["20", "acc2"], ["30", "acc3"]]},
            "txns":    {"10|acc1": [["40", "acc4"]]},
            "vistos":  ["r1"],
        }
        _escribir_estado(tmp_path, "c1", estado)
        w = _crear_worker(tmp_path)

        assert "10|acc1" in w._scatter["c1"]
        assert ("20", "acc2") in w._scatter["c1"]["10|acc1"]
        assert ("30", "acc3") in w._scatter["c1"]["10|acc1"]
        assert ("40", "acc4") in w._txns["c1"]["10|acc1"]
        assert w._vistos["c1"] == {"r1"}

    def test_arranca_limpio_sin_estado_en_disco(self, tmp_path):
        w = _crear_worker(tmp_path)
        assert "c1" not in w._scatter
        assert "c1" not in w._txns

    def test_txns_se_recuperan_como_set(self, tmp_path):
        """_txns debe ser un set de tuples, no una lista."""
        estado = {
            "scatter": {},
            "txns": {"bank|acc": [["b2", "a2"], ["b3", "a3"]]},
            "vistos": [],
        }
        _escribir_estado(tmp_path, "c1", estado)
        w = _crear_worker(tmp_path)
        assert isinstance(w._txns["c1"]["bank|acc"], set)

    def test_scatter_se_recupera_como_lista_de_tuples(self, tmp_path):
        """_scatter debe ser una lista de tuples, no de listas."""
        estado = {
            "scatter": {"bank|acc": [["b2", "a2"], ["b3", "a3"]]},
            "txns": {},
            "vistos": [],
        }
        _escribir_estado(tmp_path, "c1", estado)
        w = _crear_worker(tmp_path)
        elementos = w._scatter["c1"]["bank|acc"]
        assert all(isinstance(e, tuple) for e in elementos)

    def test_multiples_clientes_se_recuperan_independientemente(self, tmp_path):
        _escribir_estado(tmp_path, "c1", {"scatter": {"k1|v1": [["a", "b"]]}, "txns": {}, "vistos": []})
        _escribir_estado(tmp_path, "c2", {"scatter": {}, "txns": {"k2|v2": [["c", "d"]]}, "vistos": ["x"]})
        w = _crear_worker(tmp_path)
        assert "k1|v1" in w._scatter["c1"]
        assert "k2|v2" in w._txns["c2"]
        assert w._vistos["c2"] == {"x"}


# ──────────────────────────────────────────────────────────────────
# Caso 8 — barrier_completada previene re-flush
# ──────────────────────────────────────────────────────────────────

class TestJoinerQ4BarrierCompletada:

    def test_estado_con_barrier_completada_no_se_carga_en_memoria(self, tmp_path):
        estado = {
            "scatter": {"10|acc1": [["20", "acc2"]]},
            "txns": {"10|acc1": [["30", "acc3"]]},
            "vistos": ["r1"],
            "barrier_completada": True,
        }
        _escribir_estado(tmp_path, "c1", estado)
        w = _crear_worker(tmp_path)
        assert "c1" not in w._scatter
        assert "c1" not in w._txns
        assert "c1" not in w._vistos

    def test_estado_con_barrier_completada_se_borra_del_disco(self, tmp_path):
        _escribir_estado(tmp_path, "c1", {"scatter": {}, "txns": {}, "vistos": [], "barrier_completada": True})
        _crear_worker(tmp_path)
        filepath = tmp_path / _nombre_nodo("c1") / "estado.json"
        assert not filepath.exists()

    def test_estado_sin_barrier_completada_si_se_carga(self, tmp_path):
        estado = {
            "scatter": {"10|acc1": [["20", "acc2"]]},
            "txns": {},
            "vistos": [],
            "barrier_completada": False,
        }
        _escribir_estado(tmp_path, "c1", estado)
        w = _crear_worker(tmp_path)
        assert "c1" in w._scatter


# ──────────────────────────────────────────────────────────────────
# Caso 4 — _vistos evita entradas duplicadas en _scatter (no-idempotente)
# ──────────────────────────────────────────────────────────────────

class TestJoinerQ4DedupPropio:

    def test_request_id_duplicado_no_agrega_a_scatter(self, tmp_path):
        """
        _scatter usa list.append (no-idempotente). Sin _vistos, un mensaje
        reentregado agregaría la misma arista dos veces → caminos duplicados.
        """
        estado = {
            "scatter": {"10|acc1": [["20", "acc2"]]},
            "txns": {},
            "vistos": ["req-dup"],
        }
        _escribir_estado(tmp_path, "c1", estado)
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

        # La lista sigue con solo 1 elemento (no se duplicó)
        assert len(w._scatter["c1"]["10|acc1"]) == 1
        ack.assert_called_once()
        nack.assert_not_called()

    def test_request_id_nuevo_agrega_a_scatter_y_persiste(self, tmp_path):
        estado = {
            "scatter": {"10|acc1": [["20", "acc2"]]},
            "txns": {},
            "vistos": ["req-viejo"],
        }
        _escribir_estado(tmp_path, "c1", estado)
        w = _crear_worker(tmp_path)

        payload = {
            "client_id": "c1",
            "request_id": "req-nuevo",
            "batches": [{
                "header": {
                    "schema": ["from_bank", "from_account", "to_bank", "to_account"],
                    "client_id": "c1", "count": 1,
                },
                "payload": [["30", "acc3", "10", "acc1"]],
            }],
        }
        import workers.joiner_q4.joiner_q4 as mod
        with patch.object(mod, "BASE_DIR", str(tmp_path)):
            w.procesar_payload("q4_scatter_edges_1", "c1", payload, json.dumps(payload).encode(), MagicMock(), MagicMock())

        assert ("30", "acc3") in w._scatter["c1"]["10|acc1"]
        assert "req-nuevo" in w._vistos["c1"]

    def test_txns_son_idempotentes_pero_vistos_igual_protege(self, tmp_path):
        """
        _txns usa set.add (idempotente), pero _vistos igualmente evita el
        re-procesamiento para mantener la semántica at-most-once del pipeline.
        """
        estado = {
            "scatter": {},
            "txns": {"10|acc1": [["20", "acc2"]]},
            "vistos": ["req-dup"],
        }
        _escribir_estado(tmp_path, "c1", estado)
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

        # Set sigue con solo 1 elemento
        assert len(w._txns["c1"]["10|acc1"]) == 1
        ack.assert_called_once()
