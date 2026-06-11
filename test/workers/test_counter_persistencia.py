"""
Tests de persistencia para CounterWorker
=========================================
Cubren:
  - Caso 1: recovery del estado desde disco al reiniciarse
  - Caso 4: dedup propio (_vistos) en ventana crash-entre-persist-y-ack
  - Caso 8: barrier_completada previene re-flush tras caída en al_completar_cliente
"""
import json
import os
import pytest
from unittest.mock import MagicMock, patch
from common.persistencia import PersistidorEstado


BASE_ENV = {
    "MOM_HOST": "rabbitmq",
    "NODE_PREFIX": "q5_counter",
    "ID": "1",
    "TOTAL_WORKERS": "1",
    "INPUT_QUEUES": '["q5_in"]',
    "OUTPUT_QUEUES": '["q5_results"]',
    "HEARTBEAT_INTERVAL_SECONDS": "0",
    "CRASH_AFTER_PERSIST": "false",
}


def _escribir_estado(tmp_path, client_id, estado):
    PersistidorEstado(f"counter_1_{client_id}", base_dir=str(tmp_path)).guardar(estado)


def _crear_worker(tmp_path, extra_env=None):
    import workers.counter.counter as mod
    env = {**BASE_ENV, **(extra_env or {})}
    with patch.dict("os.environ", env), \
         patch("common.middleware.MessageMiddlewareQueueRabbitMQ"), \
         patch("common.middleware.FanoutQueueRabbitMQ"), \
         patch("common.middleware.FanoutExchangeRabbitMQ"), \
         patch.object(mod, "BASE_DIR", str(tmp_path)):
        w = mod.CounterWorker()
    return w


# ──────────────────────────────────────────────────────────────────
# Caso 1 — Recovery de estado desde disco
# ──────────────────────────────────────────────────────────────────

class TestCounterRecovery:

    def test_carga_count_y_vistos_desde_disco(self, tmp_path):
        _escribir_estado(tmp_path, "c1", {"count": 42, "vistos": ["r1", "r2"]})
        w = _crear_worker(tmp_path)
        assert w._conteos["c1"] == 42
        assert w._vistos["c1"] == {"r1", "r2"}

    def test_arranca_limpio_sin_estado_en_disco(self, tmp_path):
        w = _crear_worker(tmp_path)
        assert "c1" not in w._conteos
        assert "c1" not in w._vistos

    def test_multiples_clientes_se_recuperan_independientemente(self, tmp_path):
        _escribir_estado(tmp_path, "c1", {"count": 10, "vistos": []})
        _escribir_estado(tmp_path, "c2", {"count": 20, "vistos": ["x"]})
        w = _crear_worker(tmp_path)
        assert w._conteos["c1"] == 10
        assert w._conteos["c2"] == 20
        assert w._vistos["c2"] == {"x"}

    def test_carpeta_con_estado_vacio_se_ignora(self, tmp_path):
        # Carpeta existe pero sin estado.json válido
        os.makedirs(tmp_path / "counter_1_c1", exist_ok=True)
        w = _crear_worker(tmp_path)
        assert "c1" not in w._conteos


# ──────────────────────────────────────────────────────────────────
# Caso 8 — barrier_completada previene re-flush
# ──────────────────────────────────────────────────────────────────

class TestCounterBarrierCompletada:

    def test_estado_con_barrier_completada_no_se_carga_en_memoria(self, tmp_path):
        _escribir_estado(tmp_path, "c1", {"count": 100, "vistos": ["r1"], "barrier_completada": True})
        w = _crear_worker(tmp_path)
        assert "c1" not in w._conteos
        assert "c1" not in w._vistos

    def test_estado_con_barrier_completada_se_borra_del_disco(self, tmp_path):
        _escribir_estado(tmp_path, "c1", {"count": 100, "vistos": [], "barrier_completada": True})
        _crear_worker(tmp_path)
        filepath = tmp_path / "counter_1_c1" / "estado.json"
        assert not filepath.exists()

    def test_estado_sin_barrier_completada_si_se_carga(self, tmp_path):
        _escribir_estado(tmp_path, "c1", {"count": 7, "vistos": [], "barrier_completada": False})
        w = _crear_worker(tmp_path)
        assert w._conteos["c1"] == 7


# ──────────────────────────────────────────────────────────────────
# Caso 4 — _vistos evita doble conteo en ventana crash-antes-de-ack
# ──────────────────────────────────────────────────────────────────

class TestCounterDedupPropio:

    def test_request_id_ya_en_vistos_no_incrementa_conteo(self, tmp_path):
        _escribir_estado(tmp_path, "c1", {"count": 5, "vistos": ["req-dup"]})
        w = _crear_worker(tmp_path)

        ack = MagicMock()
        nack = MagicMock()
        payload = {
            "client_id": "c1",
            "request_id": "req-dup",
            "batches": [{"header": {"schema": [], "client_id": "c1", "count": 3}, "payload": [[], [], []]}],
        }
        import workers.counter.counter as mod
        with patch.object(mod, "BASE_DIR", str(tmp_path)):
            w.procesar_payload("q5_in", "c1", payload, json.dumps(payload).encode(), ack, nack)

        assert w._conteos["c1"] == 5  # no cambió
        ack.assert_called_once()
        nack.assert_not_called()

    def test_request_id_nuevo_incrementa_conteo_y_lo_persiste(self, tmp_path):
        _escribir_estado(tmp_path, "c1", {"count": 5, "vistos": ["req-viejo"]})
        w = _crear_worker(tmp_path)

        payload = {
            "client_id": "c1",
            "request_id": "req-nuevo",
            "batches": [{"header": {"schema": [], "client_id": "c1", "count": 3}, "payload": [[], [], []]}],
        }
        import workers.counter.counter as mod
        with patch.object(mod, "BASE_DIR", str(tmp_path)):
            w.procesar_payload("q5_in", "c1", payload, json.dumps(payload).encode(), MagicMock(), MagicMock())

        assert w._conteos["c1"] == 8  # 5 + 3
        assert "req-nuevo" in w._vistos["c1"]
        # verificar que se persistió en disco
        estado = PersistidorEstado("counter_1_c1", base_dir=str(tmp_path)).cargar()
        assert estado["count"] == 8
        assert "req-nuevo" in estado["vistos"]
