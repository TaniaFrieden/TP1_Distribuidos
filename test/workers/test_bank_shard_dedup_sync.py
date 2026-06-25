"""
Tests para el bug de DedupFilter adelantado al estado del agregador.

Escenario del bug:
  1. Mensaje A procesado por el agregador (max actualizado en memoria,
     request_id en _ids_procesados, ack en _acks_pendientes).
  2. Mismo mensaje A re-entregado por RabbitMQ.
  3. Agregador lo detecta como "duplicado propio" y llama ack() directamente,
     lo que marca el DedupFilter (y potencialmente lo persiste a disco).
  4. CRASH antes del flush del agregador.
  5. En recovery: DedupFilter tiene el request_id -> descarta el mensaje.
     Pero datos_bancos NO tiene la contribucion del mensaje -> dato perdido.

El fix: _sincronizar_dedup_con_estado() elimina del DedupFilter los IDs
que no estan en _ids_procesados del agregador al recuperar estado.
"""
import json
import os
import pytest
from unittest.mock import MagicMock, patch, call
from common.persistencia import PersistidorEstado
from common.dedup_filter import DedupFilter

from constantes import (
    CLAVE_TX_EOF_COUNT, CLAVE_BANK_EOF_COUNT, CLAVE_EOF_MENSAJE,
    CLAVE_FLUSH_INICIADO, CLAVE_BARRERA_COMPLETADA,
    CLAVE_BANCOS, CLAVE_MONTO_MAXIMO, CLAVE_NOMBRE_BANCO, CLAVE_CUENTAS,
    COLA_TRANSACCIONES,
)

BASE_ENV = {
    "MOM_HOST": "rabbitmq",
    "NODE_PREFIX": "q2_agregador_shard",
    "ID": "1",
    "TOTAL_WORKERS": "1",
    "INPUT_QUEUES": '["q2_transactions_1", "q2_banks_1"]',
    "OUTPUT_QUEUES": '["q2_results"]',
    "HEARTBEAT_INTERVAL_SECONDS": "0",
    "CRASH_PRE_BARRERA": "false",
    "TOTAL_TX_UPSTREAM": "1",
    "TOTAL_BANK_UPSTREAM": "1",
}

NODE_PREFIX = "bank_shard_1"
DEDUP_PREFIX = "dedup_q2_agregador_shard_1"


def _escribir_estado(tmp_path, client_id, estado):
    PersistidorEstado(f"{NODE_PREFIX}_{client_id}", base_dir=str(tmp_path)).guardar(estado)


def _escribir_dedup(tmp_path, data: dict):
    PersistidorEstado(DEDUP_PREFIX, base_dir=str(tmp_path)).guardar(data)


def _crear_worker(tmp_path, extra_env=None):
    import agregador_bancario as mod
    import config_agregador
    env = {**BASE_ENV, **(extra_env or {})}
    base = str(tmp_path)

    def patched_config_init(self, nid):
        self.base_dir = base
        self.prefijo_nodo = NODE_PREFIX
        self.total_tx_upstream = int(env.get("TOTAL_TX_UPSTREAM", "1"))
        self.total_bank_upstream = int(env.get("TOTAL_BANK_UPSTREAM", "1"))

    original_dedup_init = DedupFilter.__init__

    def patched_dedup_init(self, node_name, base_dir=None):
        original_dedup_init(self, node_name, base_dir=base)

    with patch.dict("os.environ", env), \
         patch("common.middleware.MessageMiddlewareQueueRabbitMQ"), \
         patch("common.middleware.FanoutQueueRabbitMQ"), \
         patch("common.middleware.FanoutExchangeRabbitMQ"), \
         patch.object(config_agregador.ConfigAgregador, "__init__", patched_config_init), \
         patch.object(DedupFilter, "__init__", patched_dedup_init):
        w = mod.AgregadorBancarioWorker()
    return w


def _payload_transaccion(client_id, request_id, bank_id, account, amount):
    return {
        "client_id": client_id,
        "request_id": request_id,
        "batches": [{
            "header": {
                "schema": ["From Bank", "Account", "Amount Paid"],
                "client_id": client_id,
                "count": 1,
            },
            "payload": [[bank_id, account, amount]],
        }],
    }


# ──────────────────────────────────────────────────────────────────
# Reproduccion del bug: DedupFilter adelantado causa perdida de datos
# ──────────────────────────────────────────────────────────────────

class TestDedupFilterAdelantadoBug:
    """Reproduce el escenario exacto del bug."""

    def test_dedup_adelantado_sin_fix_pierde_dato(self, tmp_path):
        """
        Simula: el agregador proceso un mensaje (351M) pero no persistio.
        El DedupFilter SI tiene el request_id (por ack directo de duplicado).
        Sin el fix, al recuperar, DedupFilter bloquearia el reprocesamiento
        y el max quedaria en el valor viejo.

        Con el fix (_sincronizar_dedup_con_estado), el ID extra se elimina
        del DedupFilter, permitiendo el reprocesamiento.
        """
        client_id = "c1"
        req_351m = "c1:session:raw_data:204:s1"

        _escribir_estado(tmp_path, client_id, {
            "bancos": {
                "3202": {
                    "bank_name": "Plateau Credit Union",
                    "max_amount": 238875044.36,
                    "accounts": ["804853B90"],
                }
            },
            "ids_procesados": ["r1", "r2", "r3"],
            "tx_eof_count": 0,
            "bank_eof_count": 0,
            "mensaje_eof_hex": None,
            "flush_iniciado": False,
            "barrera_completada": False,
        })

        _escribir_dedup(tmp_path, {
            client_id: ["r1", "r2", "r3", req_351m],
        })

        w = _crear_worker(tmp_path)

        assert req_351m not in w.filtro_dedup._seen.get(client_id, set()), \
            "El fix debe eliminar IDs del DedupFilter que no estan en ids_procesados"

        assert req_351m not in w._ids_procesados.get(client_id, set())

    def test_ids_validos_no_se_eliminan_del_dedup(self, tmp_path):
        """Los IDs que SI estan en ids_procesados no deben eliminarse."""
        client_id = "c1"

        _escribir_estado(tmp_path, client_id, {
            "bancos": {"3202": {"bank_name": "Test", "max_amount": 100.0, "accounts": []}},
            "ids_procesados": ["r1", "r2", "r3"],
            "tx_eof_count": 0,
            "bank_eof_count": 0,
            "mensaje_eof_hex": None,
            "flush_iniciado": False,
            "barrera_completada": False,
        })

        _escribir_dedup(tmp_path, {
            client_id: ["r1", "r2"],
        })

        w = _crear_worker(tmp_path)

        dedup_ids = w.filtro_dedup._seen.get(client_id, set())
        assert "r1" in dedup_ids
        assert "r2" in dedup_ids

    def test_multiples_clientes_sync_independiente(self, tmp_path):
        """La sincronizacion es por cliente, no afecta a otros."""
        _escribir_estado(tmp_path, "c1", {
            "bancos": {},
            "ids_procesados": ["r1"],
            "tx_eof_count": 0,
            "bank_eof_count": 0,
            "mensaje_eof_hex": None,
            "flush_iniciado": False,
            "barrera_completada": False,
        })
        _escribir_estado(tmp_path, "c2", {
            "bancos": {},
            "ids_procesados": ["r10"],
            "tx_eof_count": 0,
            "bank_eof_count": 0,
            "mensaje_eof_hex": None,
            "flush_iniciado": False,
            "barrera_completada": False,
        })

        _escribir_dedup(tmp_path, {
            "c1": ["r1", "r_extra_c1"],
            "c2": ["r10", "r_extra_c2"],
        })

        w = _crear_worker(tmp_path)

        assert "r_extra_c1" not in w.filtro_dedup._seen.get("c1", set())
        assert "r_extra_c2" not in w.filtro_dedup._seen.get("c2", set())
        assert "r1" in w.filtro_dedup._seen.get("c1", set())
        assert "r10" in w.filtro_dedup._seen.get("c2", set())


# ──────────────────────────────────────────────────────────────────
# Simulacion end-to-end del ciclo proceso -> dup -> crash -> recovery
# ──────────────────────────────────────────────────────────────────

class TestCicloCompletoDupCrashRecovery:
    """
    Simula el ciclo completo:
    1. Procesar mensaje con max alto (sin flush)
    2. Recibir duplicado -> ack directo marca DedupFilter
    3. Simular crash (no flush del agregador)
    4. Crear nuevo worker (recovery)
    5. Verificar que el mensaje puede reprocesarse
    """

    def test_mensaje_reprocesable_tras_crash_con_duplicado(self, tmp_path):
        client_id = "c1"
        req_id = "c1:session:batch:100:s1"
        bank_id = "3202"

        _escribir_estado(tmp_path, client_id, {
            "bancos": {
                bank_id: {
                    "bank_name": "Plateau Credit Union",
                    "max_amount": 50.0,
                    "accounts": ["old_acc"],
                }
            },
            "ids_procesados": [],
            "tx_eof_count": 0,
            "bank_eof_count": 0,
            "mensaje_eof_hex": None,
            "flush_iniciado": False,
            "barrera_completada": False,
        })

        w1 = _crear_worker(tmp_path)

        payload = _payload_transaccion(client_id, req_id, bank_id, "80CADECA0", 351026858.97)
        msg = json.dumps(payload).encode()
        ack1 = MagicMock()
        w1.procesar_payload("q2_transactions_1", client_id, payload, msg, ack1, MagicMock())

        assert w1._datos_bancos[client_id][bank_id]["max_amount"] == 351026858.97
        assert req_id in w1._ids_procesados[client_id]
        ack1.assert_not_called()

        ack2 = MagicMock()
        w1.procesar_payload("q2_transactions_1", client_id, payload, msg, ack2, MagicMock())
        ack2.assert_called_once()

        w1.filtro_dedup._seen.setdefault(client_id, set()).add(req_id)
        w1.filtro_dedup._persistir()

        estado_pre_crash = w1._persistencia._persistidor(client_id).cargar()
        assert req_id not in estado_pre_crash.get("ids_procesados", []), \
            "El agregador NO persistio el estado (simula crash antes de flush)"

        w2 = _crear_worker(tmp_path)

        assert req_id not in w2.filtro_dedup._seen.get(client_id, set()), \
            "El fix debe limpiar el ID extra del DedupFilter en recovery"

        assert w2._datos_bancos[client_id][bank_id]["max_amount"] == 50.0, \
            "El estado recuperado tiene el max viejo (pre-crash)"

        ack3 = MagicMock()
        w2.procesar_payload("q2_transactions_1", client_id, payload, msg, ack3, MagicMock())

        assert w2._datos_bancos[client_id][bank_id]["max_amount"] == 351026858.97, \
            "Tras reprocesar, el max debe ser el correcto (351M)"
        assert req_id in w2._ids_procesados[client_id]

    def test_sin_dedup_adelantado_no_hay_cambios(self, tmp_path):
        """Si el DedupFilter no esta adelantado, la sync no cambia nada."""
        client_id = "c1"

        _escribir_estado(tmp_path, client_id, {
            "bancos": {"b1": {"bank_name": "Test", "max_amount": 100.0, "accounts": []}},
            "ids_procesados": ["r1", "r2", "r3"],
            "tx_eof_count": 0,
            "bank_eof_count": 0,
            "mensaje_eof_hex": None,
            "flush_iniciado": False,
            "barrera_completada": False,
        })

        _escribir_dedup(tmp_path, {
            client_id: ["r1", "r2"],
        })

        w = _crear_worker(tmp_path)

        dedup_ids = w.filtro_dedup._seen.get(client_id, set())
        assert dedup_ids == {"r1", "r2"}

    def test_cliente_sin_estado_no_afecta_su_dedup(self, tmp_path):
        """
        Si un cliente tiene entradas en DedupFilter pero no tiene estado
        en el agregador (ya completo), la sync no lo toca.
        """
        _escribir_estado(tmp_path, "c_activo", {
            "bancos": {},
            "ids_procesados": ["r1"],
            "tx_eof_count": 0,
            "bank_eof_count": 0,
            "mensaje_eof_hex": None,
            "flush_iniciado": False,
            "barrera_completada": False,
        })

        _escribir_dedup(tmp_path, {
            "c_activo": ["r1"],
            "c_terminado": ["old1", "old2"],
        })

        w = _crear_worker(tmp_path)

        assert w.filtro_dedup._seen.get("c_terminado", set()) == {"old1", "old2"}, \
            "Clientes no activos no deben ser modificados por la sync"


# ──────────────────────────────────────────────────────────────────
# Persistencia del fix: el DedupFilter corregido se guarda a disco
# ──────────────────────────────────────────────────────────────────

class TestPersistenciaDedupSync:

    def test_dedup_corregido_se_persiste_a_disco(self, tmp_path):
        """Verificar que el DedupFilter limpiado se guarda a disco."""
        client_id = "c1"
        id_extra = "extra_req"

        _escribir_estado(tmp_path, client_id, {
            "bancos": {},
            "ids_procesados": ["r1"],
            "tx_eof_count": 0,
            "bank_eof_count": 0,
            "mensaje_eof_hex": None,
            "flush_iniciado": False,
            "barrera_completada": False,
        })

        _escribir_dedup(tmp_path, {
            client_id: ["r1", id_extra],
        })

        _crear_worker(tmp_path)

        dedup_disco = PersistidorEstado(DEDUP_PREFIX, base_dir=str(tmp_path)).cargar()
        ids_en_disco = set(dedup_disco.get(client_id, []))
        assert id_extra not in ids_en_disco, \
            "El ID extra debe haberse eliminado del disco"
        assert "r1" in ids_en_disco, \
            "Los IDs validos deben permanecer en disco"

    def test_sin_cambios_no_reescribe_dedup(self, tmp_path):
        """Si no hay IDs extra, no se llama a _persistir."""
        client_id = "c1"

        _escribir_estado(tmp_path, client_id, {
            "bancos": {},
            "ids_procesados": ["r1", "r2"],
            "tx_eof_count": 0,
            "bank_eof_count": 0,
            "mensaje_eof_hex": None,
            "flush_iniciado": False,
            "barrera_completada": False,
        })

        _escribir_dedup(tmp_path, {
            client_id: ["r1"],
        })

        w = _crear_worker(tmp_path)
        dedup_ids = w.filtro_dedup._seen.get(client_id, set())
        assert dedup_ids == {"r1"}
