import time
import unittest
from unittest.mock import MagicMock, patch, call

from watchdog.ring_election import RingElection


def make_config(watchdog_id=1, num_watchdogs=3):
    config = MagicMock()
    config.watchdog_id = watchdog_id
    config.num_watchdogs = num_watchdogs
    config.mom_host = "localhost"
    config.leader_heartbeat_interval = 5.0
    config.leader_timeout_seconds = 20.0
    config.election_startup_delay_max = 3.0
    config.check_leader_interval = 5.0
    config.election_timeout = 30.0
    config.suspected_dead_ttl = 60.0
    return config


def make_election(watchdog_id=1, num_watchdogs=3):
    config = make_config(watchdog_id, num_watchdogs)
    on_become_leader = MagicMock()
    on_lose_leader = MagicMock()
    on_standby_dead = MagicMock()
    election = RingElection(config, on_become_leader, on_lose_leader, on_standby_dead)
    election._send_to = MagicMock()
    election._leader_hb_loop = MagicMock()  # evita conectar a RabbitMQ en tests
    return election, on_become_leader, on_lose_leader, on_standby_dead


class TestComputeNextTarget(unittest.TestCase):

    def test_devuelve_siguiente_directo_si_no_hay_muertos(self):
        election, *_ = make_election(watchdog_id=1)
        with election._lock:
            target = election._compute_next_target()
        self.assertEqual(target, 2)

    def test_saltea_nodo_sospechado(self):
        election, *_ = make_election(watchdog_id=1)
        election._suspected_dead_ids[2] = time.time()
        with election._lock:
            target = election._compute_next_target()
        self.assertEqual(target, 3)

    def test_saltea_multiples_nodos_sospechados(self):
        """Con nodos 2 y 3 muertos, se cae de vuelta en el propio nodo (self)."""
        election, *_ = make_election(watchdog_id=1)
        election._suspected_dead_ids[2] = time.time()
        election._suspected_dead_ids[3] = time.time()
        with election._lock:
            target = election._compute_next_target()
        self.assertEqual(target, 1)  # solo queda el propio nodo

    def test_no_saltea_nodo_con_ttl_expirado(self):
        """Un nodo sospechado hace más de suspected_dead_ttl segundos no se saltea."""
        election, *_ = make_election(watchdog_id=1)
        election._suspected_dead_ids[2] = time.time() - 61  # TTL = 60s
        with election._lock:
            target = election._compute_next_target()
        self.assertEqual(target, 2)


class TestHandleElection(unittest.TestCase):

    def test_propio_id_declara_lider(self):
        """Al recibir su propio ID de vuelta, el nodo se declara líder."""
        election, on_become_leader, *_ = make_election(watchdog_id=2)
        with patch.object(election, '_declare_leader') as mock_declare:
            election._handle_election(received_id=2, skip=[])
        mock_declare.assert_called_once()

    def test_id_mayor_se_reenvía_sin_cambio(self):
        """Al recibir un ID mayor, se reenvía ese ID."""
        election, *_ = make_election(watchdog_id=1)
        election._handle_election(received_id=3, skip=[])
        election._send_to.assert_called_once()
        _, payload = election._send_to.call_args[0]
        self.assertEqual(payload["id"], 3)

    def test_id_menor_se_reemplaza_por_propio(self):
        """Al recibir un ID menor, se reenvía el propio ID."""
        election, *_ = make_election(watchdog_id=3)
        election._handle_election(received_id=1, skip=[])
        election._send_to.assert_called_once()
        _, payload = election._send_to.call_args[0]
        self.assertEqual(payload["id"], 3)

    def test_lider_absorbe_mensaje(self):
        """Si el nodo es líder, absorbe el mensaje sin reenviarlo."""
        election, *_ = make_election(watchdog_id=2)
        election._is_leader = True
        election._handle_election(received_id=1, skip=[])
        election._send_to.assert_not_called()

    def test_lider_activo_conocido_absorbe_mensaje(self):
        """Si hay un líder activo con heartbeat reciente, absorbe el mensaje."""
        election, *_ = make_election(watchdog_id=1)
        election._leader_id = 3
        election._last_leader_hb = time.time()
        election._handle_election(received_id=2, skip=[])
        election._send_to.assert_not_called()

    def test_propagacion_de_skip(self):
        """Los nodos en el campo skip se agregan a _suspected_dead_ids."""
        election, *_ = make_election(watchdog_id=1)
        election._handle_election(received_id=3, skip=[2])
        self.assertIn(2, election._suspected_dead_ids)

    def test_skip_no_sobreescribe_timestamp_existente(self):
        """setdefault no pisa un timestamp ya conocido para un nodo sospechado."""
        election, *_ = make_election(watchdog_id=1)
        ts_original = time.time() - 10
        election._suspected_dead_ids[2] = ts_original
        election._handle_election(received_id=3, skip=[2])
        self.assertAlmostEqual(election._suspected_dead_ids[2], ts_original, places=1)


class TestDeclareLeader(unittest.TestCase):

    def test_llama_on_become_leader_con_nodos_caidos(self):
        election, on_become_leader, *_ = make_election(watchdog_id=3)
        election._suspected_dead_ids[1] = time.time()
        with patch("watchdog.ring_election.threading.Thread"):
            election._declare_leader()
        on_become_leader.assert_called_once()
        dead_ids = on_become_leader.call_args[0][0]
        self.assertIn(1, dead_ids)

    def test_idempotente_si_ya_es_lider(self):
        """Llamar _declare_leader dos veces no invoca on_become_leader dos veces."""
        election, on_become_leader, *_ = make_election(watchdog_id=3)
        with patch("watchdog.ring_election.threading.Thread"):
            election._declare_leader()
            election._declare_leader()
        on_become_leader.assert_called_once()

    def test_envia_coordinador_al_siguiente(self):
        election, *_ = make_election(watchdog_id=3)
        with patch("watchdog.ring_election.threading.Thread"):
            election._declare_leader()
        election._send_to.assert_called_once()
        _, payload = election._send_to.call_args[0]
        self.assertEqual(payload["tipo"], "coordinador")
        self.assertEqual(payload["id"], 3)

    def test_excluye_nodos_caidos_con_ttl_expirado(self):
        """Nodos sospechados fuera del TTL no se incluyen como dead_nodes."""
        election, on_become_leader, *_ = make_election(watchdog_id=3)
        election._suspected_dead_ids[1] = time.time() - 61  # TTL expirado
        with patch("watchdog.ring_election.threading.Thread"):
            election._declare_leader()
        dead_ids = on_become_leader.call_args[0][0]
        self.assertNotIn(1, dead_ids)


class TestHandleCoordinator(unittest.TestCase):

    def test_registra_nuevo_lider(self):
        election, *_ = make_election(watchdog_id=1)
        election._handle_coordinator(leader_id=3)
        self.assertEqual(election._leader_id, 3)
        self.assertFalse(election._is_leader)

    def test_reenvía_coordinador_si_no_lo_hizo(self):
        election, *_ = make_election(watchdog_id=1)
        election._handle_coordinator(leader_id=3)
        election._send_to.assert_called_once()
        _, payload = election._send_to.call_args[0]
        self.assertEqual(payload["tipo"], "coordinador")

    def test_no_reenvía_coordinador_duplicado(self):
        election, *_ = make_election(watchdog_id=1)
        election._handle_coordinator(leader_id=3)
        election._handle_coordinator(leader_id=3)
        self.assertEqual(election._send_to.call_count, 1)

    def test_cede_liderazgo_si_era_lider(self):
        election, _, on_lose_leader, _ = make_election(watchdog_id=2)
        election._is_leader = True
        election._handle_coordinator(leader_id=3)
        on_lose_leader.assert_called_once()

    def test_ignora_coordinador_con_propio_id_si_es_lider(self):
        """El coordinador con el propio ID llega cuando la vuelta está completa — no hace nada."""
        election, _, on_lose_leader, _ = make_election(watchdog_id=3)
        election._is_leader = True
        election._handle_coordinator(leader_id=3)
        on_lose_leader.assert_not_called()


class TestHandleAlive(unittest.TestCase):

    def test_elimina_de_sospechados(self):
        election, *_ = make_election(watchdog_id=1)
        election._suspected_dead_ids[2] = time.time()
        election._handle_alive(node_id=2)
        self.assertNotIn(2, election._suspected_dead_ids)

    def test_lider_actualiza_standby_last_seen(self):
        election, *_ = make_election(watchdog_id=3)
        election._is_leader = True
        election._reported_dead_standbys.add(1)
        election._handle_alive(node_id=1)
        self.assertIn(1, election._standby_last_seen)
        self.assertNotIn(1, election._reported_dead_standbys)


class TestTickLeaderTimeout(unittest.TestCase):

    def test_no_actua_si_es_lider(self):
        election, *_ = make_election(watchdog_id=1)
        election._is_leader = True
        with patch.object(election, '_initiate_election') as mock_init:
            election._tick_leader_timeout(startup_time=time.time())
        mock_init.assert_not_called()

    def test_no_actua_si_no_expiro_timeout(self):
        election, *_ = make_election(watchdog_id=1)
        election._last_leader_hb = time.time()  # heartbeat reciente
        with patch.object(election, '_initiate_election') as mock_init:
            election._tick_leader_timeout(startup_time=time.time())
        mock_init.assert_not_called()

    def test_inicia_eleccion_al_expirar_timeout(self):
        election, *_ = make_election(watchdog_id=1)
        election._last_leader_hb = time.time() - 25  # > leader_timeout_seconds (20)
        election._leader_id = 3
        with patch.object(election, '_initiate_election') as mock_init:
            election._tick_leader_timeout(startup_time=time.time() - 25)
        mock_init.assert_called_once()
        self.assertIn(3, election._suspected_dead_ids)

    def test_espera_si_eleccion_en_curso_sin_timeout(self):
        election, *_ = make_election(watchdog_id=1)
        election._last_leader_hb = time.time() - 25
        election._in_election = True
        election._election_started_at = time.time() - 5  # solo 5s < election_timeout (30)
        with patch.object(election, '_initiate_election') as mock_init:
            election._tick_leader_timeout(startup_time=time.time() - 25)
        mock_init.assert_not_called()

    def test_reintenta_si_eleccion_expiro(self):
        election, *_ = make_election(watchdog_id=1)
        election._last_leader_hb = time.time() - 25
        election._in_election = True
        election._election_started_at = time.time() - 35  # 35s > election_timeout (30)
        with patch.object(election, '_initiate_election') as mock_init:
            election._tick_leader_timeout(startup_time=time.time() - 25)
        mock_init.assert_called_once()


class TestTickStandbysCheck(unittest.TestCase):

    def test_no_actua_si_no_es_lider(self):
        election, _, _, on_standby_dead = make_election(watchdog_id=3)
        election._tick_standbys_check()
        on_standby_dead.assert_not_called()

    def test_no_actua_durante_periodo_de_gracia(self):
        election, _, _, on_standby_dead = make_election(watchdog_id=3)
        election._is_leader = True
        election._became_leader_at = time.time()  # recién elegido
        election._tick_standbys_check()
        on_standby_dead.assert_not_called()

    def test_detecta_standby_silencioso(self):
        election, _, _, on_standby_dead = make_election(watchdog_id=3)
        election._is_leader = True
        election._became_leader_at = time.time() - 25  # gracia superada
        # nodos 1 y 2 nunca mandaron HB (_standby_last_seen vacío)
        election._tick_standbys_check()
        calls = [c[0][0] for c in on_standby_dead.call_args_list]
        self.assertIn(1, calls)
        self.assertIn(2, calls)

    def test_no_reporta_standby_dos_veces(self):
        election, _, _, on_standby_dead = make_election(watchdog_id=3)
        election._is_leader = True
        election._became_leader_at = time.time() - 25
        election._tick_standbys_check()
        election._tick_standbys_check()
        # el segundo tick no debe llamar on_standby_dead de nuevo
        self.assertEqual(on_standby_dead.call_count, 2)  # solo 1 y 2, una vez cada uno

    def test_standby_vuelve_a_mandar_hb_sale_de_reportados(self):
        election, _, _, on_standby_dead = make_election(watchdog_id=3)
        election._is_leader = True
        election._became_leader_at = time.time() - 25
        election._tick_standbys_check()  # reporta 1 y 2
        election._handle_standby_hb(node_id=1)  # 1 vuelve a mandar HB
        election._tick_standbys_check()  # 1 ya no está en reported, podría detectarse de nuevo
        # pero ahora _standby_last_seen[1] es reciente → no se detecta como muerto
        total_calls_node_1 = sum(1 for c in on_standby_dead.call_args_list if c[0][0] == 1)
        self.assertEqual(total_calls_node_1, 1)


class TestInitiateElectionTimeout(unittest.TestCase):

    def test_timeout_agrega_ultimo_destino_a_sospechados(self):
        """Al reintentar una elección expirada, el último destino se marca como sospechoso."""
        election, *_ = make_election(watchdog_id=1)
        election._in_election = True
        election._election_started_at = time.time() - 35  # > election_timeout
        election._last_election_target = 2
        election._initiate_election()
        self.assertIn(2, election._suspected_dead_ids)

    def test_no_reintenta_si_eleccion_vigente(self):
        election, *_ = make_election(watchdog_id=1)
        election._in_election = True
        election._election_started_at = time.time() - 5  # < election_timeout
        election._initiate_election()
        election._send_to.assert_not_called()


if __name__ == "__main__":
    unittest.main()
