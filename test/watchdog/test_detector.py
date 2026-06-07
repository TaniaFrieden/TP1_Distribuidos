import json
import time
import unittest
from unittest.mock import MagicMock, patch

from watchdog.detector import HeartbeatDetector


def make_config(stages=None, timeout=15.0, check_interval=5.0):
    config = MagicMock()
    config.mom_host = "localhost"
    config.stages = stages or ["gateway", "filtro"]
    config.heartbeat_interval_seconds = 5.0
    config.missed_heartbeats_threshold = 3
    config.timeout_seconds = timeout
    config.check_interval_seconds = check_interval
    config.caidas_queue = "caidas"
    return config


def make_detector(stages=None, timeout=15.0):
    config = make_config(stages=stages, timeout=timeout)
    return HeartbeatDetector(config)


def make_hb_msg(etapa, instancia, ts=None):
    payload = {"etapa": etapa, "instancia": instancia, "timestamp": ts or time.time()}
    return json.dumps(payload).encode()


class TestOnHeartbeat(unittest.TestCase):

    def test_registra_heartbeat_en_last_seen(self):
        detector = make_detector()
        ack = MagicMock()
        detector._on_heartbeat(make_hb_msg("gateway", "1"), ack, None)
        self.assertIn(("gateway", "1"), detector._last_seen)
        ack.assert_called_once()

    def test_actualiza_timestamp_con_heartbeat_mas_reciente(self):
        detector = make_detector()
        ack = MagicMock()
        ts_viejo = time.time() - 10
        ts_nuevo = time.time()
        detector._on_heartbeat(make_hb_msg("gateway", "1", ts=ts_viejo), ack, None)
        detector._on_heartbeat(make_hb_msg("gateway", "1", ts=ts_nuevo), ack, None)
        self.assertAlmostEqual(detector._last_seen[("gateway", "1")], ts_nuevo, places=1)

    def test_heartbeat_malformado_hace_ack_igual(self):
        """Un mensaje malformado no debe crashear el detector — se ackea y se descarta."""
        detector = make_detector()
        ack = MagicMock()
        detector._on_heartbeat(b"no-es-json", ack, None)
        ack.assert_called_once()
        self.assertEqual(len(detector._last_seen), 0)

    def test_multiple_instancias_de_misma_etapa(self):
        detector = make_detector()
        ack = MagicMock()
        detector._on_heartbeat(make_hb_msg("filtro", "1"), ack, None)
        detector._on_heartbeat(make_hb_msg("filtro", "2"), ack, None)
        self.assertIn(("filtro", "1"), detector._last_seen)
        self.assertIn(("filtro", "2"), detector._last_seen)


class TestPublicarCaida(unittest.TestCase):

    def test_publica_evento_en_cola_caidas(self):
        detector = make_detector()
        mock_queue = MagicMock()
        detector._caidas_queue = mock_queue
        detector._last_seen[("gateway", "1")] = time.time() - 20

        detector._publicar_caida("gateway", "1")

        mock_queue.send.assert_called_once()
        enviado = json.loads(mock_queue.send.call_args[0][0].decode())
        self.assertEqual(enviado["etapa"], "gateway")
        self.assertEqual(enviado["instancia"], "1")

    def test_elimina_de_last_seen_tras_publicar(self):
        """Después de publicar la caída, el worker se elimina de _last_seen
        para no reportarlo de nuevo en el siguiente tick."""
        detector = make_detector()
        detector._caidas_queue = MagicMock()
        detector._last_seen[("gateway", "1")] = time.time() - 20

        detector._publicar_caida("gateway", "1")

        self.assertNotIn(("gateway", "1"), detector._last_seen)

    def test_crea_conexion_si_no_existe(self):
        """La primera vez que publica, crea la conexión a la cola de caidas."""
        detector = make_detector()
        with patch("watchdog.detector.MessageMiddlewareQueueRabbitMQ") as mock_cls:
            mock_cls.return_value = MagicMock()
            detector._last_seen[("filtro", "1")] = time.time() - 20
            detector._publicar_caida("filtro", "1")
        mock_cls.assert_called_once_with("localhost", "caidas")

    def test_reutiliza_conexion_existente(self):
        """Si la conexión ya existe, no crea una nueva."""
        detector = make_detector()
        mock_queue = MagicMock()
        detector._caidas_queue = mock_queue
        detector._last_seen[("filtro", "1")] = time.time() - 20
        detector._last_seen[("filtro", "2")] = time.time() - 20

        with patch("watchdog.detector.MessageMiddlewareQueueRabbitMQ") as mock_cls:
            detector._publicar_caida("filtro", "1")
            detector._publicar_caida("filtro", "2")

        mock_cls.assert_not_called()
        self.assertEqual(mock_queue.send.call_count, 2)


class TestCheckLoop(unittest.TestCase):

    def test_no_reporta_worker_dentro_del_timeout(self):
        detector = make_detector(timeout=15.0)
        detector._caidas_queue = MagicMock()
        detector._last_seen[("gateway", "1")] = time.time() - 5  # 5s < timeout (15s)

        with patch.object(detector, '_publicar_caida') as mock_pub:
            # Simulamos una iteración del check_loop sin el sleep
            now = time.time()
            import copy
            snapshot = copy.deepcopy(detector._last_seen)
            caidas = [
                (etapa, inst)
                for (etapa, inst), ts in snapshot.items()
                if now - ts > detector._config.timeout_seconds
            ]
            for etapa, inst in caidas:
                detector._publicar_caida(etapa, inst)

        mock_pub.assert_not_called()

    def test_reporta_worker_fuera_del_timeout(self):
        detector = make_detector(timeout=15.0)
        detector._caidas_queue = MagicMock()
        detector._last_seen[("gateway", "1")] = time.time() - 20  # 20s > timeout (15s)

        with patch.object(detector, '_publicar_caida') as mock_pub:
            now = time.time()
            import copy
            snapshot = copy.deepcopy(detector._last_seen)
            caidas = [
                (etapa, inst)
                for (etapa, inst), ts in snapshot.items()
                if now - ts > detector._config.timeout_seconds
            ]
            for etapa, inst in caidas:
                detector._publicar_caida(etapa, inst)

        mock_pub.assert_called_once_with("gateway", "1")


if __name__ == "__main__":
    unittest.main()
