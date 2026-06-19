import json
import time
import unittest
from unittest.mock import MagicMock, patch

from watchdog.detector import DetectorLatidos


def crear_config(etapas=None, timeout=15.0, intervalo_chequeo=5.0):
    config = MagicMock()
    config.host_mom = "localhost"
    config.etapas = etapas or ["gateway", "filtro"]
    config.intervalo_latido_segundos = 5.0
    config.umbral_latidos_perdidos = 3
    config.timeout_segundos = timeout
    config.intervalo_chequeo_segundos = intervalo_chequeo
    config.cola_caidas = "caidas"
    return config


def crear_detector(etapas=None, timeout=15.0):
    config = crear_config(etapas=etapas, timeout=timeout)
    return DetectorLatidos(config)


def crear_msg_latido(etapa, instancia, ts=None):
    payload = {"etapa": etapa, "instancia": instancia, "timestamp": ts or time.time()}
    return json.dumps(payload).encode()


class TestAlRecibirLatido(unittest.TestCase):

    def test_registra_latido_en_ultimo_visto(self):
        detector = crear_detector()
        ack = MagicMock()
        detector._al_recibir_latido(crear_msg_latido("gateway", "1"), ack, None)
        self.assertIn(("gateway", "1"), detector._ultimo_visto)
        ack.assert_called_once()

    def test_actualiza_timestamp_con_latido_mas_reciente(self):
        detector = crear_detector()
        ack = MagicMock()
        ts_viejo = time.time() - 10
        ts_nuevo = time.time()
        detector._al_recibir_latido(crear_msg_latido("gateway", "1", ts=ts_viejo), ack, None)
        detector._al_recibir_latido(crear_msg_latido("gateway", "1", ts=ts_nuevo), ack, None)
        self.assertAlmostEqual(detector._ultimo_visto[("gateway", "1")], ts_nuevo, places=1)

    def test_latido_malformado_hace_ack_igual(self):
        detector = crear_detector()
        ack = MagicMock()
        detector._al_recibir_latido(b"no-es-json", ack, None)
        ack.assert_called_once()
        self.assertEqual(len(detector._ultimo_visto), 0)

    def test_multiples_instancias_de_misma_etapa(self):
        detector = crear_detector()
        ack = MagicMock()
        detector._al_recibir_latido(crear_msg_latido("filtro", "1"), ack, None)
        detector._al_recibir_latido(crear_msg_latido("filtro", "2"), ack, None)
        self.assertIn(("filtro", "1"), detector._ultimo_visto)
        self.assertIn(("filtro", "2"), detector._ultimo_visto)


class TestPublicarCaida(unittest.TestCase):

    def test_publica_evento_en_cola_caidas(self):
        detector = crear_detector()
        mock_cola = MagicMock()
        detector._cola_caidas = mock_cola
        detector._ultimo_visto[("gateway", "1")] = time.time() - 20

        detector._publicar_caida("gateway", "1")

        mock_cola.send.assert_called_once()
        enviado = json.loads(mock_cola.send.call_args[0][0].decode())
        self.assertEqual(enviado["etapa"], "gateway")
        self.assertEqual(enviado["instancia"], "1")

    def test_elimina_de_ultimo_visto_tras_publicar(self):
        detector = crear_detector()
        detector._cola_caidas = MagicMock()
        detector._ultimo_visto[("gateway", "1")] = time.time() - 20

        detector._publicar_caida("gateway", "1")

        self.assertNotIn(("gateway", "1"), detector._ultimo_visto)

    def test_crea_conexion_si_no_existe(self):
        detector = crear_detector()
        with patch("watchdog.detector.MessageMiddlewareQueueRabbitMQ") as mock_cls:
            mock_cls.return_value = MagicMock()
            detector._ultimo_visto[("filtro", "1")] = time.time() - 20
            detector._publicar_caida("filtro", "1")
        mock_cls.assert_called_once_with("localhost", "caidas")

    def test_reutiliza_conexion_existente(self):
        detector = crear_detector()
        mock_cola = MagicMock()
        detector._cola_caidas = mock_cola
        detector._ultimo_visto[("filtro", "1")] = time.time() - 20
        detector._ultimo_visto[("filtro", "2")] = time.time() - 20

        with patch("watchdog.detector.MessageMiddlewareQueueRabbitMQ") as mock_cls:
            detector._publicar_caida("filtro", "1")
            detector._publicar_caida("filtro", "2")

        mock_cls.assert_not_called()
        self.assertEqual(mock_cola.send.call_count, 2)


class TestBucleChequeo(unittest.TestCase):

    def test_no_reporta_worker_dentro_del_timeout(self):
        detector = crear_detector(timeout=15.0)
        detector._cola_caidas = MagicMock()
        detector._ultimo_visto[("gateway", "1")] = time.time() - 5

        with patch.object(detector, '_publicar_caida') as mock_pub:
            ahora = time.time()
            import copy
            snapshot = copy.deepcopy(detector._ultimo_visto)
            caidas = [
                (etapa, inst)
                for (etapa, inst), ts in snapshot.items()
                if ahora - ts > detector._config.timeout_segundos
            ]
            for etapa, inst in caidas:
                detector._publicar_caida(etapa, inst)

        mock_pub.assert_not_called()

    def test_reporta_worker_fuera_del_timeout(self):
        detector = crear_detector(timeout=15.0)
        detector._cola_caidas = MagicMock()
        detector._ultimo_visto[("gateway", "1")] = time.time() - 20

        with patch.object(detector, '_publicar_caida') as mock_pub:
            ahora = time.time()
            import copy
            snapshot = copy.deepcopy(detector._ultimo_visto)
            caidas = [
                (etapa, inst)
                for (etapa, inst), ts in snapshot.items()
                if ahora - ts > detector._config.timeout_segundos
            ]
            for etapa, inst in caidas:
                detector._publicar_caida(etapa, inst)

        mock_pub.assert_called_once_with("gateway", "1")


if __name__ == "__main__":
    unittest.main()
