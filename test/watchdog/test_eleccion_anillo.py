import time
import unittest
from unittest.mock import MagicMock, patch

from watchdog.eleccion_anillo import EleccionAnillo


def crear_config(id_watchdog=1, cantidad_watchdogs=3):
    config = MagicMock()
    config.id_watchdog = id_watchdog
    config.cantidad_watchdogs = cantidad_watchdogs
    config.host_mom = "localhost"
    config.intervalo_latido_lider = 5.0
    config.timeout_lider_segundos = 20.0
    config.demora_inicial_eleccion_max = 3.0
    config.intervalo_chequeo_lider = 5.0
    config.timeout_eleccion = 30.0
    config.ttl_sospechados_caidos = 60.0
    return config


def crear_eleccion(id_watchdog=1, cantidad_watchdogs=3):
    config = crear_config(id_watchdog, cantidad_watchdogs)
    al_ser_lider = MagicMock()
    al_perder_liderazgo = MagicMock()
    al_caer_standby = MagicMock()
    eleccion = EleccionAnillo(config, al_ser_lider, al_perder_liderazgo, al_caer_standby)
    eleccion._enviar_a = MagicMock()
    eleccion._bucle_latido_lider = MagicMock()
    return eleccion, al_ser_lider, al_perder_liderazgo, al_caer_standby


class TestCalcularProximoDestino(unittest.TestCase):

    def test_devuelve_siguiente_directo_si_no_hay_muertos(self):
        eleccion, *_ = crear_eleccion(id_watchdog=1)
        with eleccion._lock:
            destino = eleccion._calcular_proximo_destino()
        self.assertEqual(destino, 2)

    def test_saltea_nodo_sospechado(self):
        eleccion, *_ = crear_eleccion(id_watchdog=1)
        eleccion._ids_sospechados_caidos[2] = time.time()
        with eleccion._lock:
            destino = eleccion._calcular_proximo_destino()
        self.assertEqual(destino, 3)

    def test_saltea_multiples_nodos_sospechados(self):
        eleccion, *_ = crear_eleccion(id_watchdog=1)
        eleccion._ids_sospechados_caidos[2] = time.time()
        eleccion._ids_sospechados_caidos[3] = time.time()
        with eleccion._lock:
            destino = eleccion._calcular_proximo_destino()
        self.assertEqual(destino, 1)

    def test_no_saltea_nodo_con_ttl_expirado(self):
        eleccion, *_ = crear_eleccion(id_watchdog=1)
        eleccion._ids_sospechados_caidos[2] = time.time() - 61
        with eleccion._lock:
            destino = eleccion._calcular_proximo_destino()
        self.assertEqual(destino, 2)


class TestManejarEleccion(unittest.TestCase):

    def test_propio_id_declara_lider(self):
        eleccion, al_ser_lider, *_ = crear_eleccion(id_watchdog=2)
        with patch.object(eleccion, '_declarar_lider') as mock_declarar:
            eleccion._manejar_eleccion(id_recibido=2, saltar=[])
        mock_declarar.assert_called_once()

    def test_id_mayor_se_reenvia_sin_cambio(self):
        eleccion, *_ = crear_eleccion(id_watchdog=1)
        eleccion._manejar_eleccion(id_recibido=3, saltar=[])
        eleccion._enviar_a.assert_called_once()
        _, payload = eleccion._enviar_a.call_args[0]
        self.assertEqual(payload["id"], 3)

    def test_id_menor_se_reemplaza_por_propio(self):
        eleccion, *_ = crear_eleccion(id_watchdog=3)
        eleccion._manejar_eleccion(id_recibido=1, saltar=[])
        eleccion._enviar_a.assert_called_once()
        _, payload = eleccion._enviar_a.call_args[0]
        self.assertEqual(payload["id"], 3)

    def test_lider_absorbe_mensaje(self):
        eleccion, *_ = crear_eleccion(id_watchdog=2)
        eleccion._es_lider = True
        eleccion._manejar_eleccion(id_recibido=1, saltar=[])
        eleccion._enviar_a.assert_not_called()

    def test_lider_activo_conocido_absorbe_mensaje(self):
        eleccion, *_ = crear_eleccion(id_watchdog=1)
        eleccion._id_lider = 3
        eleccion._ultimo_latido_lider = time.time()
        eleccion._manejar_eleccion(id_recibido=2, saltar=[])
        eleccion._enviar_a.assert_not_called()

    def test_propagacion_de_saltar(self):
        eleccion, *_ = crear_eleccion(id_watchdog=1)
        eleccion._manejar_eleccion(id_recibido=3, saltar=[2])
        self.assertIn(2, eleccion._ids_sospechados_caidos)

    def test_saltar_no_sobreescribe_timestamp_existente(self):
        eleccion, *_ = crear_eleccion(id_watchdog=1)
        ts_original = time.time() - 10
        eleccion._ids_sospechados_caidos[2] = ts_original
        eleccion._manejar_eleccion(id_recibido=3, saltar=[2])
        self.assertAlmostEqual(eleccion._ids_sospechados_caidos[2], ts_original, places=1)


class TestDeclararLider(unittest.TestCase):

    def test_llama_al_ser_lider_con_nodos_caidos(self):
        eleccion, al_ser_lider, *_ = crear_eleccion(id_watchdog=3)
        eleccion._ids_sospechados_caidos[1] = time.time()
        with patch("watchdog.eleccion_anillo.threading.Thread"):
            eleccion._declarar_lider()
        al_ser_lider.assert_called_once()
        ids_caidos = al_ser_lider.call_args[0][0]
        self.assertIn(1, ids_caidos)

    def test_idempotente_si_ya_es_lider(self):
        eleccion, al_ser_lider, *_ = crear_eleccion(id_watchdog=3)
        with patch("watchdog.eleccion_anillo.threading.Thread"):
            eleccion._declarar_lider()
            eleccion._declarar_lider()
        al_ser_lider.assert_called_once()

    def test_envia_coordinador_al_siguiente(self):
        eleccion, *_ = crear_eleccion(id_watchdog=3)
        with patch("watchdog.eleccion_anillo.threading.Thread"):
            eleccion._declarar_lider()
        eleccion._enviar_a.assert_called_once()
        _, payload = eleccion._enviar_a.call_args[0]
        self.assertEqual(payload["tipo"], "coordinador")
        self.assertEqual(payload["id"], 3)

    def test_excluye_nodos_caidos_con_ttl_expirado(self):
        eleccion, al_ser_lider, *_ = crear_eleccion(id_watchdog=3)
        eleccion._ids_sospechados_caidos[1] = time.time() - 61
        with patch("watchdog.eleccion_anillo.threading.Thread"):
            eleccion._declarar_lider()
        ids_caidos = al_ser_lider.call_args[0][0]
        self.assertNotIn(1, ids_caidos)


class TestManejarCoordinador(unittest.TestCase):

    def test_registra_nuevo_lider(self):
        eleccion, *_ = crear_eleccion(id_watchdog=1)
        eleccion._manejar_coordinador(id_lider=3)
        self.assertEqual(eleccion._id_lider, 3)
        self.assertFalse(eleccion._es_lider)

    def test_reenvia_coordinador_si_no_lo_hizo(self):
        eleccion, *_ = crear_eleccion(id_watchdog=1)
        eleccion._manejar_coordinador(id_lider=3)
        eleccion._enviar_a.assert_called_once()
        _, payload = eleccion._enviar_a.call_args[0]
        self.assertEqual(payload["tipo"], "coordinador")

    def test_no_reenvia_coordinador_duplicado(self):
        eleccion, *_ = crear_eleccion(id_watchdog=1)
        eleccion._manejar_coordinador(id_lider=3)
        eleccion._manejar_coordinador(id_lider=3)
        self.assertEqual(eleccion._enviar_a.call_count, 1)

    def test_cede_liderazgo_si_era_lider(self):
        eleccion, _, al_perder_liderazgo, _ = crear_eleccion(id_watchdog=2)
        eleccion._es_lider = True
        eleccion._manejar_coordinador(id_lider=3)
        al_perder_liderazgo.assert_called_once()

    def test_ignora_coordinador_con_propio_id_si_es_lider(self):
        eleccion, _, al_perder_liderazgo, _ = crear_eleccion(id_watchdog=3)
        eleccion._es_lider = True
        eleccion._manejar_coordinador(id_lider=3)
        al_perder_liderazgo.assert_not_called()


class TestManejarVivo(unittest.TestCase):

    def test_elimina_de_sospechados(self):
        eleccion, *_ = crear_eleccion(id_watchdog=1)
        eleccion._ids_sospechados_caidos[2] = time.time()
        eleccion._manejar_vivo(id_nodo=2)
        self.assertNotIn(2, eleccion._ids_sospechados_caidos)

    def test_lider_actualiza_ultimo_visto_standby(self):
        eleccion, *_ = crear_eleccion(id_watchdog=3)
        eleccion._es_lider = True
        eleccion._standbys_caidos_reportados.add(1)
        eleccion._manejar_vivo(id_nodo=1)
        self.assertIn(1, eleccion._ultimo_visto_standby)
        self.assertNotIn(1, eleccion._standbys_caidos_reportados)


class TestTickTimeoutLider(unittest.TestCase):

    def test_no_actua_si_es_lider(self):
        eleccion, *_ = crear_eleccion(id_watchdog=1)
        eleccion._es_lider = True
        with patch.object(eleccion, '_iniciar_eleccion') as mock_init:
            eleccion._tick_timeout_lider(tiempo_inicio=time.time())
        mock_init.assert_not_called()

    def test_no_actua_si_no_expiro_timeout(self):
        eleccion, *_ = crear_eleccion(id_watchdog=1)
        eleccion._ultimo_latido_lider = time.time()
        with patch.object(eleccion, '_iniciar_eleccion') as mock_init:
            eleccion._tick_timeout_lider(tiempo_inicio=time.time())
        mock_init.assert_not_called()

    def test_inicia_eleccion_al_expirar_timeout(self):
        eleccion, *_ = crear_eleccion(id_watchdog=1)
        eleccion._ultimo_latido_lider = time.time() - 25
        eleccion._id_lider = 3
        with patch.object(eleccion, '_iniciar_eleccion') as mock_init:
            eleccion._tick_timeout_lider(tiempo_inicio=time.time() - 25)
        mock_init.assert_called_once()
        self.assertIn(3, eleccion._ids_sospechados_caidos)

    def test_espera_si_eleccion_en_curso_sin_timeout(self):
        eleccion, *_ = crear_eleccion(id_watchdog=1)
        eleccion._ultimo_latido_lider = time.time() - 25
        eleccion._en_eleccion = True
        eleccion._eleccion_iniciada_en = time.time() - 5
        with patch.object(eleccion, '_iniciar_eleccion') as mock_init:
            eleccion._tick_timeout_lider(tiempo_inicio=time.time() - 25)
        mock_init.assert_not_called()

    def test_reintenta_si_eleccion_expiro(self):
        eleccion, *_ = crear_eleccion(id_watchdog=1)
        eleccion._ultimo_latido_lider = time.time() - 25
        eleccion._en_eleccion = True
        eleccion._eleccion_iniciada_en = time.time() - 35
        with patch.object(eleccion, '_iniciar_eleccion') as mock_init:
            eleccion._tick_timeout_lider(tiempo_inicio=time.time() - 25)
        mock_init.assert_called_once()


class TestTickChequeoStandbys(unittest.TestCase):

    def test_no_actua_si_no_es_lider(self):
        eleccion, _, _, al_caer_standby = crear_eleccion(id_watchdog=3)
        eleccion._tick_chequeo_standbys()
        al_caer_standby.assert_not_called()

    def test_no_actua_durante_periodo_de_gracia(self):
        eleccion, _, _, al_caer_standby = crear_eleccion(id_watchdog=3)
        eleccion._es_lider = True
        eleccion._lider_desde = time.time()
        eleccion._tick_chequeo_standbys()
        al_caer_standby.assert_not_called()

    def test_detecta_standby_silencioso(self):
        eleccion, _, _, al_caer_standby = crear_eleccion(id_watchdog=3)
        eleccion._es_lider = True
        eleccion._lider_desde = time.time() - 25
        eleccion._tick_chequeo_standbys()
        llamadas = [c[0][0] for c in al_caer_standby.call_args_list]
        self.assertIn(1, llamadas)
        self.assertIn(2, llamadas)

    def test_no_reporta_standby_dos_veces(self):
        eleccion, _, _, al_caer_standby = crear_eleccion(id_watchdog=3)
        eleccion._es_lider = True
        eleccion._lider_desde = time.time() - 25
        eleccion._tick_chequeo_standbys()
        eleccion._tick_chequeo_standbys()
        self.assertEqual(al_caer_standby.call_count, 2)

    def test_standby_vuelve_a_mandar_latido_sale_de_reportados(self):
        eleccion, _, _, al_caer_standby = crear_eleccion(id_watchdog=3)
        eleccion._es_lider = True
        eleccion._lider_desde = time.time() - 25
        eleccion._tick_chequeo_standbys()
        eleccion._manejar_latido_standby(id_nodo=1)
        eleccion._tick_chequeo_standbys()
        total_llamadas_nodo_1 = sum(1 for c in al_caer_standby.call_args_list if c[0][0] == 1)
        self.assertEqual(total_llamadas_nodo_1, 1)


class TestIniciarEleccionTimeout(unittest.TestCase):

    def test_timeout_agrega_ultimo_destino_a_sospechados(self):
        eleccion, *_ = crear_eleccion(id_watchdog=1)
        eleccion._en_eleccion = True
        eleccion._eleccion_iniciada_en = time.time() - 35
        eleccion._ultimo_destino_eleccion = 2
        eleccion._iniciar_eleccion()
        self.assertIn(2, eleccion._ids_sospechados_caidos)

    def test_no_reintenta_si_eleccion_vigente(self):
        eleccion, *_ = crear_eleccion(id_watchdog=1)
        eleccion._en_eleccion = True
        eleccion._eleccion_iniciada_en = time.time() - 5
        eleccion._iniciar_eleccion()
        eleccion._enviar_a.assert_not_called()


if __name__ == "__main__":
    unittest.main()
