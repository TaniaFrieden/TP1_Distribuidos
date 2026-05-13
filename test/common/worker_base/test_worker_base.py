"""
Tests para BaseWorker
=====================

"""
import signal
import pytest
from unittest.mock import MagicMock, patch, call

from common.worker_base.base import BaseWorker


# ------------------------------------------------------------------
# Worker concreto mínimo para poder instanciar BaseWorker en tests
# ------------------------------------------------------------------

class WorkerDePrueba(BaseWorker):
    """Implementación mínima de BaseWorker para usar en tests."""

    def __init__(self, middleware_mock=None):
        self._middleware_inyectado = middleware_mock or MagicMock()
        self._mensajes_procesados = []
        self._al_cerrar_llamado = False
        super().__init__()

    def inicializar_middleware(self):
        return self._middleware_inyectado

    def procesar_mensaje(self, mensaje: bytes, ack, nack):
        self._mensajes_procesados.append(mensaje)
        ack()

    def al_cerrar(self):
        self._al_cerrar_llamado = True


# ------------------------------------------------------------------
# Fixtures
# ------------------------------------------------------------------

@pytest.fixture
def middleware_mock():
    mock = MagicMock()
    mock.start_consuming = MagicMock()
    mock.stop_consuming = MagicMock()
    mock.close = MagicMock()
    return mock


@pytest.fixture
def worker(middleware_mock):
    return WorkerDePrueba(middleware_mock)


# ------------------------------------------------------------------
# Tests: arranque y ciclo de vida
# ------------------------------------------------------------------

class TestCicloDeVida:

    def test_iniciar_llama_a_inicializar_middleware(self, worker, middleware_mock):
        """Al iniciar, el worker debe conectarse al middleware."""
        worker.iniciar()
        middleware_mock.start_consuming.assert_called_once_with(worker._callback_interno)

    def test_iniciar_cierra_middleware_al_terminar(self, worker, middleware_mock):
        """El middleware se cierra siempre, incluso si todo va bien."""
        worker.iniciar()
        middleware_mock.close.assert_called_once()

    def test_iniciar_llama_al_cerrar(self, worker):
        """El hook al_cerrar() se ejecuta al finalizar."""
        worker.iniciar()
        assert worker._al_cerrar_llamado is True

    def test_cierre_ocurre_aunque_start_consuming_lance_excepcion(self, worker, middleware_mock):
        """Si start_consuming lanza, el middleware igual se cierra."""
        middleware_mock.start_consuming.side_effect = RuntimeError("fallo de red")

        with pytest.raises(RuntimeError):
            worker.iniciar()

        middleware_mock.close.assert_called_once()

    def test_excepcion_inesperada_se_propaga(self, worker, middleware_mock):
        """Errores no relacionados con el cierre deben propagarse al caller."""
        middleware_mock.start_consuming.side_effect = RuntimeError("error inesperado")

        with pytest.raises(RuntimeError, match="error inesperado"):
            worker.iniciar()


# ------------------------------------------------------------------
# Tests: shutdown graceful ante señales
# ------------------------------------------------------------------

class TestShutdownGraceful:

    def test_sigterm_setea_cierre_solicitado(self, worker):
        """SIGTERM debe marcar _cierre_solicitado = True."""
        worker._manejar_senal_cierre(signal.SIGTERM, None)
        assert worker._cierre_solicitado is True

    def test_sigint_setea_cierre_solicitado(self, worker):
        """SIGINT debe marcar _cierre_solicitado = True."""
        worker._manejar_senal_cierre(signal.SIGINT, None)
        assert worker._cierre_solicitado is True

    def test_sigterm_llama_stop_consuming_si_hay_middleware(self, worker, middleware_mock):
        """Con middleware activo, SIGTERM debe llamar a stop_consuming."""
        worker._middleware = middleware_mock
        worker._manejar_senal_cierre(signal.SIGTERM, None)
        middleware_mock.stop_consuming.assert_called_once()

    def test_sigterm_sin_middleware_no_falla(self, worker):
        """Si el middleware todavía no se inicializó, SIGTERM no debe lanzar."""
        worker._middleware = None
        # No debe lanzar ninguna excepción
        worker._manejar_senal_cierre(signal.SIGTERM, None)

    def test_sigterm_con_stop_consuming_que_falla_no_propaga_excepcion(self, worker, middleware_mock):
        """Si stop_consuming lanza, el worker no debe caerse."""
        middleware_mock.stop_consuming.side_effect = Exception("error al detener")
        worker._middleware = middleware_mock

        # No debe lanzar
        worker._manejar_senal_cierre(signal.SIGTERM, None)

    def test_iniciar_no_propaga_excepcion_si_cierre_fue_solicitado(self, worker, middleware_mock):
        """
        Si start_consuming lanza porque stop_consuming lo interrumpió,
        iniciar() no debe propagar esa excepción.
        """
        def simular_consumo_interrumpido(callback):
            worker._cierre_solicitado = True
            raise Exception("consumo interrumpido por cierre")

        middleware_mock.start_consuming.side_effect = simular_consumo_interrumpido

        # No debe lanzar
        worker.iniciar()


# ------------------------------------------------------------------
# Tests: callback interno
# ------------------------------------------------------------------

class TestCallbackInterno:

    def test_mensaje_normal_llama_a_procesar_mensaje(self, worker):
        """Con cierre no solicitado, el mensaje se pasa a procesar_mensaje."""
        ack = MagicMock()
        nack = MagicMock()
        mensaje = b'{"monto": 10}'

        worker._callback_interno(mensaje, ack, nack)

        assert mensaje in worker._mensajes_procesados
        ack.assert_called_once()
        nack.assert_not_called()

    def test_cierre_solicitado_hace_nack_sin_procesar(self, worker):
        """Si se pidió cierre, el mensaje se devuelve a la cola sin procesarse."""
        worker._cierre_solicitado = True
        ack = MagicMock()
        nack = MagicMock()

        worker._callback_interno(b"mensaje", ack, nack)

        nack.assert_called_once()
        ack.assert_not_called()
        assert len(worker._mensajes_procesados) == 0

    def test_excepcion_en_procesar_mensaje_llama_nack(self, middleware_mock):
        """Si procesar_mensaje lanza, el callback debe hacer nack para no perder el mensaje."""
        class WorkerQueExplota(BaseWorker):
            def inicializar_middleware(self):
                return middleware_mock
            def procesar_mensaje(self, mensaje, ack, nack):
                raise ValueError("error de negocio")

        worker_roto = WorkerQueExplota()
        ack = MagicMock()
        nack = MagicMock()

        worker_roto._callback_interno(b"mensaje", ack, nack)

        nack.assert_called_once()
        ack.assert_not_called()

    def test_excepcion_en_procesar_mensaje_no_tira_el_worker(self, middleware_mock):
        """Un error en un mensaje no debe propagar la excepción hacia el loop."""
        class WorkerQueExplota(BaseWorker):
            def inicializar_middleware(self):
                return middleware_mock
            def procesar_mensaje(self, mensaje, ack, nack):
                raise ValueError("error de negocio")

        worker_roto = WorkerQueExplota()

        # No debe lanzar
        worker_roto._callback_interno(b"mensaje", MagicMock(), MagicMock())

    def test_multiples_mensajes_se_procesan_en_orden(self, worker):
        """Los mensajes se procesan en el orden en que llegan."""
        mensajes = [b"msg_1", b"msg_2", b"msg_3"]

        for msg in mensajes:
            worker._callback_interno(msg, MagicMock(), MagicMock())

        assert worker._mensajes_procesados == mensajes


# ------------------------------------------------------------------
# Tests: hook al_cerrar
# ------------------------------------------------------------------

class TestAlCerrar:

    def test_al_cerrar_se_ejecuta_antes_de_cerrar_middleware(self, middleware_mock):
        """al_cerrar() debe ejecutarse antes de que se cierre la conexión."""
        orden = []

        class WorkerConOrden(BaseWorker):
            def inicializar_middleware(self):
                return middleware_mock
            def procesar_mensaje(self, mensaje, ack, nack):
                ack()
            def al_cerrar(self):
                orden.append("al_cerrar")

        middleware_mock.close.side_effect = lambda: orden.append("close")

        w = WorkerConOrden()
        w._middleware = middleware_mock
        w._cerrar()

        assert orden == ["al_cerrar", "close"]

    def test_excepcion_en_al_cerrar_no_impide_cerrar_middleware(self, middleware_mock):
        """Aunque al_cerrar() falle, el middleware igual debe cerrarse."""
        class WorkerAlCerrarFalla(BaseWorker):
            def inicializar_middleware(self):
                return middleware_mock
            def procesar_mensaje(self, mensaje, ack, nack):
                ack()
            def al_cerrar(self):
                raise RuntimeError("fallo en cleanup")

        w = WorkerAlCerrarFalla()
        w._middleware = middleware_mock
        w._cerrar()

        middleware_mock.close.assert_called_once()
