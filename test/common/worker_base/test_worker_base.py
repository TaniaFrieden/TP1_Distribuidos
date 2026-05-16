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

    def __init__(self):
        self._mensajes_procesados = []
        self._al_cerrar_llamado = False
        super().__init__()

    def procesar_mensaje(self, mensaje: bytes, ack, nack):
        self._mensajes_procesados.append(mensaje)
        ack()

    def al_cerrar(self):
        self._al_cerrar_llamado = True


# ------------------------------------------------------------------
# Fixtures
# ------------------------------------------------------------------

@pytest.fixture(autouse=True)
def mock_middleware():
    """Parchea las 3 conexiones al middleware para todos los tests."""
    with patch("common.worker_base.base.middleware.MessageMiddlewareQueueRabbitMQ") as mock_queue_cls, \
         patch("common.worker_base.base.middleware.FanoutExchangeRabbitMQ") as mock_exchange_cls:

        mock_input_queue   = MagicMock()
        mock_control_queue = MagicMock()
        mock_exchange      = MagicMock()

        # El constructor llama a MessageMiddlewareQueueRabbitMQ dos veces:
        # primera vez → input_queue, segunda vez → control_queue
        mock_queue_cls.side_effect = [mock_input_queue, mock_control_queue]
        mock_exchange_cls.return_value = mock_exchange

        yield {
            "input_queue":   mock_input_queue,
            "control_queue": mock_control_queue,
            "exchange":      mock_exchange,
            "queue_cls":     mock_queue_cls,
            "exchange_cls":  mock_exchange_cls,
        }


@pytest.fixture
def worker():
    return WorkerDePrueba()


# ------------------------------------------------------------------
# Tests: ciclo de vida
# ------------------------------------------------------------------

class TestCicloDeVida:

    def test_iniciar_llama_start_consuming_en_input_queue(self, worker, mock_middleware):
        """Al iniciar, se debe consumir de la input_queue con el callback interno."""
        worker.iniciar()
        worker.input_queue.start_consuming.assert_called_once_with(worker._callback_interno)

    def test_iniciar_llama_start_consuming_en_control_queue(self, worker, mock_middleware):
        """Al iniciar, se debe consumir de la control_queue en un thread separado."""
        worker.iniciar()
        worker.control_queue.start_consuming.assert_called_once_with(worker._process_control_message)

    def test_iniciar_cierra_input_queue_al_terminar(self, worker, mock_middleware):
        """La input_queue siempre se cierra al finalizar."""
        worker.iniciar()
        worker.input_queue.close.assert_called_once()

    def test_iniciar_cierra_control_queue_al_terminar(self, worker, mock_middleware):
        """La control_queue siempre se cierra al finalizar."""
        worker.iniciar()
        worker.control_queue.close.assert_called_once()

    def test_iniciar_cierra_control_exchange_al_terminar(self, worker, mock_middleware):
        """El control_exchange siempre se cierra al finalizar."""
        worker.iniciar()
        worker.control_exchange.close.assert_called_once()

    def test_iniciar_llama_al_cerrar(self, worker, mock_middleware):
        """El hook al_cerrar() se ejecuta al finalizar."""
        worker.iniciar()
        assert worker._al_cerrar_llamado is True

    def test_cierre_ocurre_aunque_start_consuming_lance_excepcion(self, worker, mock_middleware):
        """Si start_consuming lanza, el middleware igual se cierra."""
        worker.input_queue.start_consuming.side_effect = RuntimeError("fallo de red")

        with pytest.raises(RuntimeError):
            worker.iniciar()

        worker.input_queue.close.assert_called_once()

    def test_excepcion_inesperada_se_propaga(self, worker, mock_middleware):
        """Errores no relacionados con cierre deben propagarse al caller."""
        worker.input_queue.start_consuming.side_effect = RuntimeError("error inesperado")

        with pytest.raises(RuntimeError, match="error inesperado"):
            worker.iniciar()


# ------------------------------------------------------------------
# Tests: shutdown graceful ante señales
# ------------------------------------------------------------------

class TestShutdownGraceful:

    def test_sigterm_setea_cierre_solicitado(self, worker):
        worker._manejar_senal_cierre(signal.SIGTERM, None)
        assert worker._cierre_solicitado is True

    def test_sigint_setea_cierre_solicitado(self, worker):
        worker._manejar_senal_cierre(signal.SIGINT, None)
        assert worker._cierre_solicitado is True

    def test_sigterm_llama_stop_consuming_en_input_queue(self, worker):
        worker._manejar_senal_cierre(signal.SIGTERM, None)
        worker.input_queue.stop_consuming.assert_called_once()

    def test_sigterm_llama_stop_consuming_en_control_queue(self, worker):
        worker._manejar_senal_cierre(signal.SIGTERM, None)
        worker.control_queue.stop_consuming.assert_called_once()

    def test_sigterm_notifica_condicion_pendiente(self, worker):
        """SIGTERM debe notificar a threads que esperan en condicion_pendiente."""
        notificado = []

        def esperar():
            with worker.condicion_pendiente:
                worker.condicion_pendiente.wait(timeout=2)
                notificado.append(True)

        import threading
        t = threading.Thread(target=esperar)
        t.start()
        worker._manejar_senal_cierre(signal.SIGTERM, None)
        t.join(timeout=3)

        assert notificado == [True]

    def test_iniciar_no_propaga_excepcion_si_cierre_fue_solicitado(self, worker, mock_middleware):
        """Si start_consuming lanza porque el cierre lo interrumpió, no se propaga."""
        def simular_consumo_interrumpido(callback):
            worker._cierre_solicitado = True
            raise Exception("consumo interrumpido por cierre")

        worker.input_queue.start_consuming.side_effect = simular_consumo_interrumpido
        worker.iniciar()  # no debe lanzar


# ------------------------------------------------------------------
# Tests: callback interno
# ------------------------------------------------------------------

class TestCallbackInterno:

    def test_mensaje_normal_llama_a_procesar_mensaje(self, worker):
        ack  = MagicMock()
        nack = MagicMock()
        mensaje = b'{"monto": 10}'

        worker._callback_interno(mensaje, ack, nack)

        assert mensaje in worker._mensajes_procesados
        ack.assert_called_once()
        nack.assert_not_called()

    def test_cierre_solicitado_hace_nack_sin_procesar(self, worker):
        worker._cierre_solicitado = True
        ack  = MagicMock()
        nack = MagicMock()

        worker._callback_interno(b"mensaje", ack, nack)

        nack.assert_called_once()
        ack.assert_not_called()
        assert len(worker._mensajes_procesados) == 0

    def test_excepcion_en_procesar_mensaje_llama_nack(self, mock_middleware):
        class WorkerQueExplota(BaseWorker):
            def procesar_mensaje(self, mensaje, ack, nack):
                raise ValueError("error de negocio")
            def al_cerrar(self):
                pass

        mock_middleware["queue_cls"].side_effect = [MagicMock(), MagicMock()]
        w   = WorkerQueExplota()
        ack  = MagicMock()
        nack = MagicMock()

        w._callback_interno(b"mensaje", ack, nack)

        nack.assert_called_once()
        ack.assert_not_called()

    def test_excepcion_en_procesar_mensaje_no_tira_el_worker(self, mock_middleware):
        class WorkerQueExplota(BaseWorker):
            def procesar_mensaje(self, mensaje, ack, nack):
                raise ValueError("error de negocio")
            def al_cerrar(self):
                pass

        mock_middleware["queue_cls"].side_effect = [MagicMock(), MagicMock()]
        w = WorkerQueExplota()

        w._callback_interno(b"mensaje", MagicMock(), MagicMock())  # no debe lanzar

    def test_multiples_mensajes_se_procesan_en_orden(self, worker):
        mensajes = [b"msg_1", b"msg_2", b"msg_3"]

        for msg in mensajes:
            worker._callback_interno(msg, MagicMock(), MagicMock())

        assert worker._mensajes_procesados == mensajes


# ------------------------------------------------------------------
# Tests: hook al_cerrar
# ------------------------------------------------------------------

class TestAlCerrar:

    def test_al_cerrar_se_ejecuta_antes_de_cerrar_middleware(self, mock_middleware):
        orden = []

        class WorkerConOrden(BaseWorker):
            def procesar_mensaje(self, mensaje, ack, nack):
                ack()
            def al_cerrar(self):
                orden.append("al_cerrar")

        mock_middleware["queue_cls"].side_effect = [MagicMock(), MagicMock()]
        mock_middleware["input_queue"].close.side_effect = lambda: orden.append("close")

        w = WorkerConOrden()
        w.input_queue.close.side_effect = lambda: orden.append("close")
        w._cerrar()

        assert orden[0] == "al_cerrar"
        assert "close" in orden

    def test_excepcion_en_al_cerrar_no_impide_cerrar_middleware(self, mock_middleware):
        class WorkerAlCerrarFalla(BaseWorker):
            def procesar_mensaje(self, mensaje, ack, nack):
                ack()
            def al_cerrar(self):
                raise RuntimeError("fallo en cleanup")

        mock_middleware["queue_cls"].side_effect = [MagicMock(), MagicMock()]
        w = WorkerAlCerrarFalla()
        w._cerrar()

        w.input_queue.close.assert_called_once()
        w.control_queue.close.assert_called_once()
        w.control_exchange.close.assert_called_once()