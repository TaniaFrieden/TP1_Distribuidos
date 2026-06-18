import pytest
from unittest.mock import MagicMock, patch, call
from types import SimpleNamespace

from workers.base.coordinacion.coordinador import CoordinadorDistribuido
from workers.base.coordinacion.contador_vuelos import ContadorVuelos
from common.constantes_protocolo import (
    TIPO_MENSAJE,
    TIPO_EOF_RECIBIDO,
    TIPO_WORKER_FINALIZADO,
    TIPO_BARRERA_COMPLETA,
    ID_CLIENTE,
    ORIGINADOR,
    ID_WORKER,
)


def _crear_config(id_nodo=0, total_workers=2, sharded=True):
    return SimpleNamespace(
        id_nodo=id_nodo,
        total_workers=total_workers,
        prefijo_nodo="test",
        host_mom="localhost",
        colas_entrada=[f"cola_{id_nodo}"] if sharded else ["cola_comun"],
        tiene_cola_sharded=sharded,
    )


@pytest.fixture
def mocks_infra():
    with patch("workers.base.coordinacion.coordinador.PersistenciaCoordinacion") as mock_pers, \
         patch("workers.base.coordinacion.coordinador.TransporteControl") as mock_trans:
        mock_pers.return_value.cargar.return_value = ({}, [], [])
        mock_pers.return_value.guardar = MagicMock()
        mock_trans.return_value.enviar = MagicMock()
        yield {
            "persistencia": mock_pers.return_value,
            "transporte": mock_trans.return_value,
        }


def _crear_coordinador(mocks, hooks=None, id_nodo=0, total_workers=2, sharded=True):
    config = _crear_config(id_nodo, total_workers, sharded)
    coord = CoordinadorDistribuido(
        config,
        al_completar_sincronizacion=MagicMock(),
        al_completar_barrera=MagicMock(),
        contador_vuelos=ContadorVuelos(),
        hooks=hooks,
    )
    return coord


class TestEjecutarHook:

    def test_hook_registrado_se_ejecuta(self, mocks_infra):
        invocado = []
        coord = _crear_coordinador(mocks_infra, hooks={
            "pre_finished": lambda: invocado.append(True),
        })

        coord._ejecutar_hook("pre_finished")

        assert invocado == [True]

    def test_hook_no_registrado_no_falla(self, mocks_infra):
        coord = _crear_coordinador(mocks_infra, hooks={})

        coord._ejecutar_hook("pre_finished")

    def test_sin_hooks_no_falla(self, mocks_infra):
        coord = _crear_coordinador(mocks_infra)

        coord._ejecutar_hook("pre_finished")

    def test_multiples_hooks_independientes(self, mocks_infra):
        registro = []
        coord = _crear_coordinador(mocks_infra, hooks={
            "pre_finished": lambda: registro.append("pre_finished"),
            "post_barrera": lambda: registro.append("post_barrera"),
        })

        coord._ejecutar_hook("pre_finished")
        coord._ejecutar_hook("post_barrera")

        assert registro == ["pre_finished", "post_barrera"]


class TestHookPreFinished:

    def test_hook_pre_finished_se_ejecuta_durante_flush(self, mocks_infra):
        invocado = []
        coord = _crear_coordinador(mocks_infra, hooks={
            "pre_finished": lambda: invocado.append(True),
        })

        coord._ejecutar_flush_y_notificar("c1", 0)

        assert invocado == [True]

    def test_hook_pre_finished_se_ejecuta_antes_de_enviar_worker_finalizado(self, mocks_infra):
        orden = []
        transporte = mocks_infra["transporte"]
        transporte.enviar.side_effect = lambda msg: orden.append("envio")

        coord = _crear_coordinador(mocks_infra, hooks={
            "pre_finished": lambda: orden.append("hook"),
        })

        coord._ejecutar_flush_y_notificar("c1", 0)

        assert orden.index("hook") < orden.index("envio")

    def test_hook_que_lanza_excepcion_impide_envio_finished(self, mocks_infra):
        def hook_crash():
            raise RuntimeError("crash simulado")

        coord = _crear_coordinador(mocks_infra, hooks={
            "pre_finished": hook_crash,
        })

        with pytest.raises(RuntimeError, match="crash simulado"):
            coord._ejecutar_flush_y_notificar("c1", 0)

        mocks_infra["transporte"].enviar.assert_not_called()

    def test_flush_sin_hooks_envia_worker_finalizado(self, mocks_infra):
        coord = _crear_coordinador(mocks_infra)

        coord._ejecutar_flush_y_notificar("c1", 0)

        mocks_infra["transporte"].enviar.assert_called_once()
        msg = mocks_infra["transporte"].enviar.call_args[0][0]
        assert msg[TIPO_MENSAJE] == TIPO_WORKER_FINALIZADO

    def test_flush_llama_al_completar_sincronizacion(self, mocks_infra):
        coord = _crear_coordinador(mocks_infra)

        coord._ejecutar_flush_y_notificar("c1", 0)

        coord._al_completar_sincronizacion.assert_called_once_with("c1", None)

    def test_flush_ya_realizado_no_llama_sincronizacion_de_nuevo(self, mocks_infra):
        coord = _crear_coordinador(mocks_infra)

        coord._ejecutar_flush_y_notificar("c1", 0)
        coord._al_completar_sincronizacion.reset_mock()

        coord._ejecutar_flush_y_notificar("c1", 0)

        coord._al_completar_sincronizacion.assert_not_called()


class TestBarreraConHooks:

    def test_iniciar_barrera_difunde_eof_recibido(self, mocks_infra):
        coord = _crear_coordinador(mocks_infra, id_nodo=0)

        coord.iniciar_barrera("c1", b"msg_original")

        mocks_infra["transporte"].enviar.assert_called_once()
        msg = mocks_infra["transporte"].enviar.call_args[0][0]
        assert msg[TIPO_MENSAJE] == TIPO_EOF_RECIBIDO
        assert msg[ORIGINADOR] == 0

    def test_barrera_completa_con_todos_los_workers(self, mocks_infra):
        coord = _crear_coordinador(mocks_infra, id_nodo=0, total_workers=2)

        coord.iniciar_barrera("c1", b"msg_original")
        mocks_infra["transporte"].enviar.reset_mock()

        coord._manejar_worker_finalizado({
            ID_CLIENTE: "c1", ORIGINADOR: 0, ID_WORKER: 0,
        })
        coord._manejar_worker_finalizado({
            ID_CLIENTE: "c1", ORIGINADOR: 0, ID_WORKER: 1,
        })

        msgs_enviados = [c[0][0] for c in mocks_infra["transporte"].enviar.call_args_list]
        tipos = [m[TIPO_MENSAJE] for m in msgs_enviados]
        assert TIPO_BARRERA_COMPLETA in tipos

    def test_worker_finalizado_de_otro_originador_se_ignora(self, mocks_infra):
        coord = _crear_coordinador(mocks_infra, id_nodo=0)

        coord.iniciar_barrera("c1", b"msg_original")
        mocks_infra["transporte"].enviar.reset_mock()

        coord._manejar_worker_finalizado({
            ID_CLIENTE: "c1", ORIGINADOR: 99, ID_WORKER: 1,
        })

        mocks_infra["transporte"].enviar.assert_not_called()
