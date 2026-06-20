import os
import signal
from common.logger import obtener_logger
import threading
import json
from abc import ABC, abstractmethod
from common.message_protocol.internal import ParseadorMensajes
from common.constantes_protocolo import (
    ID_CLIENTE,
    ID_SOLICITUD,
    FIN_DE_ARCHIVO,
    DESCONEXION_CLIENTE,
)

from .configuracion import ConfiguracionWorker
from .enrutamiento import EnrutadorMensajes
from .coordinacion import CoordinadorDistribuido, ManejadorCoordinacionEof, ContadorVuelos
from .coordinacion.hooks import HOOK_PRE_FINISHED, crear_hook_crash_pre_finished
from .latido import Latido

from common.dedup_filter import DedupFilter
from common.persistencia import PersistidorEstado

logger = obtener_logger(__name__)


class WorkerBase(ABC):
    def __init__(self):
        self._cierre_solicitado = False
        self.condicion_pendiente = threading.Condition(threading.Lock())
        self._evento_cierre_latido = threading.Event()
        self._hilo_local = threading.local()

        self.configuracion = ConfiguracionWorker()
        self.contador_vuelos = ContadorVuelos()
        self.enrutador = EnrutadorMensajes(self.configuracion)
        self.filtro_dedup = DedupFilter(
            f"{self.configuracion.prefijo_nodo}_{self.configuracion.id_nodo}"
        )

        self._persistidor_conteos = PersistidorEstado(
            f"conteos_{self.configuracion.prefijo_nodo}_{self.configuracion.id_nodo}"
        )
        if not hasattr(self, "_mensajes_procesados"):
            self._mensajes_procesados = {}
        if not hasattr(self, "_mensajes_emitidos"):
            self._mensajes_emitidos = {}
        self._cargar_conteos()

        self._manejador_eof = ManejadorCoordinacionEof(
            total_colas_entrada=len(self.enrutador.colas_entrada),
            enviar_fn=self._enviar,
            interceptar_eof_fn=self.interceptar_eof,
            al_completar_eof_local_fn=self.al_completar_eof_local,
            al_completar_cliente_fn=self.al_completar_cliente,
            nombre_clase=self.__class__.__name__,
            obtener_cantidad_procesados_fn=self.obtener_cantidad_procesados,
        )

        self.coordinador = CoordinadorDistribuido(
            self.configuracion,
            al_completar_sincronizacion=self._manejador_eof.al_completar_sincronizacion,
            al_completar_barrera=self._manejador_eof.al_completar_barrera,
            contador_vuelos=self.contador_vuelos,
            hooks=self._crear_hooks_coordinador(),
            obtener_conteos_fn=lambda cid: (self.obtener_cantidad_procesados(cid), self.obtener_cantidad_emitidos(cid))
        )
        self._manejador_eof.coordinador = self.coordinador

        self._latido = Latido(
            self.configuracion.host_mom,
            self.configuracion.prefijo_nodo,
            self.configuracion.id_nodo,
            self.configuracion.intervalo_latido,
            self._evento_cierre_latido,
            self.__class__.__name__,
        )

        logger.info(
            f"[{self.__class__.__name__}] Inicializando worker: "
            f"etapa={self.configuracion.prefijo_nodo}, "
            f"id={self.configuracion.id_nodo}, "
            f"total_workers={self.configuracion.total_workers}, "
            f"input_queues={self.configuracion.colas_entrada}, "
            f"output_queues={self.configuracion.colas_salida}"
        )

        self._registrar_senales()

    def _registrar_senales(self):
        signal.signal(signal.SIGTERM, self._manejar_senal_cierre)
        signal.signal(signal.SIGINT, self._manejar_senal_cierre)

    def _manejar_senal_cierre(self, num_senal, frame):
        logger.info(f"[{self.__class__.__name__}] Señal recibida. Cierre graceful…")
        self._cierre_solicitado = True
        self._evento_cierre_latido.set()

        with self.condicion_pendiente:
            self.condicion_pendiente.notify_all()

        self.enrutador.detener_consumo()
        self.coordinador.detener_consumo()

    def iniciar(self):
        logger.info(f"[{self.__class__.__name__}] Arrancando worker…")
        try:
            hilos_entrada = []
            self._latido.iniciar()

            for nombre_cola, cola in self.enrutador.colas_entrada.items():
                hilo = threading.Thread(
                    target=self._ejecutar_hilo_consumo,
                    args=(nombre_cola, cola),
                )
                hilo.start()
                hilos_entrada.append(hilo)

            hilo_control = threading.Thread(target=self._ejecutar_coordinador)
            hilo_control.start()

            self.coordinador.procesar_barreras_recuperadas()
            self.al_iniciar_post_arranque()

            hilo_control.join()
            for hilo in hilos_entrada:
                hilo.join()

        except Exception as e:
            if not self._cierre_solicitado:
                logger.error(f"Error inesperado: {e}", exc_info=True)
                raise
        finally:
            self._cerrar()

    def _ejecutar_hilo_consumo(self, nombre_cola, cola):
        try:
            cola.start_consuming(
                lambda msg, ack, nack, q=nombre_cola: self._callback_interno(
                    q, msg, ack, nack
                )
            )
        except Exception as e:
            if not self._cierre_solicitado:
                logger.critical(
                    f"[{self.__class__.__name__}] Hilo de consumo "
                    f"'{nombre_cola}' terminó inesperadamente: {e}",
                    exc_info=True,
                )
                os._exit(1)

    def _ejecutar_coordinador(self):
        try:
            self.coordinador.iniciar_consumo()
        except Exception as e:
            if not self._cierre_solicitado:
                logger.critical(
                    f"[{self.__class__.__name__}] Hilo coordinador "
                    f"terminó inesperadamente: {e}",
                    exc_info=True,
                )
                os._exit(1)

    def _cerrar(self):
        self._evento_cierre_latido.set()

        try:
            self.al_cerrar()
        except Exception as e:
            logger.warning(f"Error en al_cerrar(): {e}")

        self.enrutador.cerrar()
        self.coordinador.cerrar()

    def _callback_interno(self, nombre_cola, mensaje, ack, nack):
        if self._cierre_solicitado:
            return nack()

        try:
            mensaje_json = ParseadorMensajes.deserializar(mensaje)
            client_id = mensaje_json.get(ID_CLIENTE)

            if not client_id:
                return ack()

            if mensaje_json.get(DESCONEXION_CLIENTE):
                self._procesar_desconexion(client_id, mensaje, mensaje_json, ack)
            elif mensaje_json.get(FIN_DE_ARCHIVO):
                self._manejador_eof.procesar_eof(
                    nombre_cola, client_id, mensaje_json, mensaje, ack
                )
            else:
                self._procesar_mensaje_datos(
                    nombre_cola, client_id, mensaje_json, mensaje, ack, nack
                )

        except json.JSONDecodeError:
            logger.warning("Mensaje no JSON omitido.")
            ack()
        except Exception as e:
            logger.error(f"Error procesando mensaje: {e}", exc_info=True)
            nack()

    def _procesar_desconexion(self, client_id, mensaje, mensaje_json, ack):
        logger.info(
            f"[{self.__class__.__name__}] {DESCONEXION_CLIENTE} "
            f"para {client_id}. Limpiando estado."
        )
        self._manejador_eof.limpiar_cliente(client_id)
        self.al_desconectar_cliente(client_id)
        self.coordinador.limpiar_cliente(client_id)
        self.filtro_dedup.limpiar_cliente(client_id)
        self._enviar(mensaje, mensaje_json)
        ack()

    def _procesar_mensaje_datos(self, nombre_cola, client_id, mensaje_json,
                                mensaje, ack, nack):
        request_id = mensaje_json.get(ID_SOLICITUD)

        if self.filtro_dedup.es_duplicado(client_id, request_id):
            logger.info(
                f"[{self.__class__.__name__}] Duplicado descartado "
                f"request_id={request_id} client_id={client_id}."
            )
            return ack()

        self._hilo_local.id_solicitud_actual = request_id
        self._hilo_local.client_id_actual = client_id
        self._hilo_local.mensajes_emitidos_temporales = 0
        self.contador_vuelos.registrar(client_id)

        def ack_wrapper():
            self.filtro_dedup.marcar_procesado(client_id, request_id)
            if isinstance(self._mensajes_procesados, dict):
                self._mensajes_procesados[client_id] = self._mensajes_procesados.get(client_id, 0) + 1
                if isinstance(self._mensajes_emitidos, dict) and hasattr(self._hilo_local, "mensajes_emitidos_temporales"):
                    self._mensajes_emitidos[client_id] = self._mensajes_emitidos.get(client_id, 0) + self._hilo_local.mensajes_emitidos_temporales
                self._persistir_conteos()
            self.contador_vuelos.descontar(client_id)
            ack()

        def nack_wrapper():
            self.contador_vuelos.descontar(client_id)
            nack()

        try:
            self.procesar_payload(
                nombre_cola, client_id, mensaje_json, mensaje,
                ack_wrapper, nack_wrapper,
            )
        finally:
            self._hilo_local.id_solicitud_actual = None
            self._hilo_local.mensajes_emitidos_temporales = 0

    def _enviar(self, mensaje: bytes, payload: dict = None):
        id_solicitud_origen = getattr(
            self._hilo_local, "id_solicitud_actual", None
        )
        client_id = getattr(
            self._hilo_local, "client_id_actual", None
        )
        is_control = False
        if payload:
            is_control = "EOF" in payload or "CLIENT_DISCONNECT" in payload
        else:
            try:
                datos = json.loads(mensaje.decode("utf-8"))
                is_control = "EOF" in datos or "CLIENT_DISCONNECT" in datos
            except Exception:
                pass

        if client_id and not is_control and isinstance(self._mensajes_emitidos, dict):
            if hasattr(self._hilo_local, "mensajes_emitidos_temporales"):
                self._hilo_local.mensajes_emitidos_temporales += 1
            else:
                self._mensajes_emitidos[client_id] = self._mensajes_emitidos.get(client_id, 0) + 1
                self._persistir_conteos()

        self.enrutador.enviar(mensaje, payload, id_solicitud_origen=id_solicitud_origen)

    def al_iniciar_post_arranque(self):
        pass

    def interceptar_eof(self, nombre_cola, client_id, payload, mensaje_original) -> bool:
        return False

    @abstractmethod
    def procesar_payload(self, nombre_cola, client_id, payload, mensaje_original, ack, nack):
        pass

    @abstractmethod
    def al_cerrar(self):
        pass

    def al_completar_eof_local(self, client_id):
        pass

    def al_completar_cliente(self, client_id):
        pass

    def al_desconectar_cliente(self, client_id):
        pass

    def _cargar_conteos(self):
        datos = self._persistidor_conteos.cargar()
        if isinstance(self._mensajes_procesados, dict):
            self._mensajes_procesados.update(datos.get("procesados", {}))
        if isinstance(self._mensajes_emitidos, dict):
            self._mensajes_emitidos.update(datos.get("emitidos", {}))

    def _persistir_conteos(self):
        self._persistidor_conteos.guardar({
            "procesados": self._mensajes_procesados,
            "emitidos": self._mensajes_emitidos
        })

    def obtener_cantidad_procesados(self, client_id: str) -> int:
        conteo = 0
        if isinstance(self._mensajes_procesados, dict):
            conteo = self._mensajes_procesados.get(client_id, 0)
        if hasattr(self, "estado") and hasattr(self.estado, "_ids_procesados"):
            return max(conteo, len(self.estado._ids_procesados.get(client_id, set())))
        if hasattr(self, "estado_clientes"):
            estado = self.estado_clientes.get(client_id)
            if estado and isinstance(estado, dict) and "ids_procesados" in estado:
                return max(conteo, len(estado["ids_procesados"]))
        return conteo

    def obtener_cantidad_emitidos(self, client_id: str) -> int:
        if isinstance(self._mensajes_emitidos, dict):
            return self._mensajes_emitidos.get(client_id, 0)
        return 0

    def _crear_hooks_coordinador(self):
        hooks = {}
        hook = crear_hook_crash_pre_finished(
            self.configuracion.prefijo_nodo, self.configuracion.id_nodo,
        )
        if hook:
            hooks[HOOK_PRE_FINISHED] = hook
        return hooks
