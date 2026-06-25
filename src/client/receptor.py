import os
import json
import logging
import time
from common.message_protocol.external import TipoMensaje
from common.constantes_protocolo import CLAVE_QUERY, CLAVE_RESULTADO, CLAVE_EOF_REPORTE, CLAVE_COLUMNAS
from constantes import ARCHIVO_SOLUCION
from common.progreso import ProgresoRecepcion


class Receptor:
    """Escucha y procesa los resultados enviados por el gateway."""

    def __init__(self, conexion, queries, inicio, client_id,
                 evento_completado, persistencia):
        self._conexion = conexion
        self._inicio = inicio
        self._client_id = client_id
        self._completado = evento_completado
        self._persistencia = persistencia
        self._progreso = ProgresoRecepcion()

        self._directorio = persistencia.directorio_cliente(client_id)
        self._queries_terminadas = persistencia.cargar_queries_completadas(client_id)
        self._batch_ids_vistos = persistencia.cargar_batch_ids(client_id)
        self._archivos = {}
        self._cabeceras = {}
        self._tiempos = {q: inicio for q in queries if q not in self._queries_terminadas}

    def escuchar(self):
        try:
            while True:
                try:
                    tipo, payload = self._conexion.recibir()
                except Exception as e:
                    logging.error(f"Error de red recibiendo mensaje: {e}")
                    break

                if tipo == TipoMensaje.REPORTE:
                    batch_id = self._procesar_resultado(payload)
                    if batch_id:
                        self._enviar_ack(batch_id)
                elif tipo == TipoMensaje.FIN_DE_REGISTROS:
                    elapsed = time.perf_counter() - self._inicio
                    self._progreso.mostrar()
                    self._progreso.finalizar()
                    logging.info(f"Todas las queries completadas en {elapsed:.2f}s")
                    self._completado.set()
                    break
        finally:
            for f in self._archivos.values():
                f.close()

    def _enviar_ack(self, batch_id):
        try:
            payload = json.dumps({"batch_id": batch_id})
            self._conexion.enviar(TipoMensaje.ACK_RESULTADO, payload)
        except Exception as e:
            logging.warning(f"No se pudo enviar ACK al gateway: {e}")

    def _procesar_resultado(self, payload):
        try:
            data = json.loads(payload) if isinstance(payload, str) else payload
        except json.JSONDecodeError:
            return None

        batch_id = data.get("batch_id")
        q_id = data.get(CLAVE_QUERY)
        resultado = data.get(CLAVE_RESULTADO)
        columnas_hint = data.get(CLAVE_COLUMNAS)

        if q_id is None or q_id in self._queries_terminadas:
            logging.warning(f"[RECV] Q{q_id} ya terminada, descartando batch_id={batch_id}")
            return batch_id

        if batch_id and batch_id in self._batch_ids_vistos.get(q_id, set()):
            logging.warning(f"[RECV] Q{q_id} batch_id={batch_id} ya visto, descartando")
            return batch_id

        if q_id not in self._tiempos:
            self._tiempos[q_id] = self._inicio

        if q_id not in self._archivos:
            self._abrir_archivo(q_id)

        items = resultado if isinstance(resultado, list) else [resultado]
        datos_escritos = False
        logging.info(f"[RECV] Q{q_id} batch_id={batch_id}: {len(items)} items")

        for item in items:
            es_eof = self._es_eof(item)

            if isinstance(item, dict) and not (len(item) == 1 and es_eof):
                self._escribir_cabecera(q_id, item)
                self._escribir_datos(q_id, item)
                campos = {k: v for k, v in item.items() if k != "eof"}
                valor = str(list(campos.values())[0]) if len(campos) == 1 else None
                self._progreso.registrar_fila(q_id, valor)
                datos_escritos = True

            if es_eof:
                if columnas_hint and q_id in self._archivos and not self._cabeceras.get(q_id):
                    self._archivos[q_id].write(",".join(columnas_hint) + "\n")
                self._cerrar_archivo(q_id)
                self._finalizar_query(q_id)
                break

        if datos_escritos and batch_id:
            self._batch_ids_vistos.setdefault(q_id, set()).add(batch_id)
            self._persistencia.guardar_batch_ids(
                self._client_id, q_id, self._batch_ids_vistos[q_id]
            )

        if datos_escritos:
            self._progreso.mostrar()

        return batch_id

    def _abrir_archivo(self, q_id):
        ruta = os.path.join(self._directorio, ARCHIVO_SOLUCION.format(q_id=q_id))
        if os.path.exists(ruta) and os.path.getsize(ruta) > 0:
            self._archivos[q_id] = open(ruta, "a", encoding="utf-8")
            self._cabeceras[q_id] = True
        else:
            self._archivos[q_id] = open(ruta, "w", encoding="utf-8")
            self._cabeceras[q_id] = False

    def _escribir_cabecera(self, q_id, resultado):
        if self._cabeceras[q_id] is False:
            claves = [k for k in resultado.keys() if str(k).lower() != 'eof']
            self._cabeceras[q_id] = claves
            self._archivos[q_id].write(",".join(str(k) for k in claves) + "\n")
        elif self._cabeceras[q_id] is True:
            claves = [k for k in resultado.keys() if str(k).lower() != 'eof']
            self._cabeceras[q_id] = claves

    def _escribir_datos(self, q_id, resultado):
        claves = self._cabeceras[q_id]
        valores = [str(resultado.get(k, '')) for k in claves]
        self._archivos[q_id].write(",".join(valores) + "\n")
        self._archivos[q_id].flush()

    def _cerrar_archivo(self, q_id):
        logging.info(f"Resultados de Query {q_id} recibidos por completo.")
        if q_id in self._archivos:
            self._archivos[q_id].close()
            del self._archivos[q_id]

    def _finalizar_query(self, q_id):
        if q_id in self._batch_ids_vistos:
            del self._batch_ids_vistos[q_id]
            self._persistencia.limpiar_batch_ids(self._client_id, q_id)
        inicio_query = self._tiempos.pop(q_id, None)
        if inicio_query is not None:
            logging.info(f"[QUERY {q_id}] Finalizada en {time.perf_counter() - inicio_query:.3f} s")
        else:
            logging.info(f"[QUERY {q_id}] EOF recibido")
        self._queries_terminadas.add(q_id)
        self._progreso.marcar_completa(q_id)
        self._progreso.mostrar()
        self._persistencia.guardar_queries_completadas(
            self._client_id, self._queries_terminadas
        )

    @staticmethod
    def _es_eof(resultado):
        return isinstance(resultado, dict) and resultado.get(CLAVE_EOF_REPORTE) is True
