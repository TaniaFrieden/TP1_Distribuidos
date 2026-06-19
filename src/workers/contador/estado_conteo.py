import threading
from common.constantes_protocolo import CABECERA, CANTIDAD, LOTES, PAYLOAD
from persistencia_conteo import PersistenciaConteo


def calcular_cantidad(payload: dict) -> int:
    if LOTES in payload:
        return sum(
            int(lote[CABECERA].get(CANTIDAD, len(lote[PAYLOAD])))
            for lote in payload[LOTES]
        )
    return 1


class EstadoConteo:

    def __init__(self, id_nodo: int):
        self._persistencia = PersistenciaConteo(id_nodo)
        self._conteos: dict[str, int] = self._persistencia.recuperar_conteos()
        self._lock = threading.Lock()

    def incrementar(self, client_id: str, cantidad: int):
        with self._lock:
            self._conteos[client_id] = self._conteos.get(client_id, 0) + cantidad
            self._persistencia.guardar(client_id, self._conteos[client_id])

    def obtener_y_limpiar(self, client_id: str) -> int:
        with self._lock:
            return self._conteos.pop(client_id, 0)

    def descartar(self, client_id: str):
        with self._lock:
            self._conteos.pop(client_id, None)
        self._persistencia.borrar(client_id)

    def marcar_completado(self, client_id: str):
        self._persistencia.marcar_completado(client_id)
