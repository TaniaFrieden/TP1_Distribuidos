import os
import sys
import threading


_SPINNER = ["⠋", "⠙", "⠹", "⠸", "⠼", "⠴", "⠦", "⠧", "⠇", "⠏"]
_HABILITADO = os.environ.get("PROGRESS_BAR", "1") != "0"
_ETIQUETA = os.environ.get("CLIENT_ID_SUFFIX", "")


def _ancho_terminal():
    try:
        return os.get_terminal_size(sys.stderr.fileno()).columns
    except (OSError, ValueError):
        return 120


def _prefijo():
    return f"[C{_ETIQUETA}] " if _ETIQUETA else ""


class ProgresoEnvio:
    """Barras de progreso para el envío de archivos."""

    def __init__(self):
        self._lock = threading.Lock()
        self._estado = {}
        self._lineas_escritas = 0

    def actualizar(self, nombre, enviados, total):
        if not _HABILITADO:
            return
        with self._lock:
            self._estado[nombre] = (enviados, total)
            if self._lineas_escritas > 0:
                sys.stderr.write(f"\033[{self._lineas_escritas}A")
            for nom, (env, tot) in sorted(self._estado.items()):
                pct = env / tot * 100 if tot > 0 else 100
                ancho = 25
                lleno = int(ancho * env / tot) if tot > 0 else ancho
                barra = "█" * lleno + "░" * (ancho - lleno)
                sys.stderr.write(f"\033[2K  {_prefijo()}{barra} {pct:5.1f}% {nom}\n")
            self._lineas_escritas = len(self._estado)
            sys.stderr.flush()


class ProgresoRecepcion:
    """Spinner y estado de queries recibidas."""

    def __init__(self):
        self._idx_spinner = 0
        self._filas_por_query = {}
        self._valor_query = {}
        self._queries_completas = set()

    def registrar_fila(self, q_id, valor=None):
        self._filas_por_query[q_id] = self._filas_por_query.get(q_id, 0) + 1
        if valor is not None:
            self._valor_query[q_id] = str(valor)

    def marcar_completa(self, q_id):
        self._queries_completas.add(q_id)
        self._filas_por_query.setdefault(q_id, 0)

    def mostrar(self):
        if not _HABILITADO:
            return
        self._idx_spinner = (self._idx_spinner + 1) % len(_SPINNER)
        partes = []
        for q_id in sorted(self._filas_por_query.keys()):
            filas = self._filas_por_query[q_id]
            display = f"={self._valor_query[q_id]}" if q_id in self._valor_query else f"{filas:,}"
            marca = "✔" if q_id in self._queries_completas else _SPINNER[self._idx_spinner]
            partes.append(f"Q{q_id}: {marca} {display}")
        linea = f"\r  {_prefijo()}Recibiendo: {' | '.join(partes)}"
        ancho = _ancho_terminal()
        if len(linea) > ancho:
            linea = linea[:ancho]
        sys.stderr.write(f"\033[2K{linea}")
        sys.stderr.flush()

    def finalizar(self):
        sys.stderr.write("\n")
        sys.stderr.flush()
