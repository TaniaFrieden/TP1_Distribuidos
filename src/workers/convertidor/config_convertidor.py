import os
from common.constantes_protocolo import (
    ENV_FECHA_INICIO,
    ENV_START_DATE,
    ENV_FECHA_FIN,
    ENV_END_DATE,
)

class ConfigConvertidor:
    def __init__(self):
        self.fecha_inicio = os.environ.get(ENV_FECHA_INICIO) or os.environ.get(ENV_START_DATE, "2022-09-01")
        self.fecha_fin = os.environ.get(ENV_FECHA_FIN) or os.environ.get(ENV_END_DATE, "2022-09-05")
