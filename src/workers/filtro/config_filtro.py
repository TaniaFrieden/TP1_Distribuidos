import os
from common.constantes_protocolo import (
    ENV_CAMPO_FILTRO,
    ENV_FILTER_FIELD,
    ENV_OPERADOR_FILTRO,
    ENV_FILTER_OPERATOR,
    ENV_VALOR_FILTRO,
    ENV_FILTER_VALUE,
)
from operadores import OP_IGUAL

class ConfigFiltro:
    def __init__(self):
        self.campo_objetivo = os.environ.get(ENV_CAMPO_FILTRO) or os.environ[ENV_FILTER_FIELD]
        self.operador_str = (os.environ.get(ENV_OPERADOR_FILTRO) or os.environ.get(ENV_FILTER_OPERATOR, OP_IGUAL)).lower()
        self.valor_objetivo_crudo = os.environ.get(ENV_VALOR_FILTRO) or os.environ[ENV_FILTER_VALUE]
