from constantes import OP_IGUAL, OP_MENOR, OP_ENTRE, OP_EN
from .base import ReglaFiltro
from .igual import ReglaIgual
from .menor import ReglaMenor
from .entre import ReglaEntre
from .en import ReglaEn

_REGISTRO = {
    OP_IGUAL: ReglaIgual,
    OP_MENOR: ReglaMenor,
    OP_ENTRE: ReglaEntre,
    OP_EN: ReglaEn,
}


class FabricaReglas:
    @classmethod
    def crear(cls, operador: str, campo: str, valor_crudo: str) -> ReglaFiltro:
        clase = _REGISTRO.get(operador.lower())
        if not clase:
            raise ValueError(f"Operador no soportado: {operador}")
        return clase(campo, valor_crudo)
