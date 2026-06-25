from .base import ReglaComparacionBase


class ReglaIgual(ReglaComparacionBase):
    def _analizar_referencia(self, valor_crudo: str):
        return valor_crudo

    def _comparar(self, valor, valor_ref) -> bool:
        return valor == valor_ref
