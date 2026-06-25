from .base import ReglaComparacionBase


class ReglaMenor(ReglaComparacionBase):
    def _analizar_referencia(self, valor_crudo: str):
        try:
            return float(valor_crudo)
        except ValueError:
            return valor_crudo

    def _comparar(self, valor, valor_ref) -> bool:
        return valor < valor_ref
