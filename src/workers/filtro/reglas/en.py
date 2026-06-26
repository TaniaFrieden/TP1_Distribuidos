from .base import ReglaFiltro


class ReglaEn(ReglaFiltro):
    def _analizar_referencia(self, valor_crudo: str):
        return {opt.strip() for opt in valor_crudo.split(",")}

    def coincide(self, transaccion: dict) -> bool:
        if self.campo not in transaccion:
            return False
        valor = str(transaccion[self.campo])
        return valor in self.valor_ref
