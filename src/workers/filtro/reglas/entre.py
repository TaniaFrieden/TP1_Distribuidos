from .base import ReglaFiltro


class ReglaEntre(ReglaFiltro):
    def _analizar_referencia(self, valor_crudo: str):
        limites = [lim.strip() for lim in valor_crudo.split(",")]
        if len(limites) != 2:
            raise ValueError(f"FILTER_VALUE incorrecto para between: {valor_crudo}")
        return limites

    def coincide(self, transaccion: dict) -> bool:
        if self.campo not in transaccion:
            return False
        valor = str(transaccion[self.campo])
        lim_inf, lim_sup = self.valor_ref
        largo_prefijo = min(len(lim_inf), len(lim_sup))
        return lim_inf <= valor[:largo_prefijo] <= lim_sup
