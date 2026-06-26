from acumulador_grupos import AcumuladorGrupos
from common.constantes_protocolo import CABECERA, ESQUEMA, PAYLOAD, LOTES


class ProcesadorLotes:

    def __init__(self, acumulador: AcumuladorGrupos, campos_grupo: list[str], campos_valor: list[str]):
        self._acumulador = acumulador
        self._campos_grupo = campos_grupo
        self._campos_valor = campos_valor

    def procesar_payload(self, payload: dict, client_id: str):
        if LOTES in payload:
            for lote in payload[LOTES]:
                self._procesar_lote(lote, client_id)
        else:
            clave_grupo = self._construir_clave(payload, self._campos_grupo)
            clave_valor = self._construir_clave(payload, self._campos_valor)
            self._acumulador.agregar(client_id, clave_grupo, clave_valor)

    def _procesar_lote(self, lote: dict, client_id: str):
        esquema = lote[CABECERA][ESQUEMA]
        registros = lote[PAYLOAD]

        indices_grupo = self._resolver_indices(esquema, self._campos_grupo)
        indices_valor = self._resolver_indices(esquema, self._campos_valor)

        for valores_registro in registros:
            clave_grupo = self._clave_desde_indices(valores_registro, indices_grupo)
            clave_valor = self._clave_desde_indices(valores_registro, indices_valor)
            self._acumulador.agregar(client_id, clave_grupo, clave_valor)

    def _construir_clave(self, registro: dict, campos: list[str]) -> tuple:
        return tuple(str(registro.get(f, "")) for f in campos)

    def _resolver_indices(self, esquema: list[str], campos: list[str]) -> list:
        return [esquema.index(f) if f in esquema else None for f in campos]

    def _clave_desde_indices(self, registro: list, indices: list) -> tuple:
        return tuple(str(registro[i]) if i is not None else "" for i in indices)
