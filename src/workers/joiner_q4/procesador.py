from acumulador import AcumuladorJoiner
from constantes import (
    CLAVE_SCATTER,
    CAMPO_TO_BANK, CAMPO_TO_ACCOUNT, CAMPO_FROM_BANK, CAMPO_FROM_ACCOUNT,
    CAMPO_FROM_BANK_TXN, CAMPO_ACCOUNT_TXN, CAMPO_TO_BANK_TXN, CAMPO_TO_ACCOUNT_TXN,
)
from common.constantes_protocolo import CABECERA, ESQUEMA, PAYLOAD, LOTES


def _norm(v) -> str:
    return str(v).strip().lstrip("0") or "0"


class ProcesadorJoin:
    def __init__(self, acumulador: AcumuladorJoiner):
        self._acumulador = acumulador

    def procesar_payload(self, payload: dict, queue_name: str, client_id: str):
        es_scatter = CLAVE_SCATTER in queue_name
        if LOTES in payload:
            for lote in payload[LOTES]:
                if es_scatter:
                    self._procesar_lote_aristas(lote, client_id)
                else:
                    self._procesar_lote_transacciones(lote, client_id)
        else:
            if es_scatter:
                b_key = f"{_norm(payload[CAMPO_TO_BANK])}|{_norm(payload[CAMPO_TO_ACCOUNT])}"
                a_info = (_norm(payload[CAMPO_FROM_BANK]), _norm(payload[CAMPO_FROM_ACCOUNT]))
                self._acumulador.agregar_arista(client_id, b_key, a_info)
            else:
                b_key = f"{_norm(payload.get(CAMPO_FROM_BANK_TXN, ''))}|{_norm(payload.get(CAMPO_ACCOUNT_TXN, ''))}"
                c_info = (_norm(payload.get(CAMPO_TO_BANK_TXN, "")), _norm(payload.get(CAMPO_TO_ACCOUNT_TXN, "")))
                self._acumulador.agregar_transaccion(client_id, b_key, c_info)

    def _procesar_lote_aristas(self, lote: dict, client_id: str):
        esquema = lote[CABECERA][ESQUEMA]
        registros = lote[PAYLOAD]

        to_bank_idx = self._indice(esquema, CAMPO_TO_BANK)
        to_account_idx = self._indice(esquema, CAMPO_TO_ACCOUNT)
        from_bank_idx = self._indice(esquema, CAMPO_FROM_BANK)
        from_account_idx = self._indice(esquema, CAMPO_FROM_ACCOUNT)

        for valores in registros:
            b_key = f"{self._valor_norm(valores, to_bank_idx)}|{self._valor_norm(valores, to_account_idx)}"
            a_info = (self._valor_norm(valores, from_bank_idx), self._valor_norm(valores, from_account_idx))
            self._acumulador.agregar_arista(client_id, b_key, a_info)

    def _procesar_lote_transacciones(self, lote: dict, client_id: str):
        esquema = lote[CABECERA][ESQUEMA]
        registros = lote[PAYLOAD]

        from_bank_idx = self._indice(esquema, CAMPO_FROM_BANK_TXN)
        account_idx = self._indice(esquema, CAMPO_ACCOUNT_TXN)
        to_bank_idx = self._indice(esquema, CAMPO_TO_BANK_TXN)
        to_account_idx = self._indice(esquema, CAMPO_TO_ACCOUNT_TXN)

        for valores in registros:
            b_key = f"{self._valor_norm(valores, from_bank_idx)}|{self._valor_norm(valores, account_idx)}"
            c_info = (self._valor_norm(valores, to_bank_idx), self._valor_norm(valores, to_account_idx))
            self._acumulador.agregar_transaccion(client_id, b_key, c_info)

    def _indice(self, esquema: list, campo: str) -> int | None:
        return esquema.index(campo) if campo in esquema else None

    def _valor_norm(self, registro: list, indice: int | None) -> str:
        return _norm(registro[indice] if indice is not None else "")
