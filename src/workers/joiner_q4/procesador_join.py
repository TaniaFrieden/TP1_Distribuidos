from acumulador_joiner import AcumuladorJoiner
from common.constantes_protocolo import CABECERA, ESQUEMA, PAYLOAD, LOTES

# Campos de aristas scatter (nombres de salida del contador_distinto)
_CAMPO_TO_BANK = "to_bank"
_CAMPO_TO_ACCOUNT = "to_account"
_CAMPO_FROM_BANK = "from_bank"
_CAMPO_FROM_ACCOUNT = "from_account"

# Campos de transacciones (nombres originales del dataset)
_CAMPO_FROM_BANK_TXN = "From Bank"
_CAMPO_ACCOUNT_TXN = "Account"
_CAMPO_TO_BANK_TXN = "To Bank"
_CAMPO_TO_ACCOUNT_TXN = "Account.1"


def _norm(v) -> str:
    """Normaliza un valor: convierte a string y elimina ceros iniciales."""
    return str(v).strip().lstrip("0") or "0"


class ProcesadorLotes:
    """
    Parsea mensajes entrantes (aristas scatter o transacciones) y acumula
    el estado en el AcumuladorJoiner.

    Distingue el tipo de mensaje por el nombre de la cola: si contiene
    "scatter" son aristas A→B; de lo contrario son transacciones B→C.
    """

    def __init__(self, acumulador: AcumuladorJoiner):
        self._acumulador = acumulador

    def procesar_payload(self, payload: dict, queue_name: str, client_id: str):
        """Despacha el procesamiento según tipo de cola y formato del mensaje."""
        es_scatter = "scatter" in queue_name
        if LOTES in payload:
            for lote in payload[LOTES]:
                if es_scatter:
                    self._procesar_lote_aristas(lote, client_id)
                else:
                    self._procesar_lote_transacciones(lote, client_id)
        else:
            if es_scatter:
                b_key = f"{_norm(payload['to_bank'])}|{_norm(payload['to_account'])}"
                a_info = (_norm(payload["from_bank"]), _norm(payload["from_account"]))
                self._acumulador.agregar_arista(client_id, b_key, a_info)
            else:
                b_key = f"{_norm(payload.get('From Bank', ''))}|{_norm(payload.get('Account', ''))}"
                c_info = (_norm(payload.get("To Bank", "")), _norm(payload.get("Account.1", "")))
                self._acumulador.agregar_transaccion(client_id, b_key, c_info)

    def _procesar_lote_aristas(self, lote: dict, client_id: str):
        """Procesa un lote de aristas A→B del flujo scatter."""
        esquema = lote[CABECERA][ESQUEMA]
        registros = lote[PAYLOAD]

        to_bank_idx = self._indice(esquema, _CAMPO_TO_BANK)
        to_account_idx = self._indice(esquema, _CAMPO_TO_ACCOUNT)
        from_bank_idx = self._indice(esquema, _CAMPO_FROM_BANK)
        from_account_idx = self._indice(esquema, _CAMPO_FROM_ACCOUNT)

        for valores in registros:
            b_key = f"{self._valor_norm(valores, to_bank_idx)}|{self._valor_norm(valores, to_account_idx)}"
            a_info = (self._valor_norm(valores, from_bank_idx), self._valor_norm(valores, from_account_idx))
            self._acumulador.agregar_arista(client_id, b_key, a_info)

    def _procesar_lote_transacciones(self, lote: dict, client_id: str):
        """Procesa un lote de transacciones B→C del flujo de txns."""
        esquema = lote[CABECERA][ESQUEMA]
        registros = lote[PAYLOAD]

        from_bank_idx = self._indice(esquema, _CAMPO_FROM_BANK_TXN)
        account_idx = self._indice(esquema, _CAMPO_ACCOUNT_TXN)
        to_bank_idx = self._indice(esquema, _CAMPO_TO_BANK_TXN)
        to_account_idx = self._indice(esquema, _CAMPO_TO_ACCOUNT_TXN)

        for valores in registros:
            b_key = f"{self._valor_norm(valores, from_bank_idx)}|{self._valor_norm(valores, account_idx)}"
            c_info = (self._valor_norm(valores, to_bank_idx), self._valor_norm(valores, to_account_idx))
            self._acumulador.agregar_transaccion(client_id, b_key, c_info)

    def _indice(self, esquema: list, campo: str) -> int | None:
        return esquema.index(campo) if campo in esquema else None

    def _valor_norm(self, registro: list, indice: int | None) -> str:
        return _norm(registro[indice] if indice is not None else "")
