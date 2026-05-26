import logging
import json
import threading

from base import BaseWorker
from common.logging_setup import setup_logging

logger = logging.getLogger(__name__)


class JoinerQ4Worker(BaseWorker):
    """
    Box 2 (Gather join): sharded por B = (to_bank, to_account) del scatter /
                                          (From Bank, Account) de las transacciones.

    Recibe dos flujos:
      - scatter_edges : aristas (A→B) donde A dispersó a exactamente 5 Bs
      - transacciones : todas las transacciones del período (para detectar B→C)

    Al flush emite tripletas (A, B, C) para armar los caminos del scatter-gather.
    Incluye "Filtro celes": descarta caminos degenerados donde A==B, B==C o A==C.
    """
    def __init__(self):
        super().__init__()
        # { client_id: { b_key: [(a_bank, a_account)] } }
        self._scatter: dict = {}
        # { client_id: { b_key: set((c_bank, c_account)) } }
        self._txns: dict = {}
        self._lock = threading.Lock()

    def procesar_payload(self, queue_name: str, client_id: str, payload: dict,
                          mensaje_original: bytes, ack, nack):
        try:
            if "batches" in payload:
                with self._lock:
                    for batch in payload["batches"]:
                        header = batch["header"]
                        schema = header["schema"]
                        records = batch["payload"]
                        
                        if "scatter" in queue_name:
                            to_bank_idx = schema.index("to_bank") if "to_bank" in schema else None
                            to_account_idx = schema.index("to_account") if "to_account" in schema else None
                            from_bank_idx = schema.index("from_bank") if "from_bank" in schema else None
                            from_account_idx = schema.index("from_account") if "from_account" in schema else None
                            
                            for record_values in records:
                                to_bank = record_values[to_bank_idx] if to_bank_idx is not None else ""
                                to_account = record_values[to_account_idx] if to_account_idx is not None else ""
                                from_bank = record_values[from_bank_idx] if from_bank_idx is not None else ""
                                from_account = record_values[from_account_idx] if from_account_idx is not None else ""
                                
                                b_key = f"{to_bank}|{to_account}"
                                a_info = (from_bank, from_account)
                                self._scatter.setdefault(client_id, {}).setdefault(b_key, []).append(a_info)
                        else:
                            from_bank_idx = schema.index("From Bank") if "From Bank" in schema else None
                            account_idx = schema.index("Account") if "Account" in schema else None
                            to_bank_idx = schema.index("To Bank") if "To Bank" in schema else None
                            to_account_idx = schema.index("Account.1") if "Account.1" in schema else None
                            
                            for record_values in records:
                                from_bank = record_values[from_bank_idx] if from_bank_idx is not None else ""
                                account = record_values[account_idx] if account_idx is not None else ""
                                to_bank = record_values[to_bank_idx] if to_bank_idx is not None else ""
                                to_account = record_values[to_account_idx] if to_account_idx is not None else ""
                                
                                b_key = f"{from_bank}|{account}"
                                c_info = (to_bank, to_account)
                                self._txns.setdefault(client_id, {}).setdefault(b_key, set()).add(c_info)
            else:
                # Fallback para formato anterior
                if "scatter" in queue_name:
                    b_key = f"{payload['to_bank']}|{payload['to_account']}"
                    a_info = (payload["from_bank"], payload["from_account"])
                    with self._lock:
                        self._scatter.setdefault(client_id, {}).setdefault(b_key, []).append(a_info)
                else:
                    b_key = f"{payload.get('From Bank', '')}|{payload.get('Account', '')}"
                    c_info = (payload.get("To Bank", ""), payload.get("Account.1", ""))
                    with self._lock:
                        self._txns.setdefault(client_id, {}).setdefault(b_key, set()).add(c_info)
            ack()
        except Exception as e:
            logger.error(f"Error procesando payload: {e}", exc_info=True)
            nack()

    def al_completar_cliente(self, client_id: str):
        with self._lock:
            scatter = self._scatter.pop(client_id, {})
            txns    = self._txns.pop(client_id, {})

        records = []
        for b_key, a_list in scatter.items():
            if b_key not in txns:
                continue
            b_bank, b_account = b_key.split("|", 1)
            for c_bank, c_account in txns[b_key]:
                for a_bank, a_account in a_list:
                    # Filtro celes: sin ciclos ni auto-referencias
                    if (a_bank, a_account) == (b_bank, b_account):
                        continue
                    if (b_bank, b_account) == (c_bank, c_account):
                        continue
                    if (a_bank, a_account) == (c_bank, c_account):
                        continue

                    records.append([a_bank, a_account, b_bank, b_account, c_bank, c_account])

        if records:
            output_payload = {
                "client_id": client_id,
                "batches": [
                    {
                        "header": {
                            "schema": ["a_bank", "a_account", "b_bank", "b_account", "c_bank", "c_account"],
                            "client_id": client_id,
                            "count": len(records)
                        },
                        "payload": records
                    }
                ]
            }
            self._enviar(json.dumps(output_payload).encode("utf-8"), payload=output_payload)

        logger.info(f"[JoinerQ4] Flush completo para client_id={client_id}.")

    def al_cerrar(self):
        logger.info("[JoinerQ4] Apagado.")


def main():
    setup_logging("joiner_q4")
    JoinerQ4Worker().iniciar()


if __name__ == "__main__":
    main()
