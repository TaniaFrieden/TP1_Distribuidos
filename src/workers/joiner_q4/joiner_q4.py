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

    def _norm(self, v) -> str:
        return str(v).strip().lstrip("0") or "0"

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
                                
                                b_key = f"{self._norm(to_bank)}|{self._norm(to_account)}"
                                a_info = (self._norm(from_bank), self._norm(from_account))
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
                                
                                b_key = f"{self._norm(from_bank)}|{self._norm(account)}"
                                c_info = (self._norm(to_bank), self._norm(to_account))
                                self._txns.setdefault(client_id, {}).setdefault(b_key, set()).add(c_info)
            else:
                if "scatter" in queue_name:
                    b_key = f"{self._norm(payload['to_bank'])}|{self._norm(payload['to_account'])}"
                    a_info = (self._norm(payload["from_bank"]), self._norm(payload["from_account"]))
                    with self._lock:
                        self._scatter.setdefault(client_id, {}).setdefault(b_key, []).append(a_info)
                else:
                    b_key = f"{self._norm(payload.get('From Bank', ''))}|{self._norm(payload.get('Account', ''))}"
                    c_info = (self._norm(payload.get("To Bank", "")), self._norm(payload.get("Account.1", "")))
                    with self._lock:
                        self._txns.setdefault(client_id, {}).setdefault(b_key, set()).add(c_info)
            ack()
        except Exception as e:
            logger.error(f"Error procesando payload: {e}", exc_info=True)
            nack()

    FLUSH_BATCH_SIZE = 1000
    SCHEMA = ["a_bank", "a_account", "b_bank", "b_account", "c_bank", "c_account"]

    def _enviar_batch(self, client_id: str, records: list):
        output_payload = {
            "client_id": client_id,
            "batches": [
                {
                    "header": {
                        "schema": self.SCHEMA,
                        "client_id": client_id,
                        "count": len(records)
                    },
                    "payload": records
                }
            ]
        }
        self._enviar(json.dumps(output_payload).encode("utf-8"), payload=output_payload)

    def al_completar_cliente(self, client_id: str):
        with self._lock:
            scatter = self._scatter.pop(client_id, {})
            txns    = self._txns.pop(client_id, {})

        logger.info(f"[JoinerQ4] scatter_keys={len(scatter)} txns_keys={len(txns)}")

        matches = [k for k in scatter if k in txns]
        logger.info(f"[JoinerQ4] keys que matchean scatter∩txns: {len(matches)}")
        if matches:
            logger.info(f"[JoinerQ4] match sample: {matches[:3]}")

        batch = []
        enviados = 0
        for b_key, a_list in scatter.items():
            if b_key not in txns:
                continue
            b_bank, b_account = b_key.split("|", 1)
            for c_bank, c_account in txns[b_key]:
                for a_bank, a_account in a_list:
                    if (a_bank, a_account) == (b_bank, b_account):
                        continue
                    if (b_bank, b_account) == (c_bank, c_account):
                        continue
                    if (a_bank, a_account) == (c_bank, c_account):
                        continue

                    batch.append([a_bank, a_account, b_bank, b_account, c_bank, c_account])

                    if len(batch) >= self.FLUSH_BATCH_SIZE:
                        self._enviar_batch(client_id, batch)
                        enviados += len(batch)
                        batch = []

        if batch:
            self._enviar_batch(client_id, batch)
            enviados += len(batch)

        logger.info(f"[JoinerQ4] Flush completo para client_id={client_id}. Registros emitidos: {enviados}.")

    def al_cerrar(self):
        logger.info("[JoinerQ4] Apagado.")


def main():
    setup_logging("joiner_q4")
    JoinerQ4Worker().iniciar()


if __name__ == "__main__":
    main()
