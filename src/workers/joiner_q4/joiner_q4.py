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

                    path = {
                        "client_id": client_id,
                        "a_bank":    a_bank,
                        "a_account": a_account,
                        "b_bank":    b_bank,
                        "b_account": b_account,
                        "c_bank":    c_bank,
                        "c_account": c_account,
                    }
                    self._enviar(json.dumps(path).encode("utf-8"), payload=path)

        logger.info(f"[JoinerQ4] Flush completo para client_id={client_id}.")

    def al_desconectar_cliente(self, client_id: str):
        with self._lock:
            self._scatter.pop(client_id, None)
            self._txns.pop(client_id, None)
        logger.info(f"[JoinerQ4] Estado descartado para {client_id}.")

    def al_cerrar(self):
        logger.info("[JoinerQ4] Apagado.")


def main():
    setup_logging("joiner_q4")
    JoinerQ4Worker().iniciar()


if __name__ == "__main__":
    main()
