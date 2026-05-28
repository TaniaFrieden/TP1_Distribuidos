import logging
import json
import threading
from base import BaseWorker
from common.sharding import normalizar_valor_hash
from common.logging_setup import setup_logging

logger = logging.getLogger(__name__)

class AgregadorBancarioWorker(BaseWorker):
    def __init__(self):
        super().__init__()
        self.estado_agregador = {}
        self.estado_eof = {}
        self.lock_estado = threading.Lock()
        logger.info("[AgregadorBancario] Worker inicializado con coordinación estricta de dos fases.")

    def procesar_payload(self, queue_name: str, client_id: str, payload: dict, mensaje_original: bytes, ack, nack):
        try:
            if "batches" in payload:
                with self.lock_estado:
                    if client_id not in self.estado_agregador:
                        logger.info(f"[CLIENTE NUEVO] Inicializando estado para {client_id}")
                        self.estado_agregador[client_id] = {}

                    for batch in payload["batches"]:
                        header = batch["header"]
                        schema = header["schema"]
                        records = batch["payload"]

                        if "transactions" in queue_name:
                            from_bank_idx = schema.index("From Bank") if "From Bank" in schema else None
                            amount_paid_idx = schema.index("Amount Paid") if "Amount Paid" in schema else None
                            amount_received_idx = schema.index("Amount Received") if "Amount Received" in schema else None
                            account_idx = schema.index("Account") if "Account" in schema else None

                            for record_values in records:
                                bank_val = record_values[from_bank_idx] if from_bank_idx is not None else None
                                bank_id = normalizar_valor_hash(bank_val)
                                if not bank_id:
                                    continue

                                if bank_id not in self.estado_agregador[client_id]:
                                    self.estado_agregador[client_id][bank_id] = {
                                        "bank_name": "Desconocido",
                                        "max_amount": 0.0,
                                        "account": "Desconocida"
                                    }

                                monto_str = "0"
                                if amount_paid_idx is not None:
                                    monto_str = record_values[amount_paid_idx]
                                elif amount_received_idx is not None:
                                    monto_str = record_values[amount_received_idx]
                                monto = float(monto_str)

                                if monto > self.estado_agregador[client_id][bank_id]["max_amount"]:
                                    self.estado_agregador[client_id][bank_id]["max_amount"] = monto
                                    if account_idx is not None:
                                        self.estado_agregador[client_id][bank_id]["account"] = record_values[account_idx]

                        elif "banks" in queue_name:
                            bank_id_idx = schema.index("Bank ID") if "Bank ID" in schema else None
                            bank_name_idx = schema.index("Bank Name") if "Bank Name" in schema else None
                            account_number_idx = schema.index("Account Number") if "Account Number" in schema else None

                            for record_values in records:
                                bank_val = record_values[bank_id_idx] if bank_id_idx is not None else None
                                bank_id = normalizar_valor_hash(bank_val)
                                if not bank_id:
                                    continue

                                if bank_id not in self.estado_agregador[client_id]:
                                    self.estado_agregador[client_id][bank_id] = {
                                        "bank_name": "Desconocido",
                                        "max_amount": 0.0,
                                        "account": "Desconocida"
                                    }

                                if bank_name_idx is not None:
                                    self.estado_agregador[client_id][bank_id]["bank_name"] = record_values[bank_name_idx]
                                if account_number_idx is not None and self.estado_agregador[client_id][bank_id]["account"] == "Desconocida":
                                    self.estado_agregador[client_id][bank_id]["account"] = record_values[account_number_idx]
            else:
                if "transactions" in queue_name:
                    bank_id = normalizar_valor_hash(payload.get("From Bank"))
                elif "banks" in queue_name:
                    bank_id = normalizar_valor_hash(payload.get("Bank ID"))
                else:
                    ack()
                    return

                with self.lock_estado:
                    if client_id not in self.estado_agregador:
                        logger.info(f"[CLIENTE NUEVO] Inicializando estado para {client_id}")
                        self.estado_agregador[client_id] = {}

                    if bank_id not in self.estado_agregador[client_id]:
                        self.estado_agregador[client_id][bank_id] = {
                            "bank_name": "Desconocido",
                            "max_amount": 0.0,
                            "account": "Desconocida"
                        }

                    if "banks" in queue_name:
                        self.estado_agregador[client_id][bank_id]["bank_name"] = payload.get("Bank Name", "Desconocido")
                        if self.estado_agregador[client_id][bank_id]["account"] == "Desconocida":
                            self.estado_agregador[client_id][bank_id]["account"] = payload.get("Account Number", "Desconocida")

                    elif "transactions" in queue_name:
                        monto_str = payload.get("Amount Paid", payload.get("Amount Received", "0"))
                        monto = float(monto_str)
                        if monto > self.estado_agregador[client_id][bank_id]["max_amount"]:
                            self.estado_agregador[client_id][bank_id]["max_amount"] = monto
                            self.estado_agregador[client_id][bank_id]["account"] = payload.get("Account", "Desconocida")

            ack()

        except ValueError as e:
            logger.error(f"Error de conversión numérica para el cliente {client_id}: {e}")
            nack()
        except Exception as e:
            logger.error(f"Error procesando mensaje: {e}", exc_info=True)
            nack()

    def interceptar_eof(self, queue_name: str, client_id: str, payload: dict, mensaje_original: bytes) -> bool:
        disparar_flush = False
        mensaje_barrera = None

        with self.lock_estado:
            if client_id not in self.estado_eof:
                self.estado_eof[client_id] = {
                    "transacciones_cerrado": False,
                    "bancos_cerrado": False,
                    "eof_mensaje": None,
                    "flush_iniciado": False
                }

            estado = self.estado_eof[client_id]

            if not estado["eof_mensaje"]:
                estado["eof_mensaje"] = mensaje_original

            if "transactions" in queue_name:
                logger.info(f"[BankShard] EOF Transacciones recibido para {client_id}.")
                estado["transacciones_cerrado"] = True
            elif "banks" in queue_name:
                logger.info(f"[BankShard] EOF Bancos recibido para {client_id}.")
                estado["bancos_cerrado"] = True

            if estado["transacciones_cerrado"] and estado["bancos_cerrado"] and not estado["flush_iniciado"]:
                logger.info(f"[BankShard] Ambas colas cerradas para {client_id}. Solicitando barrera de flush.")
                estado["flush_iniciado"] = True
                disparar_flush = True
                mensaje_barrera = estado["eof_mensaje"]

        if disparar_flush:
            self.coordinator.iniciar_barrera(client_id, mensaje_barrera)

        return True

    def al_completar_cliente(self, client_id: str):
        """Callback ejecutado cuando la barrera de EOFs local y global se completó."""
        with self.lock_estado:
            if client_id in self.estado_agregador:
                records = []
                for bank_id, datos in self.estado_agregador[client_id].items():
                    if datos["max_amount"] <= 0.0:
                        continue
                    if datos["bank_name"] == "Desconocido":
                        logger.warning(f"[FILTRO] Descartando banco {bank_id} para cliente {client_id}: Nombre desconocido.")
                        continue
                    
                    records.append([bank_id, datos["account"], datos["bank_name"], datos["max_amount"]])

                if records:
                    batch_payload = {
                        "client_id": client_id,
                        "batches": [
                            {
                                "header": {
                                    "schema": ["From Bank", "Account", "Bank Name", "Amount Paid"],
                                    "client_id": client_id,
                                    "count": len(records)
                                },
                                "payload": records
                            }
                        ]
                    }
                    mensaje_bytes = json.dumps(batch_payload).encode('utf-8')
                    self._enviar(mensaje_bytes, payload=batch_payload)

                logger.info(f"[BARRERA CONTROL] Envío finalizado con éxito para cliente {client_id}.")
                del self.estado_agregador[client_id]
            else:
                logger.warning(f"[BARRERA CONTROL] Se disparó al_completar_cliente para {client_id} sin datos locales registrados.")

            if client_id in self.estado_eof:
                del self.estado_eof[client_id]

    def al_cerrar(self):
        logger.info("[AgregadorBancario] Solicitud de apagado recibida de las señales del sistema.")

def __main__():
    setup_logging("bank_shard")
    worker = AgregadorBancarioWorker()
    worker.iniciar()

if __name__ == "__main__":
    __main__()