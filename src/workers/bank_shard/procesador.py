from common.sharding import normalizar_valor_hash

class ProcesadorRegistros:
    """
    Procesa lotes y registros individuales de transacciones y bancos
    acumulando los valores máximos de transacción en un diccionario.
    """
    @staticmethod
    def inicializar_banco_si_no_existe(client_id: str, bank_id: str, estado_agregador: dict):
        if bank_id not in estado_agregador[client_id]:
            estado_agregador[client_id][bank_id] = {
                "bank_name": "Desconocido",
                "max_amount": 0.0,
                "account": "Desconocida"
            }

    def procesar_batch_transacciones(self, client_id: str, schema: list, records: list, estado_agregador: dict) -> bool:
        from_bank_idx = schema.index("From Bank") if "From Bank" in schema else None
        amount_paid_idx = schema.index("Amount Paid") if "Amount Paid" in schema else None
        amount_received_idx = schema.index("Amount Received") if "Amount Received" in schema else None
        account_idx = schema.index("Account") if "Account" in schema else None

        hubo_cambio = False
        for record_values in records:
            bank_val = record_values[from_bank_idx] if from_bank_idx is not None else None
            bank_id = normalizar_valor_hash(bank_val)
            if not bank_id:
                continue

            self.inicializar_banco_si_no_existe(client_id, bank_id, estado_agregador)

            monto_str = "0"
            if amount_paid_idx is not None:
                monto_str = record_values[amount_paid_idx]
            elif amount_received_idx is not None:
                monto_str = record_values[amount_received_idx]
            monto = float(monto_str)

            if monto > estado_agregador[client_id][bank_id]["max_amount"]:
                estado_agregador[client_id][bank_id]["max_amount"] = monto
                if account_idx is not None:
                    estado_agregador[client_id][bank_id]["account"] = record_values[account_idx]
                hubo_cambio = True

        return hubo_cambio

    def procesar_batch_bancos(self, client_id: str, schema: list, records: list, estado_agregador: dict) -> bool:
        bank_id_idx = schema.index("Bank ID") if "Bank ID" in schema else None
        bank_name_idx = schema.index("Bank Name") if "Bank Name" in schema else None
        account_number_idx = schema.index("Account Number") if "Account Number" in schema else None

        hubo_cambio = False
        for record_values in records:
            bank_val = record_values[bank_id_idx] if bank_id_idx is not None else None
            bank_id = normalizar_valor_hash(bank_val)
            if not bank_id:
                continue

            self.inicializar_banco_si_no_existe(client_id, bank_id, estado_agregador)

            if bank_name_idx is not None:
                nuevo_nombre = record_values[bank_name_idx]
                if estado_agregador[client_id][bank_id]["bank_name"] != nuevo_nombre:
                    estado_agregador[client_id][bank_id]["bank_name"] = nuevo_nombre
                    hubo_cambio = True

            if account_number_idx is not None and estado_agregador[client_id][bank_id]["account"] == "Desconocida":
                estado_agregador[client_id][bank_id]["account"] = record_values[account_number_idx]
                hubo_cambio = True

        return hubo_cambio

    def procesar_registro_individual(self, queue_name: str, client_id: str, payload: dict, estado_agregador: dict) -> bool:
        if "transactions" in queue_name:
            bank_id = normalizar_valor_hash(payload.get("From Bank"))
        elif "banks" in queue_name:
            bank_id = normalizar_valor_hash(payload.get("Bank ID"))
        else:
            return False

        if not bank_id:
            return False

        self.inicializar_banco_si_no_existe(client_id, bank_id, estado_agregador)

        hubo_cambio = False

        if "banks" in queue_name:
            nuevo_nombre = payload.get("Bank Name", "Desconocido")
            if estado_agregador[client_id][bank_id]["bank_name"] != nuevo_nombre:
                estado_agregador[client_id][bank_id]["bank_name"] = nuevo_nombre
                hubo_cambio = True
            if estado_agregador[client_id][bank_id]["account"] == "Desconocida":
                nueva_cuenta = payload.get("Account Number", "Desconocida")
                if nueva_cuenta != "Desconocida":
                    estado_agregador[client_id][bank_id]["account"] = nueva_cuenta
                    hubo_cambio = True

        elif "transactions" in queue_name:
            monto_str = payload.get("Amount Paid", payload.get("Amount Received", "0"))
            monto = float(monto_str)
            if monto > estado_agregador[client_id][bank_id]["max_amount"]:
                estado_agregador[client_id][bank_id]["max_amount"] = monto
                estado_agregador[client_id][bank_id]["account"] = payload.get("Account", "Desconocida")
                hubo_cambio = True

        return hubo_cambio