from common.sharding import normalizar_valor_hash

class PayloadProcessor:
    @staticmethod
    def _get_or_create_bank(state: dict, bank_id: str) -> dict:
        if bank_id not in state:
            state[bank_id] = {
                "bank_name": "Desconocido",
                "max_amount": 0.0,
                "account": "Desconocida"
            }
        return state[bank_id]

    def process_transactions(self, state: dict, schema: list, records: list):
        """Processes transaction records and updates the bank aggregate state."""
        from_bank_idx = schema.index("From Bank") if "From Bank" in schema else None
        amount_paid_idx = schema.index("Amount Paid") if "Amount Paid" in schema else None
        amount_received_idx = schema.index("Amount Received") if "Amount Received" in schema else None
        account_idx = schema.index("Account") if "Account" in schema else None

        for record_values in records:
            bank_val = record_values[from_bank_idx] if from_bank_idx is not None else None
            bank_id = normalizar_valor_hash(bank_val)
            if not bank_id:
                continue

            bank_data = self._get_or_create_bank(state, bank_id)

            monto_str = "0"
            if amount_paid_idx is not None:
                monto_str = record_values[amount_paid_idx]
            elif amount_received_idx is not None:
                monto_str = record_values[amount_received_idx]
            monto = float(monto_str)

            if monto > bank_data["max_amount"]:
                bank_data["max_amount"] = monto
                if account_idx is not None:
                    bank_data["account"] = record_values[account_idx]

    def process_banks(self, state: dict, schema: list, records: list):
        """Processes bank metadata records and updates the bank aggregate state."""
        bank_id_idx = schema.index("Bank ID") if "Bank ID" in schema else None
        bank_name_idx = schema.index("Bank Name") if "Bank Name" in schema else None
        account_number_idx = schema.index("Account Number") if "Account Number" in schema else None

        for record_values in records:
            bank_val = record_values[bank_id_idx] if bank_id_idx is not None else None
            bank_id = normalizar_valor_hash(bank_val)
            if not bank_id:
                continue

            bank_data = self._get_or_create_bank(state, bank_id)

            if bank_name_idx is not None:
                bank_data["bank_name"] = record_values[bank_name_idx]
            if account_number_idx is not None and bank_data["account"] == "Desconocida":
                bank_data["account"] = record_values[account_number_idx]

    def process_single_bank(self, state: dict, payload: dict):
        """Processes a single bank metadata payload."""
        bank_id = normalizar_valor_hash(payload.get("Bank ID"))
        if not bank_id:
            return
        bank_data = self._get_or_create_bank(state, bank_id)
        bank_data["bank_name"] = payload.get("Bank Name", "Desconocido")
        if bank_data["account"] == "Desconocida":
            bank_data["account"] = payload.get("Account Number", "Desconocida")

    def process_single_transaction(self, state: dict, payload: dict):
        """Processes a single transaction payload."""
        bank_id = normalizar_valor_hash(payload.get("From Bank"))
        if not bank_id:
            return
        bank_data = self._get_or_create_bank(state, bank_id)
        monto_str = payload.get("Amount Paid", payload.get("Amount Received", "0"))
        monto = float(monto_str)
        if monto > bank_data["max_amount"]:
            bank_data["max_amount"] = monto
            bank_data["account"] = payload.get("Account", "Desconocida")
