class ProjectionProcessor:
    def __init__(self, fields: list, int_fields: set):
        self.fields = fields
        self.int_fields = int_fields

    def _cast_value(self, field_name: str, raw_value) -> any:
        """Casts the value to int if it's defined in int_fields, otherwise returns it as-is."""
        if field_name in self.int_fields:
            try:
                return int(raw_value)
            except (ValueError, TypeError):
                pass
        return raw_value

    def process_batch(self, batch: dict, client_id: str) -> dict | None:
        """Projects a single batch of records."""
        schema = batch["header"]["schema"]
        records = batch["payload"]

        new_schema = [col for col in self.fields if col in schema]
        col_indices = {col: i for i, col in enumerate(schema)}
        
        projected_records = []
        for record_values in records:
            new_record_values = [
                self._cast_value(col, record_values[col_indices[col]])
                for col in new_schema
            ]
            projected_records.append(new_record_values)
        
        if not projected_records:
            return None
            
        return {
            "header": {
                "schema": new_schema,
                "client_id": batch["header"].get("client_id", client_id),
                "count": len(projected_records)
            },
            "payload": projected_records
        }

    def process_payload(self, payload: dict, client_id: str) -> dict | None:
        """Filters and projects all batches inside a payload."""
        projected_batches = []
        for batch in payload.get("batches", []):
            projected = self.process_batch(batch, client_id)
            if projected:
                projected_batches.append(projected)
                
        if not projected_batches:
            return None

        resultado = {
            "client_id": client_id,
            "batches": projected_batches
        }
        if "msg_id" in payload:
            resultado["msg_id"] = payload["msg_id"]
        return resultado

    def process_single(self, transaction: dict, client_id: str) -> dict:
        """Projects a single transaction dictionary."""
        projected = {"client_id": transaction.get("client_id", client_id)}
        for campo in self.fields:
            if campo in transaction:
                projected[campo] = self._cast_value(campo, transaction[campo])
        return projected
