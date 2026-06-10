class BatchProcessor:
    def __init__(self, filter_worker):
        self.worker = filter_worker

    def process_batch(self, batch: dict) -> dict | None:
        """Filters a single batch of records. Returns the filtered batch or None if empty."""
        schema = batch["header"]["schema"]
        records = batch["payload"]
        
        filtered = [
            rec for rec in records
            if self.worker.matches(dict(zip(schema, rec)))
        ]
        
        if not filtered:
            return None
            
        return {
            "header": {
                "schema": schema,
                "client_id": batch["header"].get("client_id"),
                "count": len(filtered)
            },
            "payload": filtered
        }

    def process_payload(self, payload: dict) -> dict | None:
        """Processes a payload containing multiple batches. Returns the filtered payload or None if empty."""
        client_id = payload.get("client_id")
        filtered_batches = []
        
        for batch in payload.get("batches", []):
            filtered = self.process_batch(batch)
            if filtered:
                filtered_batches.append(filtered)
                
        if not filtered_batches:
            return None

        resultado = {
            "client_id": client_id,
            "batches": filtered_batches
        }
        if "msg_id" in payload:
            resultado["msg_id"] = payload["msg_id"]
        return resultado
