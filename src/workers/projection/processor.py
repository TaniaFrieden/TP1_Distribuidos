from common.constantes_protocolo import (
    CABECERA,
    ESQUEMA,
    PAYLOAD,
    ID_CLIENTE,
    CANTIDAD,
    LOTES,
    ID_SOLICITUD
)

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
        schema = batch[CABECERA][ESQUEMA]
        records = batch[PAYLOAD]

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
            CABECERA: {
                ESQUEMA: new_schema,
                ID_CLIENTE: batch[CABECERA].get(ID_CLIENTE, client_id),
                CANTIDAD: len(projected_records)
            },
            PAYLOAD: projected_records
        }

    def process_payload(self, payload: dict, client_id: str) -> dict | None:
        """Filters and projects all batches inside a payload."""
        projected_batches = []
        for batch in payload.get(LOTES, []):
            projected = self.process_batch(batch, client_id)
            if projected:
                projected_batches.append(projected)
                
        if not projected_batches:
            return None

        resultado = {
            ID_CLIENTE: client_id,
            LOTES: projected_batches
        }
        if ID_SOLICITUD in payload:
            resultado[ID_SOLICITUD] = payload[ID_SOLICITUD]
        return resultado

    def process_single(self, transaction: dict, client_id: str) -> dict:
        """Projects a single transaction dictionary."""
        projected = {ID_CLIENTE: transaction.get(ID_CLIENTE, client_id)}
        for campo in self.fields:
            if campo in transaction:
                projected[campo] = self._cast_value(campo, transaction[campo])
        return projected

