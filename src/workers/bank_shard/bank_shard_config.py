import os
class ShardConfig:
    def __init__(self, node_id: int):
        self.base_dir = "/app/volumen"
        self.node_name_prefix = f"bank_shard_{node_id}"
        # EOFs esperados por cola: cada worker upstream envía el suyo (por su conexión TCP),
        # más el EOF real del originador de la barrera.
        self.total_tx_upstream = int(os.getenv("TOTAL_TX_UPSTREAM", "1"))
        self.total_bank_upstream = int(os.getenv("TOTAL_BANK_UPSTREAM", "1"))
