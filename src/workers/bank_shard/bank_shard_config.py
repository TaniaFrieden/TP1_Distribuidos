import os
class ShardConfig:
    def __init__(self, node_id: int):
        self.base_dir = "/app/volumen"
        self.node_name_prefix = f"bank_shard_{node_id}"
