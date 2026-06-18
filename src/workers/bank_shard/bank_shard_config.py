from common.persistencia import VOLUMEN_DIR


class ShardConfig:
    def __init__(self, node_id: int):
        self.base_dir = VOLUMEN_DIR
        self.node_name_prefix = f"bank_shard_{node_id}"
