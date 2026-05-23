import hashlib

class ShardHasher:
    HEX_BASE = 16
    SHARD_OFFSET = 1
    
    @classmethod
    def obtener_id_shard(cls, valor_hash: str, total_shards: int) -> int:
        hash_hex = hashlib.md5(str(valor_hash).encode('utf-8')).hexdigest()
        return (int(hash_hex, cls.HEX_BASE) % total_shards) + cls.SHARD_OFFSET