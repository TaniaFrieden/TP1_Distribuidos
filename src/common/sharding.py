import hashlib
import logging

# Configuración del logger para este módulo
logger = logging.getLogger(__name__)

def obtener_id_shard(valor_hash: str, total_shards: int) -> int:
    hash_hex = hashlib.md5(str(valor_hash).encode('utf-8')).hexdigest()
    shard_id = (int(hash_hex, 16) % total_shards) + 1
    
    # Log del valor entrante y el shard asignado
    logger.info(f"[ROUTING] Valor de entrada: '{valor_hash}' -> Asignado al Shard: {shard_id} (Total: {total_shards})")
    
    return shard_id

class ShardHasher:
    HEX_BASE = 16
    SHARD_OFFSET = 1

    @classmethod
    def obtener_id_shard(cls, valor_hash: str, total_shards: int) -> int:
        hash_hex = hashlib.md5(str(valor_hash).encode('utf-8')).hexdigest()
        shard_id = (int(hash_hex, cls.HEX_BASE) % total_shards) + cls.SHARD_OFFSET
        
        # Log del valor entrante y el shard asignado (versión clase)
        logger.info(f"[ROUTING] Valor de entrada: '{valor_hash}' -> Asignado al Shard: {shard_id} (Total: {total_shards})")
        
        return shard_id