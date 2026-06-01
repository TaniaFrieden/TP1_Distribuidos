import os
import json

class WorkerConfig:
    """Encapsula la configuración del entorno para el worker."""
    def __init__(self):
        self.mom_host = os.getenv("MOM_HOST", "localhost")
        self.node_prefix = os.getenv("NODE_PREFIX", "node")
        self.node_id = int(os.getenv("ID", "0"))
        self.total_workers = int(os.getenv("TOTAL_WORKERS", "1"))
        self.heartbeat_interval_seconds = float(
            os.getenv("HEARTBEAT_INTERVAL_SECONDS", "5")
        )
        
        self.input_queues = self._parse_json_env("INPUT_QUEUES")
        self.output_queues = self._parse_json_env("OUTPUT_QUEUES")

    def _parse_json_env(self, env_var):
        val = os.getenv(env_var, "[]")
        return json.loads(val) if val.startswith("[") else [val]
