import os
import json


class ConfiguracionWorker:
    def __init__(self):
        self.host_mom = os.getenv("MOM_HOST", "localhost")
        self.prefijo_nodo = os.getenv("NODE_PREFIX", "node")
        self.id_nodo = int(os.getenv("ID", "0"))
        self.total_workers = int(os.getenv("TOTAL_WORKERS", "1"))
        self.intervalo_latido = float(os.getenv("HEARTBEAT_INTERVAL_SECONDS", "5"))
        self.colas_entrada = self._parsear_json_env("INPUT_QUEUES")
        self.colas_salida = self._parsear_json_env("OUTPUT_QUEUES")

    def _parsear_json_env(self, variable_env):
        valor = os.getenv(variable_env, "[]")
        return json.loads(valor) if valor.startswith("[") else [valor]
