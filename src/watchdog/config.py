import os
import json


class WatchdogConfig:
    def __init__(self):
        self.mom_host = os.getenv("MOM_HOST", "localhost")
        self.stages = json.loads(os.getenv("WATCHDOG_STAGES", "[]"))
        self.heartbeat_interval_seconds = float(os.getenv("HEARTBEAT_INTERVAL_SECONDS", "5"))
        self.missed_heartbeats_threshold = int(os.getenv("MISSED_HEARTBEATS_THRESHOLD", "3"))
        self.check_interval_seconds = float(os.getenv("CHECK_INTERVAL_SECONDS", "2"))
        self.caidas_queue = os.getenv("CAIDAS_QUEUE", "caidas")

    @property
    def timeout_seconds(self):
        return self.heartbeat_interval_seconds * self.missed_heartbeats_threshold
