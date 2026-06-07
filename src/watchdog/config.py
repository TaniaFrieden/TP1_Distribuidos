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

        # Ring election
        self.watchdog_id = int(os.getenv("WATCHDOG_ID", "1"))
        self.num_watchdogs = int(os.getenv("NUM_WATCHDOGS", "3"))
        self.leader_heartbeat_interval = float(os.getenv("LEADER_HEARTBEAT_INTERVAL", "5"))
        self.leader_timeout_seconds = float(os.getenv("LEADER_TIMEOUT_SECONDS", "20"))
        self.election_startup_delay_max = float(os.getenv("ELECTION_STARTUP_DELAY_MAX", "3"))
        self.check_leader_interval = float(os.getenv("CHECK_LEADER_INTERVAL", "5"))
        self.election_timeout = float(os.getenv("ELECTION_TIMEOUT", "30"))
        self.suspected_dead_ttl = float(os.getenv("SUSPECTED_DEAD_TTL", "60"))

    @property
    def timeout_seconds(self):
        return self.heartbeat_interval_seconds * self.missed_heartbeats_threshold
