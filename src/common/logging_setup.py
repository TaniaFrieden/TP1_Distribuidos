import logging
import os
from pathlib import Path


def setup_logging(service_name: str, log_file: str | None = None, level: int = logging.INFO):
    log_level_str = os.environ.get("LOG_LEVEL", "INFO").upper()
    level = getattr(logging, log_level_str, logging.INFO)

    log_path = Path(log_file or os.environ.get("LOG_FILE") or f"logs/{service_name}.txt")
    log_path.parent.mkdir(parents=True, exist_ok=True)

    formatter = logging.Formatter("%(asctime)s %(levelname)s [%(name)s] %(message)s")

    root_logger = logging.getLogger()
    root_logger.setLevel(level)

    for handler in list(root_logger.handlers):
        root_logger.removeHandler(handler)

    file_handler = logging.FileHandler(log_path, encoding="utf-8")
    file_handler.setFormatter(formatter)

    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(formatter)

    root_logger.addHandler(file_handler)
    root_logger.addHandler(stream_handler)

    logging.getLogger("pika").setLevel(logging.WARNING)

    return log_path