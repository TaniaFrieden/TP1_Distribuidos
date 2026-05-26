import os
import socket

TRANSACTIONS_FILE = os.environ.get("TRANSACTIONS_FILE", "transactions.csv")
ACCOUNTS_FILE     = os.environ.get("ACCOUNTS_FILE", "accounts_sample.csv")
SERVER_HOST = os.environ.get("SERVER_HOST", "localhost")
SERVER_PORT = int(os.environ.get("SERVER_PORT", 8080))
LOTE_SIZE = int(os.environ.get("BATCH_SIZE", 100))

_output_base = os.environ.get("OUTPUT_DIR", "outputs")
_append_hostname = os.environ.get("OUTPUT_APPEND_HOSTNAME", "false").lower() == "true"
OUTPUT_DIR = os.path.join(_output_base, socket.gethostname()) if _append_hostname else _output_base