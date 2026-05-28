import os
import socket

def _process_file_path(val, default):
    if not val:
        val = default
    if not val.endswith('.csv'):
        val = f"{val}.csv"
    if '/' not in val and '\\' not in val:
        val = f"datasets/{val}"
    return val

TRANSACTIONS_FILE = _process_file_path(os.environ.get("TRANSACTIONS_FILE"), "transactions.csv")
ACCOUNTS_FILE     = _process_file_path(os.environ.get("ACCOUNTS_FILE"), "accounts_sample.csv")
SERVER_HOST = os.environ.get("SERVER_HOST", "localhost")
SERVER_PORT = int(os.environ.get("SERVER_PORT", 8080))
LOTE_SIZE = int(os.environ.get("BATCH_SIZE", 100))

_output_base = os.environ.get("OUTPUT_DIR", "output")
if _output_base != "output" and '/' not in _output_base and '\\' not in _output_base:
    _output_base = f"output/{_output_base}"

_append_hostname = os.environ.get("OUTPUT_APPEND_HOSTNAME", "false").lower() == "true"
OUTPUT_DIR = os.path.join(_output_base, socket.gethostname()) if _append_hostname else _output_base