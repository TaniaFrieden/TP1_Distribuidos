import os

TRANSACTIONS_FILE = os.environ.get("TRANSACTIONS_FILE", "transactions.csv")
ACCOUNTS_FILE     = os.environ.get("ACCOUNTS_FILE", "accounts_sample.csv")
OUTPUT_DIR = os.environ.get("OUTPUT_DIR", "outputs")
SERVER_HOST = os.environ.get("SERVER_HOST", "localhost")
SERVER_PORT = int(os.environ.get("SERVER_PORT", 8080))
LOTE_SIZE = int(os.environ.get("BATCH_SIZE", 100))