import sys
import os

_SRC = os.path.join(os.path.dirname(__file__), "src")
_WORKERS = os.path.join(_SRC, "workers")

sys.path.insert(0, _SRC)
sys.path.insert(0, _WORKERS)

for _worker_dir in os.listdir(_WORKERS):
    _full = os.path.join(_WORKERS, _worker_dir)
    if os.path.isdir(_full) and _worker_dir != "base":
        sys.path.append(_full)
