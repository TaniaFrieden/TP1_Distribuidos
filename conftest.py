import sys
import os

_SRC = os.path.join(os.path.dirname(__file__), "src")
sys.path.insert(0, _SRC)
# Los workers usan `from base import BaseWorker` (path Docker donde todo queda en /app/).
# En tests exponemos src/workers/base directamente para que ese import resuelva.
sys.path.insert(0, os.path.join(_SRC, "workers", "base"))
