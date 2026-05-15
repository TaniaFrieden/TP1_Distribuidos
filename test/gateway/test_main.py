"""
Tests para helpers del gateway.
"""

import importlib
from unittest.mock import patch


ENV = {
    "SERVER_HOST": "127.0.0.1",
    "SERVER_PORT": "5678",
    "MOM_HOST": "localhost",
}


def _load_gateway_main():
    with patch.dict("os.environ", ENV, clear=False):
        module = importlib.import_module("gateway.main")
        return importlib.reload(module)


class TestIterCsvBatches:

    def test_divide_csv_in_batches(self):
        gateway_main = _load_gateway_main()
        csv_text = "a,b\n1,2\n3,4\n5,6\n"

        batches = list(gateway_main._iter_csv_batches(csv_text, 2))

        assert len(batches) == 2
        assert batches[0] == [{"a": "1", "b": "2"}, {"a": "3", "b": "4"}]
        assert batches[1] == [{"a": "5", "b": "6"}]

    def test_csv_without_headers_fails(self):
        gateway_main = _load_gateway_main()

        try:
            list(gateway_main._iter_csv_batches("", 2))
        except ValueError as exc:
            assert "encabezados" in str(exc)
        else:
            raise AssertionError("Se esperaba ValueError")
