import os

class FilterConfig:
    def __init__(self):
        self.target_field = os.environ["FILTER_FIELD"]
        self.operator_str = os.environ.get("FILTER_OPERATOR", "eq").lower()
        self.raw_target_value = os.environ["FILTER_VALUE"]
