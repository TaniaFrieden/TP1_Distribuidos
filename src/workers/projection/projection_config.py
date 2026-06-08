import os

class ProjectionConfig:
    def __init__(self):
        fields_str = os.environ.get("CAMPOS", "")
        self.fields = [c.strip() for c in fields_str.split(",") if c.strip()]

        int_fields_str = os.environ.get("INT_FIELDS", "")
        self.int_fields = {f.strip() for f in int_fields_str.split(",") if f.strip()}
