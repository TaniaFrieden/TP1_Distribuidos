import os


class ConfigProyeccion:

    def __init__(self):
        campos_str = os.environ.get("CAMPOS", "")
        self.campos = [c.strip() for c in campos_str.split(",") if c.strip()]

        enteros_str = os.environ.get("INT_FIELDS", "")
        self.campos_enteros = {f.strip() for f in enteros_str.split(",") if f.strip()}
