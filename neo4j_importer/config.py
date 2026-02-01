import json
import os


class ImporterConfig:
    def __init__(self):
        self.uri = os.getenv("NEO4J_URI", "bolt://neo4j:7687")
        self.user = os.getenv("NEO4J_USER", "neo4j")
        self.password = os.getenv("NEO4J_PASSWORD", "neo4j")
        self.database = os.getenv("NEO4J_DATABASE", "neo4j")

        self.import_dir = os.getenv("IMPORT_DIR", "/data/import")
        self.cypher_dir = os.getenv("CYPHER_DIR", "/data/cypher")
        self.log_dir = os.getenv("LOG_DIR", "/data/logs")

        self.rel_label_map = self._load_json_env("REL_LABEL_MAP_JSON")
        self.dual_labels = self._load_json_env("DUAL_LABELS_JSON")

    def ensure_dirs(self):
        for path in [self.import_dir, self.cypher_dir, self.log_dir]:
            os.makedirs(path, exist_ok=True)

    @staticmethod
    def _load_json_env(var_name):
        raw = os.getenv(var_name, "").strip()
        if not raw:
            return {}
        try:
            return json.loads(raw)
        except json.JSONDecodeError:
            raise ValueError(f"Invalid JSON in {var_name}")
