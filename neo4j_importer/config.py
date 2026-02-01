import json
import os
from typing import Any


class ImporterConfig:
    def __init__(self):
        self._config = self._load_yaml_config()

        self.uri = self._get("NEO4J_URI", "neo4j_uri", "bolt://neo4j:7687")
        self.user = self._get("NEO4J_USER", "neo4j_user", "neo4j")
        self.password = self._get("NEO4J_PASSWORD", "neo4j_password", "neo4j")
        self.database = self._get("NEO4J_DATABASE", "neo4j_database", "neo4j")

        self.import_dir = self._get("IMPORT_DIR", "import_dir", "/data/import")
        self.cypher_dir = self._get("CYPHER_DIR", "cypher_dir", "/data/cypher")
        self.log_dir = self._get("LOG_DIR", "log_dir", "/data/logs")

        self.rel_label_map = self._load_json_or_yaml("REL_LABEL_MAP_JSON", "rel_label_map")
        self.dual_labels = self._load_json_or_yaml("DUAL_LABELS_JSON", "dual_labels")

    def ensure_dirs(self):
        for path in [self.import_dir, self.cypher_dir, self.log_dir]:
            os.makedirs(path, exist_ok=True)

    def _get(self, env_key: str, yaml_key: str, default: str) -> str:
        env_val = os.getenv(env_key)
        if env_val is not None and env_val != "":
            return env_val
        return str(self._config.get(yaml_key, default))

    @staticmethod
    def _load_json_env(var_name):
        raw = os.getenv(var_name, "").strip()
        if not raw:
            return {}
        try:
            return json.loads(raw)
        except json.JSONDecodeError:
            raise ValueError(f"Invalid JSON in {var_name}")

    def _load_json_or_yaml(self, env_key: str, yaml_key: str) -> dict:
        env_raw = os.getenv(env_key, "").strip()
        if env_raw:
            return self._load_json_env(env_key)
        yaml_val = self._config.get(yaml_key, {})
        if isinstance(yaml_val, str):
            try:
                return json.loads(yaml_val)
            except json.JSONDecodeError:
                raise ValueError(f"Invalid JSON in config.yaml for {yaml_key}")
        if isinstance(yaml_val, dict):
            return yaml_val
        return {}

    @staticmethod
    def _load_yaml_config() -> dict[str, Any]:
        path = os.getenv("CONFIG_PATH", "config.yaml")
        if not os.path.exists(path):
            return {}
        try:
            import yaml
        except ImportError as exc:
            raise RuntimeError("PyYAML is required to use config.yaml") from exc

        with open(path, "r", encoding="utf-8") as f:
            data = yaml.safe_load(f) or {}
        if not isinstance(data, dict):
            raise ValueError("config.yaml must contain a top-level mapping")
        return data
