import logging
from neo4j import GraphDatabase
from .config import ImporterConfig

logger = logging.getLogger("neo4j_importer")


class Neo4jDatabase:
    def __init__(self, cfg: ImporterConfig | None = None):
        self.cfg = cfg or ImporterConfig()
        self.driver = GraphDatabase.driver(self.cfg.uri, auth=(self.cfg.user, self.cfg.password))

    def close(self):
        if self.driver:
            self.driver.close()

    def run(self, query, params=None):
        params = params or {}
        with self._session() as session:
            return session.run(query, params)

    def _session(self):
        if self.cfg.database:
            return self.driver.session(database=self.cfg.database)
        return self.driver.session()

    def is_connected(self):
        try:
            self.run("RETURN 1")
            return True
        except Exception as exc:
            logger.error(f"[NEO4J] Neo4j not reachable: {exc}")
            return False

    def bulk_add_nodes(self, label, nodes):
        query = f"""
        UNWIND $nodes AS node
        MERGE (n:{label} {{id: node.id}})
        SET n += node
        """
        self.run(query, {"nodes": nodes})

    def bulk_add_nodes_with_two_labels(self, label_a, label_b, nodes):
        query = f"""
        UNWIND $nodes AS node
        MERGE (n:{label_a}:{label_b} {{id: node.id}})
        SET n += node
        """
        self.run(query, {"nodes": nodes})
