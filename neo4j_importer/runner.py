import logging

from .service import Neo4jDatabase
from .importer import Neo4jImporter

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("neo4j_importer")


def main():
    logger.info("[NEO4J] Importer started.")
    db = Neo4jDatabase()

    if not db.is_connected():
        logger.error("[NEO4J] Cannot connect to Neo4j. Please ensure the database is running.")
        return

    importer = Neo4jImporter(db, batch_size=2000, apoc_chunk=5000)

    logger.info("[NEO4J] Executing initialization scripts...")
    importer.import_cypher_scripts()

    logger.info("[NEO4J] Importing CSV files using APOC bulk mode...")
    importer.import_csvs()

    logger.info("[NEO4J] Import complete with APOC acceleration.")


if __name__ == "__main__":
    main()
