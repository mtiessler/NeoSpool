#!/bin/bash
set -e

NEO4J_URI="${NEO4J_URI:-bolt://neo4j:7687}"
NEO4J_USER="${NEO4J_USER:-neo4j}"
NEO4J_PASSWORD="${NEO4J_PASSWORD:-neo4j}"

echo "Waiting for Neo4j to be ready at ${NEO4J_URI}..."

max_attempts=30
attempt=0

until python - <<'PY' || [ $attempt -eq $max_attempts ]; do
import os
import sys
from neo4j import GraphDatabase

uri = os.getenv("NEO4J_URI", "bolt://neo4j:7687")
user = os.getenv("NEO4J_USER", "neo4j")
password = os.getenv("NEO4J_PASSWORD", "neo4j")

try:
    driver = GraphDatabase.driver(uri, auth=(user, password))
    with driver.session() as session:
        session.run("RETURN 1")
    driver.close()
    sys.exit(0)
except Exception as e:
    print(f"Connection failed: {e}")
    sys.exit(1)
PY
    attempt=$((attempt + 1))
    echo "Attempt $attempt/$max_attempts: Neo4j not ready yet..."
    sleep 2
done

if [ $attempt -eq $max_attempts ]; then
    echo "Failed to connect to Neo4j after $max_attempts attempts"
    exit 1
fi

echo "Neo4j is ready! Starting import..."
python -m neo4j_importer.runner
