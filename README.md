# NeoSpool

A reusable, schema-agnostic importer that loads CSV node/relationship files into Neo4j using APOC.

## Expected CSV conventions

- **Node files**: `*_nodes.csv`
  - Required column: `id`
  - All other columns are imported as properties
- **Relationship files**: `*_relationships.csv`
  - Required columns (any of these patterns):
    - `start_id` / `end_id`
    - `source_id` / `target_id`
  - All other columns are imported as relationship properties
- Relationship type and label are derived from the filename (prefix numbers are ignored):
  - `001_Person_nodes.csv` -> label `Person`
  - `101_authored_relationships.csv` -> rel type `AUTHORED`

## Environment variables

- `NEO4J_URI` (default: `bolt://neo4j:7687`)
- `NEO4J_USER` (default: `neo4j`)
- `NEO4J_PASSWORD` (default: `neo4j`)
- `NEO4J_DATABASE` (default: `neo4j`)
- `IMPORT_DIR` (default: `/data/import`)
- `CYPHER_DIR` (default: `/data/cypher`)
- `LOG_DIR` (default: `/data/logs`)

Optional schema hints:
- `REL_LABEL_MAP_JSON`
  - JSON mapping of relationship type -> `[startLabel, endLabel]`
  - Example: `{"authored": ["Person", "Publication"]}`
- `DUAL_LABELS_JSON`
  - JSON mapping of node label -> `[labelA, labelB]` to attach two labels
  - Example: `{"OrgUnit": ["OrgUnit", "Institution"]}`

## config.yaml (optional)

You can place a `config.yaml` in the repo root (or set `CONFIG_PATH`) instead of exporting env vars.
Environment variables still override values from the file.

Example (see `config.yaml.example`):

```yaml
neo4j_uri: "bolt://localhost:7687"
neo4j_user: "neo4j"
neo4j_password: "your_password"
neo4j_database: "neo4j"
import_dir: "./import"
cypher_dir: "./cypher"
log_dir: "./logs"
```

## Build and run

```bash
docker build -t neospool:local .
docker compose -f docker-compose.example.yml up
```

Place CSVs in `./import` and optional Cypher scripts in `./cypher`.

## Replication steps

1) Put your `*_nodes.csv` and `*_relationships.csv` files in `import/` and any `.cypher` scripts in `cypher/`.
2) Build the image: `docker build -t neospool:local .`
3) Start services: `docker compose -f docker-compose.example.yml up`
4) Check logs in `logs/` and the container output for import status.

## Notes

- If `id` is missing or blank in node files, UUIDs are generated.
- If no label map is provided, relationships are matched by `id` only.
- APOC must be enabled in Neo4j (see the example compose file).
