import logging
import os
import re
import time
import uuid
from datetime import datetime

import pandas as pd
from tqdm import tqdm

from .service import Neo4jDatabase

logger = logging.getLogger("neo4j_importer")


class Neo4jImporter:
    def __init__(self, db: Neo4jDatabase, batch_size: int = 10000, apoc_chunk: int = 50000):
        self.db = db
        self.cfg = db.cfg
        self.cfg.ensure_dirs()
        self.batch_size = batch_size
        self.apoc_chunk = apoc_chunk

        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        self.log_file = os.path.join(self.cfg.log_dir, f"import_log_{timestamp}.txt")

    def _strip_numeric_prefix(self, name):
        return re.sub(r"^\d+_", "", name)

    def _clean_id(self, value):
        if pd.isna(value):
            return None
        s = str(value).strip()
        s = re.sub(r"\.0$", "", s)
        return None if s in ["", "nan", "None"] else s

    def _stringify_dict(self, d):
        return {k: self._clean_id(v) if "id" in k.lower() else v for k, v in d.items()}

    def _load_csv_as_strings(self, file_path):
        return pd.read_csv(file_path, dtype=str, keep_default_na=False).fillna("")

    def _get_label_from_filename(self, filename):
        return self._strip_numeric_prefix(os.path.splitext(filename)[0].replace("_nodes", ""))

    def _get_rel_type_from_filename(self, filename):
        return self._strip_numeric_prefix(os.path.splitext(filename)[0].replace("_relationships", ""))

    def _extract_relationship_ids(self, row):
        start_id = end_id = None
        for key in row.index:
            k = key.lower()
            if ("start" in k or "source" in k) and "id" in k:
                start_id = self._clean_id(row[key])
            elif ("end" in k or "target" in k) and "id" in k:
                end_id = self._clean_id(row[key])
        return start_id, end_id

    def _extract_relationship_properties(self, row):
        return {
            k: v for k, v in row.items()
            if "id" not in k.lower() and k.lower() != "type" and v not in ("", "nan", "None")
        }

    def import_cypher_scripts(self):
        files = sorted(f for f in os.listdir(self.cfg.cypher_dir) if f.endswith(".cypher"))
        if not files:
            logger.info("[NEO4J] No Cypher scripts found.")
            return
        for name in files:
            path = os.path.join(self.cfg.cypher_dir, name)
            logger.info(f"[NEO4J] Executing Cypher script: {name}")
            with open(path, "r", encoding="utf-8") as f:
                script = f.read()
            for stmt in [s.strip() for s in script.split(";") if s.strip()]:
                self.db.run(stmt)
        logger.info("[NEO4J] Cypher scripts executed successfully.")

    def _import_node_file(self, file_name):
        path = os.path.join(self.cfg.import_dir, file_name)
        label = self._get_label_from_filename(file_name)
        df = self._load_csv_as_strings(path)

        if "id" not in df.columns:
            df["id"] = [str(uuid.uuid4()) for _ in range(len(df))]
            logger.warning(f"[NEO4J] '{file_name}' missing id -> generated UUIDs.")

        mask = df["id"].str.strip().isin(["", "nan", "None"])
        if mask.any():
            count = mask.sum()
            df.loc[mask, "id"] = [str(uuid.uuid4()) for _ in range(count)]
            logger.warning(f"[NEO4J] {count} null IDs in '{file_name}' -> replaced with UUIDs.")

        records = [self._stringify_dict(r) for r in df.to_dict(orient="records")]
        total = len(records)
        logger.info(f"[NEO4J] Ingesting {total} nodes for '{label}'...")

        with tqdm(total=total, desc=f"Nodes:{label}", unit="rows") as pbar:
            for i in range(0, total, self.batch_size):
                batch = records[i : i + self.batch_size]
                try:
                    if label in self.cfg.dual_labels:
                        primary, secondary = self.cfg.dual_labels[label]
                        self.db.bulk_add_nodes_with_two_labels(primary, secondary, batch)
                    else:
                        self.db.bulk_add_nodes(label, batch)
                except Exception as e:
                    logger.error(f"[NEO4J] Node batch failed ({label}): {e}")
                pbar.update(len(batch))
        logger.info(f"[NEO4J] Completed node import for '{label}'.")

    def _import_relationship_file(self, file_name):
        path = os.path.join(self.cfg.import_dir, file_name)
        rel_type = self._get_rel_type_from_filename(file_name)
        df = self._load_csv_as_strings(path)

        formatted, skipped = [], 0
        for _, row in df.iterrows():
            start_id, end_id = self._extract_relationship_ids(row)
            if not start_id or not end_id:
                skipped += 1
                continue
            formatted.append(
                {
                    "start_id": start_id,
                    "end_id": end_id,
                    "props": self._extract_relationship_properties(row),
                }
            )

        if skipped:
            logger.warning(f"[NEO4J] Skipped {skipped} malformed rows in '{file_name}'.")
        if not formatted:
            logger.warning(f"[NEO4J] No valid relationships found in '{file_name}'.")
            return

        total = len(formatted)
        logger.info(f"[NEO4J] Creating {total} '{rel_type}' relationships via APOC (streamed)...")

        start_label, end_label = (None, None)
        rel_key = rel_type.lower()
        if rel_key in self.cfg.rel_label_map:
            start_label, end_label = self.cfg.rel_label_map[rel_key]

        if start_label and end_label:
            query = f"""
            CALL apoc.periodic.iterate(
              "UNWIND $rels AS rel RETURN rel",
              "
              MATCH (a:{start_label} {{id: rel.start_id}})
              MATCH (b:{end_label} {{id: rel.end_id}})
              MERGE (a)-[r:{rel_type.upper()}]->(b)
              SET r += rel.props
              ",
              {{params:{{rels:$rels}}, batchSize:10000, parallel:true}}
            )
            """
        else:
            query = f"""
            CALL apoc.periodic.iterate(
              "UNWIND $rels AS rel RETURN rel",
              "
              MATCH (a {{id: rel.start_id}})
              MATCH (b {{id: rel.end_id}})
              MERGE (a)-[r:{rel_type.upper()}]->(b)
              SET r += rel.props
              ",
              {{params:{{rels:$rels}}, batchSize:10000, parallel:true}}
            )
            """

        with tqdm(total=total, desc=f"Rels:{rel_type}", unit="rels") as pbar:
            for i in range(0, total, self.apoc_chunk):
                chunk = formatted[i : i + self.apoc_chunk]
                try:
                    self.db.run(query, {"rels": chunk})
                except Exception as e:
                    logger.error(f"[NEO4J] Error in APOC batch {i//self.apoc_chunk}: {e}")
                pbar.update(len(chunk))
                time.sleep(0.5)

        logger.info(f"[NEO4J] Relationship import complete for '{rel_type}'.")

    def import_csvs(self):
        csv_files = sorted(f for f in os.listdir(self.cfg.import_dir) if f.endswith(".csv"))
        if not csv_files:
            logger.info("[NEO4J] No CSV files found in import dir.")
            return

        node_files = [f for f in csv_files if f.endswith("_nodes.csv")]
        rel_files = [f for f in csv_files if f.endswith("_relationships.csv")]

        logger.info("[NEO4J] Importing nodes...")
        for f in node_files:
            self._import_node_file(f)

        logger.info("[NEO4J] Importing relationships (streamed batches)...")
        for f in rel_files:
            self._import_relationship_file(f)

        logger.info("[NEO4J] CSV ingestion completed successfully.")
