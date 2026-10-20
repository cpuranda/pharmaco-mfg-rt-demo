"""
Load the synthetic SOP corpus into Unity Catalog and build the Vector Search index.
====================================================================================

Idempotent. Creates:
  * schema  {CATALOG}.{SCHEMA}
  * volume  {CATALOG}.{SCHEMA}.sop_docs        (raw markdown — used by Agent Bricks KA)
  * table   {CATALOG}.{SCHEMA}.sop_chunks       (CDF-enabled, PK=chunk_id)
  * index   {CATALOG}.{SCHEMA}.sop_index         (Delta-Sync, managed embeddings)

and grants the app service principal read access.

Run with the workspace-authenticated introspect venv:
  .venv-introspect/bin/python knowledge/load_knowledge.py
"""
from __future__ import annotations

import io
import json
import os
import time

from databricks.sdk import WorkspaceClient

PROFILE = os.environ.get("DBX_PROFILE",
                         os.environ.get("DATABRICKS_PROFILE", "fe-vm-hls-streaming-solution-demo"))
WAREHOUSE_ID = os.environ.get("DBX_WAREHOUSE_ID", "1b3fb1ab4eaa9511")
CATALOG = os.environ.get("DEMO_CATALOG", "hls_streaming_solution_demo_catalog")
SCHEMA = os.environ.get("DEMO_KNOWLEDGE_SCHEMA", "pharmaco_mfg_rt_knowledge")
VOLUME = os.environ.get("SOP_VOLUME", "sop_docs")
TABLE = f"{CATALOG}.{SCHEMA}.sop_chunks"
INDEX = f"{CATALOG}.{SCHEMA}.sop_index"
VS_ENDPOINT = os.environ.get("VS_ENDPOINT", "pharmaco-mfg-rt-vs")
EMBED_ENDPOINT = os.environ.get("EMBED_ENDPOINT", "databricks-gte-large-en")
# app service principal to grant read access; set APP_SP at deploy time
APP_SP = os.environ.get("APP_SP", "")

HERE = os.path.dirname(os.path.abspath(__file__))
w = WorkspaceClient(profile=PROFILE)


def sql(statement: str, params: list | None = None):
    r = w.statement_execution.execute_statement(
        statement=statement, warehouse_id=WAREHOUSE_ID, wait_timeout="50s",
        parameters=params or None)
    st = r.status.state.value if r.status and r.status.state else "UNKNOWN"
    if st != "SUCCEEDED":
        err = r.status.error.message if (r.status and r.status.error) else st
        raise RuntimeError(f"SQL failed ({st}): {err}\n{statement[:200]}")
    return r


def p(name, value):
    from databricks.sdk.service.sql import StatementParameterListItem
    return StatementParameterListItem(name=name, value=value)


def step_schema_volume():
    print(">> schema + volume")
    sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA} "
        f"COMMENT 'Synthetic SOP knowledge base for the PharmaCo MFG RT demo (Fix Assistant + Agent Bricks KA).'")
    sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.{SCHEMA}.{VOLUME} "
        f"COMMENT 'Raw synthetic SOP markdown documents.'")


def step_upload_docs():
    print(">> upload SOP markdown to volume")
    vol_dir = f"/Volumes/{CATALOG}/{SCHEMA}/{VOLUME}"
    for fn in sorted(os.listdir(os.path.join(HERE, "sops"))):
        if not fn.endswith(".md"):
            continue
        with open(os.path.join(HERE, "sops", fn), "rb") as f:
            data = f.read()
        w.files.upload(f"{vol_dir}/{fn}", io.BytesIO(data), overwrite=True)
    print(f"   uploaded to {vol_dir}")


def step_table():
    print(">> table sop_chunks (CDF + PK)")
    sql(f"DROP TABLE IF EXISTS {TABLE}")
    sql(f"""
        CREATE TABLE {TABLE} (
          chunk_id     STRING NOT NULL,
          sop_id       STRING,
          title        STRING,
          revision     STRING,
          section      STRING,
          asset_types  STRING,
          sensor_types STRING,
          scenarios    STRING,
          source_file  STRING,
          content      STRING,
          CONSTRAINT sop_chunks_pk PRIMARY KEY (chunk_id)
        ) TBLPROPERTIES (delta.enableChangeDataFeed = true)
    """)
    chunks = json.load(open(os.path.join(HERE, "sop_chunks.json")))
    cols = ["chunk_id", "sop_id", "title", "revision", "section",
            "asset_types", "sensor_types", "scenarios", "source_file", "content"]
    ins = (f"INSERT INTO {TABLE} ({','.join(cols)}) "
           f"VALUES ({','.join(':' + c for c in cols)})")
    for rec in chunks:
        sql(ins, [p(c, rec[c]) for c in cols])
    n = sql(f"SELECT COUNT(*) AS n FROM {TABLE}").result.data_array[0][0]
    print(f"   loaded {n} chunks")


def step_grants():
    if not APP_SP:
        print(">> grants skipped (APP_SP not set; grant after the app exists)")
        return
    print(">> grants to app service principal")
    sql(f"GRANT USE CATALOG ON CATALOG {CATALOG} TO `{APP_SP}`")
    sql(f"GRANT USE SCHEMA ON SCHEMA {CATALOG}.{SCHEMA} TO `{APP_SP}`")
    sql(f"GRANT SELECT ON TABLE {TABLE} TO `{APP_SP}`")
    sql(f"GRANT READ VOLUME ON VOLUME {CATALOG}.{SCHEMA}.{VOLUME} TO `{APP_SP}`")


def step_index():
    print(">> vector search index")
    from databricks.sdk.errors.platform import ResourceDoesNotExist
    try:
        existing = w.api_client.do("GET", f"/api/2.0/vector-search/indexes/{INDEX}")
    except ResourceDoesNotExist:
        existing = None
    if existing and existing.get("name"):
        print("   index already exists; triggering sync")
    else:
        body = {
            "name": INDEX,
            "endpoint_name": VS_ENDPOINT,
            "primary_key": "chunk_id",
            "index_type": "DELTA_SYNC",
            "delta_sync_index_spec": {
                "source_table": TABLE,
                "pipeline_type": "TRIGGERED",
                "embedding_source_columns": [
                    {"name": "content", "embedding_model_endpoint_name": EMBED_ENDPOINT}
                ],
            },
        }
        w.api_client.do("POST", "/api/2.0/vector-search/indexes", body=body)
        print("   index create requested")
    # trigger a sync (safe if just created) and wait for readiness
    try:
        w.api_client.do("POST", f"/api/2.0/vector-search/indexes/{INDEX}/sync")
    except Exception as e:
        print(f"   (sync trigger note: {e})")
    print("   waiting for index to come online + first sync ...")
    for _ in range(60):
        time.sleep(10)
        s = w.api_client.do("GET", f"/api/2.0/vector-search/indexes/{INDEX}")
        status = s.get("status", {})
        detailed = status.get("detailed_state", status.get("index_status", {}).get("detailed_state", "?"))
        ready = status.get("ready", False)
        rows = status.get("indexed_row_count", status.get("index_status", {}).get("indexed_row_count"))
        print(f"     detailed_state={detailed} ready={ready} indexed_rows={rows}")
        if ready and (rows or 0) > 0:
            print("   INDEX READY")
            return
    print("   (index not fully ready yet; it will finish syncing shortly)")


def main():
    step_schema_volume()
    step_upload_docs()
    step_table()
    step_grants()
    step_index()
    print("\nDONE. Index:", INDEX, "on endpoint:", VS_ENDPOINT)


if __name__ == "__main__":
    main()
