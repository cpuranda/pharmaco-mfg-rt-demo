# Databricks notebook source
# MAGIC %md
# MAGIC # Shared configuration — PharmaCo Manufacturing Real-Time Line Monitoring
# MAGIC
# MAGIC Included by every numbered notebook via `%run ./_config`. One place for every
# MAGIC Unity Catalog name, endpoint and resource used by the build. **All data is 100% synthetic.**

# COMMAND ----------

import os, sys, json, time, uuid
from datetime import datetime, timezone

from databricks.sdk import WorkspaceClient

# ---- Unity Catalog namespace ------------------------------------------------
CATALOG    = "hls_streaming_solution_demo_catalog"
BRONZE     = "pharmaco_mfg_rt_bronze"       # Zerobus landing (raw, immutable)
SILVER     = "pharmaco_mfg_rt_silver"       # validated / deduplicated / quarantine
GOLD       = "pharmaco_mfg_rt_gold"         # operational gold (alerts state machine)
LDP        = "pharmaco_mfg_rt_ldp"          # Lakeflow Declarative Pipeline outputs
ML         = "pharmaco_mfg_rt_ml"           # MLflow models registered in UC
KNOWLEDGE  = "pharmaco_mfg_rt_knowledge"    # SOP corpus + Vector Search index
SCHEMAS    = [BRONZE, SILVER, GOLD, LDP, ML, KNOWLEDGE]

BRONZE_TABLE    = f"{CATALOG}.{BRONZE}.telemetry_raw"
CHECKPOINT_ROOT = f"/Volumes/{CATALOG}/{BRONZE}/checkpoints"

# ---- Ingest identity (Zerobus) ---------------------------------------------
SECRET_SCOPE   = "pharmaco_mfg_rt_demo"           # keys: sp_client_id, sp_oauth_secret
WORKSPACE_ID   = "7474651570340482"
REGION         = "us-east-1"
ZB_ENDPOINT    = f"https://{WORKSPACE_ID}.zerobus.{REGION}.cloud.databricks.com"

# ---- Lakebase (operational serving) -----------------------------------------
LAKEBASE_INSTANCE = "pharmaco-mfg-rt-demo-lb"
LAKEBASE_DB       = "databricks_postgres"
PG_SCHEMA         = "pharmaco_mfg_rt"

# ---- AI ---------------------------------------------------------------------
VS_ENDPOINT    = "pharmaco-mfg-rt-vs"
EMBED_ENDPOINT = "databricks-gte-large-en"
LLM_ENDPOINT   = "databricks-claude-sonnet-4-5"
SOP_TABLE      = f"{CATALOG}.{KNOWLEDGE}.sop_chunks"
SOP_INDEX      = f"{CATALOG}.{KNOWLEDGE}.sop_index"
ML_MODEL_NAME  = f"{CATALOG}.{ML}.asset_anomaly_detector"
KA_NAME        = "pharmaco-mfg-sop-ka"
SUPERVISOR_NAME = "pharmaco-mfg-supervisor"
GENIE_TITLE    = "PharmaCo Manufacturing — Real-Time Ops (SYNTHETIC)"

# ---- Compute / jobs / app ---------------------------------------------------
WAREHOUSE_ID   = "1b3fb1ab4eaa9511"
LDP_PIPELINE_NAME = "pharmaco_mfg_rt_ldp_medallion"
APP_NAME       = "pharmaco-mfg-rt-demo"

# ---- Ingest service principal ------------------------------------------------
INGEST_SP_APP_ID = "3663de8f-6de6-4c10-adf5-5ba7946eefed"   # pharmaco-mfg-rt-ingest-sp

# ---- Repo location in the workspace (this notebook lives in <repo>/notebooks) --
_nb_path = (dbutils.notebook.entry_point.getDbutils().notebook().getContext()
            .notebookPath().get())
REPO_ROOT = "/Workspace" + os.path.dirname(_nb_path)
while not os.path.isdir(os.path.join(REPO_ROOT, "pipeline")) and REPO_ROOT.count("/") > 2:
    REPO_ROOT = os.path.dirname(REPO_ROOT)
for sub in ["", "generator", "ingest", "pipeline", "knowledge"]:
    p = os.path.join(REPO_ROOT, sub)
    if p not in sys.path:
        sys.path.insert(0, p)

w = WorkspaceClient()
ME = w.current_user.me().user_name

def sql(stmt: str):
    """Run a Spark SQL statement and return the DataFrame (for display)."""
    return spark.sql(stmt)

def banner(msg: str):
    print("\n" + "=" * 78 + f"\n{msg}\n" + "=" * 78)

print(f"repo root : {REPO_ROOT}")
print(f"catalog   : {CATALOG}  schemas={SCHEMAS}")
print(f"run as    : {ME}   at {datetime.now(timezone.utc).isoformat(timespec='seconds')}")
