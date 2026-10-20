# Databricks notebook source
# MAGIC %md
# MAGIC # 11 · End-to-end validation — ingest → govern → serve → intelligence → Genie → app
# MAGIC
# MAGIC Runs after the app is deployed (`deploy/deploy.sh app grants`) and the app acceptance
# MAGIC test (`tests/acceptance.py`) has exercised the operator workflow through the app API.
# MAGIC 1. PASS/FAIL matrix across every stage of the journey
# MAGIC 2. Unity Catalog **lineage** captured across the stages
# MAGIC 3. App-originated workflow writes visible in Lakebase

# COMMAND ----------

# MAGIC %pip install -q "psycopg[binary]==3.2.3" "mlflow[databricks]==2.22.1" scikit-learn==1.5.2
# MAGIC %restart_python

# COMMAND ----------

# MAGIC %run ./_config

# COMMAND ----------

import psycopg
inst = w.api_client.do("GET", f"/api/2.0/database/instances/{LAKEBASE_INSTANCE}")
cred = w.api_client.do("POST", "/api/2.0/database/credentials",
                       body={"request_id": str(uuid.uuid4()), "instance_names": [LAKEBASE_INSTANCE]})
def pg1(q):
    with psycopg.connect(host=inst["read_write_dns"], dbname=LAKEBASE_DB, user=ME,
                         password=cred["token"], sslmode="require") as c, c.cursor() as cur:
        cur.execute(q)
        return cur.fetchone()[0]

def s1(q):
    return spark.sql(q).first()[0]

banner("1. End-to-end validation matrix")
job = next(iter(w.jobs.list(name="pharmaco_mfg_rt_operational")))
last = next(iter(w.jobs.list_runs(job_id=job.job_id, completed_only=True, limit=1)), None)
last_run_state = last.state.result_state.value if last and last.state.result_state else "none"
G, L = f"{CATALOG}.{GOLD}", f"{CATALOG}.{LDP}"
spaces = w.api_client.do("GET", "/api/2.0/genie/spaces").get("spaces", [])
genie_ok = any(s.get("title") == GENIE_TITLE for s in spaces)
app = w.api_client.do("GET", f"/api/2.0/apps/{APP_NAME}")
idx = w.api_client.do("GET", f"/api/2.0/vector-search/indexes/{SOP_INDEX}")["status"]
from mlflow import MlflowClient
import mlflow; mlflow.set_registry_uri("databricks-uc")
champ = MlflowClient().get_model_version_by_alias(ML_MODEL_NAME, "champion")
# the supervisor endpoint the app is bound to (app resource "mas-supervisor")
sup_ep = next((r["serving_endpoint"]["name"] for r in app.get("resources", [])
               if r.get("name") == "mas-supervisor"), None)
sup_ready = str(w.serving_endpoints.get(sup_ep).state.ready) if sup_ep else "missing"

checks = [
    ("Lakeflow Connect", "Zerobus records landed in bronze", s1(f"SELECT count(*) FROM {BRONZE_TABLE} WHERE ingest_source='zerobus'"), lambda v: v > 50000),
    ("Lakeflow Connect", "ingest health rows", s1(f"SELECT count(*) FROM {CATALOG}.{BRONZE}.ingest_health"), lambda v: v >= 2),
    ("Lakeflow (streaming)", "silver deduplicated rows", s1(f"SELECT count(*) FROM {CATALOG}.{SILVER}.telemetry_clean"), lambda v: v > 50000),
    ("Lakeflow (streaming)", "duplicate source_event_id in silver", s1(f"SELECT count(*) - count(DISTINCT source_event_id) FROM {CATALOG}.{SILVER}.telemetry_clean"), lambda v: v == 0),
    ("Lakeflow (streaming)", "quarantined malformed rows", s1(f"SELECT count(*) FROM {CATALOG}.{SILVER}.telemetry_quarantine"), lambda v: v >= 8),
    ("Lakeflow (streaming)", "assets in current state", s1(f"SELECT count(*) FROM {G}.asset_current_state"), lambda v: v == 15),
    ("Lakeflow (streaming)", "CRITICAL alerts raised", s1(f"SELECT count(*) FROM {G}.active_alerts WHERE severity='CRITICAL'"), lambda v: v >= 1),
    ("Lakeflow (declarative)", "LDP silver rows (expectations enforced)", s1(f"SELECT count(*) FROM {L}.silver_telemetry"), lambda v: v > 50000),
    ("Lakeflow (declarative)", "LDP quarantine rows", s1(f"SELECT count(*) FROM {L}.silver_quarantine"), lambda v: v >= 8),
    ("Lakeflow (declarative)", "ML feature windows", s1(f"SELECT count(*) FROM {L}.gold_asset_features_20s"), lambda v: v > 1000),
    ("Lakeflow Jobs", "last operational job run", last_run_state, lambda v: v == "SUCCESS"),
    ("Unity Catalog", "tables governed in demo schemas", s1(f"SELECT count(*) FROM {CATALOG}.information_schema.tables WHERE table_schema LIKE 'pharmaco_mfg_rt%'"), lambda v: v >= 15),
    ("Unity Catalog", "governance tags on schemas", s1(f"SELECT count(*) FROM {CATALOG}.information_schema.schema_tags WHERE schema_name LIKE 'pharmaco_mfg_rt%'"), lambda v: v >= 20),
    ("Unity Catalog", "column masks", s1(f"SELECT count(*) FROM {CATALOG}.information_schema.column_masks WHERE table_schema='{GOLD}'"), lambda v: v >= 1),
    ("Lakebase", "assets served from Postgres", pg1(f"SELECT count(*) FROM {PG_SCHEMA}.asset_current_state"), lambda v: v == 15),
    ("Lakebase", "operator acknowledgements (audit log)", pg1(f"SELECT count(*) FROM {PG_SCHEMA}.operator_acknowledgements"), lambda v: v >= 1),
    ("Lakebase", "ML scores served", pg1(f"SELECT count(*) FROM {PG_SCHEMA}.asset_ml_scores"), lambda v: v == 15),
    ("ML", f"UC model {ML_MODEL_NAME.split('.')[-1]} @champion version", champ.version, lambda v: int(v) >= 1),
    ("ML", "ML score rows in gold", s1(f"SELECT count(*) FROM {G}.asset_ml_anomaly_scores"), lambda v: v >= 15),
    ("GenAI", "SOP chunks indexed in Vector Search", idx.get("indexed_row_count"), lambda v: (v or 0) >= 42),
    ("GenAI", f"Agent Bricks supervisor bound to app ({sup_ep})", sup_ready, lambda v: v.endswith("READY") and "NOT" not in v),
    ("Genie", "Genie space present", genie_ok, lambda v: v is True),
    ("Databricks App", "app compute / deployment", f"{app.get('compute_status',{}).get('state')}/{app.get('app_status',{}).get('state')}", lambda v: v == "ACTIVE/RUNNING"),
]
rows = [(stage, check, str(v), "PASS" if ok(v) else "FAIL") for stage, check, v, ok in checks]
display(spark.createDataFrame(rows, "stage string, check string, value string, result string"))
n_pass = sum(r[3] == "PASS" for r in rows)
print(f"{n_pass}/{len(rows)} checks passed")
print(f"app url: {app.get('url')}")

# COMMAND ----------

banner("2. Unity Catalog lineage captured across the journey")
display(sql(f"""
  SELECT source_table_full_name AS source, target_table_full_name AS target,
         entity_type, max(event_time) AS last_seen
  FROM system.access.table_lineage
  WHERE (target_table_full_name LIKE '{CATALOG}.pharmaco_mfg_rt%'
         OR source_table_full_name LIKE '{CATALOG}.pharmaco_mfg_rt%')
    AND event_time >= current_timestamp() - INTERVAL 2 DAYS
    AND source_table_full_name IS NOT NULL AND target_table_full_name IS NOT NULL
  GROUP BY 1, 2, 3 ORDER BY 2, 1"""))

# COMMAND ----------

banner("3. App-originated workflow writes visible in Lakebase")
with psycopg.connect(host=inst["read_write_dns"], dbname=LAKEBASE_DB, user=ME,
                     password=cred["token"], sslmode="require") as c, c.cursor() as cur:
    cur.execute(f"""SELECT alert_id, action, operator, note, created_ts
                    FROM {PG_SCHEMA}.operator_acknowledgements ORDER BY created_ts DESC LIMIT 10""")
    for row in cur.fetchall():
        print("  ", row[0][:12] + "…", row[1:])
    cur.execute(f"SELECT scenario, run_state, updated_by, updated_ts FROM {PG_SCHEMA}.demo_control_state")
    print("demo_control_state:", cur.fetchone())
assert n_pass == len(rows), "some end-to-end checks failed — see matrix above"
print("ALL END-TO-END CHECKS PASSED")
