# Executed notebook: `11_end_to_end_validation`

> Exported from Databricks job run **533307938457043** / task run **22514294817370** — result **SUCCESS**, executed 2026-10-20T13:56:02+00:00 on serverless compute in workspace `https://fevm-hls-streaming-solution-demo.cloud.databricks.com`. Outputs below are the real cell outputs of that run.

# 11 · End-to-end validation — ingest → govern → serve → intelligence → Genie → app

Runs after the app is deployed (`deploy/deploy.sh app grants`) and the app acceptance
test (`tests/acceptance.py`) has exercised the operator workflow through the app API.
1. PASS/FAIL matrix across every stage of the journey
2. Unity Catalog **lineage** captured across the stages
3. App-originated workflow writes visible in Lakebase

```python
%pip install -q "psycopg[binary]==3.2.3" "mlflow[databricks]==2.22.1" scikit-learn==1.5.2
%restart_python
```

**Output:**

```text
ERROR: pip's dependency resolver does not currently take into account all the packages that are installed. This behaviour is the source of the following dependency conflicts.
jupyter-server 1.23.4 requires anyio<4,>=3.1.0, but you have anyio 4.15.1 which is incompatible.
Note: you may need to restart the kernel using %restart_python or dbutils.library.restartPython() to use updated packages.
```

```python
%run ./_config
```

```python

```

**Output:**

```text
repo root : /Workspace/Users/chandrashekhar.purandare@databricks.com/pharmaco_mfg_rt_demo
catalog   : hls_streaming_solution_demo_catalog  schemas=['pharmaco_mfg_rt_bronze', 'pharmaco_mfg_rt_silver', 'pharmaco_mfg_rt_gold', 'pharmaco_mfg_rt_ldp', 'pharmaco_mfg_rt_ml', 'pharmaco_mfg_rt_knowledge']
run as    : chandrashekhar.purandare@databricks.com   at 2026-10-20T13:57:08+00:00
```

```python
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
```

**Output:**

```text

==============================================================================
1. End-to-end validation matrix
==============================================================================
```

| stage | check | value | result |
|---|---|---|---|
| Lakeflow Connect | Zerobus records landed in bronze | 121493 | PASS |
| Lakeflow Connect | ingest health rows | 2 | PASS |
| Lakeflow (streaming) | silver deduplicated rows | 121380 | PASS |
| Lakeflow (streaming) | duplicate source_event_id in silver | 0 | PASS |
| Lakeflow (streaming) | quarantined malformed rows | 8 | PASS |
| Lakeflow (streaming) | assets in current state | 15 | PASS |
| Lakeflow (streaming) | CRITICAL alerts raised | 2 | PASS |
| Lakeflow (declarative) | LDP silver rows (expectations enforced) | 121380 | PASS |
| Lakeflow (declarative) | LDP quarantine rows | 8 | PASS |
| Lakeflow (declarative) | ML feature windows | 1590 | PASS |
| Lakeflow Jobs | last operational job run | SUCCESS | PASS |
| Unity Catalog | tables governed in demo schemas | 25 | PASS |
| Unity Catalog | governance tags on schemas | 42 | PASS |
| Unity Catalog | column masks | 1 | PASS |
| Lakebase | assets served from Postgres | 15 | PASS |
| Lakebase | operator acknowledgements (audit log) | 2 | PASS |
| Lakebase | ML scores served | 15 | PASS |
| ML | UC model asset_anomaly_detector @champion version | 4 | PASS |
| ML | ML score rows in gold | 90 | PASS |
| GenAI | SOP chunks indexed in Vector Search | 42 | PASS |
| GenAI | Agent Bricks supervisor bound to app (mas-ed49a452-endpoint) | EndpointStateReady.READY | PASS |
| Genie | Genie space present | True | PASS |
| Databricks App | app compute / deployment | ACTIVE/RUNNING | PASS |

```text
23/23 checks passed
app url: https://pharmaco-mfg-rt-demo-7474651570340482.aws.databricksapps.com
```

```python
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
```

**Output:**

```text

==============================================================================
2. Unity Catalog lineage captured across the journey
==============================================================================
```

| source | target | entity_type | last_seen |
|---|---|---|---|
| hls_streaming_solution_demo_catalog.pharmaco_mfg_rt_silver.telemetry_clean | hls_streaming_solution_demo_catalog.pharmaco_mfg_rt_gold.active_alerts | JOB | 2026-10-20T13:09:12.020Z |
| hls_streaming_solution_demo_catalog.pharmaco_mfg_rt_silver.telemetry_clean | hls_streaming_solution_demo_catalog.pharmaco_mfg_rt_gold.asset_anomaly_events | JOB | 2026-10-20T13:09:06.331Z |
| hls_streaming_solution_demo_catalog.pharmaco_mfg_rt_silver.telemetry_clean | hls_streaming_solution_demo_catalog.pharmaco_mfg_rt_gold.asset_current_state | JOB | 2026-10-20T13:09:02.981Z |
| hls_streaming_solution_demo_catalog.pharmaco_mfg_rt_silver.telemetry_clean | hls_streaming_solution_demo_catalog.pharmaco_mfg_rt_gold.line_health_summary | JOB | 2026-10-20T13:09:23.470Z |
| hls_streaming_solution_demo_catalog.pharmaco_mfg_rt_silver.telemetry_clean | hls_streaming_solution_demo_catalog.pharmaco_mfg_rt_gold.telemetry_quality_metrics | JOB | 2026-10-20T13:09:25.190Z |
| hls_streaming_solution_demo_catalog.pharmaco_mfg_rt_knowledge.sop_chunks | hls_streaming_solution_demo_catalog.pharmaco_mfg_rt_knowledge.sop_index | PIPELINE | 2026-10-20T11:44:08.653Z |
| hls_streaming_solution_demo_catalog.pharmaco_mfg_rt_ldp.silver_telemetry | hls_streaming_solution_demo_catalog.pharmaco_mfg_rt_ldp.gold_asset_features_20s | PIPELINE | 2026-10-20T13:33:36.352Z |
| hls_streaming_solution_demo_catalog.pharmaco_mfg_rt_ldp.gold_asset_reliability | hls_streaming_solution_demo_catalog.pharmaco_mfg_rt_ldp.gold_asset_reliability | PIPELINE | 2026-10-20T13:11:06.313Z |
| hls_streaming_solution_demo_catalog.pharmaco_mfg_rt_ldp.silver_telemetry | hls_streaming_solution_demo_catalog.pharmaco_mfg_rt_ldp.gold_asset_reliability | PIPELINE | 2026-10-20T13:33:36.446Z |
| hls_streaming_solution_demo_catalog.pharmaco_mfg_rt_ldp.silver_telemetry | hls_streaming_solution_demo_catalog.pharmaco_mfg_rt_ldp.gold_line_kpis_1m | PIPELINE | 2026-10-20T13:33:36.305Z |
| hls_streaming_solution_demo_catalog.pharmaco_mfg_rt_bronze.telemetry_raw | hls_streaming_solution_demo_catalog.pharmaco_mfg_rt_ldp.silver_quarantine | PIPELINE | 2026-10-20T13:33:19.383Z |
| hls_streaming_solution_demo_catalog.pharmaco_mfg_rt_bronze.telemetry_raw | hls_streaming_solution_demo_catalog.pharmaco_mfg_rt_ldp.silver_telemetry | PIPELINE | 2026-10-20T13:33:31.646Z |
| hls_streaming_solution_demo_catalog.pharmaco_mfg_rt_bronze.telemetry_raw | hls_streaming_solution_demo_catalog.pharmaco_mfg_rt_silver.telemetry_clean | JOB | 2026-10-20T13:08:51.502Z |
| hls_streaming_solution_demo_catalog.pharmaco_mfg_rt_bronze.telemetry_raw | hls_streaming_solution_demo_catalog.pharmaco_mfg_rt_silver.telemetry_quarantine | JOB | 2026-10-20T12:40:42.787Z |

```python
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
```

**Output:**

```text

==============================================================================
3. App-originated workflow writes visible in Lakebase
==============================================================================
   adf1314330df… ('ACKNOWLEDGE', 'app-acceptance-test', 'Acknowledged via app API (acceptance test)', datetime.datetime(2026, 10, 20, 13, 53, 28, 657672, tzinfo=zoneinfo.ZoneInfo(key='GMT')))
   584e1538ea51… ('ACKNOWLEDGE', 'operator.shift-a (notebook)', 'Tablet press running hot - cooling check started per SOP-TEMP-014', datetime.datetime(2026, 10, 20, 13, 35, 33, 337252, tzinfo=zoneinfo.ZoneInfo(key='GMT')))
demo_control_state: ('normal', 'stopped', None, datetime.datetime(2026, 10, 20, 13, 35, 20, 300350, tzinfo=zoneinfo.ZoneInfo(key='GMT')))
ALL END-TO-END CHECKS PASSED
```
