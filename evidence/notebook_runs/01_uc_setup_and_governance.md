# Executed notebook: `01_uc_setup_and_governance`

> Exported from Databricks job run **24808449534624** / task run **475620826123141** — result **SUCCESS**, executed 2026-10-20T13:24:38+00:00 on serverless compute in workspace `https://fevm-hls-streaming-solution-demo.cloud.databricks.com`. Outputs below are the real cell outputs of that run.

# 01 · Unity Catalog — namespace, tables & governance

**Business problem.** PharmaCo's oral-solid-dose, sterile-fill and packaging lines lose
batches and hours of OEE when equipment drifts out of its validated operating band
(tablet-press temperature, aseptic-filler vibration, lyophilizer pressure) and nobody
notices until QA raises a deviation. This build turns raw line telemetry into governed,
real-time operator action.

This notebook creates the **governed foundation**:
* medallion schemas (bronze / silver / gold) + LDP, ML and knowledge schemas
* Delta tables from `sql/01_bronze.sql`, `02_silver.sql`, `03_gold.sql`
* governance tags, comments, a column mask on operator identity, and least-privilege
  grants for the Zerobus ingest service principal

All data is **synthetic**.

```python
%run ./_config
```

```python

```

**Output:**

```text
repo root : /Workspace/Users/chandrashekhar.purandare@databricks.com/pharmaco_mfg_rt_demo
catalog   : hls_streaming_solution_demo_catalog  schemas=['pharmaco_mfg_rt_bronze', 'pharmaco_mfg_rt_silver', 'pharmaco_mfg_rt_gold', 'pharmaco_mfg_rt_ldp', 'pharmaco_mfg_rt_ml', 'pharmaco_mfg_rt_knowledge']
run as    : chandrashekhar.purandare@databricks.com   at 2026-10-20T13:24:53+00:00
```

```python
banner("1. Schemas + volumes")
schema_comments = {
    BRONZE:    "Raw synthetic plant telemetry landed by Zerobus Ingest (immutable).",
    SILVER:    "Validated, normalized, deduplicated telemetry + quarantine.",
    GOLD:      "Operational gold: asset state, alerts state-machine, line health, ML scores.",
    LDP:       "Lakeflow Declarative Pipeline outputs: DQ-enforced silver + analytical features.",
    ML:        "MLflow models registered in Unity Catalog (asset anomaly detector).",
    KNOWLEDGE: "Synthetic SOP library + Vector Search index for the Fix Assistant.",
}
for s, c in schema_comments.items():
    sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{s} COMMENT '{c}'")
    print(f"schema  {CATALOG}.{s}")
for v in ["checkpoints", "artifacts"]:
    sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.{BRONZE}.{v}")
    print(f"volume  {CATALOG}.{BRONZE}.{v}")
```

**Output:**

```text

==============================================================================
1. Schemas + volumes
==============================================================================
schema  hls_streaming_solution_demo_catalog.pharmaco_mfg_rt_bronze
schema  hls_streaming_solution_demo_catalog.pharmaco_mfg_rt_silver
schema  hls_streaming_solution_demo_catalog.pharmaco_mfg_rt_gold
schema  hls_streaming_solution_demo_catalog.pharmaco_mfg_rt_ldp
schema  hls_streaming_solution_demo_catalog.pharmaco_mfg_rt_ml
schema  hls_streaming_solution_demo_catalog.pharmaco_mfg_rt_knowledge
volume  hls_streaming_solution_demo_catalog.pharmaco_mfg_rt_bronze.checkpoints
volume  hls_streaming_solution_demo_catalog.pharmaco_mfg_rt_bronze.artifacts
```

```python
import re
banner("2. Delta tables from the versioned DDL in sql/")
subs = {"${cat}": CATALOG, "${bronze}": BRONZE, "${silver}": SILVER, "${gold}": GOLD}
for fn in ["01_bronze.sql", "02_silver.sql", "03_gold.sql"]:
    text = open(os.path.join(REPO_ROOT, "sql", fn)).read()
    for k, v in subs.items():
        text = text.replace(k, v)
    # strip line comments, split on statement terminators
    body = "\n".join(l for l in text.splitlines() if not l.strip().startswith("--"))
    stmts = [s.strip() for s in re.split(r";\s*(?:\n|$)", body) if s.strip()]
    for st in stmts:
        sql(st)
    print(f"{fn}: {len(stmts)} statements applied")

# ML score table (written by notebook 06 + the scoring job task)
sql(f"""
CREATE TABLE IF NOT EXISTS {CATALOG}.{GOLD}.asset_ml_anomaly_scores (
  asset_id STRING, line_id STRING, window_start TIMESTAMP, window_end TIMESTAMP,
  ml_anomaly_score DOUBLE COMMENT '0..1, higher = more anomalous (IsolationForest)',
  ml_is_anomaly BOOLEAN, top_sensor STRING COMMENT 'sensor contributing most to the score',
  model_name STRING, model_version STRING, scored_ts TIMESTAMP)
COMMENT 'ML anomaly-detector scores per asset per 20s window (SYNTHETIC)'
""")
display(sql(f"""
  SELECT table_schema, table_name, table_type, comment
  FROM {CATALOG}.information_schema.tables
  WHERE table_schema IN ('{BRONZE}','{SILVER}','{GOLD}')
  ORDER BY table_schema, table_name"""))
```

**Output:**

```text

==============================================================================
2. Delta tables from the versioned DDL in sql/
==============================================================================
01_bronze.sql: 2 statements applied
02_silver.sql: 2 statements applied
03_gold.sql: 5 statements applied
```

| table_schema | table_name | table_type | comment |
|---|---|---|---|
| pharmaco_mfg_rt_bronze | ingest_health | MANAGED | Zerobus / ingest driver health, ack latency, rejects (SYNTHETIC demo) |
| pharmaco_mfg_rt_bronze | telemetry_raw | MANAGED | SYNTHETIC raw plant telemetry landed via Zerobus Ingest (PharmaCo MFG RT demo) |
| pharmaco_mfg_rt_gold | active_alerts | MANAGED | Current alert view + recently resolved (SYNTHETIC). History preserved here and in asset_anomaly_events. |
| pharmaco_mfg_rt_gold | asset_anomaly_events | MANAGED | Immutable anomaly/threshold/recovery event history (SYNTHETIC) |
| pharmaco_mfg_rt_gold | asset_current_state | MANAGED | Latest state per asset (SYNTHETIC) |
| pharmaco_mfg_rt_gold | asset_ml_anomaly_scores | MANAGED | ML anomaly-detector scores per asset per 20s window (SYNTHETIC) |
| pharmaco_mfg_rt_gold | line_health_summary | MANAGED | Per-line health & slowdown summary (SYNTHETIC) |
| pharmaco_mfg_rt_gold | telemetry_quality_metrics | MANAGED | Pipeline DQ & throughput metrics per window (SYNTHETIC) |
| pharmaco_mfg_rt_silver | telemetry_clean | MANAGED | Validated/normalized/deduplicated telemetry (SYNTHETIC) |
| pharmaco_mfg_rt_silver | telemetry_quarantine | MANAGED | Rejected/malformed telemetry for DQ inspection (SYNTHETIC) |

```python
banner("3. Governance tags (schema + table level)")
# keys/values respect the account's tag policies (domain, industry, medallion_layer, contains_pii)
layer_of = {BRONZE: "bronze", SILVER: "silver", GOLD: "gold", LDP: "silver", ML: "gold",
            KNOWLEDGE: "gold"}
workload_of = {BRONZE: "ingest", SILVER: "streaming", GOLD: "operational_serving",
               LDP: "declarative_pipeline", ML: "machine_learning", KNOWLEDGE: "genai_rag"}
for s, layer in layer_of.items():
    sql(f"""ALTER SCHEMA {CATALOG}.{s} SET TAGS (
            'domain'='operations', 'industry'='HLS', 'sub_industry'='pharma_manufacturing',
            'medallion_layer'='{layer}', 'demo_workload'='{workload_of[s]}',
            'data_origin'='synthetic', 'gxp_relevance'='demo_only')""")
sql(f"ALTER TABLE {BRONZE_TABLE} SET TAGS ('ingest_method'='zerobus_ingest', 'contains_pii'='false')")
sql(f"ALTER TABLE {CATALOG}.{GOLD}.active_alerts SET TAGS ('contains_operator_identity'='true')")
display(sql(f"""
  SELECT schema_name, tag_name, tag_value FROM {CATALOG}.information_schema.schema_tags
  WHERE schema_name LIKE 'pharmaco_mfg_rt%' ORDER BY schema_name, tag_name"""))
```

**Output:**

```text

==============================================================================
3. Governance tags (schema + table level)
==============================================================================
```

| schema_name | tag_name | tag_value |
|---|---|---|
| pharmaco_mfg_rt_bronze | data_origin | synthetic |
| pharmaco_mfg_rt_bronze | demo_workload | ingest |
| pharmaco_mfg_rt_bronze | domain | operations |
| pharmaco_mfg_rt_bronze | gxp_relevance | demo_only |
| pharmaco_mfg_rt_bronze | industry | HLS |
| pharmaco_mfg_rt_bronze | medallion_layer | bronze |
| pharmaco_mfg_rt_bronze | sub_industry | pharma_manufacturing |
| pharmaco_mfg_rt_gold | data_origin | synthetic |
| pharmaco_mfg_rt_gold | demo_workload | operational_serving |
| pharmaco_mfg_rt_gold | domain | operations |
| pharmaco_mfg_rt_gold | gxp_relevance | demo_only |
| pharmaco_mfg_rt_gold | industry | HLS |
| pharmaco_mfg_rt_gold | medallion_layer | gold |
| pharmaco_mfg_rt_gold | sub_industry | pharma_manufacturing |
| pharmaco_mfg_rt_knowledge | data_origin | synthetic |
| pharmaco_mfg_rt_knowledge | demo_workload | genai_rag |
| pharmaco_mfg_rt_knowledge | domain | operations |
| pharmaco_mfg_rt_knowledge | gxp_relevance | demo_only |
| pharmaco_mfg_rt_knowledge | industry | HLS |
| pharmaco_mfg_rt_knowledge | medallion_layer | gold |
| pharmaco_mfg_rt_knowledge | sub_industry | pharma_manufacturing |
| pharmaco_mfg_rt_ldp | data_origin | synthetic |
| pharmaco_mfg_rt_ldp | demo_workload | declarative_pipeline |
| pharmaco_mfg_rt_ldp | domain | operations |
| pharmaco_mfg_rt_ldp | gxp_relevance | demo_only |
| pharmaco_mfg_rt_ldp | industry | HLS |
| pharmaco_mfg_rt_ldp | medallion_layer | silver |
| pharmaco_mfg_rt_ldp | sub_industry | pharma_manufacturing |
| pharmaco_mfg_rt_ml | data_origin | synthetic |
| pharmaco_mfg_rt_ml | demo_workload | machine_learning |
| pharmaco_mfg_rt_ml | domain | operations |
| pharmaco_mfg_rt_ml | gxp_relevance | demo_only |
| pharmaco_mfg_rt_ml | industry | HLS |
| pharmaco_mfg_rt_ml | medallion_layer | gold |
| pharmaco_mfg_rt_ml | sub_industry | pharma_manufacturing |
| pharmaco_mfg_rt_silver | data_origin | synthetic |
| pharmaco_mfg_rt_silver | demo_workload | streaming |
| pharmaco_mfg_rt_silver | domain | operations |
| pharmaco_mfg_rt_silver | gxp_relevance | demo_only |
| pharmaco_mfg_rt_silver | industry | HLS |

_(2 more rows not shown)_

```python
banner("4. Column mask — operator identity on alerts is visible only to plant supervisors")
sql(f"""
CREATE OR REPLACE FUNCTION {CATALOG}.{GOLD}.mask_operator(op STRING)
RETURNS STRING
COMMENT 'Shows acknowledging operator only to workspace admins / plant supervisors'
RETURN CASE WHEN is_account_group_member('admins') OR is_member('admins') THEN op
            WHEN op IS NULL THEN NULL ELSE 'REDACTED' END
""")
sql(f"ALTER TABLE {CATALOG}.{GOLD}.active_alerts ALTER COLUMN acknowledged_by "
    f"SET MASK {CATALOG}.{GOLD}.mask_operator")
display(sql(f"""
  SELECT table_name, column_name, mask_name FROM {CATALOG}.information_schema.column_masks
  WHERE table_schema = '{GOLD}'"""))
```

**Output:**

```text

==============================================================================
4. Column mask — operator identity on alerts is visible only to plant supervisors
==============================================================================
```

| table_name | column_name | mask_name |
|---|---|---|
| active_alerts | acknowledged_by | hls_streaming_solution_demo_catalog.pharmaco_mfg_rt_gold.mask_operator |

```python
banner("5. Least-privilege grants for the Zerobus ingest service principal")
sp = f"`{INGEST_SP_APP_ID}`"
sql(f"GRANT USE CATALOG ON CATALOG {CATALOG} TO {sp}")
sql(f"GRANT USE SCHEMA ON SCHEMA {CATALOG}.{BRONZE} TO {sp}")
sql(f"GRANT SELECT, MODIFY ON TABLE {BRONZE_TABLE} TO {sp}")
print("ingest SP can write ONLY the bronze landing table:")
display(sql(f"SHOW GRANTS {sp} ON TABLE {BRONZE_TABLE}").filter("ObjectType <> 'METASTORE'"))
display(sql(f"SHOW GRANTS ON SCHEMA {CATALOG}.{BRONZE}").filter("ObjectType = 'SCHEMA'"))
```

**Output:**

```text

==============================================================================
5. Least-privilege grants for the Zerobus ingest service principal
==============================================================================
ingest SP can write ONLY the bronze landing table:
```

| Principal | ActionType | ObjectType | ObjectKey |
|---|---|---|---|
| 3663de8f-6de6-4c10-adf5-5ba7946eefed | MODIFY | TABLE | hls_streaming_solution_demo_catalog.pharmaco_mfg_rt_bronze.telemetry_raw |
| 3663de8f-6de6-4c10-adf5-5ba7946eefed | SELECT | TABLE | hls_streaming_solution_demo_catalog.pharmaco_mfg_rt_bronze.telemetry_raw |

| Principal | ActionType | ObjectType | ObjectKey |
|---|---|---|---|
| 3663de8f-6de6-4c10-adf5-5ba7946eefed | USE SCHEMA | SCHEMA | hls_streaming_solution_demo_catalog.pharmaco_mfg_rt_bronze |

```python
banner("6. Governed footprint summary")
display(sql(f"""
  SELECT table_schema, count(*) AS tables
  FROM {CATALOG}.information_schema.tables
  WHERE table_schema LIKE 'pharmaco_mfg_rt%'
  GROUP BY table_schema ORDER BY table_schema"""))
print("UC namespace ready.")
```

**Output:**

```text

==============================================================================
6. Governed footprint summary
==============================================================================
```

| table_schema | tables |
|---|---|
| pharmaco_mfg_rt_bronze | 2 |
| pharmaco_mfg_rt_gold | 6 |
| pharmaco_mfg_rt_knowledge | 4 |
| pharmaco_mfg_rt_silver | 2 |

```text
UC namespace ready.
```
