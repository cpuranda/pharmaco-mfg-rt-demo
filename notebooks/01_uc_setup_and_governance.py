# Databricks notebook source
# MAGIC %md
# MAGIC # 01 · Unity Catalog — namespace, tables & governance
# MAGIC
# MAGIC **Business problem.** PharmaCo's oral-solid-dose, sterile-fill and packaging lines lose
# MAGIC batches and hours of OEE when equipment drifts out of its validated operating band
# MAGIC (tablet-press temperature, aseptic-filler vibration, lyophilizer pressure) and nobody
# MAGIC notices until QA raises a deviation. This build turns raw line telemetry into governed,
# MAGIC real-time operator action.
# MAGIC
# MAGIC This notebook creates the **governed foundation**:
# MAGIC * medallion schemas (bronze / silver / gold) + LDP, ML and knowledge schemas
# MAGIC * Delta tables from `sql/01_bronze.sql`, `02_silver.sql`, `03_gold.sql`
# MAGIC * governance tags, comments, a column mask on operator identity, and least-privilege
# MAGIC   grants for the Zerobus ingest service principal
# MAGIC
# MAGIC All data is **synthetic**.

# COMMAND ----------

# MAGIC %run ./_config

# COMMAND ----------

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

# COMMAND ----------

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

# COMMAND ----------

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

# COMMAND ----------

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

# COMMAND ----------

banner("5. Least-privilege grants for the Zerobus ingest service principal")
sp = f"`{INGEST_SP_APP_ID}`"
sql(f"GRANT USE CATALOG ON CATALOG {CATALOG} TO {sp}")
sql(f"GRANT USE SCHEMA ON SCHEMA {CATALOG}.{BRONZE} TO {sp}")
sql(f"GRANT SELECT, MODIFY ON TABLE {BRONZE_TABLE} TO {sp}")
print("ingest SP can write ONLY the bronze landing table:")
display(sql(f"SHOW GRANTS {sp} ON TABLE {BRONZE_TABLE}").filter("ObjectType <> 'METASTORE'"))
display(sql(f"SHOW GRANTS ON SCHEMA {CATALOG}.{BRONZE}").filter("ObjectType = 'SCHEMA'"))

# COMMAND ----------

banner("6. Governed footprint summary")
display(sql(f"""
  SELECT table_schema, count(*) AS tables
  FROM {CATALOG}.information_schema.tables
  WHERE table_schema LIKE 'pharmaco_mfg_rt%'
  GROUP BY table_schema ORDER BY table_schema"""))
print("UC namespace ready.")
