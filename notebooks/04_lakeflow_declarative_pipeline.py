# Databricks notebook source
# MAGIC %md
# MAGIC # 04 · Lakeflow Spark Declarative Pipeline — DQ-enforced silver + analytical gold
# MAGIC
# MAGIC Creates (idempotently) and runs the serverless declarative pipeline defined in
# MAGIC `pipelines/ldp_medallion.py`, reading the same Zerobus bronze table:
# MAGIC
# MAGIC | Dataset | Type | Purpose |
# MAGIC |---|---|---|
# MAGIC | `silver_telemetry` | streaming table | 5 **expectations** (`expect_all_or_drop`) + dedup within watermark |
# MAGIC | `silver_quarantine` | streaming table | rows violating expectations + the failed rule names |
# MAGIC | `gold_asset_features_20s` | materialized view | ML feature vectors per asset per 20 s + ground-truth label |
# MAGIC | `gold_line_kpis_1m` | materialized view | throughput vs baseline, availability, out-of-band rate per line-minute |
# MAGIC | `gold_asset_reliability` | materialized view | minutes in breach per asset / sensor |
# MAGIC
# MAGIC Evidence is read back from the pipeline **event log** (expectation pass/fail counts).

# COMMAND ----------

# MAGIC %run ./_config

# COMMAND ----------

lib_path = f"{REPO_ROOT}/pipelines/ldp_medallion.py"
spec = {
    "name": LDP_PIPELINE_NAME,
    "catalog": CATALOG,
    "schema": LDP,
    "serverless": True,
    "continuous": False,
    "development": False,
    "channel": "CURRENT",
    "photon": True,
    "libraries": [{"file": {"path": lib_path}}],
    "configuration": {"pharmaco.bronze_table": BRONZE_TABLE},
    "tags": {"project": "pharmaco_mfg_rt", "data": "synthetic"},
}
existing = [p for p in w.pipelines.list_pipelines(filter=f"name LIKE '{LDP_PIPELINE_NAME}'")
            if p.name == LDP_PIPELINE_NAME]
if existing:
    pipeline_id = existing[0].pipeline_id
    w.api_client.do("PUT", f"/api/2.0/pipelines/{pipeline_id}", body={**spec, "id": pipeline_id})
    print(f"pipeline updated: {pipeline_id}")
else:
    pipeline_id = w.api_client.do("POST", "/api/2.0/pipelines", body=spec)["pipeline_id"]
    print(f"pipeline created: {pipeline_id}")
print(f"source: {lib_path}")

# COMMAND ----------

banner("Run a full-refresh update and wait for completion")
upd = w.api_client.do("POST", f"/api/2.0/pipelines/{pipeline_id}/updates",
                      body={"full_refresh": True})
update_id = upd["update_id"]
print(f"update_id={update_id}")
last = None
for _ in range(120):
    u = w.api_client.do("GET", f"/api/2.0/pipelines/{pipeline_id}/updates/{update_id}")["update"]
    if u["state"] != last:
        print(f"  {datetime.now(timezone.utc).strftime('%H:%M:%S')}  state={u['state']}")
        last = u["state"]
    if u["state"] in ("COMPLETED", "FAILED", "CANCELED"):
        break
    time.sleep(10)
assert last == "COMPLETED", f"pipeline update ended in {last}"
print("PIPELINE UPDATE COMPLETED")

# COMMAND ----------

banner("Event log — per-flow rows written")
display(sql(f"""
  SELECT origin.flow_name AS flow,
         sum(details:flow_progress.metrics.num_output_rows::bigint) AS output_rows
  FROM event_log('{pipeline_id}')
  WHERE event_type = 'flow_progress' AND origin.update_id = '{update_id}'
    AND details:flow_progress.metrics.num_output_rows IS NOT NULL
  GROUP BY 1 ORDER BY 1"""))

# COMMAND ----------

banner("Event log — data-quality expectations (passed / failed per rule)")
display(sql(f"""
  WITH e AS (
    SELECT explode(from_json(details:flow_progress.data_quality.expectations,
             'array<struct<name:string,dataset:string,passed_records:bigint,failed_records:bigint>>')) AS x
    FROM event_log('{pipeline_id}')
    WHERE event_type = 'flow_progress' AND origin.update_id = '{update_id}'
      AND details:flow_progress.data_quality.expectations IS NOT NULL)
  SELECT x.dataset, x.name AS expectation, sum(x.passed_records) AS passed,
         sum(x.failed_records) AS failed
  FROM e GROUP BY 1, 2 ORDER BY 1, 2"""))

# COMMAND ----------

banner("Quarantine — which rules the malformed probe records broke")
display(sql(f"""SELECT failed_rules, sensor_type, count(*) AS records
                FROM {CATALOG}.{LDP}.silver_quarantine GROUP BY 1, 2 ORDER BY 3 DESC"""))

# COMMAND ----------

banner("Gold MV — per-line minute KPIs (last 12 minutes of history)")
display(sql(f"""SELECT minute, line_id, round(avg_throughput_units_min,1) AS throughput,
                       throughput_pct_of_baseline, round(availability*100,1) AS availability_pct,
                       round(out_of_band_rate*100,2) AS oob_pct
                FROM {CATALOG}.{LDP}.gold_line_kpis_1m
                WHERE minute >= (SELECT max(minute) FROM {CATALOG}.{LDP}.gold_line_kpis_1m) - INTERVAL 4 MINUTES
                ORDER BY minute DESC, line_id"""))

# COMMAND ----------

banner("Gold MV — ML feature table: windows per scenario / label")
display(sql(f"""SELECT coalesce(active_scenario,'normal') AS scenario, label_is_anomalous,
                       count(*) AS windows, count(DISTINCT asset_id) AS assets
                FROM {CATALOG}.{LDP}.gold_asset_features_20s GROUP BY 1, 2 ORDER BY 1, 2"""))
display(sql(f"""SELECT asset_id, sensor_type, out_of_band_readings, minutes_in_breach
                FROM {CATALOG}.{LDP}.gold_asset_reliability
                ORDER BY minutes_in_breach DESC LIMIT 10"""))
dbutils.jobs.taskValues.set("ldp_pipeline_id", pipeline_id)
print(f"LDP pipeline id: {pipeline_id}")
