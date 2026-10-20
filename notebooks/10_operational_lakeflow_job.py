# Databricks notebook source
# MAGIC %md
# MAGIC # 10 · Lakeflow Jobs — the operational orchestration
# MAGIC
# MAGIC Creates the scheduled **operational Lakeflow Job** `pharmaco_mfg_rt_operational`
# MAGIC (every 5 min; schedule PAUSED by default to save cost) and runs it once:
# MAGIC
# MAGIC `rt_pipeline` (Structured Streaming, AvailableNow, alert state machine)
# MAGIC → `ldp_refresh` (Lakeflow Declarative Pipeline: DQ + ML features)
# MAGIC → `ml_score` (UC `@champion` model → Delta gold + Lakebase)
# MAGIC
# MAGIC One job keeps every downstream consumer — Lakebase/app, Genie, agents — fresh from the same
# MAGIC governed event flow.

# COMMAND ----------

# MAGIC %pip install -q databricks-zerobus-ingest-sdk==1.4.0 protobuf==5.29.5 googleapis-common-protos==1.65.0
# MAGIC %restart_python

# COMMAND ----------

# MAGIC %run ./_config

# COMMAND ----------

banner("0. New live events arrive: 75 s of telemetry with vibration degradation on B-FILL (Zerobus)")
from telemetry_generator import TelemetryGenerator, GeneratorConfig, Scenario
from zerobus_ingest import ZerobusIngestor
gen = TelemetryGenerator(GeneratorConfig(seed=int(time.time()) % 100000, deterministic_time=False))
gen.inject_scenario(Scenario.VIBRATION_DEGRADATION, duration_ticks=600, target_asset="B-FILL")
ing = ZerobusIngestor(ZB_ENDPOINT, "https://" + spark.conf.get("spark.databricks.workspaceUrl"), BRONZE_TABLE,
                      dbutils.secrets.get(SECRET_SCOPE, "sp_client_id"),
                      dbutils.secrets.get(SECRET_SCOPE, "sp_oauth_secret"))
for t in range(75):
    t0 = time.time()
    for ev in gen.tick():
        ing.ingest_event(ev)
    time.sleep(max(0.0, 1.0 - (time.time() - t0)))
ing.close()
print(f"ingested {ing.accepted} new records via Zerobus (batch {ing.ingest_batch_id})")

# COMMAND ----------

banner("1. Operational Lakeflow Job: streaming → declarative pipeline → ML scoring")
ldp_id = next(p.pipeline_id for p in w.pipelines.list_pipelines(filter=f"name LIKE '{LDP_PIPELINE_NAME}'"))
JOB_NAME = "pharmaco_mfg_rt_operational"
nb = f"{REPO_ROOT}/notebooks/ops"
settings = {
    "name": JOB_NAME,
    "description": "Every 5 min: Zerobus bronze → operational gold (alerts) → LDP features → ML scores. SYNTHETIC.",
    "max_concurrent_runs": 1,
    "schedule": {"quartz_cron_expression": "0 0/5 * * * ?", "timezone_id": "UTC", "pause_status": "PAUSED"},
    "tags": {"project": "pharmaco_mfg_rt"},
    "tasks": [
        {"task_key": "rt_pipeline", "notebook_task": {"notebook_path": f"{nb}/ops_rt_pipeline",
                                                      "base_parameters": {"window_minutes": "10"}}},
        {"task_key": "ldp_refresh", "depends_on": [{"task_key": "rt_pipeline"}],
         "pipeline_task": {"pipeline_id": ldp_id, "full_refresh": False}},
        {"task_key": "ml_score", "depends_on": [{"task_key": "ldp_refresh"}],
         "notebook_task": {"notebook_path": f"{nb}/ops_ml_score"}},
    ],
}
jobs = [j for j in w.jobs.list(name=JOB_NAME)]
if jobs:
    job_id = jobs[0].job_id
    w.api_client.do("POST", "/api/2.2/jobs/reset", body={"job_id": job_id, "new_settings": settings})
else:
    job_id = w.api_client.do("POST", "/api/2.2/jobs/create", body=settings)["job_id"]
print(f"job {JOB_NAME} id={job_id}  (schedule every 5 min, PAUSED)")
run_id = w.api_client.do("POST", "/api/2.2/jobs/run-now", body={"job_id": job_id})["run_id"]
print(f"run-now run_id={run_id}")
for _ in range(120):
    r = w.api_client.do("GET", "/api/2.2/jobs/runs/get", query={"run_id": run_id})
    if r["state"]["life_cycle_state"] in ("TERMINATED", "INTERNAL_ERROR", "SKIPPED"):
        break
    time.sleep(15)
for t in r["tasks"]:
    dur = (t.get("end_time", 0) - t.get("start_time", 0)) / 1000
    print(f"  task {t['task_key']:<12} {t['state'].get('result_state')}  {dur:.0f}s")
print(f"job run result: {r['state'].get('result_state')}")
dbutils.jobs.taskValues.set("operational_job_id", job_id)
assert r["state"].get("result_state") == "SUCCESS", "operational job run failed"

# COMMAND ----------

banner("Result: the new B-FILL vibration drift flowed through every downstream consumer")
display(sql(f"""SELECT asset_id, sensor_type, severity, status, observed_value, threshold_value, updated_ts
                FROM {CATALOG}.{GOLD}.active_alerts WHERE asset_id = 'B-FILL' ORDER BY updated_ts DESC LIMIT 5"""))
display(sql(f"""SELECT asset_id, ml_anomaly_score, ml_is_anomaly, top_sensor, window_end, scored_ts
                FROM {CATALOG}.{GOLD}.asset_ml_anomaly_scores
                WHERE scored_ts = (SELECT max(scored_ts) FROM {CATALOG}.{GOLD}.asset_ml_anomaly_scores)
                ORDER BY ml_anomaly_score DESC LIMIT 5"""))
