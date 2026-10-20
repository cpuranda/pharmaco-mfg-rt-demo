# Executed notebook: `10_operational_lakeflow_job`

> Exported from Databricks job run **24808449534624** / task run **315227353714749** — result **SUCCESS**, executed 2026-10-20T13:44:09+00:00 on serverless compute in workspace `https://fevm-hls-streaming-solution-demo.cloud.databricks.com`. Outputs below are the real cell outputs of that run.

# 10 · Lakeflow Jobs — the operational orchestration

Creates the scheduled **operational Lakeflow Job** `pharmaco_mfg_rt_operational`
(every 5 min; schedule PAUSED by default to save cost) and runs it once:

`rt_pipeline` (Structured Streaming, AvailableNow, alert state machine)
→ `ldp_refresh` (Lakeflow Declarative Pipeline: DQ + ML features)
→ `ml_score` (UC `@champion` model → Delta gold + Lakebase)

One job keeps every downstream consumer — Lakebase/app, Genie, agents — fresh from the same
governed event flow.

```python
%pip install -q databricks-zerobus-ingest-sdk==1.4.0 protobuf==5.29.5 googleapis-common-protos==1.65.0
%restart_python
```

**Output:**

```text
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
run as    : chandrashekhar.purandare@databricks.com   at 2026-10-20T13:44:51+00:00
```

```python
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
```

**Output:**

```text

==============================================================================
0. New live events arrive: 75 s of telemetry with vibration degradation on B-FILL (Zerobus)
==============================================================================
2026-10-20T13:44:52.586310Z  INFO databricks_zerobus_ingest_sdk::default_token_factory: minted UC OAuth token table=hls_streaming_solution_demo_catalog.pharmaco_mfg_rt_bronze.telemetry_raw reason="cold_miss" expires_in_secs=3600 elapsed_ms=439
2026-10-20T13:44:53.011268Z  INFO databricks_zerobus_ingest_sdk::stream::grpc::connection: Successfully created stream stream_id=0bc330c7-78c1-4f4a-9905-3e1fe4384e82
2026-10-20T13:44:53.011436Z  INFO databricks_zerobus_ingest_sdk::stream::grpc::supervisor: Successfully created stream stream_id=0bc330c7-78c1-4f4a-9905-3e1fe4384e82
2026-10-20T13:46:08.069871Z  INFO databricks_zerobus_ingest_sdk::stream::grpc::close: Closing stream stream_id=0bc330c7-78c1-4f4a-9905-3e1fe4384e82
ingested 7875 new records via Zerobus (batch zb-2608f021b6ed)
```

```python
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
```

**Output:**

```text

==============================================================================
1. Operational Lakeflow Job: streaming → declarative pipeline → ML scoring
==============================================================================
job pharmaco_mfg_rt_operational id=1012398663888207  (schedule every 5 min, PAUSED)
run-now run_id=486272570044111
  task rt_pipeline  SUCCESS  97s
  task ldp_refresh  SUCCESS  93s
  task ml_score     SUCCESS  72s
job run result: SUCCESS
```

```python
banner("Result: the new B-FILL vibration drift flowed through every downstream consumer")
display(sql(f"""SELECT asset_id, sensor_type, severity, status, observed_value, threshold_value, updated_ts
                FROM {CATALOG}.{GOLD}.active_alerts WHERE asset_id = 'B-FILL' ORDER BY updated_ts DESC LIMIT 5"""))
display(sql(f"""SELECT asset_id, ml_anomaly_score, ml_is_anomaly, top_sensor, window_end, scored_ts
                FROM {CATALOG}.{GOLD}.asset_ml_anomaly_scores
                WHERE scored_ts = (SELECT max(scored_ts) FROM {CATALOG}.{GOLD}.asset_ml_anomaly_scores)
                ORDER BY ml_anomaly_score DESC LIMIT 5"""))
```

**Output:**

```text

==============================================================================
Result: the new B-FILL vibration drift flowed through every downstream consumer
==============================================================================
```

| asset_id | sensor_type | severity | status | observed_value | threshold_value | updated_ts |
|---|---|---|---|---|---|---|
| B-FILL | vibration | CRITICAL | NEW | 6.002 | 4.5 | 2026-10-20T13:47:21.649Z |

| asset_id | ml_anomaly_score | ml_is_anomaly | top_sensor | window_end | scored_ts |
|---|---|---|---|---|---|
| B-FILL | 1.0 | True | vibration | 2026-10-20T13:45:40.000Z | 2026-10-20T13:50:21.371Z |
| B-FILL | 1.0 | True | vibration | 2026-10-20T13:46:00.000Z | 2026-10-20T13:50:21.371Z |
| B-FILL | 1.0 | True | vibration | 2026-10-20T13:46:20.000Z | 2026-10-20T13:50:21.371Z |
| B-WASH | 0.3587 | False | speed | 2026-10-20T13:46:20.000Z | 2026-10-20T13:50:21.371Z |
| A-GRANULATE | 0.3431 | False | motor_current | 2026-10-20T13:46:00.000Z | 2026-10-20T13:50:21.371Z |
