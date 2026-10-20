# Executed notebook: `04_lakeflow_declarative_pipeline`

> Exported from Databricks job run **24808449534624** / task run **142563862331698** — result **SUCCESS**, executed 2026-10-20T13:32:13+00:00 on serverless compute in workspace `https://fevm-hls-streaming-solution-demo.cloud.databricks.com`. Outputs below are the real cell outputs of that run.

# 04 · Lakeflow Spark Declarative Pipeline — DQ-enforced silver + analytical gold

Creates (idempotently) and runs the serverless declarative pipeline defined in
`pipelines/ldp_medallion.py`, reading the same Zerobus bronze table:

| Dataset | Type | Purpose |
|---|---|---|
| `silver_telemetry` | streaming table | 5 **expectations** (`expect_all_or_drop`) + dedup within watermark |
| `silver_quarantine` | streaming table | rows violating expectations + the failed rule names |
| `gold_asset_features_20s` | materialized view | ML feature vectors per asset per 20 s + ground-truth label |
| `gold_line_kpis_1m` | materialized view | throughput vs baseline, availability, out-of-band rate per line-minute |
| `gold_asset_reliability` | materialized view | minutes in breach per asset / sensor |

Evidence is read back from the pipeline **event log** (expectation pass/fail counts).

```python
%run ./_config
```

```python

```

**Output:**

```text
repo root : /Workspace/Users/chandrashekhar.purandare@databricks.com/pharmaco_mfg_rt_demo
catalog   : hls_streaming_solution_demo_catalog  schemas=['pharmaco_mfg_rt_bronze', 'pharmaco_mfg_rt_silver', 'pharmaco_mfg_rt_gold', 'pharmaco_mfg_rt_ldp', 'pharmaco_mfg_rt_ml', 'pharmaco_mfg_rt_knowledge']
run as    : chandrashekhar.purandare@databricks.com   at 2026-10-20T13:32:21+00:00
```

```python
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
```

**Output:**

```text
pipeline created: a6417d3b-a1ce-4f68-8942-c6701b8db161
source: /Workspace/Users/chandrashekhar.purandare@databricks.com/pharmaco_mfg_rt_demo/pipelines/ldp_medallion.py
```

```python
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
```

**Output:**

```text

==============================================================================
Run a full-refresh update and wait for completion
==============================================================================
update_id=4c16be35-36f1-45df-800c-a8acc550770b
  13:32:25  state=CREATED
  13:32:35  state=WAITING_FOR_RESOURCES
  13:32:45  state=INITIALIZING
  13:33:06  state=SETTING_UP_TABLES
  13:33:16  state=RUNNING
  13:33:46  state=COMPLETED
PIPELINE UPDATE COMPLETED
```

```python
banner("Event log — per-flow rows written")
display(sql(f"""
  SELECT origin.flow_name AS flow,
         sum(details:flow_progress.metrics.num_output_rows::bigint) AS output_rows
  FROM event_log('{pipeline_id}')
  WHERE event_type = 'flow_progress' AND origin.update_id = '{update_id}'
    AND details:flow_progress.metrics.num_output_rows IS NOT NULL
  GROUP BY 1 ORDER BY 1"""))
```

**Output:**

```text

==============================================================================
Event log — per-flow rows written
==============================================================================
```

| flow | output_rows |
|---|---|
| hls_streaming_solution_demo_catalog.pharmaco_mfg_rt_ldp.gold_asset_features_20s | 1515 |
| hls_streaming_solution_demo_catalog.pharmaco_mfg_rt_ldp.gold_asset_reliability | 63 |
| hls_streaming_solution_demo_catalog.pharmaco_mfg_rt_ldp.gold_line_kpis_1m | 105 |
| hls_streaming_solution_demo_catalog.pharmaco_mfg_rt_ldp.silver_quarantine | 8 |
| hls_streaming_solution_demo_catalog.pharmaco_mfg_rt_ldp.silver_telemetry | 113505 |

```python
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
```

**Output:**

```text

==============================================================================
Event log — data-quality expectations (passed / failed per rule)
==============================================================================
```

| dataset | expectation | passed | failed |
|---|---|---|---|
| hls_streaming_solution_demo_catalog.pharmaco_mfg_rt_ldp.silver_telemetry | in_nominal_band_or_flagged | 113513 | 0 |
| hls_streaming_solution_demo_catalog.pharmaco_mfg_rt_ldp.silver_telemetry | valid_asset | 113513 | 0 |
| hls_streaming_solution_demo_catalog.pharmaco_mfg_rt_ldp.silver_telemetry | valid_event_ts | 113513 | 0 |
| hls_streaming_solution_demo_catalog.pharmaco_mfg_rt_ldp.silver_telemetry | valid_sensor_type | 113508 | 5 |
| hls_streaming_solution_demo_catalog.pharmaco_mfg_rt_ldp.silver_telemetry | valid_sensor_value | 113510 | 3 |
| hls_streaming_solution_demo_catalog.pharmaco_mfg_rt_ldp.silver_telemetry | valid_source_event_id | 113513 | 0 |

```python
banner("Quarantine — which rules the malformed probe records broke")
display(sql(f"""SELECT failed_rules, sensor_type, count(*) AS records
                FROM {CATALOG}.{LDP}.silver_quarantine GROUP BY 1, 2 ORDER BY 3 DESC"""))
```

**Output:**

```text

==============================================================================
Quarantine — which rules the malformed probe records broke
==============================================================================
```

| failed_rules | sensor_type | records |
|---|---|---|
| valid_sensor_type | humidity_x | 5 |
| valid_sensor_value | temperature | 1 |
| valid_sensor_value | vibration | 1 |
| valid_sensor_value | pressure | 1 |

```python
banner("Gold MV — per-line minute KPIs (last 12 minutes of history)")
display(sql(f"""SELECT minute, line_id, round(avg_throughput_units_min,1) AS throughput,
                       throughput_pct_of_baseline, round(availability*100,1) AS availability_pct,
                       round(out_of_band_rate*100,2) AS oob_pct
                FROM {CATALOG}.{LDP}.gold_line_kpis_1m
                WHERE minute >= (SELECT max(minute) FROM {CATALOG}.{LDP}.gold_line_kpis_1m) - INTERVAL 4 MINUTES
                ORDER BY minute DESC, line_id"""))
```

**Output:**

```text

==============================================================================
Gold MV — per-line minute KPIs (last 12 minutes of history)
==============================================================================
```

| minute | line_id | throughput | throughput_pct_of_baseline | availability_pct | oob_pct |
|---|---|---|---|---|---|
| 2026-10-20T13:30:00.000Z | LINE-A | 116.6 | 106.0 | 97.5 | 3.57 |
| 2026-10-20T13:30:00.000Z | LINE-B | 119.2 | 108.4 | 99.4 | 0.5 |
| 2026-10-20T13:30:00.000Z | LINE-C | 116.0 | 105.5 | 96.3 | 1.37 |
| 2026-10-20T13:29:00.000Z | LINE-A | 117.9 | 107.1 | 98.3 | 3.29 |
| 2026-10-20T13:29:00.000Z | LINE-B | 118.0 | 107.3 | 98.3 | 0.71 |
| 2026-10-20T13:29:00.000Z | LINE-C | 116.5 | 105.9 | 97.1 | 1.13 |
| 2026-10-20T13:28:00.000Z | LINE-A | 115.9 | 105.4 | 96.4 | 3.93 |
| 2026-10-20T13:28:00.000Z | LINE-B | 119.0 | 108.1 | 98.7 | 0.29 |
| 2026-10-20T13:28:00.000Z | LINE-C | 119.0 | 108.2 | 98.8 | 0.77 |
| 2026-10-20T13:27:00.000Z | LINE-A | 118.8 | 108.0 | 99.4 | 0.71 |
| 2026-10-20T13:27:00.000Z | LINE-B | 116.9 | 106.3 | 97.0 | 1.06 |
| 2026-10-20T13:27:00.000Z | LINE-C | 119.0 | 108.2 | 99.1 | 0.53 |

```python
banner("Gold MV — ML feature table: windows per scenario / label")
display(sql(f"""SELECT coalesce(active_scenario,'normal') AS scenario, label_is_anomalous,
                       count(*) AS windows, count(DISTINCT asset_id) AS assets
                FROM {CATALOG}.{LDP}.gold_asset_features_20s GROUP BY 1, 2 ORDER BY 1, 2"""))
display(sql(f"""SELECT asset_id, sensor_type, out_of_band_readings, minutes_in_breach
                FROM {CATALOG}.{LDP}.gold_asset_reliability
                ORDER BY minutes_in_breach DESC LIMIT 10"""))
dbutils.jobs.taskValues.set("ldp_pipeline_id", pipeline_id)
print(f"LDP pipeline id: {pipeline_id}")
```

**Output:**

```text

==============================================================================
Gold MV — ML feature table: windows per scenario / label
==============================================================================
```

| scenario | label_is_anomalous | windows | assets |
|---|---|---|---|
| line_slowdown | 0 | 117 | 9 |
| line_slowdown | 1 | 78 | 6 |
| normal | 0 | 780 | 15 |
| pressure_instability | 0 | 182 | 14 |
| pressure_instability | 1 | 13 | 1 |
| temp_excursion | 0 | 140 | 14 |
| temp_excursion | 1 | 10 | 1 |
| vibration_degradation | 0 | 182 | 14 |
| vibration_degradation | 1 | 13 | 1 |

| asset_id | sensor_type | out_of_band_readings | minutes_in_breach |
|---|---|---|---|
| B-FILL | throughput | 40 | 24 |
| A-INSPECT | throughput | 140 | 23 |
| A-COAT | throughput | 126 | 22 |
| B-FILL | speed | 34 | 21 |
| C-CASEPACK | throughput | 36 | 21 |
| B-FILL | motor_current | 36 | 21 |
| A-DISPENSE | throughput | 129 | 21 |
| C-SERIALIZE | throughput | 34 | 21 |
| C-CARTON | throughput | 32 | 21 |
| A-GRANULATE | throughput | 115 | 20 |

```text
LDP pipeline id: a6417d3b-a1ce-4f68-8942-c6701b8db161
```
