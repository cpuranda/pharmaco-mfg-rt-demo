# Executed notebook: `03_streaming_medallion_pipeline`

> Exported from Databricks job run **24808449534624** / task run **164380550435414** — result **SUCCESS**, executed 2026-10-20T13:30:54+00:00 on serverless compute in workspace `https://fevm-hls-streaming-solution-demo.cloud.databricks.com`. Outputs below are the real cell outputs of that run.

# 03 · Spark Structured Streaming medallion — bronze → silver → operational gold

Runs the production pipeline module `pipeline/rt_pipeline.py` (the same code the scheduled
Lakeflow Job runs). Business logic is pure DataFrame functions, so it behaves identically
in stream / AvailableNow / batch-replay modes.

* **Silver:** schema validation, sensor normalisation, **dedup on `source_event_id`**,
  malformed records → **quarantine**
* **Gold:** per-asset current state, sustained-breach anomaly score, WARNING/CRITICAL
  **alert state machine** (operator acknowledgements preserved, auto-resolve, re-fire),
  line health & slowdown hint, pipeline DQ metrics

*Real-Time Mode note:* `Trigger.RealTime` supports message-bus sources/sinks only, not
Delta, so this Delta medallion uses standard Structured Streaming (AvailableNow on
serverless; ProcessingTime on classic compute for ~5 s latency).

```python
%run ./_config
```

```python

```

**Output:**

```text
repo root : /Workspace/Users/chandrashekhar.purandare@databricks.com/pharmaco_mfg_rt_demo
catalog   : hls_streaming_solution_demo_catalog  schemas=['pharmaco_mfg_rt_bronze', 'pharmaco_mfg_rt_silver', 'pharmaco_mfg_rt_gold', 'pharmaco_mfg_rt_ldp', 'pharmaco_mfg_rt_ml', 'pharmaco_mfg_rt_knowledge']
run as    : chandrashekhar.purandare@databricks.com   at 2026-10-20T13:31:03+00:00
```

```python
import importlib, rt_pipeline
importlib.reload(rt_pipeline)
from pyspark.sql import functions as F

conf = {
    "catalog": CATALOG, "bronze": BRONZE, "silver": SILVER, "gold": GOLD,
    "mode": "once", "trigger_seconds": "5", "window_minutes": "10",
    "checkpoint": CHECKPOINT_ROOT, "warn_margin": "0.0", "crit_margin": "0.10",
    "slowdown_pct": "80.0", "lakebase_sync": "false", "strict_sync": "false",
    "lakebase_host": "", "lakebase_instance": LAKEBASE_INSTANCE, "lakebase_db": LAKEBASE_DB,
}
print(json.dumps(conf, indent=1))
```

**Output:**

```text
{
 "catalog": "hls_streaming_solution_demo_catalog",
 "bronze": "pharmaco_mfg_rt_bronze",
 "silver": "pharmaco_mfg_rt_silver",
 "gold": "pharmaco_mfg_rt_gold",
 "mode": "once",
 "trigger_seconds": "5",
 "window_minutes": "10",
 "checkpoint": "/Volumes/hls_streaming_solution_demo_catalog/pharmaco_mfg_rt_bronze/checkpoints",
 "warn_margin": "0.0",
 "crit_margin": "0.10",
 "slowdown_pct": "80.0",
 "lakebase_sync": "false",
 "strict_sync": "false",
 "lakebase_host": "",
 "lakebase_instance": "pharmaco-mfg-rt-demo-lb",
 "lakebase_db": "databricks_postgres"
}
```

```python
banner("Run: readStream(bronze Delta) → foreachBatch(process_batch_df) with Trigger.AvailableNow")
# foreachBatch runs server-side on serverless (Spark Connect); progress is measured from
# the target tables before/after rather than from driver-side counters.
cnt = lambda t: spark.table(f"{CATALOG}.{SILVER}.{t}").count()
silver0, quar0, t0 = cnt("telemetry_clean"), cnt("telemetry_quarantine"), time.time()
q = (spark.readStream.table(BRONZE_TABLE).writeStream
     .option("checkpointLocation", f"{CHECKPOINT_ROOT}/rt_pipeline")
     .foreachBatch(lambda df, batch_id: rt_pipeline.process_batch_df(df.sparkSession, df, conf))
     .trigger(availableNow=True).start())
q.awaitTermination()
print(f"AvailableNow stream drained bronze in {time.time()-t0:.0f}s")
print(f"  silver rows added      : {cnt('telemetry_clean') - silver0}")
print(f"  quarantine rows added  : {cnt('telemetry_quarantine') - quar0}")
print(f"  checkpoint             : {CHECKPOINT_ROOT}/rt_pipeline (next run is incremental)")
```

**Output:**

```text

==============================================================================
Run: readStream(bronze Delta) → foreachBatch(process_batch_df) with Trigger.AvailableNow
==============================================================================
AvailableNow stream drained bronze in 47s
  silver rows added      : 113505
  quarantine rows added  : 8
  checkpoint             : /Volumes/hls_streaming_solution_demo_catalog/pharmaco_mfg_rt_bronze/checkpoints/rt_pipeline (next run is incremental)
```

```python
banner("Silver: dedup + quarantine evidence")
b = spark.table(BRONZE_TABLE)
s = spark.table(f"{CATALOG}.{SILVER}.telemetry_clean")
qtn = spark.table(f"{CATALOG}.{SILVER}.telemetry_quarantine")
bronze_rows = b.count()
bronze_logical = b.select("source_event_id").distinct().count()
silver_rows = s.count()
print(f"bronze rows (physical emissions) : {bronze_rows}")
print(f"bronze distinct source_event_id  : {bronze_logical}")
print(f"silver rows (deduplicated)       : {silver_rows}")
print(f"duplicates removed               : {bronze_rows - silver_rows - qtn.count()}")
print(f"quarantined                      : {qtn.count()}")
display(qtn.groupBy("reject_reason").count())
display(s.groupBy("line_id").agg(F.count("*").alias("clean_readings"),
                                 F.round(F.avg(F.col("is_out_of_band").cast("int")) * 100, 2)
                                 .alias("pct_out_of_band")).orderBy("line_id"))
```

**Output:**

```text

==============================================================================
Silver: dedup + quarantine evidence
==============================================================================
bronze rows (physical emissions) : 113618
bronze distinct source_event_id  : 113513
silver rows (deduplicated)       : 113505
duplicates removed               : 105
quarantined                      : 8
```

| reject_reason | count |
|---|---|
| unknown_sensor_type | 5 |
| null_sensor_value | 3 |

| line_id | clean_readings | pct_out_of_band |
|---|---|---|
| LINE-A | 45402 | 4.05 |
| LINE-B | 37835 | 1.32 |
| LINE-C | 30268 | 1.19 |

```python
banner("Gold: line health (what the plant manager sees)")
display(sql(f"""SELECT line_id, product_family, health_status, throughput_pct, running_assets,
                       total_assets, open_warnings, open_criticals, is_slowing_down, slowdown_hint
                FROM {CATALOG}.{GOLD}.line_health_summary ORDER BY line_id"""))
```

**Output:**

```text

==============================================================================
Gold: line health (what the plant manager sees)
==============================================================================
```

| line_id | product_family | health_status | throughput_pct | running_assets | total_assets | open_warnings | open_criticals | is_slowing_down | slowdown_hint |
|---|---|---|---|---|---|---|---|---|---|
| LINE-A | OSD_TABLET | CRITICAL | 106.6 | 6 | 6 | 0 | 1 | False | nominal |
| LINE-B | STERILE_VIAL | HEALTHY | 107.6 | 5 | 5 | 0 | 0 | False | nominal |
| LINE-C | PACKAGING | HEALTHY | 106.8 | 3 | 4 | 0 | 0 | False | nominal |

```python
banner("Gold: asset current state — worst first")
display(sql(f"""SELECT line_id, asset_id, operating_state, round(temperature,1) AS temp_c,
                       round(vibration,2) AS vib_mm_s, round(pressure,2) AS pressure_bar,
                       anomaly_score, condition, active_alert_count
                FROM {CATALOG}.{GOLD}.asset_current_state
                ORDER BY anomaly_score DESC, asset_id LIMIT 15"""))
```

**Output:**

```text

==============================================================================
Gold: asset current state — worst first
==============================================================================
```

| line_id | asset_id | operating_state | temp_c | vib_mm_s | pressure_bar | anomaly_score | condition | active_alert_count |
|---|---|---|---|---|---|---|---|---|
| LINE-A | A-COMPRESS | RUNNING | 83.3 | 2.15 | 4.45 | 0.6510638297872341 | DEGRADED | 1 |
| LINE-A | A-INSPECT | RUNNING | 64.4 | 1.83 | 4.41 | 0.02654867256637168 | WATCH | 0 |
| LINE-C | C-CARTON | RUNNING | 66.0 | 2.0 | 4.49 | 0.01276595744680851 | WATCH | 0 |
| LINE-C | C-LABEL | IDLE | 62.4 | 1.75 | 4.69 | 0.012658227848101266 | WATCH | 0 |
| LINE-A | A-BLEND | RUNNING | 65.2 | 1.67 | 4.45 | 0.008583690987124463 | WATCH | 0 |
| LINE-A | A-GRANULATE | RUNNING | 65.1 | 2.12 | 4.62 | 0.008547008547008548 | WATCH | 0 |
| LINE-C | C-CASEPACK | RUNNING | 63.4 | 1.71 | 4.35 | 0.004329004329004329 | WATCH | 0 |
| LINE-B | B-LYO | RUNNING | 64.3 | 1.86 | 4.51 | 0.004310344827586207 | WATCH | 0 |
| LINE-A | A-COAT | RUNNING | 64.5 | 2.03 | 4.65 | 0.004273504273504274 | WATCH | 0 |
| LINE-B | B-WASH | RUNNING | 64.8 | 1.75 | 4.48 | 0.00425531914893617 | WATCH | 0 |
| LINE-B | B-FILL | RUNNING | 63.1 | 1.71 | 4.44 | 0.00423728813559322 | WATCH | 0 |
| LINE-A | A-DISPENSE | RUNNING | 66.5 | 1.29 | 4.48 | 0.004219409282700422 | WATCH | 0 |
| LINE-B | B-DEPYRO | RUNNING | 64.8 | 1.64 | 4.54 | 0.004219409282700422 | WATCH | 0 |
| LINE-B | B-STOPPER | RUNNING | 63.8 | 3.25 | 4.34 | 0.004219409282700422 | WATCH | 0 |
| LINE-C | C-SERIALIZE | RUNNING | 65.9 | 1.45 | 4.6 | 0.0 | HEALTHY | 0 |

```python
banner("Gold: active alerts raised by the live temperature excursion")
display(sql(f"""SELECT asset_id, sensor_type, alert_type, severity, status, observed_value,
                       threshold_value, round(confidence,2) AS confidence, recommended_action
                FROM {CATALOG}.{GOLD}.active_alerts WHERE status <> 'RESOLVED'
                ORDER BY severity, asset_id"""))
display(sql(f"""SELECT severity, alert_type, count(*) AS events
                FROM {CATALOG}.{GOLD}.asset_anomaly_events GROUP BY severity, alert_type"""))
```

**Output:**

```text

==============================================================================
Gold: active alerts raised by the live temperature excursion
==============================================================================
```

| asset_id | sensor_type | alert_type | severity | status | observed_value | threshold_value | confidence | recommended_action |
|---|---|---|---|---|---|---|---|---|
| A-COMPRESS | temperature | TREND | CRITICAL | NEW | 77.208 | 75.0 | 0.83 | Inspect cooling / reduce load; check setpoint |

| severity | alert_type | events |
|---|---|---|
| CRITICAL | TREND | 1 |

```python
crit = sql(f"""SELECT count(*) c FROM {CATALOG}.{GOLD}.active_alerts
               WHERE asset_id='A-COMPRESS' AND sensor_type='temperature'
                 AND severity='CRITICAL' AND status<>'RESOLVED'""").first().c
assert crit >= 1, "expected a CRITICAL temperature alert on A-COMPRESS"
assert qtn.count() >= 8, "expected malformed probes in quarantine"
print(f"CHECK PASSED: A-COMPRESS CRITICAL temperature alert present ({crit}); quarantine populated.")
```

**Output:**

```text
CHECK PASSED: A-COMPRESS CRITICAL temperature alert present (1); quarantine populated.
```
