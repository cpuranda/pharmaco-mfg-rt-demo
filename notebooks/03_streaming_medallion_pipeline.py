# Databricks notebook source
# MAGIC %md
# MAGIC # 03 · Spark Structured Streaming medallion — bronze → silver → operational gold
# MAGIC
# MAGIC Runs the production pipeline module `pipeline/rt_pipeline.py` (the same code the scheduled
# MAGIC Lakeflow Job runs). Business logic is pure DataFrame functions, so it behaves identically
# MAGIC in stream / AvailableNow / batch-replay modes.
# MAGIC
# MAGIC * **Silver:** schema validation, sensor normalisation, **dedup on `source_event_id`**,
# MAGIC   malformed records → **quarantine**
# MAGIC * **Gold:** per-asset current state, sustained-breach anomaly score, WARNING/CRITICAL
# MAGIC   **alert state machine** (operator acknowledgements preserved, auto-resolve, re-fire),
# MAGIC   line health & slowdown hint, pipeline DQ metrics
# MAGIC
# MAGIC *Real-Time Mode note:* `Trigger.RealTime` supports message-bus sources/sinks only, not
# MAGIC Delta, so this Delta medallion uses standard Structured Streaming (AvailableNow on
# MAGIC serverless; ProcessingTime on classic compute for ~5 s latency).

# COMMAND ----------

# MAGIC %run ./_config

# COMMAND ----------

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

# COMMAND ----------

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

# COMMAND ----------

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

# COMMAND ----------

banner("Gold: line health (what the plant manager sees)")
display(sql(f"""SELECT line_id, product_family, health_status, throughput_pct, running_assets,
                       total_assets, open_warnings, open_criticals, is_slowing_down, slowdown_hint
                FROM {CATALOG}.{GOLD}.line_health_summary ORDER BY line_id"""))

# COMMAND ----------

banner("Gold: asset current state — worst first")
display(sql(f"""SELECT line_id, asset_id, operating_state, round(temperature,1) AS temp_c,
                       round(vibration,2) AS vib_mm_s, round(pressure,2) AS pressure_bar,
                       anomaly_score, condition, active_alert_count
                FROM {CATALOG}.{GOLD}.asset_current_state
                ORDER BY anomaly_score DESC, asset_id LIMIT 15"""))

# COMMAND ----------

banner("Gold: active alerts raised by the live temperature excursion")
display(sql(f"""SELECT asset_id, sensor_type, alert_type, severity, status, observed_value,
                       threshold_value, round(confidence,2) AS confidence, recommended_action
                FROM {CATALOG}.{GOLD}.active_alerts WHERE status <> 'RESOLVED'
                ORDER BY severity, asset_id"""))
display(sql(f"""SELECT severity, alert_type, count(*) AS events
                FROM {CATALOG}.{GOLD}.asset_anomaly_events GROUP BY severity, alert_type"""))

# COMMAND ----------

crit = sql(f"""SELECT count(*) c FROM {CATALOG}.{GOLD}.active_alerts
               WHERE asset_id='A-COMPRESS' AND sensor_type='temperature'
                 AND severity='CRITICAL' AND status<>'RESOLVED'""").first().c
assert crit >= 1, "expected a CRITICAL temperature alert on A-COMPRESS"
assert qtn.count() >= 8, "expected malformed probes in quarantine"
print(f"CHECK PASSED: A-COMPRESS CRITICAL temperature alert present ({crit}); quarantine populated.")
