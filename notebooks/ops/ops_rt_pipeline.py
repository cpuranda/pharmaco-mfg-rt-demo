# Databricks notebook source
# MAGIC %md
# MAGIC # ops · Operational pipeline task (scheduled)
# MAGIC Incremental bronze → silver → operational gold with `Trigger.AvailableNow` (checkpointed).
# MAGIC Task 1 of the `pharmaco_mfg_rt_operational` Lakeflow Job.

# COMMAND ----------

# MAGIC %run ../_config

# COMMAND ----------

import rt_pipeline
dbutils.widgets.text("window_minutes", "3")
conf = {
    "catalog": CATALOG, "bronze": BRONZE, "silver": SILVER, "gold": GOLD, "mode": "once",
    "trigger_seconds": "5", "window_minutes": dbutils.widgets.get("window_minutes"),
    "checkpoint": CHECKPOINT_ROOT, "warn_margin": "0.0", "crit_margin": "0.10",
    "slowdown_pct": "80.0", "lakebase_sync": "false", "strict_sync": "false",
    "lakebase_host": "", "lakebase_instance": LAKEBASE_INSTANCE, "lakebase_db": LAKEBASE_DB,
}
silver0 = spark.table(f"{CATALOG}.{SILVER}.telemetry_clean").count()
q = (spark.readStream.table(BRONZE_TABLE).writeStream
     .option("checkpointLocation", f"{CHECKPOINT_ROOT}/rt_pipeline")
     .foreachBatch(lambda df, batch_id: rt_pipeline.process_batch_df(df.sparkSession, df, conf))
     .trigger(availableNow=True).start())
q.awaitTermination()
print(f"done: {spark.table(f'{CATALOG}.{SILVER}.telemetry_clean').count() - silver0} new silver rows")
display(sql(f"SELECT line_id, health_status, throughput_pct, open_criticals, updated_ts "
            f"FROM {CATALOG}.{GOLD}.line_health_summary ORDER BY line_id"))
