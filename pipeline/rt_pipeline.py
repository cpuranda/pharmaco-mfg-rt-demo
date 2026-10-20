"""
Real-time processing pipeline — PharmaCo Manufacturing RT demo
==============================================================

Reads the Zerobus-landed bronze telemetry stream and produces the silver and gold
layers: validated/deduplicated telemetry, current asset state, anomaly/threshold
events, active alerts, line-health summaries, and pipeline quality metrics.

RUNTIME / "Real-Time Mode" note
-------------------------------
Spark Structured Streaming *Real-Time Mode* (Trigger.RealTime) is supported only
for message-bus sources/sinks (Kafka/Event Hubs/Kinesis/foreach) — NOT for Delta
sources or sinks. This medallion pipeline reads and writes Delta, so it uses
standard Structured Streaming with a low-latency processing-time trigger, which is
the supported low-latency mechanism for Delta. The business logic below is
deliberately expressed as pure functions over a DataFrame so it is identical
whether driven by the continuous stream, an AvailableNow micro-batch, or a batch
replay (mission requirement: reuse the same logic for replay/micro-batch testing).

Run modes (widgets / argv):
  mode = "stream"  -> continuous, Trigger.ProcessingTime(<trigger_seconds>)
  mode = "once"    -> Trigger.AvailableNow (process all available, then stop)  [tests]
  mode = "batch"   -> non-streaming full recompute over bronze                 [replay/tests]
"""

from __future__ import annotations

import sys

from pyspark.sql import DataFrame, SparkSession, functions as F, Window

# --------------------------------------------------------------------------- #
# Config (widgets in a notebook/job; argv fallback for spark-submit)
# --------------------------------------------------------------------------- #
def _get_conf(spark: SparkSession) -> dict:
    def w(name, default):
        try:
            from pyspark.dbutils import DBUtils  # type: ignore
            dbutils = DBUtils(spark)
            dbutils.widgets.text(name, default)
            return dbutils.widgets.get(name)
        except Exception:
            # argv fallback: --name value
            argv = sys.argv
            if f"--{name}" in argv:
                return argv[argv.index(f"--{name}") + 1]
            return default

    return {
        "catalog": w("catalog", "hls_streaming_solution_demo_catalog"),
        "bronze": w("bronze", "pharmaco_mfg_rt_bronze"),
        "silver": w("silver", "pharmaco_mfg_rt_silver"),
        "gold": w("gold", "pharmaco_mfg_rt_gold"),
        "mode": w("mode", "once"),
        "trigger_seconds": w("trigger_seconds", "5"),
        "window_minutes": w("window_minutes", "3"),
        "checkpoint": w("checkpoint",
                        "/Volumes/hls_streaming_solution_demo_catalog/"
                        "pharmaco_mfg_rt_bronze/checkpoints"),
        "warn_margin": w("warn_margin", "0.0"),     # WARNING once outside nominal band
        "crit_margin": w("crit_margin", "0.10"),    # CRITICAL once >10% beyond band
        "slowdown_pct": w("slowdown_pct", "80.0"),  # < this % of baseline => slowing
        "lakebase_sync": w("lakebase_sync", "false"),  # write current view to Lakebase
        "lakebase_host": w("lakebase_host", ""),
        "lakebase_instance": w("lakebase_instance", "pharmaco-mfg-rt-demo-lb"),
        "lakebase_db": w("lakebase_db", "databricks_postgres"),
        "strict_sync": w("strict_sync", "false"),
    }


# --------------------------------------------------------------------------- #
# SILVER: validation, normalization, dedup, quarantine
# --------------------------------------------------------------------------- #
REQUIRED_COLS = ["source_event_id", "event_ts", "asset_id", "line_id", "sensor_type"]
KNOWN_SENSORS = ["temperature", "vibration", "pressure", "motor_current",
                 "speed", "throughput", "energy"]


def validate_and_normalize(bronze_df: DataFrame):
    """Split a bronze batch into (clean, quarantine). Pure function."""
    df = bronze_df.withColumn("sensor_type", F.lower(F.trim(F.col("sensor_type"))))
    # reject reasons
    reason = (
        F.when(F.col("source_event_id").isNull(), F.lit("null_source_event_id"))
        .when(F.col("asset_id").isNull(), F.lit("null_asset_id"))
        .when(F.col("event_ts").isNull(), F.lit("null_event_ts"))
        .when(F.col("sensor_value").isNull(), F.lit("null_sensor_value"))
        .when(~F.col("sensor_type").isin(KNOWN_SENSORS), F.lit("unknown_sensor_type"))
        .when(F.col("sensor_value").isNull() | F.isnan("sensor_value"), F.lit("nan_value"))
        .otherwise(F.lit(None))
    )
    df = df.withColumn("_reject", reason)
    quarantine = (df.filter(F.col("_reject").isNotNull())
                    .select(
                        F.to_json(F.struct(*bronze_df.columns)).alias("raw_payload"),
                        F.col("event_ts"),
                        F.current_timestamp().alias("processed_ts"),
                        F.col("_reject").alias("reject_reason"),
                        F.col("asset_id"), F.col("sensor_type")))
    clean = (df.filter(F.col("_reject").isNull())
               .withColumn("processed_ts", F.current_timestamp())
               .withColumn("is_out_of_band",
                           (F.col("sensor_value") < F.col("nominal_min")) |
                           (F.col("sensor_value") > F.col("nominal_max")))
               .withColumn("ingest_latency_ms",
                           (F.col("ingested_ts").cast("double") -
                            F.col("event_ts").cast("double")) * 1000.0)
               .select("source_event_id", "event_ts", "ingested_ts", "processed_ts",
                       "site_id", "plant_id", "line_id", "asset_id", "batch_id",
                       "product_family", "sensor_type", "sensor_value", "unit",
                       "operating_state", "nominal_min", "nominal_max",
                       "is_out_of_band", "sequence_number", "scenario",
                       "ingest_latency_ms"))
    return clean, quarantine


def merge_silver(spark, clean: DataFrame, silver_fqn: str):
    """Dedup-insert clean rows into silver on source_event_id (idempotent)."""
    from delta.tables import DeltaTable
    deduped = clean.dropDuplicates(["source_event_id"])
    tgt = DeltaTable.forName(spark, silver_fqn)
    (tgt.alias("t").merge(deduped.alias("s"), "t.source_event_id = s.source_event_id")
        .whenNotMatchedInsertAll().execute())


# --------------------------------------------------------------------------- #
# GOLD: current state, thresholds/anomalies, alerts, line health, metrics.
# Computed as pure functions over a *window* of silver telemetry.
# --------------------------------------------------------------------------- #
def _sensor_pivot(window_df: DataFrame) -> DataFrame:
    """Latest value per (asset, sensor) in the window -> one row per asset with
    sensor columns + latest operating_state + last_event_ts."""
    w = Window.partitionBy("asset_id", "sensor_type").orderBy(F.col("event_ts").desc())
    latest = (window_df.withColumn("rn", F.row_number().over(w))
                       .filter(F.col("rn") == 1))
    pivot = (latest.groupBy("site_id", "plant_id", "line_id", "asset_id")
                   .pivot("sensor_type", KNOWN_SENSORS)
                   .agg(F.first("sensor_value")))
    # latest operating_state + last_event_ts per asset (across all sensors)
    wa = Window.partitionBy("asset_id").orderBy(F.col("event_ts").desc())
    astate = (window_df.withColumn("rn", F.row_number().over(wa))
                       .filter(F.col("rn") == 1)
                       .select("asset_id",
                               F.col("operating_state"),
                               F.col("event_ts").alias("last_event_ts")))
    return pivot.join(astate, "asset_id", "left")


def _anomaly_scores(window_df: DataFrame) -> DataFrame:
    """Per (asset, sensor) sustained-abnormality stats over the window.

    Threshold/anomaly detection is based on the SUSTAINED AVERAGE of RUNNING
    readings (not a single max/min), so transient spikes don't force CRITICAL and
    low speed/throughput during normal IDLE/STARVED/BLOCKED states is NOT treated
    as a sensor breach. This yields a realistic WARNING/CRITICAL mix."""
    run = F.col("operating_state") == "RUNNING"
    stats = (window_df.groupBy("asset_id", "sensor_type")
             .agg(F.count("*").alias("n"),
                  F.sum(F.when(run, 1).otherwise(0)).alias("n_run"),
                  # RUNNING-only sustained value + out-of-band fraction
                  F.avg(F.when(run, F.col("sensor_value"))).alias("avg_run"),
                  F.sum(F.when(run & F.col("is_out_of_band"), 1).otherwise(0)).alias("oob_run"),
                  F.first("nominal_min").alias("nominal_min"),
                  F.first("nominal_max").alias("nominal_max"))
             .withColumn("oob_frac_run",
                         F.when(F.col("n_run") > 0, F.col("oob_run") / F.col("n_run")).otherwise(F.lit(0.0)))
             # anomaly score reflects sustained RUNNING breaches (needs >=3 samples)
             .withColumn("anomaly_score",
                         F.when(F.col("n_run") >= 3,
                                F.least(F.lit(1.0), F.col("oob_frac_run"))).otherwise(F.lit(0.0))))
    return stats


def compute_gold(spark, window_df: DataFrame, conf: dict) -> dict:
    """Return dict of gold DataFrames from a window of silver telemetry."""
    crit_margin = float(conf["crit_margin"])
    slowdown_pct = float(conf["slowdown_pct"])
    now = F.current_timestamp()

    scores = _anomaly_scores(window_df)  # no .cache(): serverless disallows PERSIST

    # ---- asset_current_state ----
    pivot = _sensor_pivot(window_df)
    asset_scores = (scores.groupBy("asset_id")
                    .agg(F.max("anomaly_score").alias("anomaly_score")))
    current_state = (pivot.join(asset_scores, "asset_id", "left")
        .withColumn("anomaly_score", F.coalesce("anomaly_score", F.lit(0.0)))
        .withColumn("condition",
                    F.when(F.col("anomaly_score") >= 0.7, "CRITICAL")
                     .when(F.col("anomaly_score") >= 0.4, "DEGRADED")
                     .when(F.col("anomaly_score") > 0.0, "WATCH")
                     .otherwise("HEALTHY"))
        .withColumn("updated_ts", now)
        .withColumnRenamed("motor_current", "motor_current")
        .select("site_id", "plant_id", "line_id", "asset_id", "last_event_ts",
                "updated_ts", "operating_state",
                F.col("temperature"), F.col("vibration"), F.col("pressure"),
                F.col("motor_current"), F.col("speed"), F.col("throughput"),
                F.col("energy"), "anomaly_score", "condition"))

    # ---- threshold / anomaly events (WARNING / CRITICAL) ----
    # Severity from the SUSTAINED RUNNING average (avg_run):
    #   WARNING  = avg beyond the nominal band (mild sustained deviation)
    #   CRITICAL = avg beyond the band by > crit_margin, OR a sustained breach
    #              (>=60% of RUNNING readings out of band)
    # Requires >=3 RUNNING samples so brief/transient dips don't raise alerts.
    band = (scores.filter((F.col("n_run") >= 3) & F.col("avg_run").isNotNull())
        .withColumn("upper_crit", F.col("nominal_max") * (1 + crit_margin))
        .withColumn("lower_crit", F.col("nominal_min") * (1 - crit_margin))
        .withColumn("breach_high", F.col("avg_run") > F.col("nominal_max"))
        .withColumn("breach_low", F.col("avg_run") < F.col("nominal_min"))
        .withColumn("crit_mag", (F.col("avg_run") > F.col("upper_crit")) |
                                (F.col("avg_run") < F.col("lower_crit")))
        .withColumn("severity",
            F.when(F.col("crit_mag") |
                   ((F.col("breach_high") | F.col("breach_low")) & (F.col("oob_frac_run") >= 0.6)),
                   "CRITICAL")
             .when(F.col("breach_high") | F.col("breach_low"), "WARNING")
             .otherwise(F.lit(None)))
        .filter(F.col("severity").isNotNull()))

    events = (band.join(window_df.select("asset_id", "site_id", "plant_id", "line_id")
                                 .dropDuplicates(["asset_id"]), "asset_id", "left")
        .withColumn("alert_type",
                    F.when(F.col("sensor_type").isin("vibration", "temperature") &
                           (F.col("anomaly_score") >= 0.4), "TREND")
                     .otherwise("THRESHOLD"))
        .withColumn("observed_value", F.round(F.col("avg_run"), 3))
        .withColumn("threshold_value",
                    F.when(F.col("breach_high"), F.col("nominal_max")).otherwise(F.col("nominal_min")))
        .withColumn("deviation", F.col("observed_value") - F.col("threshold_value"))
        .withColumn("created_ts", now)
        .withColumn("event_id", F.sha2(F.concat_ws("|", "asset_id", "sensor_type",
                    F.date_format(now, "yyyyMMddHHmm")), 256))
        .withColumn("reason", F.concat(
            F.lit("sensor="), F.col("sensor_type"),
            F.lit(" observed="), F.round("observed_value", 2).cast("string"),
            F.lit(" threshold="), F.round("threshold_value", 2).cast("string"),
            F.lit(" oob_frac="), F.round("oob_frac_run", 2).cast("string")))
        .select("event_id", "created_ts", "site_id", "plant_id", "line_id", "asset_id",
                "sensor_type", "alert_type", "severity", "observed_value",
                "threshold_value", "deviation", "anomaly_score", "reason"))

    # ---- active_alerts (stable alert_id per asset+sensor+type) ----
    def _reco(sensor, alert_type):
        return (F.when(sensor == "temperature", "Inspect cooling / reduce load; check setpoint")
                 .when(sensor == "vibration", "Inspect bearings/alignment; schedule maintenance")
                 .when(sensor == "pressure", "Check valves/seals; verify pressure control loop")
                 .when(sensor == "throughput", "Check upstream feed & downstream blockage")
                 .when(sensor == "speed", "Verify drive/motor; check for mechanical drag")
                 .otherwise("Investigate asset; correlate with recent events"))

    alerts = (events
        .withColumn("alert_id", F.sha2(F.concat_ws("|", "asset_id", "sensor_type", "alert_type"), 256))
        .withColumn("status", F.lit("NEW"))
        .withColumn("confidence",
                    F.least(F.lit(0.99), F.lit(0.5) + F.col("anomaly_score") * 0.5))
        .withColumn("recommended_action", _reco(F.col("sensor_type"), F.col("alert_type")))
        .withColumn("root_cause_hint",
            F.concat(F.lit("Likely "), F.col("sensor_type"),
                     F.lit(" issue on "), F.col("asset_id"),
                     F.when(F.col("alert_type") == "TREND",
                            F.lit(" (sustained trend)")).otherwise(F.lit(" (threshold breach)"))))
        .withColumn("updated_ts", now)
        .withColumn("last_event_ts", now)
        .select("alert_id", F.col("created_ts"), "updated_ts", "site_id", "plant_id",
                "line_id", "asset_id", "alert_type", "severity", "status", "sensor_type",
                "observed_value", "threshold_value", "anomaly_score", "confidence",
                "recommended_action", "root_cause_hint", "last_event_ts"))

    # ---- line_health_summary ----
    # baseline throughput = midpoint of nominal band for throughput sensor
    thr = (window_df.filter(F.col("sensor_type") == "throughput")
           .groupBy("site_id", "plant_id", "line_id", "product_family")
           .agg(F.avg("sensor_value").alias("avg_throughput"),
                F.avg((F.col("nominal_min") + F.col("nominal_max")) / 2).alias("baseline_throughput")))
    spd = (window_df.filter(F.col("sensor_type") == "speed")
           .groupBy("line_id").agg(F.avg("sensor_value").alias("avg_speed")))
    # asset state counts per line (latest state per asset)
    wa = Window.partitionBy("asset_id").orderBy(F.col("event_ts").desc())
    latest_state = (window_df.withColumn("rn", F.row_number().over(wa))
                    .filter(F.col("rn") == 1)
                    .select("line_id", "asset_id", "operating_state"))
    counts = (latest_state.groupBy("line_id").agg(
        F.countDistinct("asset_id").alias("total_assets"),
        F.sum(F.when(F.col("operating_state") == "RUNNING", 1).otherwise(0)).alias("running_assets"),
        F.sum(F.when(F.col("operating_state") == "IDLE", 1).otherwise(0)).alias("idle_assets"),
        F.sum(F.when(F.col("operating_state") == "FAULT", 1).otherwise(0)).alias("faulted_assets")))
    alert_counts = (alerts.groupBy("line_id").agg(
        F.sum(F.when(F.col("severity") == "WARNING", 1).otherwise(0)).alias("open_warnings"),
        F.sum(F.when(F.col("severity") == "CRITICAL", 1).otherwise(0)).alias("open_criticals")))
    # worst asset per line (highest anomaly score) for slowdown hint
    line_worst = (scores.join(window_df.select("asset_id", "line_id").dropDuplicates(["asset_id"]),
                              "asset_id", "left")
                  .groupBy("line_id")
                  .agg(F.max_by(F.struct("anomaly_score", "asset_id", "sensor_type"),
                                "anomaly_score").alias("worst")))
    line_health = (thr.join(spd, "line_id", "left")
        .join(counts, "line_id", "left")
        .join(alert_counts, "line_id", "left")
        .join(line_worst, "line_id", "left")
        .withColumn("throughput_pct",
                    F.round(F.col("avg_throughput") / F.col("baseline_throughput") * 100, 1))
        .withColumn("is_slowing_down", F.col("throughput_pct") < slowdown_pct)
        .withColumn("open_warnings", F.coalesce("open_warnings", F.lit(0)))
        .withColumn("open_criticals", F.coalesce("open_criticals", F.lit(0)))
        .withColumn("health_status",
            F.when(F.col("open_criticals") > 0, "CRITICAL")
             .when(F.col("is_slowing_down") | (F.col("open_warnings") > 0), "DEGRADED")
             .when(F.col("faulted_assets") > 0, "WATCH")
             .otherwise("HEALTHY"))
        .withColumn("slowdown_hint",
            F.when(F.col("is_slowing_down"),
                   F.concat(F.lit("Throughput at "), F.col("throughput_pct").cast("string"),
                            F.lit("% of baseline; check "), F.col("worst.asset_id"),
                            F.lit(" ("), F.col("worst.sensor_type"), F.lit(")")))
             .otherwise(F.lit("nominal")))
        .withColumn("updated_ts", now)
        .select("site_id", "plant_id", "line_id", "product_family", "updated_ts",
                "health_status", "running_assets", "idle_assets", "faulted_assets",
                "total_assets", "avg_throughput", "baseline_throughput", "throughput_pct",
                "avg_speed", "open_warnings", "open_criticals", "is_slowing_down", "slowdown_hint"))

    # ---- quality metrics ----
    metrics = window_df.agg(
        F.current_timestamp().alias("window_ts"),
        F.count("*").alias("records_in"),
        F.count("*").alias("records_clean"),
        F.lit(0).cast("bigint").alias("records_dup"),
        F.sum(F.when(F.col("ingest_latency_ms") > 60000, 1).otherwise(0)).cast("bigint").alias("records_late"),
        F.lit(0).cast("bigint").alias("records_quarantined"),
        F.lit(0.0).alias("events_per_sec"),
        F.avg("ingest_latency_ms").alias("processing_latency_ms"))

    # add active_alert_count to current state (target table has this column)
    alert_ct = alerts.groupBy("asset_id").agg(F.count("*").alias("active_alert_count"))
    current_state = (current_state.join(alert_ct, "asset_id", "left")
                     .withColumn("active_alert_count", F.coalesce("active_alert_count", F.lit(0))))

    return {"current_state": current_state, "events": events, "alerts": alerts,
            "line_health": line_health, "metrics": metrics}


# --------------------------------------------------------------------------- #
# MERGE gold outputs
# --------------------------------------------------------------------------- #
def _merge(spark, df, fqn, keys):
    from delta.tables import DeltaTable
    cond = " AND ".join([f"t.{k} = s.{k}" for k in keys])
    tgt = DeltaTable.forName(spark, fqn)
    (tgt.alias("t").merge(df.alias("s"), cond)
        .whenMatchedUpdateAll().whenNotMatchedInsertAll().execute())


def write_gold(spark, gold: dict, conf: dict):
    g = f"{conf['catalog']}.{conf['gold']}"
    _merge(spark, gold["current_state"], f"{g}.asset_current_state", ["asset_id"])
    gold["events"].write.mode("append").saveAsTable(f"{g}.asset_anomaly_events")
    # alerts: preserve acknowledgements — only update alert-derived fields, keep ack cols
    _merge_alerts(spark, gold["alerts"], f"{g}.active_alerts")
    _auto_resolve_alerts(spark, gold["alerts"], f"{g}.active_alerts")
    _merge(spark, gold["line_health"], f"{g}.line_health_summary", ["line_id"])
    gold["metrics"].write.mode("append").saveAsTable(f"{g}.telemetry_quality_metrics")


def _auto_resolve_alerts(spark, current_df, fqn):
    """Mark still-'NEW' alerts that are no longer breaching (not in the current
    window's alert set) as RESOLVED. Operator-owned states (ACKNOWLEDGED /
    MAINTENANCE / operator RESOLVED) are left untouched. History is retained in
    asset_anomaly_events (append-only)."""
    from delta.tables import DeltaTable
    ids = [r["alert_id"] for r in current_df.select("alert_id").distinct().collect()]
    tgt = DeltaTable.forName(spark, fqn)
    if ids:
        idlist = ",".join("'" + i.replace("'", "") + "'" for i in ids)
        cond = f"status = 'NEW' AND alert_id NOT IN ({idlist})"
    else:
        cond = "status = 'NEW'"
    tgt.update(condition=cond, set={
        "status": "'RESOLVED'",
        "resolved_ts": "current_timestamp()",
        "updated_ts": "current_timestamp()",
    })


def _merge_alerts(spark, df, fqn):
    """Upsert alerts by alert_id WITHOUT clobbering acknowledgement/resolution state."""
    from delta.tables import DeltaTable
    tgt = DeltaTable.forName(spark, fqn)
    (tgt.alias("t").merge(df.alias("s"), "t.alert_id = s.alert_id")
        .whenMatchedUpdate(set={
            "updated_ts": "s.updated_ts",
            "severity": "s.severity",
            "observed_value": "s.observed_value",
            "threshold_value": "s.threshold_value",
            "anomaly_score": "s.anomaly_score",
            "confidence": "s.confidence",
            "recommended_action": "s.recommended_action",
            "root_cause_hint": "s.root_cause_hint",
            "last_event_ts": "s.last_event_ts",
            # NOTE: status / acknowledged_by / acknowledged_ts / resolved_ts are
            # preserved (operator-owned; only reset if re-fires after resolve).
            "status": "CASE WHEN t.status = 'RESOLVED' THEN 'NEW' ELSE t.status END",
        })
        .whenNotMatchedInsert(values={c: f"s.{c}" for c in df.columns})
        .execute())


# --------------------------------------------------------------------------- #
# Lakebase direct-write serving sync (inline so it deploys as a single notebook)
#
# This is the DIRECT-WRITE serving path: the pipeline upserts the small current
# operational view (current_state, line_health, active_alerts) straight into
# Lakebase Postgres via psycopg. Lower latency and full control vs. managed synced
# tables (the documented managed alternative, ~15s). Operator-owned columns on
# active_alerts (status / acknowledged_* / resolved_ts) are PRESERVED on upsert so
# acknowledgements made in the app are never clobbered by the pipeline.
# --------------------------------------------------------------------------- #
def _ensure_psycopg():
    try:
        import psycopg  # noqa
    except ImportError:
        import subprocess as _sp
        _sp.check_call([sys.executable, "-m", "pip", "install", "-q", "psycopg[binary]"])


def _lakebase_conn(conf):
    """Open a psycopg connection using a short-lived Lakebase OAuth token
    minted via the Databricks SDK (job identity). Never hardcodes secrets."""
    _ensure_psycopg()
    import uuid
    import psycopg
    from databricks.sdk import WorkspaceClient
    w = WorkspaceClient()
    user = w.current_user.me().user_name
    # Mint a Lakebase OAuth token via the REST API (version-independent; the
    # typed w.database client is not present in all SDK versions).
    cred = w.api_client.do("POST", "/api/2.0/database/credentials",
                           body={"request_id": str(uuid.uuid4()),
                                 "instance_names": [conf["lakebase_instance"]]})
    token = cred["token"] if isinstance(cred, dict) else cred.token
    return psycopg.connect(
        host=conf["lakebase_host"], port=5432, dbname=conf["lakebase_db"],
        user=user, password=token, sslmode="require", autocommit=True)


def _upsert(cur, table, rows, cols, pk, preserve=(), conflict_set=None):
    """Upsert `rows` into a Lakebase serving table.

    - `preserve`: columns whose existing value is kept untouched on conflict
      (operator-owned state that the pipeline must never clobber).
    - `conflict_set`: {column: sql_expr} giving an explicit ON CONFLICT ... DO
      UPDATE assignment for a column (may reference the target table for the OLD
      value and EXCLUDED for the incoming value). Used to model re-fire semantics
      that a plain EXCLUDED assignment can't express. Overrides both the default
      EXCLUDED assignment and `preserve` for that column.
    """
    if not rows:
        return 0
    conflict_set = conflict_set or {}
    collist = ",".join(cols)
    placeholders = ",".join(["%s"] * len(cols))
    upd = [f"{c}=EXCLUDED.{c}" for c in cols
           if c not in pk and c not in preserve and c not in conflict_set]
    for c, expr in conflict_set.items():  # explicit re-fire / conditional updates
        upd.append(f"{c}={expr}")
    upd.append("synced_ts=now()")  # advance serving freshness on every upsert
    sql = (f"INSERT INTO pharmaco_mfg_rt.{table} ({collist}) VALUES ({placeholders}) "
           f"ON CONFLICT ({','.join(pk)}) DO UPDATE SET {','.join(upd)}")
    cur.executemany(sql, [[r[c] for c in cols] for r in rows])
    return len(rows)


def _sync_to_lakebase(gold: dict, conf: dict):
    import time as _t
    t0 = _t.perf_counter()
    cs = [r.asDict() for r in gold["current_state"].collect()]
    lh = [r.asDict() for r in gold["line_health"].collect()]
    al = [r.asDict() for r in gold["alerts"].collect()]
    with _lakebase_conn(conf) as conn:
        with conn.cursor() as cur:
            n1 = _upsert(cur, "asset_current_state", cs,
                         ["asset_id", "site_id", "plant_id", "line_id", "last_event_ts",
                          "updated_ts", "operating_state", "temperature", "vibration",
                          "pressure", "motor_current", "speed", "throughput", "energy",
                          "anomaly_score", "condition", "active_alert_count"], pk=["asset_id"])
            n2 = _upsert(cur, "line_health_summary", lh,
                         ["line_id", "site_id", "plant_id", "product_family", "updated_ts",
                          "health_status", "running_assets", "idle_assets", "faulted_assets",
                          "total_assets", "avg_throughput", "baseline_throughput",
                          "throughput_pct", "avg_speed", "open_warnings", "open_criticals",
                          "is_slowing_down", "slowdown_hint"], pk=["line_id"])
            # active_alerts: preserve operator-owned columns on conflict, but honor
            # RE-FIRE — an alert that was RESOLVED (by operator or auto-resolve) and
            # is breaching again returns to NEW so it reappears in the app. This
            # mirrors the Delta gold merge (_merge_alerts). Without this, a re-firing
            # alert stays stuck 'RESOLVED' in the serving layer and the asset's
            # "Active alerts" panel shows nothing despite an active breach.
            # ACKNOWLEDGED / MAINTENANCE are operator-owned and left untouched; only a
            # RESOLVED->NEW transition clears the now-stale resolution/ack metadata.
            # NB: inside ON CONFLICT ... DO UPDATE SET, the existing row is
            # referenced by the bare table name (not schema-qualified).
            _WAS_RESOLVED = "active_alerts.status = 'RESOLVED'"
            n3 = _upsert(cur, "active_alerts", al,
                         ["alert_id", "created_ts", "updated_ts", "site_id", "plant_id",
                          "line_id", "asset_id", "alert_type", "severity", "sensor_type",
                          "observed_value", "threshold_value", "anomaly_score", "confidence",
                          "recommended_action", "root_cause_hint", "last_event_ts", "status"],
                         pk=["alert_id"],
                         preserve=("acknowledged_by", "acknowledged_ts", "resolved_ts"),
                         conflict_set={
                             "status": f"CASE WHEN {_WAS_RESOLVED} THEN 'NEW' "
                                       f"ELSE active_alerts.status END",
                             "resolved_ts": f"CASE WHEN {_WAS_RESOLVED} THEN NULL "
                                            f"ELSE active_alerts.resolved_ts END",
                             "acknowledged_by": f"CASE WHEN {_WAS_RESOLVED} THEN NULL "
                                                f"ELSE active_alerts.acknowledged_by END",
                             "acknowledged_ts": f"CASE WHEN {_WAS_RESOLVED} THEN NULL "
                                                f"ELSE active_alerts.acknowledged_ts END",
                         })
    dt = (_t.perf_counter() - t0) * 1000
    print(f"[pipeline] lakebase direct-write: assets={n1} lines={n2} alerts={n3} "
          f"in {dt:.0f}ms (serving write latency)")


# --------------------------------------------------------------------------- #
# Driver
# --------------------------------------------------------------------------- #
def process_batch_df(spark, bronze_batch: DataFrame, conf: dict):
    """foreachBatch body — reused for stream and batch modes."""
    clean, quarantine = validate_and_normalize(bronze_batch)
    silver_fqn = f"{conf['catalog']}.{conf['silver']}.telemetry_clean"
    quarantine_fqn = f"{conf['catalog']}.{conf['silver']}.telemetry_quarantine"
    merge_silver(spark, clean, silver_fqn)
    if not quarantine.isEmpty():
        quarantine.write.mode("append").saveAsTable(quarantine_fqn)

    # recompute gold from a recent window of silver (stateful-over-window)
    wm = int(conf["window_minutes"])
    window_df = (spark.table(silver_fqn)
                 .filter(F.col("event_ts") >= F.current_timestamp() - F.expr(f"INTERVAL {wm} MINUTES")))
    if window_df.isEmpty():
        return
    gold = compute_gold(spark, window_df, conf)
    write_gold(spark, gold, conf)
    if str(conf.get("lakebase_sync", "false")).lower() == "true":
        strict = str(conf.get("strict_sync", "false")).lower() == "true"
        try:
            _sync_to_lakebase(gold, conf)
        except Exception as e:  # never fail the batch on serving-sync issues
            import traceback as _tb
            msg = f"{type(e).__name__}: {e} | {_tb.format_exc()[-400:]}"
            print(f"[pipeline] lakebase sync skipped: {msg}")
            try:
                spark.createDataFrame(
                    [("lb-sync-err", "lakebase_sync", 0, 0, 0, 0, 0.0, msg)],
                    "ingest_batch_id string, ingest_source string, records_accepted long, "
                    "records_rejected long, schema_errors long, retry_count long, "
                    "last_ack_latency_ms double, notes string"
                ).withColumn("ts", F.current_timestamp()).write.mode("append").saveAsTable(
                    f"{conf['catalog']}.{conf['bronze']}.ingest_health")
            except Exception:
                pass
            if strict:
                raise


def main():
    spark = SparkSession.builder.getOrCreate()
    conf = _get_conf(spark)
    bronze_fqn = f"{conf['catalog']}.{conf['bronze']}.telemetry_raw"
    print(f"[pipeline] mode={conf['mode']} bronze={bronze_fqn}")

    if conf["mode"] == "batch":
        process_batch_df(spark, spark.table(bronze_fqn), conf)
        print("[pipeline] batch recompute complete")
        return

    if conf["mode"] == "loop":
        # Serverless-friendly continuous processing: repeated idempotent recompute
        # over a recent window. Runs until the job run is cancelled. (Serverless
        # does not support ProcessingTime triggers or continuous-job semantics;
        # for true ProcessingTime streaming use a CLASSIC cluster.)
        import time as _t
        interval = float(conf.get("trigger_seconds", "5"))
        print(f"[pipeline] loop mode; interval={interval}s")
        import traceback as _tb
        health_fqn = f"{conf['catalog']}.{conf['bronze']}.ingest_health"
        n = 0
        while True:
            t0 = _t.perf_counter()
            err = ""
            try:
                process_batch_df(spark, spark.table(bronze_fqn), conf)
            except Exception as e:
                err = f"{type(e).__name__}: {e} | {_tb.format_exc()[-500:]}"
                print(f"[pipeline] loop iter error (continuing): {err}")
            n += 1
            # heartbeat/error to Delta so it is observable on serverless
            try:
                hb = spark.createDataFrame(
                    [(f"loop-{n}", None, "loop_pipeline", 0, 0, 0, n, None,
                      round((_t.perf_counter()-t0)*1000, 1), err or "ok")],
                    "ingest_batch_id string, ts timestamp, ingest_source string, "
                    "records_accepted long, records_rejected long, schema_errors long, "
                    "retry_count long, last_ack_ts timestamp, last_ack_latency_ms double, notes string")
                hb = hb.withColumn("ts", F.current_timestamp())
                hb.write.mode("append").saveAsTable(health_fqn)
            except Exception:
                pass
            _t.sleep(interval)
        return

    # mode == "once": AvailableNow (drain all new bronze, then stop) — checkpointed
    stream = (spark.readStream.format("delta").table(bronze_fqn))
    q = (stream.writeStream
         .option("checkpointLocation", f"{conf['checkpoint']}/rt_pipeline")
         .foreachBatch(lambda df, _id: process_batch_df(spark, df, conf))
         .trigger(availableNow=True).start())
    q.awaitTermination()


if __name__ == "__main__":
    main()
