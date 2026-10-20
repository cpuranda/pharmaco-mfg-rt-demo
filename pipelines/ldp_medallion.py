"""
Lakeflow Spark Declarative Pipeline — PharmaCo MFG real-time demo (SYNTHETIC data)
===================================================================================

Declarative counterpart of the operational Structured Streaming job. Reads the
Zerobus-landed bronze table and publishes, into the pipeline's target schema:

  silver_telemetry          streaming table, data-quality EXPECTATIONS enforced,
                            deduplicated on source_event_id
  silver_quarantine         streaming table of rows that violate the expectations
  gold_asset_features_20s   materialized view: per-asset 20-second sensor features +
                            synthetic ground-truth label  (training set for the ML model)
  gold_line_kpis_1m         materialized view: per-line per-minute throughput vs baseline,
                            out-of-band rate, availability (OEE-style KPIs)
  gold_asset_reliability    materialized view: per-asset out-of-band minutes / breach counts

Pipeline configuration keys:
  pharmaco.bronze_table     fully-qualified bronze table (default below)
"""
from pyspark import pipelines as dp
from pyspark.sql import functions as F

BRONZE_TABLE = spark.conf.get(  # noqa: F821  (spark is injected by the pipeline runtime)
    "pharmaco.bronze_table",
    "hls_streaming_solution_demo_catalog.pharmaco_mfg_rt_bronze.telemetry_raw")

KNOWN_SENSORS = ["temperature", "vibration", "pressure", "motor_current",
                 "speed", "throughput", "energy"]
_sensor_list = ",".join(f"'{s}'" for s in KNOWN_SENSORS)

DQ_RULES = {
    "valid_source_event_id": "source_event_id IS NOT NULL",
    "valid_asset": "asset_id IS NOT NULL AND line_id IS NOT NULL",
    "valid_event_ts": "event_ts IS NOT NULL",
    "valid_sensor_type": f"lower(trim(sensor_type)) IN ({_sensor_list})",
    "valid_sensor_value": "sensor_value IS NOT NULL AND NOT isnan(sensor_value)",
}
_ALL_VALID = " AND ".join(f"({r})" for r in DQ_RULES.values())

# Synthetic ground truth: which asset a scenario physically affects (mirrors the
# generator's deterministic targets). Used ONLY to evaluate the ML model.
_TRUTH = """
  CASE
    WHEN scenario = 'temp_excursion'        AND asset_id = 'A-COMPRESS' THEN 1
    WHEN scenario = 'vibration_degradation' AND asset_id = 'B-FILL'     THEN 1
    WHEN scenario = 'pressure_instability'  AND asset_id = 'B-LYO'      THEN 1
    WHEN scenario = 'line_slowdown'         AND line_id  = 'LINE-A'     THEN 1
    WHEN scenario = 'alarm_burst'           AND line_id  = 'LINE-B'     THEN 1
    ELSE 0 END"""


@dp.temporary_view(name="bronze_telemetry_v",
                   comment="Streaming view over the Zerobus bronze landing table (SYNTHETIC).")
def bronze_telemetry_v():
    return spark.readStream.table(BRONZE_TABLE)  # noqa: F821


@dp.table(name="silver_telemetry",
          comment="Validated + deduplicated telemetry; expectations enforced (SYNTHETIC).",
          table_properties={"quality": "silver"},
          cluster_by=["asset_id"])
@dp.expect_all_or_drop(DQ_RULES)
@dp.expect("in_nominal_band_or_flagged", "sensor_value IS NULL OR nominal_min IS NOT NULL")
def silver_telemetry():
    return (spark.readStream.table("bronze_telemetry_v")  # noqa: F821
            .withColumn("sensor_type", F.lower(F.trim("sensor_type")))
            .withColumn("is_out_of_band",
                        (F.col("sensor_value") < F.col("nominal_min")) |
                        (F.col("sensor_value") > F.col("nominal_max")))
            .withWatermark("event_ts", "2 hours")
            .dropDuplicatesWithinWatermark(["source_event_id"])
            .drop("ingest_batch_id"))


@dp.table(name="silver_quarantine",
          comment="Rows failing any silver expectation, with the failed rule names (SYNTHETIC).",
          table_properties={"quality": "quarantine"})
def silver_quarantine():
    failed = F.concat_ws(",", *[F.when(~F.expr(r), F.lit(n)) for n, r in DQ_RULES.items()])
    return (spark.readStream.table("bronze_telemetry_v")  # noqa: F821
            .filter(f"NOT ({_ALL_VALID}) OR ({_ALL_VALID}) IS NULL")
            .withColumn("failed_rules", failed)
            .withColumn("quarantined_ts", F.current_timestamp()))


@dp.materialized_view(
    name="gold_asset_features_20s",
    comment="Per-asset 20-second sensor feature vectors + synthetic ground-truth label "
            "(training/scoring set for the anomaly model).",
    table_properties={"quality": "gold"})
def gold_asset_features_20s():
    s = spark.read.table("silver_telemetry")  # noqa: F821
    run = F.col("operating_state") == "RUNNING"
    aggs = []
    for sn in KNOWN_SENSORS:
        v = F.when(F.col("sensor_type") == sn, F.col("sensor_value"))
        aggs += [F.avg(v).alias(f"{sn}_avg"), F.stddev(v).alias(f"{sn}_std"),
                 F.max(v).alias(f"{sn}_max")]
    aggs += [
        F.avg(F.col("is_out_of_band").cast("double")).alias("oob_frac"),
        F.avg(run.cast("double")).alias("running_frac"),
        F.count("*").alias("n_readings"),
        F.max(F.expr(_TRUTH)).alias("label_is_anomalous"),
        F.max(F.when(F.col("scenario") != "normal", F.col("scenario"))).alias("active_scenario"),
    ]
    return (s.groupBy(F.window("event_ts", "20 seconds").alias("w"),
                      "line_id", "asset_id")
             .agg(*aggs)
             .select(F.col("w.start").alias("window_start"), F.col("w.end").alias("window_end"),
                     "*").drop("w"))


@dp.materialized_view(
    name="gold_line_kpis_1m",
    comment="Per-line per-minute KPIs: throughput vs baseline, availability, out-of-band rate.",
    table_properties={"quality": "gold"})
def gold_line_kpis_1m():
    s = spark.read.table("silver_telemetry")  # noqa: F821
    thr = F.col("sensor_type") == "throughput"
    return (s.groupBy(F.window("event_ts", "1 minute").alias("w"), "line_id", "product_family")
             .agg(F.avg(F.when(thr, F.col("sensor_value"))).alias("avg_throughput_units_min"),
                  F.avg(F.when(thr, (F.col("nominal_min") + F.col("nominal_max")) / 2))
                   .alias("baseline_units_min"),
                  F.avg((F.col("operating_state") == "RUNNING").cast("double")).alias("availability"),
                  F.avg(F.col("is_out_of_band").cast("double")).alias("out_of_band_rate"),
                  F.countDistinct("asset_id").alias("assets_reporting"))
             .withColumn("throughput_pct_of_baseline",
                         F.round(F.col("avg_throughput_units_min") / F.col("baseline_units_min") * 100, 1))
             .select(F.col("w.start").alias("minute"), "*").drop("w"))


@dp.materialized_view(
    name="gold_asset_reliability",
    comment="Per-asset reliability rollup: out-of-band readings and minutes in breach per sensor.",
    table_properties={"quality": "gold"})
def gold_asset_reliability():
    s = spark.read.table("silver_telemetry")  # noqa: F821
    return (s.filter("is_out_of_band")
             .groupBy("line_id", "asset_id", "sensor_type")
             .agg(F.count("*").alias("out_of_band_readings"),
                  F.countDistinct(F.date_trunc("minute", "event_ts")).alias("minutes_in_breach"),
                  F.min("event_ts").alias("first_breach_ts"),
                  F.max("event_ts").alias("last_breach_ts")))
