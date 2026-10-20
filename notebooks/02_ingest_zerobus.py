# Databricks notebook source
# MAGIC %md
# MAGIC # 02 · Lakeflow Connect — Zerobus Ingest of synthetic line telemetry
# MAGIC
# MAGIC A synthetic generator (`generator/telemetry_generator.py`, stdlib only, deterministic seed)
# MAGIC simulates **1 plant · 3 lines · 15 assets · 7 sensors**:
# MAGIC LINE-A oral-solid-dose tablets, LINE-B sterile vials, LINE-C packaging.
# MAGIC
# MAGIC Records are pushed **directly into the Unity Catalog bronze Delta table with Zerobus Ingest**
# MAGIC (Lakeflow Connect's direct-write gRPC API, OAuth service principal, protobuf schema) — no
# MAGIC message broker in between.
# MAGIC
# MAGIC Ingestion plan (all synthetic):
# MAGIC 1. **Historical backfill** — 30 min of simulated plant history containing three labelled
# MAGIC    failure scenarios (vibration degradation on B-FILL, pressure instability on B-LYO, line
# MAGIC    slowdown on LINE-A). This is the training history for the anomaly model (notebook 06).
# MAGIC 2. **Live stream** — ~3 min paced in real time with a **temperature excursion on the
# MAGIC    A-COMPRESS tablet press** so the operational pipeline raises live alerts.
# MAGIC 3. **Data-quality probes** — duplicate re-emissions and malformed records, to prove dedup
# MAGIC    and quarantine downstream.

# COMMAND ----------

# MAGIC %pip install -q databricks-zerobus-ingest-sdk==1.4.0 protobuf==5.29.5 googleapis-common-protos==1.65.0
# MAGIC %restart_python

# COMMAND ----------

# MAGIC %run ./_config

# COMMAND ----------

import telemetry_pb2
from telemetry_generator import TelemetryGenerator, GeneratorConfig, Scenario
from zerobus_ingest import ZerobusIngestor, event_to_proto
from datetime import timedelta

client_id = dbutils.secrets.get(SECRET_SCOPE, "sp_client_id")
client_secret = dbutils.secrets.get(SECRET_SCOPE, "sp_oauth_secret")
workspace_url = "https://" + spark.conf.get("spark.databricks.workspaceUrl")
print(f"Zerobus endpoint : {ZB_ENDPOINT}")
print(f"target table     : {BRONZE_TABLE}")
print(f"ingest identity  : service principal {client_id} (secret from scope '{SECRET_SCOPE}')")
before = spark.table(BRONZE_TABLE).count()
print(f"bronze rows before this run: {before}")

# COMMAND ----------

# MAGIC %md ### 1 · Historical backfill (30 simulated minutes, 2 s ticks, labelled scenarios)

# COMMAND ----------

TICK_S = 2.0
PLAN = [  # (scenario, ticks)
    ("normal", 240),
    ("vibration_degradation", 120),
    ("normal", 90),
    ("pressure_instability", 120),
    ("normal", 90),
    ("line_slowdown", 120),
    ("normal", 120),
]
total_ticks = sum(n for _, n in PLAN)
start = datetime.now(timezone.utc) - timedelta(seconds=total_ticks * TICK_S + 240)
gen = TelemetryGenerator(GeneratorConfig(seed=20261020, tick_seconds=TICK_S,
                                         deterministic_time=True,
                                         start_time_iso=start.isoformat()))
ing = ZerobusIngestor(ZB_ENDPOINT, workspace_url, BRONZE_TABLE, client_id, client_secret)
print(f"stream open  ingest_batch_id={ing.ingest_batch_id}  sim start={start.isoformat(timespec='seconds')}")
t0 = time.time()
for scen, n in PLAN:
    if scen == "normal":
        gen.clear_scenario()
    else:
        gen.inject_scenario(Scenario(scen), duration_ticks=n)
    for _ in range(n):
        for ev in gen.tick():
            ing.ingest_event(ev)
    ing.flush()
    print(f"  phase {scen:<22} ticks={n:<4} accepted_total={ing.accepted:>7}  "
          f"last_enqueue_latency_ms={ing.last_ack_latency_ms:.3f}")
ing.close()
dt = time.time() - t0
print(f"BACKFILL DONE: {ing.accepted} records in {dt:.1f}s ({ing.accepted/dt:.0f} rec/s)")
backfill_health = ing.health_row("historical backfill (30 simulated minutes)")

# COMMAND ----------

# MAGIC %md ### 2 · Live stream — temperature excursion on A-COMPRESS, paced in real time

# COMMAND ----------

live = TelemetryGenerator(GeneratorConfig(seed=20261021, tick_seconds=1.0, deterministic_time=False))
live.inject_scenario(Scenario.TEMP_EXCURSION, duration_ticks=600, target_asset="A-COMPRESS")
ing2 = ZerobusIngestor(ZB_ENDPOINT, workspace_url, BRONZE_TABLE, client_id, client_secret)
LIVE_TICKS = 180
t0 = time.time()
for t in range(LIVE_TICKS):
    tick_start = time.time()
    for ev in live.tick():
        ing2.ingest_event(ev)
    if (t + 1) % 30 == 0:
        ing2.flush()
        print(f"  live tick {t+1:>3}/{LIVE_TICKS}  accepted={ing2.accepted:>6}  "
              f"enqueue_latency_ms={ing2.last_ack_latency_ms:.3f}")
    time.sleep(max(0.0, 1.0 - (time.time() - tick_start)))
ing2.flush()
print(f"LIVE DONE: {ing2.accepted} records over {time.time()-t0:.0f}s wall clock")

# COMMAND ----------

# MAGIC %md ### 3 · Data-quality probes: duplicate re-emissions + malformed records

# COMMAND ----------

# (a) duplicates: re-send 200 already-emitted logical readings (same source_event_id, new event_id)
dup_src = live.tick()[:200]
for ev in dup_src:
    ing2.ingest_event(ev)
for ev in dup_src:
    ev2 = dict(ev); ev2["event_id"] = ev["event_id"] + "-retry"
    ing2.ingest_event(ev2)
# (b) malformed: unknown sensor type and missing value / asset
bad = []
for i in range(5):
    r = event_to_proto(dup_src[i], ing2.ingest_batch_id, source="zerobus")
    r.source_event_id = f"bad-unknown-sensor-{uuid.uuid4().hex[:8]}"; r.sensor_type = "humidity_x"
    bad.append(r)
for i in range(3):
    r = event_to_proto(dup_src[i], ing2.ingest_batch_id, source="zerobus")
    r.source_event_id = f"bad-null-value-{uuid.uuid4().hex[:8]}"; r.ClearField("sensor_value")
    bad.append(r)
for r in bad:
    ing2._stream.ingest_record_offset(r.SerializeToString())
ing2.accepted += len(bad)
ing2.close()
print(f"sent {len(dup_src)} originals + {len(dup_src)} duplicate re-emissions + {len(bad)} malformed records")
live_health = ing2.health_row("live temp_excursion + DQ probes")

# COMMAND ----------

# MAGIC %md ### 4 · Ingestion health + what landed in bronze

# COMMAND ----------

from pyspark.sql import Row
spark.createDataFrame([Row(**backfill_health), Row(**live_health)]).write.mode("append") \
    .saveAsTable(f"{CATALOG}.{BRONZE}.ingest_health")
display(spark.table(f"{CATALOG}.{BRONZE}.ingest_health").orderBy("ts", ascending=False).limit(5))

after = spark.table(BRONZE_TABLE).count()
print(f"bronze rows: before={before}  after={after}  landed_this_run={after-before}")
display(sql(f"""
  SELECT ingest_source, scenario, count(*) AS records,
         count(DISTINCT asset_id) AS assets, min(event_ts) AS first_event, max(event_ts) AS last_event
  FROM {BRONZE_TABLE} GROUP BY ingest_source, scenario ORDER BY first_event"""))

# COMMAND ----------

display(sql(f"""
  SELECT line_id, asset_id, sensor_type, round(sensor_value,2) AS value, unit,
         nominal_min, nominal_max, operating_state, scenario, event_ts, ingested_ts
  FROM {BRONZE_TABLE} WHERE asset_id = 'A-COMPRESS' AND sensor_type = 'temperature'
  ORDER BY event_ts DESC LIMIT 10"""))
display(sql(f"""
  SELECT count(*) AS live_records,
         round(percentile(unix_millis(ingested_ts) - unix_millis(event_ts), 0.5), 0) AS event_to_writer_ms_p50,
         round(percentile(unix_millis(ingested_ts) - unix_millis(event_ts), 0.95), 0) AS event_to_writer_ms_p95
  FROM {BRONZE_TABLE}
  WHERE ingested_ts >= current_timestamp() - INTERVAL 10 MINUTES AND scenario = 'temp_excursion'"""))
