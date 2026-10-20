# Executed notebook: `02_ingest_zerobus`

> Exported from Databricks job run **24808449534624** / task run **777130905509757** — result **SUCCESS**, executed 2026-10-20T13:26:09+00:00 on serverless compute in workspace `https://fevm-hls-streaming-solution-demo.cloud.databricks.com`. Outputs below are the real cell outputs of that run.

# 02 · Lakeflow Connect — Zerobus Ingest of synthetic line telemetry

A synthetic generator (`generator/telemetry_generator.py`, stdlib only, deterministic seed)
simulates **1 plant · 3 lines · 15 assets · 7 sensors**:
LINE-A oral-solid-dose tablets, LINE-B sterile vials, LINE-C packaging.

Records are pushed **directly into the Unity Catalog bronze Delta table with Zerobus Ingest**
(Lakeflow Connect's direct-write gRPC API, OAuth service principal, protobuf schema) — no
message broker in between.

Ingestion plan (all synthetic):
1. **Historical backfill** — 30 min of simulated plant history containing three labelled
   failure scenarios (vibration degradation on B-FILL, pressure instability on B-LYO, line
   slowdown on LINE-A). This is the training history for the anomaly model (notebook 06).
2. **Live stream** — ~3 min paced in real time with a **temperature excursion on the
   A-COMPRESS tablet press** so the operational pipeline raises live alerts.
3. **Data-quality probes** — duplicate re-emissions and malformed records, to prove dedup
   and quarantine downstream.

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
run as    : chandrashekhar.purandare@databricks.com   at 2026-10-20T13:27:23+00:00
```

```python
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
```

**Output:**

```text
Zerobus endpoint : https://7474651570340482.zerobus.us-east-1.cloud.databricks.com
target table     : hls_streaming_solution_demo_catalog.pharmaco_mfg_rt_bronze.telemetry_raw
ingest identity  : service principal [REDACTED] (secret from scope 'pharmaco_mfg_rt_demo')
bronze rows before this run: 0
```

### 1 · Historical backfill (30 simulated minutes, 2 s ticks, labelled scenarios)

```python
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
```

**Output:**

```text
2026-10-20T13:27:29.040639Z  INFO databricks_zerobus_ingest_sdk::default_token_factory: minted UC OAuth token table=hls_streaming_solution_demo_catalog.pharmaco_mfg_rt_bronze.telemetry_raw reason="cold_miss" expires_in_secs=3600 elapsed_ms=179
2026-10-20T13:27:29.521025Z  INFO databricks_zerobus_ingest_sdk::stream::grpc::connection: Successfully created stream stream_id=8d226b54-907d-4de0-b320-71f28de56f05
stream open  ingest_batch_id=zb-d02b7cbf1e8e  sim start=2026-10-20T12:53:28+00:00
2026-10-20T13:27:29.521062Z  INFO databricks_zerobus_ingest_sdk::stream::grpc::supervisor: Successfully created stream stream_id=8d226b54-907d-4de0-b320-71f28de56f05
  phase normal                 ticks=240  accepted_total=  25200  last_enqueue_latency_ms=0.003
  phase vibration_degradation  ticks=120  accepted_total=  37800  last_enqueue_latency_ms=0.002
  phase normal                 ticks=90   accepted_total=  47250  last_enqueue_latency_ms=0.010
  phase pressure_instability   ticks=120  accepted_total=  59850  last_enqueue_latency_ms=0.003
  phase normal                 ticks=90   accepted_total=  69300  last_enqueue_latency_ms=0.007
  phase line_slowdown          ticks=120  accepted_total=  81900  last_enqueue_latency_ms=0.002
  phase normal                 ticks=120  accepted_total=  94500  last_enqueue_latency_ms=0.002
2026-10-20T13:27:33.059135Z  INFO databricks_zerobus_ingest_sdk::stream::grpc::close: Closing stream stream_id=8d226b54-907d-4de0-b320-71f28de56f05
BACKFILL DONE: 94500 records in 3.5s (26677 rec/s)
```

### 2 · Live stream — temperature excursion on A-COMPRESS, paced in real time

```python
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
```

**Output:**

```text
2026-10-20T13:27:33.425617Z  INFO databricks_zerobus_ingest_sdk::default_token_factory: minted UC OAuth token table=hls_streaming_solution_demo_catalog.pharmaco_mfg_rt_bronze.telemetry_raw reason="cold_miss" expires_in_secs=3600 elapsed_ms=204
2026-10-20T13:27:33.454701Z  INFO databricks_zerobus_ingest_sdk::stream::grpc::connection: Successfully created stream stream_id=61c68340-5b82-4767-afd0-8c39056b25e3
2026-10-20T13:27:33.454735Z  INFO databricks_zerobus_ingest_sdk::stream::grpc::supervisor: Successfully created stream stream_id=61c68340-5b82-4767-afd0-8c39056b25e3
  live tick  30/180  accepted=  3150  enqueue_latency_ms=0.003
  live tick  60/180  accepted=  6300  enqueue_latency_ms=0.008
  live tick  90/180  accepted=  9450  enqueue_latency_ms=0.003
  live tick 120/180  accepted= 12600  enqueue_latency_ms=0.003
  live tick 150/180  accepted= 15750  enqueue_latency_ms=0.003
  live tick 180/180  accepted= 18900  enqueue_latency_ms=0.009
LIVE DONE: 18900 records over 180s wall clock
```

### 3 · Data-quality probes: duplicate re-emissions + malformed records

```python
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
```

**Output:**

```text
2026-10-20T13:30:33.950048Z  INFO databricks_zerobus_ingest_sdk::stream::grpc::close: Closing stream stream_id=61c68340-5b82-4767-afd0-8c39056b25e3
sent 105 originals + 105 duplicate re-emissions + 8 malformed records
```

### 4 · Ingestion health + what landed in bronze

```python
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
```

**Output:**

| ingest_batch_id | ts | ingest_source | records_accepted | records_rejected | schema_errors | retry_count | last_ack_ts | last_ack_latency_ms | notes |
|---|---|---|---|---|---|---|---|---|---|
| zb-fc4b119c10a7 | 2026-10-20T13:30:33.954Z | zerobus | 19118 | 0 | 0 | 0 | 2026-10-20T13:30:33.605Z | 0.003 | live temp_excursion + DQ probes |
| zb-d02b7cbf1e8e | 2026-10-20T13:27:33.063Z | zerobus | 94500 | 0 | 0 | 0 | 2026-10-20T13:27:33.058Z | 0.002 | historical backfill (30 simulated minutes) |

```text
bronze rows: before=0  after=113618  landed_this_run=113618
```

| ingest_source | scenario | records | assets | first_event | last_event |
|---|---|---|---|---|---|
| zerobus | normal | 56700 | 15 | 2026-10-20T12:53:28.805Z | 2026-10-20T13:23:26.805Z |
| zerobus | vibration_degradation | 12600 | 15 | 2026-10-20T13:01:28.805Z | 2026-10-20T13:05:26.805Z |
| zerobus | pressure_instability | 12600 | 15 | 2026-10-20T13:08:28.805Z | 2026-10-20T13:12:26.805Z |
| zerobus | line_slowdown | 12600 | 15 | 2026-10-20T13:15:28.805Z | 2026-10-20T13:19:26.805Z |
| zerobus | temp_excursion | 19118 | 15 | 2026-10-20T13:27:33.455Z | 2026-10-20T13:30:33.801Z |

```python
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
```

**Output:**

| line_id | asset_id | sensor_type | value | unit | nominal_min | nominal_max | operating_state | scenario | event_ts | ingested_ts |
|---|---|---|---|---|---|---|---|---|---|---|
| LINE-A | A-COMPRESS | temperature | 83.32 | degC | 55.0 | 75.0 | RUNNING | temp_excursion | 2026-10-20T13:30:33.801Z | 2026-10-20T13:30:33.805Z |
| LINE-A | A-COMPRESS | temperature | 83.32 | degC | 55.0 | 75.0 | RUNNING | temp_excursion | 2026-10-20T13:30:33.801Z | 2026-10-20T13:30:33.803Z |
| LINE-A | A-COMPRESS | temperature | 84.11 | degC | 55.0 | 75.0 | RUNNING | temp_excursion | 2026-10-20T13:30:32.604Z | 2026-10-20T13:30:32.605Z |
| LINE-A | A-COMPRESS | temperature | 82.58 | degC | 55.0 | 75.0 | RUNNING | temp_excursion | 2026-10-20T13:30:31.603Z | 2026-10-20T13:30:31.605Z |
| LINE-A | A-COMPRESS | temperature | 81.68 | degC | 55.0 | 75.0 | RUNNING | temp_excursion | 2026-10-20T13:30:30.602Z | 2026-10-20T13:30:30.604Z |
| LINE-A | A-COMPRESS | temperature | 83.2 | degC | 55.0 | 75.0 | RUNNING | temp_excursion | 2026-10-20T13:30:29.602Z | 2026-10-20T13:30:29.603Z |
| LINE-A | A-COMPRESS | temperature | 83.63 | degC | 55.0 | 75.0 | RUNNING | temp_excursion | 2026-10-20T13:30:28.601Z | 2026-10-20T13:30:28.602Z |
| LINE-A | A-COMPRESS | temperature | 82.91 | degC | 55.0 | 75.0 | RUNNING | temp_excursion | 2026-10-20T13:30:27.601Z | 2026-10-20T13:30:27.603Z |
| LINE-A | A-COMPRESS | temperature | 83.98 | degC | 55.0 | 75.0 | RUNNING | temp_excursion | 2026-10-20T13:30:26.600Z | 2026-10-20T13:30:26.601Z |
| LINE-A | A-COMPRESS | temperature | 80.79 | degC | 55.0 | 75.0 | RUNNING | temp_excursion | 2026-10-20T13:30:25.599Z | 2026-10-20T13:30:25.600Z |

| live_records | event_to_writer_ms_p50 | event_to_writer_ms_p95 |
|---|---|---|
| 19118 | 2.0 | 3.0 |
