-- =====================================================================
-- PharmaCo Manufacturing Real-Time Demo  |  BRONZE layer DDL
-- Catalog / schema are parameterized via ${cat} and ${bronze} widgets when
-- run from a notebook, or replaced by the deploy script.
--
-- BRONZE = immutable raw telemetry as landed by Zerobus Ingest, plus ingestion
-- metadata. No transformations. All data is SYNTHETIC.
-- =====================================================================

-- Managed Delta target table for Zerobus Ingest.
-- Explicit schema (Zerobus requires a well-defined target schema).
CREATE TABLE IF NOT EXISTS ${cat}.${bronze}.telemetry_raw (
  event_id          STRING    COMMENT 'Unique id per physical emission',
  source_event_id   STRING    COMMENT 'Stable logical reading id; dedup key downstream',
  event_ts          TIMESTAMP COMMENT 'Event time at the (synthetic) sensor',
  ingested_ts       TIMESTAMP COMMENT 'Wall-clock time the ingest writer accepted the record',
  site_id           STRING,
  plant_id          STRING,
  line_id           STRING,
  asset_id          STRING,
  batch_id          STRING,
  product_family    STRING,
  sensor_type       STRING,
  sensor_value      DOUBLE,
  unit              STRING,
  operating_state   STRING,
  nominal_min       DOUBLE,
  nominal_max       DOUBLE,
  sequence_number   BIGINT,
  scenario          STRING,
  payload_version   STRING,
  -- ingestion metadata written by the Zerobus writer / ingest driver
  ingest_source     STRING    COMMENT 'zerobus | fallback_delta_writer',
  ingest_batch_id   STRING    COMMENT 'writer session / micro-batch correlation id'
)
USING DELTA
TBLPROPERTIES (
  delta.enableChangeDataFeed = true,
  delta.autoOptimize.optimizeWrite = true,
  delta.autoOptimize.autoCompact = true
)
COMMENT 'SYNTHETIC raw plant telemetry landed via Zerobus Ingest (PharmaCo MFG RT demo)';

-- Ingestion health / audit table (written by the ingest driver).
CREATE TABLE IF NOT EXISTS ${cat}.${bronze}.ingest_health (
  ingest_batch_id     STRING,
  ts                  TIMESTAMP,
  ingest_source       STRING,
  records_accepted    BIGINT,
  records_rejected    BIGINT,
  schema_errors       BIGINT,
  retry_count         BIGINT,
  last_ack_ts         TIMESTAMP,
  last_ack_latency_ms DOUBLE,
  notes               STRING
)
USING DELTA
COMMENT 'Zerobus / ingest driver health, ack latency, rejects (SYNTHETIC demo)';
