-- =====================================================================
-- PharmaCo Manufacturing Real-Time Demo  |  SILVER layer DDL
-- SILVER = validated, normalized, deduplicated telemetry + quarantine.
-- =====================================================================

-- Clean, validated, deduplicated telemetry (one row per accepted reading).
CREATE TABLE IF NOT EXISTS ${cat}.${silver}.telemetry_clean (
  source_event_id   STRING,
  event_ts          TIMESTAMP,
  ingested_ts       TIMESTAMP,
  processed_ts      TIMESTAMP,
  site_id           STRING,
  plant_id          STRING,
  line_id           STRING,
  asset_id          STRING,
  batch_id          STRING,
  product_family    STRING,
  sensor_type       STRING,          -- normalized (lower snake_case)
  sensor_value      DOUBLE,
  unit              STRING,          -- normalized canonical unit
  operating_state   STRING,
  nominal_min       DOUBLE,
  nominal_max       DOUBLE,
  is_out_of_band    BOOLEAN,         -- value outside [nominal_min, nominal_max]
  sequence_number   BIGINT,
  scenario          STRING,
  ingest_latency_ms DOUBLE           -- ingested_ts - event_ts
)
USING DELTA
TBLPROPERTIES (delta.enableChangeDataFeed = true)
COMMENT 'Validated/normalized/deduplicated telemetry (SYNTHETIC)';

-- Quarantine for malformed / rejected events (data-quality visibility).
CREATE TABLE IF NOT EXISTS ${cat}.${silver}.telemetry_quarantine (
  raw_payload   STRING,
  event_ts      TIMESTAMP,
  processed_ts  TIMESTAMP,
  reject_reason STRING,
  asset_id      STRING,
  sensor_type   STRING
)
USING DELTA
COMMENT 'Rejected/malformed telemetry for DQ inspection (SYNTHETIC)';
