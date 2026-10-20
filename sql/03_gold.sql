-- =====================================================================
-- PharmaCo Manufacturing Real-Time Demo  |  GOLD layer DDL
-- GOLD = current asset state, active alerts, line health, anomaly & event history.
-- These Delta tables are the analytical system of record; Lakebase mirrors the
-- operational subset for low-latency app serving.
-- =====================================================================

-- Current state, one row per asset (upserted by the RTM pipeline).
CREATE TABLE IF NOT EXISTS ${cat}.${gold}.asset_current_state (
  site_id            STRING,
  plant_id           STRING,
  line_id            STRING,
  asset_id           STRING,
  last_event_ts      TIMESTAMP,
  updated_ts         TIMESTAMP,
  operating_state    STRING,
  -- latest per-sensor snapshot
  temperature        DOUBLE,
  vibration          DOUBLE,
  pressure           DOUBLE,
  motor_current      DOUBLE,
  speed              DOUBLE,
  throughput         DOUBLE,
  energy             DOUBLE,
  anomaly_score      DOUBLE,      -- 0..1 persistent-abnormality score
  condition          STRING,      -- HEALTHY | WATCH | DEGRADED | CRITICAL
  active_alert_count INT
)
USING DELTA
COMMENT 'Latest state per asset (SYNTHETIC)';

-- Per-asset anomaly/threshold event history (append-only).
CREATE TABLE IF NOT EXISTS ${cat}.${gold}.asset_anomaly_events (
  event_id        STRING,
  created_ts      TIMESTAMP,
  site_id         STRING,
  plant_id        STRING,
  line_id         STRING,
  asset_id        STRING,
  sensor_type     STRING,
  alert_type      STRING,   -- THRESHOLD | TREND | SLOWDOWN | RECOVERY
  severity        STRING,   -- INFO | WARNING | CRITICAL | RECOVERY
  observed_value  DOUBLE,
  threshold_value DOUBLE,
  deviation       DOUBLE,
  anomaly_score   DOUBLE,
  reason          STRING
)
USING DELTA
COMMENT 'Immutable anomaly/threshold/recovery event history (SYNTHETIC)';

-- Active (and recently resolved) alerts, keyed by stable alert_id.
CREATE TABLE IF NOT EXISTS ${cat}.${gold}.active_alerts (
  alert_id           STRING,
  created_ts         TIMESTAMP,
  updated_ts         TIMESTAMP,
  site_id            STRING,
  plant_id           STRING,
  line_id            STRING,
  asset_id           STRING,
  alert_type         STRING,
  severity           STRING,   -- WARNING | CRITICAL
  status             STRING,   -- NEW | ACKNOWLEDGED | RESOLVED | MAINTENANCE
  sensor_type        STRING,
  observed_value     DOUBLE,
  threshold_value    DOUBLE,
  anomaly_score      DOUBLE,
  confidence         DOUBLE,
  recommended_action STRING,
  root_cause_hint    STRING,
  last_event_ts      TIMESTAMP,
  acknowledged_by    STRING,
  acknowledged_ts    TIMESTAMP,
  resolved_ts        TIMESTAMP
)
USING DELTA
COMMENT 'Current alert view + recently resolved (SYNTHETIC). History preserved here and in asset_anomaly_events.';

-- Line health summary, one row per line.
CREATE TABLE IF NOT EXISTS ${cat}.${gold}.line_health_summary (
  site_id             STRING,
  plant_id            STRING,
  line_id             STRING,
  product_family      STRING,
  updated_ts          TIMESTAMP,
  health_status       STRING,   -- HEALTHY | WATCH | DEGRADED | CRITICAL
  running_assets      INT,
  idle_assets         INT,
  faulted_assets      INT,
  total_assets        INT,
  avg_throughput      DOUBLE,
  baseline_throughput DOUBLE,
  throughput_pct      DOUBLE,   -- avg_throughput / baseline * 100
  avg_speed           DOUBLE,
  open_warnings       INT,
  open_criticals      INT,
  is_slowing_down     BOOLEAN,
  slowdown_hint       STRING    -- most likely contributing asset/sensor
)
USING DELTA
COMMENT 'Per-line health & slowdown summary (SYNTHETIC)';

-- Telemetry data-quality / pipeline metrics (append-only, per micro-batch/window).
CREATE TABLE IF NOT EXISTS ${cat}.${gold}.telemetry_quality_metrics (
  window_ts        TIMESTAMP,
  records_in       BIGINT,
  records_clean    BIGINT,
  records_dup      BIGINT,
  records_late     BIGINT,
  records_quarantined BIGINT,
  events_per_sec   DOUBLE,
  processing_latency_ms DOUBLE
)
USING DELTA
COMMENT 'Pipeline DQ & throughput metrics per window (SYNTHETIC)';
