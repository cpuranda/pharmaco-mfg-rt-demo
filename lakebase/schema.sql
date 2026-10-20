-- =====================================================================
-- Lakebase (Postgres) operational schema — PharmaCo MFG RT demo
-- Low-latency serving + transactional workflow state ONLY.
-- Historical/analytical system of record stays in Delta (gold).
-- All data SYNTHETIC.
-- =====================================================================
CREATE SCHEMA IF NOT EXISTS pharmaco_mfg_rt;
SET search_path TO pharmaco_mfg_rt;

-- Current state, one row per asset (served to the operator UI).
CREATE TABLE IF NOT EXISTS asset_current_state (
  asset_id           TEXT PRIMARY KEY,
  site_id            TEXT,
  plant_id           TEXT,
  line_id            TEXT,
  last_event_ts      TIMESTAMPTZ,
  updated_ts         TIMESTAMPTZ,
  synced_ts          TIMESTAMPTZ DEFAULT now(),   -- when this row was written to Lakebase
  operating_state    TEXT,
  temperature        DOUBLE PRECISION,
  vibration          DOUBLE PRECISION,
  pressure           DOUBLE PRECISION,
  motor_current      DOUBLE PRECISION,
  speed              DOUBLE PRECISION,
  throughput         DOUBLE PRECISION,
  energy             DOUBLE PRECISION,
  anomaly_score      DOUBLE PRECISION,
  condition          TEXT,
  active_alert_count INT DEFAULT 0
);
CREATE INDEX IF NOT EXISTS idx_acs_line ON asset_current_state(line_id);
CREATE INDEX IF NOT EXISTS idx_acs_condition ON asset_current_state(condition);

-- Line health, one row per line.
CREATE TABLE IF NOT EXISTS line_health_summary (
  line_id             TEXT PRIMARY KEY,
  site_id             TEXT,
  plant_id            TEXT,
  product_family      TEXT,
  updated_ts          TIMESTAMPTZ,
  synced_ts           TIMESTAMPTZ DEFAULT now(),
  health_status       TEXT,
  running_assets      INT,
  idle_assets         INT,
  faulted_assets      INT,
  total_assets        INT,
  avg_throughput      DOUBLE PRECISION,
  baseline_throughput DOUBLE PRECISION,
  throughput_pct      DOUBLE PRECISION,
  avg_speed           DOUBLE PRECISION,
  open_warnings       INT,
  open_criticals      INT,
  is_slowing_down     BOOLEAN,
  slowdown_hint       TEXT
);

-- Active alerts (operational view). Full history stays in Delta.
CREATE TABLE IF NOT EXISTS active_alerts (
  alert_id           TEXT PRIMARY KEY,
  created_ts         TIMESTAMPTZ,
  updated_ts         TIMESTAMPTZ,
  synced_ts          TIMESTAMPTZ DEFAULT now(),
  site_id            TEXT,
  plant_id           TEXT,
  line_id            TEXT,
  asset_id           TEXT,
  alert_type         TEXT,
  severity           TEXT,
  status             TEXT DEFAULT 'NEW',   -- NEW | ACKNOWLEDGED | RESOLVED | MAINTENANCE
  sensor_type        TEXT,
  observed_value     DOUBLE PRECISION,
  threshold_value    DOUBLE PRECISION,
  anomaly_score      DOUBLE PRECISION,
  confidence         DOUBLE PRECISION,
  recommended_action TEXT,
  root_cause_hint    TEXT,
  last_event_ts      TIMESTAMPTZ,
  acknowledged_by    TEXT,
  acknowledged_ts    TIMESTAMPTZ,
  resolved_ts        TIMESTAMPTZ
);
CREATE INDEX IF NOT EXISTS idx_alerts_status ON active_alerts(status);
CREATE INDEX IF NOT EXISTS idx_alerts_severity ON active_alerts(severity);
CREATE INDEX IF NOT EXISTS idx_alerts_line ON active_alerts(line_id);
CREATE INDEX IF NOT EXISTS idx_alerts_asset ON active_alerts(asset_id);
CREATE INDEX IF NOT EXISTS idx_alerts_event_ts ON active_alerts(last_event_ts);

-- Operator acknowledgement / workflow audit log (transactional).
CREATE TABLE IF NOT EXISTS operator_acknowledgements (
  ack_id        BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  alert_id      TEXT NOT NULL,
  action        TEXT NOT NULL,          -- ACKNOWLEDGE | RESOLVE | MAINTENANCE | REOPEN
  operator      TEXT NOT NULL,
  note          TEXT,
  created_ts    TIMESTAMPTZ DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_ack_alert ON operator_acknowledgements(alert_id);

-- Generator / demo run-state controls (single-row control table).
CREATE TABLE IF NOT EXISTS demo_control_state (
  id            INT PRIMARY KEY DEFAULT 1,
  scenario      TEXT DEFAULT 'normal',
  run_state     TEXT DEFAULT 'stopped', -- running | stopped
  event_rate    DOUBLE PRECISION DEFAULT 1.0,
  anomaly_secs  INT DEFAULT 60,
  seed          INT DEFAULT 42,
  updated_by    TEXT,
  updated_ts    TIMESTAMPTZ DEFAULT now(),
  CONSTRAINT single_row CHECK (id = 1)
);
INSERT INTO demo_control_state (id) VALUES (1) ON CONFLICT (id) DO NOTHING;

-- Latest ML anomaly-detector score per asset (written by the ML scoring task;
-- served next to the rule-based condition in the app).
CREATE TABLE IF NOT EXISTS asset_ml_scores (
  asset_id          TEXT PRIMARY KEY,
  line_id           TEXT,
  window_start      TIMESTAMPTZ,
  window_end        TIMESTAMPTZ,
  ml_anomaly_score  DOUBLE PRECISION,
  ml_is_anomaly     BOOLEAN,
  top_sensor        TEXT,
  model_name        TEXT,
  model_version     TEXT,
  scored_ts         TIMESTAMPTZ,
  synced_ts         TIMESTAMPTZ DEFAULT now()
);
ALTER TABLE asset_ml_scores ADD COLUMN IF NOT EXISTS top_sensor TEXT;
