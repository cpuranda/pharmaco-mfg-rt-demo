// API response types for the PharmaCo MFG Real-Time demo backend.
// Kept tolerant (optional fields) where the backend shape varies.

export type HealthStatus = 'HEALTHY' | 'WATCH' | 'DEGRADED' | 'CRITICAL'
export type OperatingState =
  | 'RUNNING'
  | 'IDLE'
  | 'STARVED'
  | 'BLOCKED'
  | 'MAINTENANCE'
  | 'FAULT'
export type Severity = 'WARNING' | 'CRITICAL'
export type AlertStatus = 'NEW' | 'ACKNOWLEDGED' | 'RESOLVED' | 'MAINTENANCE'
export type AlertType = 'THRESHOLD' | 'TREND' | 'SLOWDOWN' | 'RECOVERY'
export type ProductFamily = 'OSD_TABLET' | 'STERILE_VIAL' | 'PACKAGING'
export type SensorType =
  | 'temperature'
  | 'vibration'
  | 'pressure'
  | 'motor_current'
  | 'speed'
  | 'throughput'
  | 'energy'

export interface PlantSummary {
  plant_status: HealthStatus
  total_assets: number
  running_assets: number
  idle_assets: number
  faulted_assets: number
  open_warnings: number
  open_criticals: number
  lines: number
  latest_event_ts: string | null
  server_ts: string
}

export interface Line {
  line_id: string
  site_id: string
  plant_id: string
  product_family: ProductFamily
  updated_ts: string
  synced_ts?: string
  health_status: HealthStatus
  running_assets: number
  idle_assets: number
  faulted_assets: number
  total_assets: number
  avg_throughput: number
  baseline_throughput: number
  throughput_pct: number
  avg_speed: number
  open_warnings: number
  open_criticals: number
  is_slowing_down: boolean
  slowdown_hint: string
}

export interface Asset {
  asset_id: string
  site_id: string
  plant_id: string
  line_id: string
  last_event_ts: string | null
  updated_ts: string
  synced_ts?: string
  operating_state: OperatingState
  temperature: number
  vibration: number
  pressure: number
  motor_current: number
  speed: number
  throughput: number
  energy: number
  anomaly_score: number
  condition: HealthStatus
  active_alert_count: number
}

export interface LineDetail {
  line: Line
  assets: Asset[]
}

export interface Alert {
  alert_id: string
  created_ts: string
  updated_ts: string
  synced_ts?: string
  site_id: string
  plant_id: string
  line_id: string
  asset_id: string
  alert_type: AlertType
  severity: Severity
  status: AlertStatus
  sensor_type: SensorType
  observed_value: number
  threshold_value: number
  anomaly_score: number
  confidence: number
  recommended_action: string
  root_cause_hint: string
  last_event_ts: string
  acknowledged_by: string | null
  acknowledged_ts: string | null
  resolved_ts: string | null
}

export interface MlScore {
  asset_id: string
  line_id: string
  window_start: string
  window_end: string
  ml_anomaly_score: number
  ml_is_anomaly: boolean
  top_sensor: string | null
  model_name: string
  model_version: string
  scored_ts: string
}

export interface AssetDetail {
  asset: Asset
  alerts: Alert[]
  ml?: MlScore | null
}

export interface AlertHistoryEntry {
  ack_id: number
  alert_id: string
  action: string
  operator: string
  note: string
  created_ts: string
}

export interface TelemetryPoint {
  asset_id: string
  sensor_type: SensorType
  event_ts: string
  sensor_value: number
  unit: string
  nominal_min: number | null
  nominal_max: number | null
  operating_state: OperatingState
}

export interface TelemetryResponse {
  asset_id: string
  minutes: number
  points: TelemetryPoint[]
}

export interface DemoStatus {
  running: boolean
  scenario: string
  rate: number
  seed: number
  anomaly_secs: number
  ingest_available: boolean
  reason: string
  total_events: number
  last_ingest_ts: string | null
  last_error: string | null
  generator_ok: boolean
  target_table: string
  valid_scenarios: string[]
  control_state?: Record<string, unknown>
}

export interface StreamingJob {
  configured: boolean
  job_id?: string
  life_cycle_state?: string | null
  result_state?: string | null
  active?: boolean
  error?: string
}

export interface ArchitectureStage {
  stage: string
  detail: string
}

export interface AgenticStatus {
  llm_endpoint: string
  vs_index: string
  supervisor_endpoint: string | null
  managed_supervisor: boolean
  genie_configured: boolean
  sop_chunks: number | null
  sop_count: number | null
  sop_error?: string
  vector_search: {
    ready?: boolean
    state?: string
    indexed_rows?: number | null
    error?: string
  }
}

export interface GenieStatus {
  configured: boolean
  suggestions: string[]
}

export type GenieAskStatus =
  | 'COMPLETED'
  | 'FAILED'
  | 'TIMEOUT'
  | 'ERROR'
  | 'UNCONFIGURED'

export interface GenieResult {
  columns: string[]
  rows: string[][]
}

export interface GenieAnswer {
  conversation_id: string | null
  message_id: string | null
  status: GenieAskStatus
  answer: string
  sql: string | null
  result: GenieResult | null
}

// ---- Fix Assistant (SOP-grounded remediation + supervisor) ----
export interface SopCitation {
  sop_id: string
  title: string
  section: string
  source_file: string
  score: number
}

export interface FixAnswer {
  status: 'OK' | 'NO_MATCH' | 'RETRIEVAL_ERROR' | 'LLM_ERROR'
  answer: string
  citations: SopCitation[]
  query?: string
}

export interface AssistantStatus {
  llm_endpoint: string
  vs_index: string
  supervisor_endpoint: string | null
  managed_supervisor: boolean
  genie_configured: boolean
  suggestions: string[]
}

export interface SupervisorAnswer {
  status: 'OK' | 'LLM_ERROR'
  answer: string
  route: 'code-supervisor' | 'managed-supervisor'
  tools_used?: string[]
  plan?: { needs_data?: boolean; needs_sops?: boolean; genie_question?: string; sop_query?: string }
  sources?: {
    data?: { answer?: string; sql?: string | null; result?: GenieResult | null; status?: string } | null
    sops?: SopCitation[]
    supervisor_endpoint?: string
  }
}

export interface ObservabilityMetrics {
  server_ts: string
  ingest: DemoStatus
  delta: {
    bronze_rows: string
    silver_rows: string
    quarantine_rows: string
    anomaly_events: string
    last_bronze_ts: string | null
    last_silver_ts: string | null
    query_latency_ms: number | null
  }
  quality_5m: {
    n: string
    avg_ingest_latency_ms: string | null
    oob: string | null
  }
  lakebase: {
    serving_read_latency_ms: number | null
    assets: number
    last_sync_ts: string | null
    available: boolean
  }
  streaming_job: StreamingJob
  architecture: ArchitectureStage[]
  agentic_architecture: ArchitectureStage[]
  agentic: AgenticStatus
}
