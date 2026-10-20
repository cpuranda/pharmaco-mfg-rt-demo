// Same-origin API client. All paths are relative (`/api/...`) so the built
// assets served by FastAPI hit the same host. Dev uses the Vite proxy.
import type {
  Alert,
  AlertHistoryEntry,
  AlertStatus,
  AlertType,
  AssetDetail,
  AssistantStatus,
  DemoStatus,
  FixAnswer,
  GenieAnswer,
  GenieStatus,
  Line,
  LineDetail,
  ObservabilityMetrics,
  PlantSummary,
  Severity,
  SupervisorAnswer,
  TelemetryResponse,
} from './types'

export class ApiError extends Error {
  status: number
  body: unknown
  constructor(status: number, message: string, body?: unknown) {
    super(message)
    this.name = 'ApiError'
    this.status = status
    this.body = body
  }
}

async function request<T>(path: string, init?: RequestInit): Promise<T> {
  let res: Response
  try {
    res = await fetch(path, {
      ...init,
      headers: {
        'Content-Type': 'application/json',
        ...(init?.headers || {}),
      },
    })
  } catch (e) {
    // Network / connection failure — treat as service unavailable.
    throw new ApiError(0, `Network error: ${(e as Error).message}`)
  }

  const text = await res.text()
  let parsed: unknown = null
  if (text) {
    try {
      parsed = JSON.parse(text)
    } catch {
      parsed = text
    }
  }

  if (!res.ok) {
    const detail =
      (parsed as { detail?: unknown; error?: unknown })?.detail ??
      (parsed as { error?: unknown })?.error ??
      res.statusText
    const msg = typeof detail === 'string' ? detail : JSON.stringify(detail)
    throw new ApiError(res.status, msg, parsed)
  }
  return parsed as T
}

// ---- Readiness ----
export interface ReadyState {
  status: string
  lakebase: boolean
  reason?: string
}
export async function getReady(): Promise<{ ok: boolean; body: ReadyState }> {
  try {
    const res = await fetch('/ready')
    const body = (await res.json()) as ReadyState
    return { ok: res.ok, body }
  } catch (e) {
    return {
      ok: false,
      body: { status: 'unreachable', lakebase: false, reason: (e as Error).message },
    }
  }
}

// ---- Operational ----
export const getPlantSummary = () => request<PlantSummary>('/api/plant/summary')
export const getLines = () => request<Line[]>('/api/lines')
export const getLine = (lineId: string) =>
  request<LineDetail>(`/api/lines/${encodeURIComponent(lineId)}`)
export const getAsset = (assetId: string) =>
  request<AssetDetail>(`/api/assets/${encodeURIComponent(assetId)}`)
export const getTelemetry = (assetId: string, minutes: number, sensor: string) =>
  request<TelemetryResponse>(
    `/api/telemetry/${encodeURIComponent(assetId)}?minutes=${minutes}&sensor=${encodeURIComponent(
      sensor,
    )}`,
  )

// ---- Alerts ----
export interface AlertFilters {
  severity?: Severity | ''
  status?: AlertStatus | ''
  line_id?: string
  asset_id?: string
  alert_type?: AlertType | ''
  limit?: number
  offset?: number
}
export function getAlerts(filters: AlertFilters = {}) {
  const q = new URLSearchParams()
  if (filters.severity) q.set('severity', filters.severity)
  if (filters.status) q.set('status', filters.status)
  if (filters.line_id) q.set('line_id', filters.line_id)
  if (filters.asset_id) q.set('asset_id', filters.asset_id)
  if (filters.alert_type) q.set('alert_type', filters.alert_type)
  q.set('limit', String(filters.limit ?? 200))
  q.set('offset', String(filters.offset ?? 0))
  return request<Alert[]>(`/api/alerts?${q.toString()}`)
}

export const acknowledgeAlert = (alertId: string, operator: string, note: string) =>
  request<Alert>(`/api/alerts/${encodeURIComponent(alertId)}/acknowledge`, {
    method: 'POST',
    body: JSON.stringify({ operator, note }),
  })

export const resolveAlert = (
  alertId: string,
  operator: string,
  note: string,
  maintenance: boolean,
) =>
  request<Alert>(`/api/alerts/${encodeURIComponent(alertId)}/resolve`, {
    method: 'POST',
    body: JSON.stringify({ operator, note, maintenance }),
  })

export const getAlertHistory = (alertId: string) =>
  request<AlertHistoryEntry[]>(`/api/alerts/${encodeURIComponent(alertId)}/history`)

// ---- Demo controls ----
export const getDemoStatus = () => request<DemoStatus>('/api/demo/status')

export interface ScenarioPayload {
  scenario: string
  rate: number
  // Optional. Normally omitted so the backend rotates the seed per start (prevents
  // dedup-freeze on restart). Only send to pin a seed for a reproducible replay.
  seed?: number
  anomaly_secs: number
  confirm_high_impact: boolean
  operator: string
}
export const setScenario = (payload: ScenarioPayload) =>
  request<DemoStatus>('/api/demo/scenario', {
    method: 'POST',
    body: JSON.stringify(payload),
  })
export const startDemo = (operator: string) =>
  request<DemoStatus>('/api/demo/start', {
    method: 'POST',
    body: JSON.stringify({ operator }),
  })
export const stopDemo = (operator: string) =>
  request<DemoStatus>('/api/demo/stop', {
    method: 'POST',
    body: JSON.stringify({ operator }),
  })

// ---- Fix Assistant (SOP-grounded remediation + supervisor) ----
export interface FixContext {
  alert_id?: string
  asset_id?: string
  line_id?: string
  sensor_type?: string
  alert_type?: string
  severity?: string
  observed_value?: number
  threshold_value?: number
  anomaly_score?: number
  condition?: string
}

export const getAssistantStatus = () => request<AssistantStatus>('/api/assistant/status')

// SOP retrieval + LLM synthesis can take ~10-30s; use a generous timeout.
export async function explainFix(ctx: FixContext, timeoutMs = 90000): Promise<FixAnswer> {
  const controller = new AbortController()
  const timer = window.setTimeout(() => controller.abort(), timeoutMs)
  try {
    return await request<FixAnswer>('/api/assistant/explain-fix', {
      method: 'POST',
      body: JSON.stringify(ctx),
      signal: controller.signal,
    })
  } finally {
    window.clearTimeout(timer)
  }
}

export async function assistantAsk(question: string, timeoutMs = 180000): Promise<SupervisorAnswer> {
  const controller = new AbortController()
  const timer = window.setTimeout(() => controller.abort(), timeoutMs)
  try {
    return await request<SupervisorAnswer>('/api/assistant/ask', {
      method: 'POST',
      body: JSON.stringify({ question }),
      signal: controller.signal,
    })
  } finally {
    window.clearTimeout(timer)
  }
}

// ---- Observability ----
export const getObservability = () =>
  request<ObservabilityMetrics>('/api/observability/metrics')

// ---- Ask Genie ----
export const getGenieStatus = () => request<GenieStatus>('/api/genie/status')

// Genie answers can take 10–60s; use a generous client-side timeout.
export async function askGenie(
  question: string,
  conversationId?: string | null,
  timeoutMs = 120000,
): Promise<GenieAnswer> {
  const controller = new AbortController()
  const timer = window.setTimeout(() => controller.abort(), timeoutMs)
  try {
    return await request<GenieAnswer>('/api/genie/ask', {
      method: 'POST',
      body: JSON.stringify({
        question,
        ...(conversationId ? { conversation_id: conversationId } : {}),
      }),
      signal: controller.signal,
    })
  } catch (e) {
    if (e instanceof ApiError && controller.signal.aborted) {
      throw new ApiError(0, `Genie request timed out after ${Math.round(timeoutMs / 1000)}s`)
    }
    throw e
  } finally {
    window.clearTimeout(timer)
  }
}
