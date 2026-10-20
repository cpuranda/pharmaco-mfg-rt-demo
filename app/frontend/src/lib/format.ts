// Timestamp + number formatting helpers.

export function parseTs(ts: string | null | undefined): number | null {
  if (!ts) return null
  const t = Date.parse(ts)
  return Number.isNaN(t) ? null : t
}

// Age of a timestamp relative to now, in seconds. null if unparseable.
export function ageSeconds(ts: string | null | undefined, now = Date.now()): number | null {
  const t = parseTs(ts)
  if (t == null) return null
  return Math.max(0, (now - t) / 1000)
}

export function fmtAge(ts: string | null | undefined, now = Date.now()): string {
  const s = ageSeconds(ts, now)
  if (s == null) return '—'
  if (s < 1) return 'just now'
  if (s < 60) return `${Math.round(s)}s ago`
  const m = s / 60
  if (m < 60) return `${Math.round(m)}m ago`
  const h = m / 60
  if (h < 24) return `${Math.round(h)}h ago`
  return `${Math.round(h / 24)}d ago`
}

export function fmtClock(ts: string | null | undefined): string {
  const t = parseTs(ts)
  if (t == null) return '—'
  return new Date(t).toLocaleTimeString([], {
    hour: '2-digit',
    minute: '2-digit',
    second: '2-digit',
  })
}

export function fmtNum(n: number | null | undefined, digits = 1): string {
  if (n == null || Number.isNaN(n)) return '—'
  return n.toLocaleString(undefined, {
    minimumFractionDigits: 0,
    maximumFractionDigits: digits,
  })
}

export function fmtInt(n: number | string | null | undefined): string {
  if (n == null) return '—'
  const v = typeof n === 'string' ? Number(n) : n
  if (Number.isNaN(v)) return String(n)
  return v.toLocaleString()
}

export function fmtMs(n: number | null | undefined): string {
  if (n == null || Number.isNaN(n)) return '—'
  if (n >= 1000) return `${(n / 1000).toFixed(2)} s`
  return `${Math.round(n)} ms`
}

export const SENSOR_UNITS: Record<string, string> = {
  temperature: 'degC',
  vibration: 'mm/s',
  pressure: 'bar',
  motor_current: 'A',
  speed: 'rpm',
  throughput: 'units/min',
  energy: 'kWh',
}

export const SENSOR_LABELS: Record<string, string> = {
  temperature: 'Temperature',
  vibration: 'Vibration',
  pressure: 'Pressure',
  motor_current: 'Motor Current',
  speed: 'Speed',
  throughput: 'Throughput',
  energy: 'Energy',
}

export const STALE_THRESHOLD_S = 60
