import { useCallback, useState } from 'react'
import {
  Area,
  AreaChart,
  CartesianGrid,
  ReferenceArea,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from 'recharts'
import { Bell, Sparkles } from 'lucide-react'
import type { Navigate } from '../App'
import type { Alert, Asset, SensorType, TelemetryResponse } from '../types'
import { getAsset, getTelemetry } from '../api'
import { usePolling } from '../hooks/usePolling'
import { useNow } from '../hooks/useNow'
import { ViewHeader } from '../components/ViewHeader'
import { PollStatus } from '../components/PollStatus'
import { Card, EmptyState, ErrorPanel, SkeletonCard } from '../components/Primitives'
import { HealthBadge, OperatingStateBadge, SeverityBadge } from '../components/Badges'
import { AnomalyMeter, SensorBand } from '../components/Widgets'
import { AlertStatusBadge } from '../components/Badges'
import { FixAssistantModal } from '../components/FixAssistantModal'
import {
  SENSOR_LABELS,
  SENSOR_UNITS,
  fmtAge,
  fmtClock,
  fmtNum,
} from '../lib/format'
import { severityColor } from '../lib/colors'

const SENSORS: SensorType[] = [
  'temperature',
  'vibration',
  'pressure',
  'motor_current',
  'speed',
  'throughput',
  'energy',
]

const MINUTES_OPTIONS = [15, 30, 60]

// Map sensor -> current value field on the asset object.
function currentValue(asset: Asset, sensor: SensorType): number {
  return asset[sensor] as number
}

// Latest nominal band for a sensor from its telemetry series.
function nominalOf(t?: TelemetryResponse): { min: number | null; max: number | null } {
  if (!t || t.points.length === 0) return { min: null, max: null }
  const last = t.points[t.points.length - 1]
  return { min: last.nominal_min, max: last.nominal_max }
}

export function AssetDetail({
  assetId,
  lineId,
  navigate,
  interval,
}: {
  assetId: string
  lineId?: string
  navigate: Navigate
  interval: number
}) {
  const now = useNow()
  const [selected, setSelected] = useState<SensorType>('temperature')
  const [minutes, setMinutes] = useState(15)
  const [fixAlert, setFixAlert] = useState<Alert | null>(null)

  const assetFetch = useCallback(() => getAsset(assetId), [assetId])
  const asset = usePolling(assetFetch, interval, [assetId])

  // Fetch all sensors so every tile can show its nominal band; the chart
  // reuses the selected sensor's series.
  const teleFetch = useCallback(async () => {
    const results = await Promise.all(
      SENSORS.map((s) =>
        getTelemetry(assetId, minutes, s).catch(() => ({
          asset_id: assetId,
          minutes,
          points: [],
        })) as Promise<TelemetryResponse>,
      ),
    )
    const map: Partial<Record<SensorType, TelemetryResponse>> = {}
    SENSORS.forEach((s, i) => (map[s] = results[i]))
    return map
  }, [assetId, minutes])
  const tele = usePolling(teleFetch, interval, [assetId, minutes])

  const a = asset.data?.asset
  const alerts = asset.data?.alerts ?? []
  const ml = asset.data?.ml ?? null
  const activeAlerts = alerts.filter((al) => al.status === 'NEW' || al.status === 'ACKNOWLEDGED')

  const selectedSeries = tele.data?.[selected]
  const unit = SENSOR_UNITS[selected]
  const chartData =
    selectedSeries?.points.map((p) => ({
      ts: fmtClock(p.event_ts),
      value: Number(p.sensor_value.toFixed(3)),
    })) ?? []
  const nb = nominalOf(selectedSeries)

  return (
    <div>
      <ViewHeader
        title={
          <span className="flex items-center gap-2">
            {assetId}
            {a && <HealthBadge status={a.condition} />}
            {a && <OperatingStateBadge state={a.operating_state} />}
          </span>
        }
        crumbs={[
          { label: 'Plant Overview', route: { view: 'overview' } },
          ...(lineId ? [{ label: lineId, route: { view: 'line' as const, lineId } }] : []),
          { label: assetId },
        ]}
        navigate={navigate}
        status={
          <PollStatus
            lastUpdated={asset.lastUpdated}
            refreshing={asset.refreshing || tele.refreshing}
            latencyMs={asset.latencyMs}
            dataTs={a?.last_event_ts}
            onReload={() => {
              asset.reload()
              tele.reload()
            }}
          />
        }
        actions={
          <button
            onClick={() => navigate({ view: 'alerts', assetId })}
            className="inline-flex items-center gap-1.5 rounded-md border border-slate-300 bg-white px-2.5 py-1 text-xs font-medium text-slate-600 hover:bg-slate-50"
          >
            <Bell size={14} /> Alerts for {assetId}
          </button>
        }
      />

      {asset.loading ? (
        <div className="grid grid-cols-1 gap-4 lg:grid-cols-3">
          <SkeletonCard />
          <SkeletonCard />
          <SkeletonCard />
        </div>
      ) : asset.error ? (
        <ErrorPanel error={asset.error} onRetry={asset.reload} context={`asset ${assetId}`} />
      ) : !a ? (
        <EmptyState title="Asset not found" hint={`No data for ${assetId}.`} />
      ) : (
        <div className="space-y-4">
          {/* Top row: anomaly + metadata */}
          <div className="grid grid-cols-1 gap-4 lg:grid-cols-3">
            <Card className="p-4">
              <h3 className="mb-3 text-sm font-semibold text-slate-700">Condition</h3>
              <AnomalyMeter score={a.anomaly_score} />
              <div className="mt-4 grid grid-cols-2 gap-3 text-sm">
                <Meta label="Operating state" value={<OperatingStateBadge state={a.operating_state} />} />
                <Meta label="Condition" value={<HealthBadge status={a.condition} />} />
                <Meta label="Active alerts" value={String(a.active_alert_count)} />
                <Meta label="Line" value={a.line_id} />
              </div>
              <div className="mt-3 text-[11px] text-slate-400">
                Last event {fmtAge(a.last_event_ts, now)} · updated {fmtAge(a.updated_ts, now)}
              </div>
              <div className="mt-4 border-t border-slate-100 pt-3">
                <div className="mb-2 flex items-center justify-between">
                  <h4 className="text-xs font-semibold uppercase tracking-wide text-slate-500">
                    ML anomaly model
                  </h4>
                  {ml && (
                    <span
                      className={`rounded px-1.5 py-0.5 text-[11px] font-semibold ${
                        ml.ml_is_anomaly ? 'bg-red-100 text-red-700' : 'bg-emerald-100 text-emerald-700'
                      }`}
                    >
                      {ml.ml_is_anomaly ? 'ANOMALY' : 'NORMAL'}
                    </span>
                  )}
                </div>
                {ml ? (
                  <>
                    <AnomalyMeter score={ml.ml_anomaly_score} />
                    <div className="mt-2 text-[11px] text-slate-500">
                      Driven by <strong>{ml.top_sensor ?? '—'}</strong> vs this asset&apos;s learned
                      baseline · model v{ml.model_version} · scored {fmtAge(ml.scored_ts, now)}
                    </div>
                  </>
                ) : (
                  <div className="text-[11px] text-slate-400">No ML score yet for this asset.</div>
                )}
              </div>
            </Card>

            {/* Sensor bands grid */}
            <Card className="p-4 lg:col-span-2">
              <h3 className="mb-3 text-sm font-semibold text-slate-700">
                Current sensor readings vs nominal band
              </h3>
              {tele.loading ? (
                <div className="grid grid-cols-1 gap-2 sm:grid-cols-2">
                  {Array.from({ length: 6 }).map((_, i) => (
                    <div key={i} className="skeleton h-16 rounded-lg" />
                  ))}
                </div>
              ) : (
                <div className="grid grid-cols-1 gap-2 sm:grid-cols-2">
                  {SENSORS.map((s) => {
                    const n = nominalOf(tele.data?.[s])
                    return (
                      <SensorBand
                        key={s}
                        label={SENSOR_LABELS[s]}
                        value={currentValue(a, s)}
                        unit={SENSOR_UNITS[s]}
                        nominalMin={n.min}
                        nominalMax={n.max}
                      />
                    )
                  })}
                </div>
              )}
            </Card>
          </div>

          {/* Telemetry chart */}
          <Card className="p-4">
            <div className="mb-3 flex flex-wrap items-center justify-between gap-3">
              <h3 className="text-sm font-semibold text-slate-700">Telemetry history</h3>
              <div className="flex flex-wrap items-center gap-2">
                <label className="sr-only" htmlFor="sensor-select">
                  Sensor
                </label>
                <select
                  id="sensor-select"
                  value={selected}
                  onChange={(e) => setSelected(e.target.value as SensorType)}
                  className="rounded-md border border-slate-300 bg-white px-2 py-1 text-xs font-medium text-slate-700"
                >
                  {SENSORS.map((s) => (
                    <option key={s} value={s}>
                      {SENSOR_LABELS[s]} ({SENSOR_UNITS[s]})
                    </option>
                  ))}
                </select>
                <div className="flex overflow-hidden rounded-md border border-slate-300">
                  {MINUTES_OPTIONS.map((m) => (
                    <button
                      key={m}
                      onClick={() => setMinutes(m)}
                      className={`px-2 py-1 text-xs font-medium ${
                        minutes === m ? 'bg-sky-600 text-white' : 'bg-white text-slate-600 hover:bg-slate-50'
                      }`}
                    >
                      {m}m
                    </button>
                  ))}
                </div>
              </div>
            </div>

            {tele.error ? (
              <ErrorPanel error={tele.error} onRetry={tele.reload} context="telemetry" />
            ) : chartData.length === 0 ? (
              <EmptyState
                title="No telemetry points in this window"
                hint="Start the generator in Scenario Controls to stream live telemetry, then return here."
              />
            ) : (
              <div className="h-72 w-full">
                <ResponsiveContainer width="100%" height="100%">
                  <AreaChart data={chartData} margin={{ top: 8, right: 12, left: -12, bottom: 0 }}>
                    <defs>
                      <linearGradient id="grad" x1="0" y1="0" x2="0" y2="1">
                        <stop offset="0%" stopColor="#0284c7" stopOpacity={0.25} />
                        <stop offset="100%" stopColor="#0284c7" stopOpacity={0} />
                      </linearGradient>
                    </defs>
                    <CartesianGrid strokeDasharray="3 3" stroke="#e2e8f0" />
                    <XAxis dataKey="ts" tick={{ fontSize: 10 }} stroke="#94a3b8" minTickGap={24} />
                    <YAxis
                      tick={{ fontSize: 10 }}
                      stroke="#94a3b8"
                      domain={['auto', 'auto']}
                      width={44}
                    />
                    <Tooltip
                      contentStyle={{ fontSize: 12, borderRadius: 8 }}
                      formatter={(v: number) => [`${v} ${unit}`, SENSOR_LABELS[selected]]}
                    />
                    {nb.min != null && nb.max != null && (
                      <ReferenceArea
                        y1={nb.min}
                        y2={nb.max}
                        fill="#16a34a"
                        fillOpacity={0.1}
                        stroke="#16a34a"
                        strokeOpacity={0.3}
                        label={{ value: 'nominal band', fontSize: 10, fill: '#16a34a', position: 'insideTopRight' }}
                      />
                    )}
                    <Area
                      type="monotone"
                      dataKey="value"
                      stroke="#0284c7"
                      strokeWidth={2}
                      fill="url(#grad)"
                      dot={false}
                      isAnimationActive={false}
                    />
                  </AreaChart>
                </ResponsiveContainer>
              </div>
            )}
            {nb.min != null && nb.max != null && (
              <div className="mt-2 text-[11px] text-slate-400">
                Nominal band {fmtNum(nb.min)}–{fmtNum(nb.max)} {unit}
              </div>
            )}
          </Card>

          {/* Active alerts for this asset */}
          <Card className="p-4">
            <h3 className="mb-3 text-sm font-semibold text-slate-700">
              Active alerts ({activeAlerts.length})
            </h3>
            {activeAlerts.length === 0 ? (
              <EmptyState title="No active alerts for this asset" />
            ) : (
              <ul className="space-y-2">
                {activeAlerts.map((al) => (
                  <li
                    key={al.alert_id}
                    className="flex flex-wrap items-center gap-3 rounded-lg border border-slate-200 p-3"
                    style={{ borderLeft: `4px solid ${severityColor(al.severity)}` }}
                  >
                    <SeverityBadge severity={al.severity} />
                    <AlertStatusBadge status={al.status} />
                    <span className="text-sm font-medium text-slate-700">
                      {SENSOR_LABELS[al.sensor_type]} · {al.alert_type}
                    </span>
                    <span className="text-xs text-slate-500">
                      observed <strong className="tabular-nums">{fmtNum(al.observed_value, 2)}</strong>{' '}
                      vs threshold{' '}
                      <strong className="tabular-nums">{fmtNum(al.threshold_value, 2)}</strong>
                    </span>
                    <button
                      onClick={() => setFixAlert(al)}
                      className="ml-auto inline-flex items-center gap-1 rounded-md border border-sky-300 bg-sky-50 px-2 py-1 text-xs font-medium text-sky-700 hover:bg-sky-100"
                    >
                      <Sparkles size={13} /> Explain &amp; Fix
                    </button>
                    <span className="w-full text-xs text-slate-500">{al.recommended_action}</span>
                  </li>
                ))}
                <li className="pt-1">
                  <button
                    onClick={() => navigate({ view: 'alerts', assetId })}
                    className="text-xs font-medium text-sky-600 hover:underline"
                  >
                    Manage in Alert Console →
                  </button>
                </li>
              </ul>
            )}
          </Card>
        </div>
      )}

      <FixAssistantModal
        open={fixAlert !== null}
        onClose={() => setFixAlert(null)}
        alert={fixAlert}
      />
    </div>
  )
}

function Meta({ label, value }: { label: string; value: React.ReactNode }) {
  return (
    <div>
      <div className="text-[11px] uppercase tracking-wide text-slate-400">{label}</div>
      <div className="mt-0.5 font-medium text-slate-700">{value}</div>
    </div>
  )
}
