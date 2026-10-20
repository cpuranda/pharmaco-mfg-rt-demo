import { useCallback } from 'react'
import {
  Bar,
  BarChart,
  Cell,
  LabelList,
  ReferenceLine,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from 'recharts'
import { Bell, TrendingDown } from 'lucide-react'
import type { Navigate } from '../App'
import { getLine } from '../api'
import { usePolling } from '../hooks/usePolling'
import { useNow } from '../hooks/useNow'
import { ViewHeader } from '../components/ViewHeader'
import { PollStatus } from '../components/PollStatus'
import { ErrorPanel, Card, SkeletonCard, EmptyState } from '../components/Primitives'
import { HealthBadge } from '../components/Badges'
import { ThroughputBar } from '../components/Widgets'
import { AssetTile } from '../components/AssetTile'
import { fmtAge, fmtNum } from '../lib/format'
import { healthColor } from '../lib/colors'

export function LineView({
  lineId,
  navigate,
  interval,
}: {
  lineId: string
  navigate: Navigate
  interval: number
}) {
  const now = useNow()
  const fetcher = useCallback(() => getLine(lineId), [lineId])
  const { data, error, loading, refreshing, lastUpdated, latencyMs, reload } = usePolling(
    fetcher,
    interval,
    [lineId],
  )

  const line = data?.line
  const assets = data?.assets ?? []

  const chartData = assets.map((a) => ({
    asset: a.asset_id.replace(/^[A-Z]-/, ''),
    throughput: Number(a.throughput.toFixed(1)),
    condition: a.condition,
  }))

  return (
    <div>
      <ViewHeader
        title={
          <span className="flex items-center gap-2">
            {lineId}
            {line && <HealthBadge status={line.health_status} />}
          </span>
        }
        crumbs={[{ label: 'Plant Overview', route: { view: 'overview' } }, { label: lineId }]}
        navigate={navigate}
        status={
          <PollStatus
            lastUpdated={lastUpdated}
            refreshing={refreshing}
            latencyMs={latencyMs}
            dataTs={line?.updated_ts}
            onReload={reload}
          />
        }
        actions={
          <button
            onClick={() => navigate({ view: 'alerts', lineId })}
            className="inline-flex items-center gap-1.5 rounded-md border border-slate-300 bg-white px-2.5 py-1 text-xs font-medium text-slate-600 hover:bg-slate-50"
          >
            <Bell size={14} /> Alerts for {lineId}
          </button>
        }
      />

      {loading ? (
        <div className="space-y-4">
          <SkeletonCard />
          <div className="grid grid-cols-1 gap-4 md:grid-cols-2 lg:grid-cols-3">
            {Array.from({ length: 6 }).map((_, i) => (
              <SkeletonCard key={i} />
            ))}
          </div>
        </div>
      ) : error ? (
        <ErrorPanel error={error} onRetry={reload} context={`line ${lineId}`} />
      ) : !line ? (
        <EmptyState title="Line not found" hint={`No data for ${lineId}.`} />
      ) : (
        <>
          {/* Line summary + throughput */}
          <div className="grid grid-cols-1 gap-4 lg:grid-cols-3">
            <Card className="overflow-hidden lg:col-span-1">
              <div className="h-1 w-full" style={{ backgroundColor: healthColor(line.health_status) }} />
              <div className="p-4">
                <div className="text-xs uppercase tracking-wide text-slate-400">
                  {line.product_family} · {line.site_id}
                </div>
                <div className="mt-3">
                  <ThroughputBar
                    pct={line.throughput_pct}
                    actual={line.avg_throughput}
                    baseline={line.baseline_throughput}
                  />
                </div>
                <div className="mt-4 grid grid-cols-2 gap-3 text-sm">
                  <KV label="Running" value={`${line.running_assets}/${line.total_assets}`} />
                  <KV label="Avg speed" value={`${fmtNum(line.avg_speed, 0)} rpm`} />
                  <KV
                    label="Open criticals"
                    value={String(line.open_criticals)}
                    tone={line.open_criticals > 0 ? 'red' : undefined}
                  />
                  <KV
                    label="Open warnings"
                    value={String(line.open_warnings)}
                    tone={line.open_warnings > 0 ? 'amber' : undefined}
                  />
                </div>
                <div className="mt-3 text-[11px] text-slate-400">
                  Updated {fmtAge(line.updated_ts, now)}
                </div>
                {line.is_slowing_down && (
                  <div className="mt-3 flex items-start gap-2 rounded-lg border border-orange-300 bg-orange-50 p-2.5 text-xs text-orange-800">
                    <TrendingDown size={14} className="mt-0.5 shrink-0" />
                    <span>
                      <strong>Slowdown detected.</strong> {line.slowdown_hint}
                    </span>
                  </div>
                )}
              </div>
            </Card>

            {/* Throughput by asset chart */}
            <Card className="p-4 lg:col-span-2">
              <div className="mb-1 flex items-center justify-between">
                <h3 className="text-sm font-semibold text-slate-700">Throughput by asset</h3>
                <span className="text-[11px] text-slate-400">
                  baseline {fmtNum(line.baseline_throughput)}/min
                </span>
              </div>
              <div className="h-56 w-full">
                <ResponsiveContainer width="100%" height="100%">
                  <BarChart data={chartData} margin={{ top: 16, right: 8, left: -16, bottom: 0 }}>
                    <XAxis dataKey="asset" tick={{ fontSize: 11 }} stroke="#94a3b8" />
                    <YAxis tick={{ fontSize: 11 }} stroke="#94a3b8" />
                    <Tooltip
                      contentStyle={{ fontSize: 12, borderRadius: 8 }}
                      formatter={(v: number) => [`${v}/min`, 'Throughput']}
                    />
                    <ReferenceLine
                      y={line.baseline_throughput}
                      stroke="#64748b"
                      strokeDasharray="4 4"
                      label={{ value: 'baseline', fontSize: 10, fill: '#64748b', position: 'right' }}
                    />
                    <Bar dataKey="throughput" radius={[4, 4, 0, 0]}>
                      <LabelList dataKey="throughput" position="top" fontSize={10} fill="#475569" />
                      {chartData.map((d, i) => (
                        <Cell key={i} fill={healthColor(d.condition)} />
                      ))}
                    </Bar>
                  </BarChart>
                </ResponsiveContainer>
              </div>
            </Card>
          </div>

          {/* Asset grid */}
          <h2 className="mb-2 mt-6 text-sm font-semibold uppercase tracking-wide text-slate-500">
            Assets ({assets.length})
          </h2>
          {assets.length === 0 ? (
            <EmptyState title="No assets" />
          ) : (
            <div className="grid grid-cols-1 gap-4 sm:grid-cols-2 lg:grid-cols-3">
              {assets.map((a) => (
                <AssetTile
                  key={a.asset_id}
                  asset={a}
                  onOpen={() => navigate({ view: 'asset', assetId: a.asset_id, lineId })}
                />
              ))}
            </div>
          )}
        </>
      )}
    </div>
  )
}

function KV({
  label,
  value,
  tone,
}: {
  label: string
  value: string
  tone?: 'red' | 'amber'
}) {
  const toneMap: Record<string, string> = {
    red: 'text-red-600',
    amber: 'text-amber-600',
  }
  return (
    <div>
      <div className="text-[11px] uppercase tracking-wide text-slate-400">{label}</div>
      <div className={`font-semibold tabular-nums ${tone ? toneMap[tone] : 'text-slate-800'}`}>
        {value}
      </div>
    </div>
  )
}
