import { useCallback } from 'react'
import { AlertOctagon, AlertTriangle, Boxes, PauseCircle, PlayCircle } from 'lucide-react'
import type { Navigate } from '../App'
import { getLines, getPlantSummary } from '../api'
import { usePolling } from '../hooks/usePolling'
import { useNow } from '../hooks/useNow'
import { ViewHeader } from '../components/ViewHeader'
import { PollStatus } from '../components/PollStatus'
import { StaleBanner } from '../components/StaleBanner'
import { ErrorPanel, SkeletonCard } from '../components/Primitives'
import { StatTile } from '../components/Widgets'
import { HealthBadge } from '../components/Badges'
import { LineCard } from '../components/LineCard'
import { ageSeconds, fmtClock, STALE_THRESHOLD_S } from '../lib/format'

export function PlantOverview({
  navigate,
  interval,
}: {
  navigate: Navigate
  interval: number
}) {
  const now = useNow()
  const summaryFetch = useCallback(() => getPlantSummary(), [])
  const linesFetch = useCallback(() => getLines(), [])
  const summary = usePolling(summaryFetch, interval)
  const lines = usePolling(linesFetch, interval)

  const s = summary.data
  const stale = s ? (ageSeconds(s.latest_event_ts, now) ?? 0) > STALE_THRESHOLD_S : false

  return (
    <div>
      <ViewHeader
        title="Plant Overview"
        status={
          <PollStatus
            lastUpdated={summary.lastUpdated}
            refreshing={summary.refreshing || lines.refreshing}
            latencyMs={summary.latencyMs}
            dataTs={s?.latest_event_ts}
            onReload={() => {
              summary.reload()
              lines.reload()
            }}
          />
        }
      />

      {stale && s && (
        <div className="mb-4">
          <StaleBanner latestTs={s.latest_event_ts} thresholdS={STALE_THRESHOLD_S} now={now} />
        </div>
      )}

      {/* Plant status + tiles */}
      {summary.loading ? (
        <div className="grid grid-cols-2 gap-3 sm:grid-cols-3 lg:grid-cols-6">
          {Array.from({ length: 6 }).map((_, i) => (
            <SkeletonCard key={i} />
          ))}
        </div>
      ) : summary.error ? (
        <ErrorPanel error={summary.error} onRetry={summary.reload} context="plant summary" />
      ) : s ? (
        <>
          <div className="mb-4 flex flex-wrap items-center gap-3 rounded-xl border border-slate-200 bg-white p-4 shadow-sm">
            <span className="text-sm font-medium text-slate-500">Overall plant status</span>
            <HealthBadge status={s.plant_status} />
            <span className="text-xs text-slate-400">
              {s.lines} lines · {s.total_assets} assets
            </span>
            <span className="ml-auto text-xs text-slate-400">
              Latest event {fmtClock(s.latest_event_ts)} ·{' '}
              <span className={stale ? 'font-semibold text-amber-600' : ''}>
                {ageSeconds(s.latest_event_ts, now) != null
                  ? `${Math.round(ageSeconds(s.latest_event_ts, now)!)}s freshness`
                  : 'no data'}
              </span>
            </span>
          </div>

          <div className="grid grid-cols-2 gap-3 sm:grid-cols-3 lg:grid-cols-6">
            <StatTile
              label="Running"
              value={`${s.running_assets}/${s.total_assets}`}
              tone="green"
              icon={<PlayCircle size={16} />}
            />
            <StatTile label="Idle" value={s.idle_assets} tone="slate" icon={<PauseCircle size={16} />} />
            <StatTile
              label="Faulted"
              value={s.faulted_assets}
              tone={s.faulted_assets > 0 ? 'red' : 'slate'}
              icon={<AlertOctagon size={16} />}
            />
            <StatTile
              label="Open criticals"
              value={s.open_criticals}
              tone={s.open_criticals > 0 ? 'red' : 'slate'}
              icon={<AlertOctagon size={16} />}
            />
            <StatTile
              label="Open warnings"
              value={s.open_warnings}
              tone={s.open_warnings > 0 ? 'amber' : 'slate'}
              icon={<AlertTriangle size={16} />}
            />
            <StatTile label="Total assets" value={s.total_assets} tone="sky" icon={<Boxes size={16} />} />
          </div>
        </>
      ) : null}

      {/* Line cards */}
      <h2 className="mb-2 mt-6 text-sm font-semibold uppercase tracking-wide text-slate-500">
        Production Lines
      </h2>
      {lines.loading ? (
        <div className="grid grid-cols-1 gap-4 lg:grid-cols-3">
          {Array.from({ length: 3 }).map((_, i) => (
            <SkeletonCard key={i} />
          ))}
        </div>
      ) : lines.error ? (
        <ErrorPanel error={lines.error} onRetry={lines.reload} context="lines" />
      ) : (
        <div className="grid grid-cols-1 gap-4 lg:grid-cols-3">
          {(lines.data ?? []).map((line) => (
            <LineCard
              key={line.line_id}
              line={line}
              now={now}
              onOpen={() => navigate({ view: 'line', lineId: line.line_id })}
            />
          ))}
        </div>
      )}
    </div>
  )
}
