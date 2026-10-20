import { RefreshCw } from 'lucide-react'
import { fmtAge, fmtClock, fmtMs } from '../lib/format'
import { Spinner } from './Primitives'

// Distinguishes API response latency from data freshness (age of source ts).
export function PollStatus({
  lastUpdated,
  refreshing,
  latencyMs,
  dataTs,
  onReload,
}: {
  lastUpdated: number | null
  refreshing: boolean
  latencyMs: number | null
  dataTs?: string | null
  onReload: () => void
}) {
  return (
    <div className="flex flex-wrap items-center gap-x-4 gap-y-1 text-xs text-slate-500">
      <span className="inline-flex items-center gap-1.5" aria-live="polite">
        {refreshing ? (
          <Spinner className="text-sky-500" />
        ) : (
          <span className="h-2 w-2 rounded-full bg-green-500" aria-hidden="true" />
        )}
        Refreshed{' '}
        <span className="font-medium text-slate-700">
          {lastUpdated ? fmtClock(new Date(lastUpdated).toISOString()) : '—'}
        </span>
      </span>
      {dataTs !== undefined && (
        <span title="Age of the latest source event timestamp">
          Data age <span className="font-medium text-slate-700">{fmtAge(dataTs)}</span>
        </span>
      )}
      <span title="API round-trip latency of the last request">
        API <span className="font-medium text-slate-700">{fmtMs(latencyMs)}</span>
      </span>
      <button
        onClick={onReload}
        title="Refresh now"
        aria-label="Refresh now"
        className="inline-flex items-center gap-1 rounded-md border border-slate-300 bg-white px-1.5 py-0.5 text-slate-500 hover:bg-slate-50"
      >
        <RefreshCw size={12} />
      </button>
    </div>
  )
}
