import { RefreshCw } from 'lucide-react'
import { fmtClock } from '../lib/format'
import { Spinner } from './Primitives'

const OPTIONS = [
  { label: '2s', value: 2000 },
  { label: '5s', value: 5000 },
  { label: '10s', value: 10000 },
  { label: '30s', value: 30000 },
]

export function RefreshControl({
  interval,
  onChange,
  lastUpdated,
  refreshing,
  onReloadNow,
}: {
  interval: number
  onChange: (v: number) => void
  lastUpdated: number | null
  refreshing: boolean
  onReloadNow: () => void
}) {
  return (
    <div className="flex items-center gap-3 text-xs text-slate-500">
      <div className="flex items-center gap-1.5" aria-live="polite">
        {refreshing ? (
          <Spinner className="text-sky-500" />
        ) : (
          <span className="h-2 w-2 rounded-full bg-green-500" aria-hidden="true" />
        )}
        <span>
          Updated{' '}
          <span className="font-medium text-slate-700">
            {lastUpdated ? fmtClock(new Date(lastUpdated).toISOString()) : '—'}
          </span>
        </span>
      </div>
      <div className="flex items-center gap-1">
        <label htmlFor="refresh-interval" className="sr-only">
          Refresh interval
        </label>
        <select
          id="refresh-interval"
          value={interval}
          onChange={(e) => onChange(Number(e.target.value))}
          className="rounded-md border border-slate-300 bg-white px-2 py-1 text-xs font-medium text-slate-700"
        >
          {OPTIONS.map((o) => (
            <option key={o.value} value={o.value}>
              Every {o.label}
            </option>
          ))}
        </select>
        <button
          onClick={onReloadNow}
          title="Refresh now"
          aria-label="Refresh now"
          className="rounded-md border border-slate-300 bg-white p-1.5 text-slate-500 hover:bg-slate-50"
        >
          <RefreshCw size={14} />
        </button>
      </div>
    </div>
  )
}
