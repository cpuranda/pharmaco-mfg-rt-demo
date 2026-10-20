import { Activity, FlaskConical, Info } from 'lucide-react'

const INTERVALS = [
  { label: '2s', value: 2000 },
  { label: '5s', value: 5000 },
  { label: '10s', value: 10000 },
  { label: '30s', value: 30000 },
]

export function Header({
  operator,
  onOperatorChange,
  interval,
  onIntervalChange,
  onAbout,
}: {
  operator: string
  onOperatorChange: (v: string) => void
  interval: number
  onIntervalChange: (v: number) => void
  onAbout: () => void
}) {
  return (
    <header className="sticky top-0 z-40 border-b border-slate-200 bg-white/95 backdrop-blur">
      <div className="mx-auto flex max-w-[1600px] flex-wrap items-center gap-x-4 gap-y-2 px-4 py-2.5">
        <div className="flex items-center gap-2">
          <div className="flex h-8 w-8 items-center justify-center rounded-lg bg-sky-600 text-white">
            <Activity size={18} />
          </div>
          <div className="leading-tight">
            <div className="text-sm font-semibold text-slate-800">
              PharmaCo Manufacturing — Real-Time Line Monitoring
            </div>
            <div className="text-[11px] text-slate-400">Operator &amp; Manager console</div>
          </div>
        </div>

        <span
          className="inline-flex items-center gap-1 rounded-full border border-amber-300 bg-amber-100 px-2.5 py-1 text-[11px] font-bold uppercase tracking-wide text-amber-800"
          title="All data in this application is synthetic"
        >
          <FlaskConical size={12} /> Synthetic Demo Data
        </span>

        <div className="ml-auto flex flex-wrap items-center gap-3">
          <label className="flex items-center gap-1.5 text-xs text-slate-500">
            <span className="font-medium">Operator</span>
            <input
              value={operator}
              onChange={(e) => onOperatorChange(e.target.value)}
              placeholder="your name"
              aria-label="Operator name"
              className="w-28 rounded-md border border-slate-300 px-2 py-1 text-xs text-slate-700"
            />
          </label>

          <label className="flex items-center gap-1.5 text-xs text-slate-500">
            <span className="font-medium">Refresh</span>
            <select
              value={interval}
              onChange={(e) => onIntervalChange(Number(e.target.value))}
              aria-label="Global refresh interval"
              className="rounded-md border border-slate-300 bg-white px-2 py-1 text-xs font-medium text-slate-700"
            >
              {INTERVALS.map((o) => (
                <option key={o.value} value={o.value}>
                  {o.label}
                </option>
              ))}
            </select>
          </label>

          <button
            onClick={onAbout}
            className="inline-flex items-center gap-1.5 rounded-md border border-slate-300 bg-white px-2.5 py-1 text-xs font-medium text-slate-600 hover:bg-slate-50"
          >
            <Info size={14} /> About this demo
          </button>
        </div>
      </div>
    </header>
  )
}
