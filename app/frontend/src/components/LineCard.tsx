import { AlertTriangle, ChevronRight, TrendingDown } from 'lucide-react'
import type { Line } from '../types'
import { Card } from './Primitives'
import { HealthBadge } from './Badges'
import { ThroughputBar } from './Widgets'
import { fmtAge } from '../lib/format'
import { healthColor } from '../lib/colors'

const FAMILY_LABEL: Record<string, string> = {
  OSD_TABLET: 'OSD Tablet',
  STERILE_VIAL: 'Sterile Vial',
  PACKAGING: 'Packaging',
}

export function LineCard({
  line,
  now,
  onOpen,
}: {
  line: Line
  now: number
  onOpen: () => void
}) {
  const openAlerts = line.open_criticals + line.open_warnings
  return (
    <Card className="overflow-hidden">
      <div className="h-1 w-full" style={{ backgroundColor: healthColor(line.health_status) }} />
      <button
        onClick={onOpen}
        className="group block w-full p-4 text-left hover:bg-slate-50/60"
        aria-label={`Open ${line.line_id} detail`}
      >
        <div className="flex items-start justify-between">
          <div>
            <div className="flex items-center gap-2">
              <span className="text-base font-semibold text-slate-800">{line.line_id}</span>
              <span className="text-xs text-slate-400">
                {FAMILY_LABEL[line.product_family] ?? line.product_family}
              </span>
            </div>
            <div className="mt-0.5 text-[11px] text-slate-400">{line.site_id}</div>
          </div>
          <div className="flex items-center gap-2">
            <HealthBadge status={line.health_status} />
            <ChevronRight size={16} className="text-slate-300 group-hover:text-slate-500" />
          </div>
        </div>

        <div className="mt-3">
          <ThroughputBar
            pct={line.throughput_pct}
            actual={line.avg_throughput}
            baseline={line.baseline_throughput}
          />
        </div>

        <div className="mt-3 grid grid-cols-3 gap-2 text-center">
          <Metric
            label="Running"
            value={`${line.running_assets}/${line.total_assets}`}
            tone="green"
          />
          <Metric
            label="Open alerts"
            value={String(openAlerts)}
            tone={line.open_criticals > 0 ? 'red' : line.open_warnings > 0 ? 'amber' : 'slate'}
            icon={openAlerts > 0 ? <AlertTriangle size={12} /> : undefined}
          />
          <Metric
            label="Faulted"
            value={String(line.faulted_assets)}
            tone={line.faulted_assets > 0 ? 'red' : 'slate'}
          />
        </div>

        <div className="mt-3 flex items-center justify-between text-[11px] text-slate-400">
          <span>Updated {fmtAge(line.updated_ts, now)}</span>
          {line.is_slowing_down && (
            <span className="inline-flex items-center gap-1 font-medium text-orange-600">
              <TrendingDown size={12} /> {line.slowdown_hint}
            </span>
          )}
        </div>
      </button>
    </Card>
  )
}

function Metric({
  label,
  value,
  tone,
  icon,
}: {
  label: string
  value: string
  tone: 'slate' | 'green' | 'amber' | 'red'
  icon?: React.ReactNode
}) {
  const toneMap: Record<string, string> = {
    slate: 'text-slate-700',
    green: 'text-green-600',
    amber: 'text-amber-600',
    red: 'text-red-600',
  }
  return (
    <div className="rounded-lg bg-slate-50 py-2">
      <div className={`flex items-center justify-center gap-1 text-lg font-semibold tabular-nums ${toneMap[tone]}`}>
        {icon}
        {value}
      </div>
      <div className="text-[10px] uppercase tracking-wide text-slate-400">{label}</div>
    </div>
  )
}
