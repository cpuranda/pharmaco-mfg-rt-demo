import type { ReactNode } from 'react'
import { Card } from './Primitives'
import { fmtNum } from '../lib/format'

// Compact KPI tile.
export function StatTile({
  label,
  value,
  sub,
  tone = 'slate',
  icon,
}: {
  label: string
  value: ReactNode
  sub?: ReactNode
  tone?: 'slate' | 'green' | 'amber' | 'red' | 'sky'
  icon?: ReactNode
}) {
  const toneMap: Record<string, string> = {
    slate: 'text-slate-800',
    green: 'text-green-600',
    amber: 'text-amber-600',
    red: 'text-red-600',
    sky: 'text-sky-600',
  }
  return (
    <Card className="p-4">
      <div className="flex items-center justify-between">
        <div className="text-xs font-medium uppercase tracking-wide text-slate-500">{label}</div>
        {icon && <div className="text-slate-300">{icon}</div>}
      </div>
      <div className={`mt-1 text-2xl font-semibold tabular-nums ${toneMap[tone]}`}>{value}</div>
      {sub && <div className="mt-1 text-xs text-slate-500">{sub}</div>}
    </Card>
  )
}

// Throughput vs baseline bar. 100% == baseline. Green near baseline,
// amber when high, red when far off, orange when low.
export function ThroughputBar({
  pct,
  actual,
  baseline,
}: {
  pct: number
  actual: number
  baseline: number
}) {
  const clamped = Math.max(0, Math.min(150, pct))
  const width = (clamped / 150) * 100
  let color = '#16a34a'
  if (pct < 85) color = '#ea580c'
  else if (pct > 115) color = '#d97706'
  // baseline marker at 100% -> position within 0..150 scale
  const baselinePos = (100 / 150) * 100

  return (
    <div>
      <div className="mb-1 flex items-baseline justify-between text-xs text-slate-500">
        <span>
          Throughput{' '}
          <span className="font-semibold tabular-nums text-slate-700">{fmtNum(actual)}</span> /min
        </span>
        <span
          className="font-semibold tabular-nums"
          style={{ color }}
          aria-label={`Throughput ${fmtNum(pct)} percent of baseline`}
        >
          {fmtNum(pct)}%
        </span>
      </div>
      <div className="relative h-3 w-full overflow-hidden rounded-full bg-slate-100">
        <div
          className="h-full rounded-full transition-all"
          style={{ width: `${width}%`, backgroundColor: color }}
        />
        <div
          className="absolute top-0 h-full w-px bg-slate-500"
          style={{ left: `${baselinePos}%` }}
          title={`Baseline ${fmtNum(baseline)}/min`}
        />
      </div>
      <div className="mt-1 text-[11px] text-slate-400">
        Baseline {fmtNum(baseline)}/min (marker)
      </div>
    </div>
  )
}

// Anomaly score meter 0..1.
export function AnomalyMeter({ score }: { score: number }) {
  const pct = Math.max(0, Math.min(1, score)) * 100
  let color = '#16a34a'
  if (score >= 0.66) color = '#dc2626'
  else if (score >= 0.33) color = '#d97706'
  return (
    <div>
      <div className="mb-1 flex items-baseline justify-between text-xs text-slate-500">
        <span>Anomaly score</span>
        <span className="font-semibold tabular-nums" style={{ color }}>
          {score.toFixed(2)}
        </span>
      </div>
      <div className="h-3 w-full overflow-hidden rounded-full bg-slate-100">
        <div className="h-full rounded-full transition-all" style={{ width: `${pct}%`, backgroundColor: color }} />
      </div>
    </div>
  )
}

// Sensor value shown against its nominal min/max band.
export function SensorBand({
  label,
  value,
  unit,
  nominalMin,
  nominalMax,
}: {
  label: string
  value: number
  unit: string
  nominalMin: number | null
  nominalMax: number | null
}) {
  const hasBand = nominalMin != null && nominalMax != null && nominalMax > nominalMin
  // Scale spans a padded window around the band + value.
  let lo: number
  let hi: number
  if (hasBand) {
    const span = (nominalMax as number) - (nominalMin as number)
    lo = Math.min(nominalMin as number, value) - span * 0.3
    hi = Math.max(nominalMax as number, value) + span * 0.3
  } else {
    lo = Math.min(0, value * 0.8)
    hi = Math.max(value * 1.2, value + 1)
  }
  const range = hi - lo || 1
  const pos = (x: number) => `${Math.max(0, Math.min(100, ((x - lo) / range) * 100))}%`

  const outOfBand =
    hasBand && (value < (nominalMin as number) || value > (nominalMax as number))

  return (
    <div className="rounded-lg border border-slate-200 bg-slate-50/60 p-3">
      <div className="flex items-baseline justify-between">
        <span className="text-xs font-medium uppercase tracking-wide text-slate-500">{label}</span>
        <span
          className={`text-sm font-semibold tabular-nums ${outOfBand ? 'text-red-600' : 'text-slate-800'}`}
        >
          {fmtNum(value, 2)} <span className="text-xs font-normal text-slate-400">{unit}</span>
        </span>
      </div>
      <div className="relative mt-2 h-2.5 w-full rounded-full bg-slate-200">
        {hasBand && (
          <div
            className="absolute top-0 h-full rounded-full bg-green-300/70"
            style={{ left: pos(nominalMin as number), width: `calc(${pos(nominalMax as number)} - ${pos(nominalMin as number)})` }}
            title={`Nominal ${fmtNum(nominalMin)}–${fmtNum(nominalMax)} ${unit}`}
          />
        )}
        <div
          className={`absolute top-1/2 h-3.5 w-3.5 -translate-x-1/2 -translate-y-1/2 rounded-full border-2 border-white shadow ${outOfBand ? 'bg-red-600' : 'bg-slate-700'}`}
          style={{ left: pos(value) }}
          title={`Current ${fmtNum(value, 2)} ${unit}`}
        />
      </div>
      {hasBand ? (
        <div className="mt-1 flex justify-between text-[11px] text-slate-400">
          <span>{fmtNum(nominalMin)}</span>
          <span>nominal band</span>
          <span>{fmtNum(nominalMax)}</span>
        </div>
      ) : (
        <div className="mt-1 text-[11px] text-slate-400">no nominal band</div>
      )}
    </div>
  )
}
