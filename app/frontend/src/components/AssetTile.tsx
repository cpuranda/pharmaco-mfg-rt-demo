import { AlertTriangle } from 'lucide-react'
import type { Asset } from '../types'
import { Card } from './Primitives'
import { HealthBadge, OperatingStateBadge } from './Badges'
import { fmtNum } from '../lib/format'

export function AssetTile({ asset, onOpen }: { asset: Asset; onOpen: () => void }) {
  return (
    <Card className="overflow-hidden">
      <button
        onClick={onOpen}
        className="block w-full p-3 text-left hover:bg-slate-50/60"
        aria-label={`Open ${asset.asset_id} detail`}
      >
        <div className="flex items-start justify-between gap-2">
          <span className="font-semibold text-slate-800">{asset.asset_id}</span>
          <HealthBadge status={asset.condition} />
        </div>
        <div className="mt-1.5">
          <OperatingStateBadge state={asset.operating_state} />
        </div>
        <dl className="mt-3 grid grid-cols-2 gap-x-3 gap-y-1.5 text-xs">
          <Row label="Temp" value={`${fmtNum(asset.temperature)}°C`} />
          <Row label="Vibration" value={`${fmtNum(asset.vibration, 2)} mm/s`} />
          <Row label="Pressure" value={`${fmtNum(asset.pressure, 2)} bar`} />
          <Row label="Speed" value={`${fmtNum(asset.speed, 0)} rpm`} />
          <Row label="Throughput" value={`${fmtNum(asset.throughput)}/min`} />
          <Row label="Anomaly" value={asset.anomaly_score.toFixed(2)} />
        </dl>
        {asset.active_alert_count > 0 && (
          <div className="mt-2 inline-flex items-center gap-1 rounded-md bg-red-50 px-2 py-0.5 text-xs font-medium text-red-700">
            <AlertTriangle size={12} /> {asset.active_alert_count} active alert
            {asset.active_alert_count > 1 ? 's' : ''}
          </div>
        )}
      </button>
    </Card>
  )
}

function Row({ label, value }: { label: string; value: string }) {
  return (
    <div className="flex items-baseline justify-between gap-1">
      <dt className="text-slate-400">{label}</dt>
      <dd className="font-medium tabular-nums text-slate-700">{value}</dd>
    </div>
  )
}
