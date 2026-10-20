import { Clock } from 'lucide-react'
import { fmtAge } from '../lib/format'

// Banner shown when the freshest data timestamp is older than the threshold.
export function StaleBanner({
  latestTs,
  thresholdS,
  now,
}: {
  latestTs: string | null | undefined
  thresholdS: number
  now: number
}) {
  return (
    <div
      role="status"
      className="flex items-center gap-2 rounded-lg border border-amber-300 bg-amber-50 px-4 py-2 text-sm text-amber-800"
    >
      <Clock size={16} />
      <span>
        <strong>Stale data.</strong> Latest event is {fmtAge(latestTs, now)} old (&gt;{' '}
        {thresholdS}s). Start the generator in Scenario Controls to resume live telemetry.
      </span>
    </div>
  )
}
