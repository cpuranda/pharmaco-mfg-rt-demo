import { FlaskConical } from 'lucide-react'
import { Modal } from './Modal'

const STAGES = [
  'Synthetic Generator',
  'Zerobus Ingest',
  'Delta (Bronze)',
  'Structured Streaming',
  'Gold Delta',
  'Lakebase',
  'Databricks App',
]

export function AboutModal({ open, onClose }: { open: boolean; onClose: () => void }) {
  return (
    <Modal open={open} onClose={onClose} title="About this demo" maxWidth="max-w-2xl">
      <div className="space-y-4 text-sm text-slate-600">
        <div className="flex items-start gap-3 rounded-lg border border-amber-300 bg-amber-50 p-3 text-amber-800">
          <FlaskConical size={18} className="mt-0.5 shrink-0" />
          <p>
            <strong>All data shown here is 100% synthetic.</strong> No real PharmaCo manufacturing
            data is used. This is a Databricks demonstration of real-time line monitoring.
          </p>
        </div>

        <p>
          A synthetic generator emits plant telemetry for <strong>1 plant, 3 lines, 15 assets</strong>{' '}
          (temperature, vibration, pressure, motor current, speed, throughput, energy). Events flow
          through a Databricks lakehouse pipeline and surface in this operator/manager app.
        </p>

        <div>
          <div className="mb-2 text-xs font-semibold uppercase tracking-wide text-slate-500">
            Architecture
          </div>
          <div className="scroll-x">
            <div className="flex min-w-max items-center gap-2">
              {STAGES.map((s, i) => (
                <div key={s} className="flex items-center gap-2">
                  <span className="rounded-md border border-slate-300 bg-slate-50 px-2.5 py-1 text-xs font-medium text-slate-700">
                    {s}
                  </span>
                  {i < STAGES.length - 1 && <span className="text-slate-400">→</span>}
                </div>
              ))}
            </div>
          </div>
        </div>

        <ul className="list-disc space-y-1 pl-5 text-slate-600">
          <li>
            <strong>Zerobus Ingest → Delta</strong>: raw telemetry lands in a managed bronze Delta
            table.
          </li>
          <li>
            <strong>Structured Streaming</strong>: validates, dedups, applies thresholds, scores
            anomalies, detects slowdowns.
          </li>
          <li>
            <strong>Gold Delta</strong>: current state, alerts, line health, event history.
          </li>
          <li>
            <strong>Lakebase</strong>: low-latency operational serving + alert acknowledgements.
          </li>
          <li>
            <strong>Databricks App</strong>: this React UI (operator + manager views).
          </li>
        </ul>
      </div>
    </Modal>
  )
}
