import { useCallback, useEffect, useState } from 'react'
import { BookOpen, FlaskConical, Sparkles, Wrench } from 'lucide-react'
import type { Alert, FixAnswer } from '../types'
import { explainFix, type FixContext } from '../api'
import { Modal } from './Modal'
import { Spinner } from './Primitives'
import { SeverityBadge } from './Badges'
import { SENSOR_LABELS, fmtNum } from '../lib/format'

/**
 * SOP-grounded "Explain & Fix" modal. Retrieves the relevant SOP sections from
 * the Vector Search index and asks the Foundation Model for a cited remediation.
 */
export function FixAssistantModal({
  open,
  onClose,
  alert,
}: {
  open: boolean
  onClose: () => void
  alert: Alert | null
}) {
  const [data, setData] = useState<FixAnswer | null>(null)
  const [error, setError] = useState<string | null>(null)
  const [loading, setLoading] = useState(false)

  const run = useCallback(async () => {
    if (!alert) return
    setLoading(true)
    setError(null)
    setData(null)
    const ctx: FixContext = {
      alert_id: alert.alert_id,
      asset_id: alert.asset_id,
      line_id: alert.line_id,
      sensor_type: alert.sensor_type,
      alert_type: alert.alert_type,
      severity: alert.severity,
      observed_value: alert.observed_value,
      threshold_value: alert.threshold_value,
      anomaly_score: alert.anomaly_score,
    }
    try {
      setData(await explainFix(ctx))
    } catch (e) {
      setError((e as Error).message)
    } finally {
      setLoading(false)
    }
  }, [alert])

  useEffect(() => {
    if (open && alert) run()
  }, [open, alert, run])

  return (
    <Modal open={open} onClose={onClose} title="Explain & Fix — SOP-grounded assistant">
      {alert && (
        <div className="mb-3 flex flex-wrap items-center gap-2 rounded-lg bg-slate-50 p-3 text-xs text-slate-600">
          <SeverityBadge severity={alert.severity} />
          <span className="font-medium text-slate-700">
            {alert.asset_id} · {SENSOR_LABELS[alert.sensor_type]} · {alert.alert_type}
          </span>
          <span className="text-slate-400">
            observed <strong className="tabular-nums">{fmtNum(alert.observed_value, 2)}</strong> vs
            threshold <strong className="tabular-nums">{fmtNum(alert.threshold_value, 2)}</strong>
          </span>
        </div>
      )}

      {loading ? (
        <div className="flex items-center gap-2 py-8 text-sm text-slate-500">
          <Spinner /> Retrieving SOPs and drafting remediation…
        </div>
      ) : error ? (
        <div className="rounded-md bg-red-50 p-3 text-sm text-red-700">{error}</div>
      ) : data ? (
        <div className="space-y-4">
          <div className="rounded-lg border border-sky-100 bg-sky-50/40 p-3">
            <div className="mb-1.5 flex items-center gap-1.5 text-xs font-semibold uppercase tracking-wide text-sky-600">
              <Sparkles size={13} /> Recommended remediation
            </div>
            <SimpleMarkdown text={data.answer} />
          </div>

          {data.citations.length > 0 && (
            <div>
              <div className="mb-1.5 flex items-center gap-1.5 text-xs font-semibold uppercase tracking-wide text-slate-500">
                <BookOpen size={13} /> Sources ({data.citations.length})
              </div>
              <ul className="space-y-1.5">
                {data.citations.map((c) => (
                  <li
                    key={c.sop_id + c.section}
                    className="flex items-center gap-2 rounded-md border border-slate-200 px-3 py-1.5 text-xs text-slate-600"
                  >
                    <span className="rounded bg-slate-100 px-1.5 py-0.5 font-mono text-[11px] text-slate-700">
                      {c.sop_id}
                    </span>
                    <span className="font-medium text-slate-700">{c.title}</span>
                    <span className="text-slate-400">· {c.section}</span>
                    <span className="ml-auto tabular-nums text-slate-400">
                      {(c.score * 100).toFixed(0)}%
                    </span>
                  </li>
                ))}
              </ul>
            </div>
          )}
        </div>
      ) : null}

      <div className="mt-4 flex items-center justify-between">
        <span className="inline-flex items-center gap-1 text-[11px] text-amber-600">
          <FlaskConical size={11} /> Grounded in synthetic demo SOPs — not real procedures.
        </span>
        <div className="flex gap-2">
          <button
            onClick={run}
            disabled={loading || !alert}
            className="inline-flex items-center gap-1.5 rounded-md border border-slate-300 bg-white px-3 py-1.5 text-sm font-medium text-slate-600 hover:bg-slate-50 disabled:opacity-50"
          >
            <Wrench size={14} /> Regenerate
          </button>
          <button
            onClick={onClose}
            className="rounded-md bg-sky-600 px-3 py-1.5 text-sm font-medium text-white hover:bg-sky-700"
          >
            Close
          </button>
        </div>
      </div>
    </Modal>
  )
}

// Lightweight markdown: **bold**, `[SOP-xxx]` chips, numbered/blank lines.
function SimpleMarkdown({ text }: { text: string }) {
  return (
    <div className="space-y-1 whitespace-pre-wrap break-words text-sm leading-relaxed text-slate-700">
      {text.split('\n').map((line, i) => (
        <p key={i}>{renderInline(line)}</p>
      ))}
    </div>
  )
}

function renderInline(line: string) {
  // split on **bold** and [SOP-...] tokens
  const parts = line.split(/(\*\*[^*]+\*\*|\[SOP-[A-Z0-9-]+\])/g)
  return parts.map((part, i) => {
    if (part.startsWith('**') && part.endsWith('**')) {
      return (
        <strong key={i} className="font-semibold">
          {part.slice(2, -2)}
        </strong>
      )
    }
    if (part.startsWith('[SOP-') && part.endsWith(']')) {
      return (
        <span
          key={i}
          className="mx-0.5 rounded bg-sky-100 px-1 py-0.5 font-mono text-[11px] text-sky-700"
        >
          {part.slice(1, -1)}
        </span>
      )
    }
    return <span key={i}>{part}</span>
  })
}
