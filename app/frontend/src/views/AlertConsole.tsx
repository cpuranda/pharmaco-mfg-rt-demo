import { useCallback, useMemo, useState } from 'react'
import {
  CheckCircle2,
  ChevronDown,
  ChevronRight,
  ChevronsUpDown,
  History as HistoryIcon,
  Sparkles,
  Wrench,
} from 'lucide-react'
import type { Navigate } from '../App'
import type {
  Alert,
  AlertHistoryEntry,
  AlertStatus,
  AlertType,
  Severity,
} from '../types'
import {
  acknowledgeAlert,
  getAlertHistory,
  getAlerts,
  resolveAlert,
  type AlertFilters,
} from '../api'
import { usePolling } from '../hooks/usePolling'
import { ViewHeader } from '../components/ViewHeader'
import { PollStatus } from '../components/PollStatus'
import { Card, EmptyState, ErrorPanel, Spinner } from '../components/Primitives'
import { AlertStatusBadge, SeverityBadge } from '../components/Badges'
import { Modal } from '../components/Modal'
import { FixAssistantModal } from '../components/FixAssistantModal'
import { SENSOR_LABELS, fmtClock, fmtNum } from '../lib/format'
import { severityColor } from '../lib/colors'

type SortKey = 'created_ts' | 'severity' | 'line_id' | 'asset_id' | 'status' | 'alert_type'
const SEVERITY_RANK: Record<Severity, number> = { CRITICAL: 0, WARNING: 1 }

export function AlertConsole({
  navigate,
  interval,
  operator,
  initialAssetId,
  initialLineId,
}: {
  navigate: Navigate
  interval: number
  operator: string
  initialAssetId?: string
  initialLineId?: string
}) {
  const [filters, setFilters] = useState<AlertFilters>({
    severity: '',
    status: '',
    alert_type: '',
    line_id: initialLineId ?? '',
    asset_id: initialAssetId ?? '',
  })
  const [sortKey, setSortKey] = useState<SortKey>('created_ts')
  const [sortDir, setSortDir] = useState<'asc' | 'desc'>('desc')
  const [expanded, setExpanded] = useState<string | null>(null)
  const [action, setAction] = useState<{ alert: Alert; kind: 'ack' | 'resolve' } | null>(null)
  const [fixAlert, setFixAlert] = useState<Alert | null>(null)

  const fetcher = useCallback(() => getAlerts({ ...filters, limit: 300 }), [filters])
  const { data, error, loading, refreshing, lastUpdated, latencyMs, reload } = usePolling(
    fetcher,
    interval,
    [JSON.stringify(filters)],
  )

  const alerts = data ?? []

  // Options for line/asset filters derived from current result set.
  const lineOptions = useMemo(
    () => Array.from(new Set(alerts.map((a) => a.line_id))).sort(),
    [alerts],
  )
  const assetOptions = useMemo(
    () => Array.from(new Set(alerts.map((a) => a.asset_id))).sort(),
    [alerts],
  )

  const sorted = useMemo(() => {
    const copy = [...alerts]
    copy.sort((a, b) => {
      let cmp = 0
      if (sortKey === 'severity') cmp = SEVERITY_RANK[a.severity] - SEVERITY_RANK[b.severity]
      else if (sortKey === 'created_ts')
        cmp = Date.parse(a.created_ts) - Date.parse(b.created_ts)
      else cmp = String(a[sortKey]).localeCompare(String(b[sortKey]))
      return sortDir === 'asc' ? cmp : -cmp
    })
    return copy
  }, [alerts, sortKey, sortDir])

  const toggleSort = (k: SortKey) => {
    if (sortKey === k) setSortDir((d) => (d === 'asc' ? 'desc' : 'asc'))
    else {
      setSortKey(k)
      setSortDir(k === 'created_ts' ? 'desc' : 'asc')
    }
  }

  const setFilter = (patch: Partial<AlertFilters>) => setFilters((f) => ({ ...f, ...patch }))

  return (
    <div>
      <ViewHeader
        title="Alert Console"
        status={
          <PollStatus
            lastUpdated={lastUpdated}
            refreshing={refreshing}
            latencyMs={latencyMs}
            onReload={reload}
          />
        }
      />

      {/* Filters */}
      <Card className="mb-4 p-3">
        <div className="flex flex-wrap items-end gap-3">
          <FilterSelect
            label="Severity"
            value={filters.severity ?? ''}
            onChange={(v) => setFilter({ severity: v as Severity | '' })}
            options={['', 'CRITICAL', 'WARNING']}
          />
          <FilterSelect
            label="Status"
            value={filters.status ?? ''}
            onChange={(v) => setFilter({ status: v as AlertStatus | '' })}
            options={['', 'NEW', 'ACKNOWLEDGED', 'RESOLVED', 'MAINTENANCE']}
          />
          <FilterSelect
            label="Type"
            value={filters.alert_type ?? ''}
            onChange={(v) => setFilter({ alert_type: v as AlertType | '' })}
            options={['', 'THRESHOLD', 'TREND', 'SLOWDOWN', 'RECOVERY']}
          />
          <FilterSelect
            label="Line"
            value={filters.line_id ?? ''}
            onChange={(v) => setFilter({ line_id: v })}
            options={['', ...lineOptions]}
          />
          <FilterSelect
            label="Asset"
            value={filters.asset_id ?? ''}
            onChange={(v) => setFilter({ asset_id: v })}
            options={['', ...assetOptions]}
          />
          <button
            onClick={() =>
              setFilters({ severity: '', status: '', alert_type: '', line_id: '', asset_id: '' })
            }
            className="ml-auto rounded-md border border-slate-300 bg-white px-3 py-1.5 text-xs font-medium text-slate-600 hover:bg-slate-50"
          >
            Clear filters
          </button>
        </div>
      </Card>

      {loading ? (
        <div className="skeleton h-64 w-full rounded-xl" />
      ) : error ? (
        <ErrorPanel error={error} onRetry={reload} context="alerts" />
      ) : sorted.length === 0 ? (
        <EmptyState
          title="No alerts match these filters"
          hint="Adjust filters or run a scenario in Scenario Controls to generate alerts."
        />
      ) : (
        <Card className="overflow-hidden">
          <div className="scroll-x">
            <table className="w-full min-w-[980px] text-sm">
              <thead>
                <tr className="border-b border-slate-200 bg-slate-50 text-left text-xs uppercase tracking-wide text-slate-500">
                  <Th />
                  <Th label="Severity" onClick={() => toggleSort('severity')} active={sortKey === 'severity'} dir={sortDir} />
                  <Th label="Status" onClick={() => toggleSort('status')} active={sortKey === 'status'} dir={sortDir} />
                  <Th label="Line" onClick={() => toggleSort('line_id')} active={sortKey === 'line_id'} dir={sortDir} />
                  <Th label="Asset" onClick={() => toggleSort('asset_id')} active={sortKey === 'asset_id'} dir={sortDir} />
                  <Th label="Type" onClick={() => toggleSort('alert_type')} active={sortKey === 'alert_type'} dir={sortDir} />
                  <th className="px-3 py-2 font-semibold">Sensor</th>
                  <th className="px-3 py-2 font-semibold">Obs / Thresh</th>
                  <th className="px-3 py-2 font-semibold">Recommended action</th>
                  <Th label="Created" onClick={() => toggleSort('created_ts')} active={sortKey === 'created_ts'} dir={sortDir} />
                  <th className="px-3 py-2 text-right font-semibold">Actions</th>
                </tr>
              </thead>
              <tbody>
                {sorted.map((al) => (
                  <AlertRow
                    key={al.alert_id}
                    alert={al}
                    expanded={expanded === al.alert_id}
                    onToggle={() =>
                      setExpanded((e) => (e === al.alert_id ? null : al.alert_id))
                    }
                    onAck={() => setAction({ alert: al, kind: 'ack' })}
                    onResolve={() => setAction({ alert: al, kind: 'resolve' })}
                    onFix={() => setFixAlert(al)}
                    onOpenAsset={() =>
                      navigate({ view: 'asset', assetId: al.asset_id, lineId: al.line_id })
                    }
                  />
                ))}
              </tbody>
            </table>
          </div>
        </Card>
      )}

      {action && (
        <AlertActionModal
          alert={action.alert}
          kind={action.kind}
          operator={operator}
          onClose={() => setAction(null)}
          onDone={() => {
            setAction(null)
            reload()
          }}
        />
      )}

      <FixAssistantModal
        open={fixAlert !== null}
        onClose={() => setFixAlert(null)}
        alert={fixAlert}
      />
    </div>
  )
}

function Th({
  label,
  onClick,
  active,
  dir,
}: {
  label?: string
  onClick?: () => void
  active?: boolean
  dir?: 'asc' | 'desc'
}) {
  if (!label) return <th className="w-8 px-2 py-2" />
  return (
    <th className="px-3 py-2 font-semibold">
      <button
        onClick={onClick}
        className={`inline-flex items-center gap-1 hover:text-slate-700 ${active ? 'text-slate-700' : ''}`}
      >
        {label}
        {active ? (
          <span className="text-[10px]">{dir === 'asc' ? '▲' : '▼'}</span>
        ) : (
          <ChevronsUpDown size={12} className="text-slate-300" />
        )}
      </button>
    </th>
  )
}

function FilterSelect({
  label,
  value,
  onChange,
  options,
}: {
  label: string
  value: string
  onChange: (v: string) => void
  options: string[]
}) {
  return (
    <label className="flex flex-col gap-1 text-xs text-slate-500">
      <span className="font-medium">{label}</span>
      <select
        value={value}
        onChange={(e) => onChange(e.target.value)}
        className="min-w-[120px] rounded-md border border-slate-300 bg-white px-2 py-1.5 text-xs text-slate-700"
      >
        {options.map((o) => (
          <option key={o} value={o}>
            {o === '' ? 'All' : o}
          </option>
        ))}
      </select>
    </label>
  )
}

function AlertRow({
  alert,
  expanded,
  onToggle,
  onAck,
  onResolve,
  onFix,
  onOpenAsset,
}: {
  alert: Alert
  expanded: boolean
  onToggle: () => void
  onAck: () => void
  onResolve: () => void
  onFix: () => void
  onOpenAsset: () => void
}) {
  const resolved = alert.status === 'RESOLVED'
  return (
    <>
      <tr
        className="border-b border-slate-100 align-top hover:bg-slate-50/60"
        style={{ borderLeft: `4px solid ${severityColor(alert.severity)}` }}
      >
        <td className="px-2 py-2">
          <button
            onClick={onToggle}
            aria-label={expanded ? 'Collapse row' : 'Expand history'}
            className="rounded p-1 text-slate-400 hover:bg-slate-100"
          >
            {expanded ? <ChevronDown size={16} /> : <ChevronRight size={16} />}
          </button>
        </td>
        <td className="px-3 py-2">
          <SeverityBadge severity={alert.severity} />
        </td>
        <td className="px-3 py-2">
          <AlertStatusBadge status={alert.status} />
          {alert.acknowledged_by && (
            <div className="mt-1 text-[11px] text-slate-400">
              by {alert.acknowledged_by}
              {alert.acknowledged_ts ? ` · ${fmtClock(alert.acknowledged_ts)}` : ''}
            </div>
          )}
        </td>
        <td className="px-3 py-2 text-slate-600">{alert.line_id}</td>
        <td className="px-3 py-2">
          <button onClick={onOpenAsset} className="font-medium text-sky-600 hover:underline">
            {alert.asset_id}
          </button>
        </td>
        <td className="px-3 py-2 text-slate-600">{alert.alert_type}</td>
        <td className="px-3 py-2 text-slate-600">{SENSOR_LABELS[alert.sensor_type]}</td>
        <td className="px-3 py-2 tabular-nums text-slate-600">
          <span className="font-semibold">{fmtNum(alert.observed_value, 2)}</span>
          <span className="text-slate-400"> / {fmtNum(alert.threshold_value, 2)}</span>
        </td>
        <td className="px-3 py-2 text-slate-600">{alert.recommended_action}</td>
        <td className="px-3 py-2 whitespace-nowrap text-xs text-slate-500">
          {fmtClock(alert.created_ts)}
        </td>
        <td className="px-3 py-2">
          <div className="flex justify-end gap-1.5">
            <button
              onClick={onFix}
              className="inline-flex items-center gap-1 rounded-md border border-sky-300 bg-sky-50 px-2 py-1 text-xs font-medium text-sky-700 hover:bg-sky-100"
            >
              <Sparkles size={13} /> Explain &amp; Fix
            </button>
            <button
              onClick={onAck}
              disabled={alert.status !== 'NEW'}
              className="inline-flex items-center gap-1 rounded-md border border-amber-300 bg-amber-50 px-2 py-1 text-xs font-medium text-amber-700 hover:bg-amber-100 disabled:cursor-not-allowed disabled:opacity-40"
            >
              <CheckCircle2 size={13} /> Ack
            </button>
            <button
              onClick={onResolve}
              disabled={resolved}
              className="inline-flex items-center gap-1 rounded-md border border-green-300 bg-green-50 px-2 py-1 text-xs font-medium text-green-700 hover:bg-green-100 disabled:cursor-not-allowed disabled:opacity-40"
            >
              <Wrench size={13} /> Resolve
            </button>
          </div>
        </td>
      </tr>
      {expanded && (
        <tr className="bg-slate-50/70">
          <td colSpan={11} className="px-6 py-3">
            <div className="grid grid-cols-1 gap-4 md:grid-cols-2">
              <div className="text-xs text-slate-600">
                <div className="mb-1 font-semibold text-slate-500">Root cause hint</div>
                <p>{alert.root_cause_hint}</p>
                <div className="mt-2 flex gap-4 text-[11px] text-slate-400">
                  <span>anomaly {alert.anomaly_score.toFixed(2)}</span>
                  <span>confidence {alert.confidence.toFixed(2)}</span>
                  <span>alert_id {alert.alert_id.slice(0, 12)}…</span>
                </div>
              </div>
              <AlertHistory alertId={alert.alert_id} />
            </div>
          </td>
        </tr>
      )}
    </>
  )
}

function AlertHistory({ alertId }: { alertId: string }) {
  const fetcher = useCallback(() => getAlertHistory(alertId), [alertId])
  const { data, loading, error } = usePolling(fetcher, 0, [alertId])
  return (
    <div className="text-xs text-slate-600">
      <div className="mb-1 flex items-center gap-1 font-semibold text-slate-500">
        <HistoryIcon size={13} /> History
      </div>
      {loading ? (
        <div className="flex items-center gap-2 text-slate-400">
          <Spinner /> loading…
        </div>
      ) : error ? (
        <span className="text-red-500">Failed to load history.</span>
      ) : !data || data.length === 0 ? (
        <span className="text-slate-400">No history yet.</span>
      ) : (
        <ul className="space-y-1">
          {data.map((h: AlertHistoryEntry) => (
            <li key={h.ack_id} className="flex flex-wrap gap-2">
              <span className="font-medium text-slate-700">{h.action}</span>
              <span className="text-slate-400">{fmtClock(h.created_ts)}</span>
              <span className="text-slate-500">· {h.operator}</span>
              {h.note && <span className="italic text-slate-500">“{h.note}”</span>}
            </li>
          ))}
        </ul>
      )}
    </div>
  )
}

function AlertActionModal({
  alert,
  kind,
  operator,
  onClose,
  onDone,
}: {
  alert: Alert
  kind: 'ack' | 'resolve'
  operator: string
  onClose: () => void
  onDone: () => void
}) {
  const [op, setOp] = useState(operator || 'operator')
  const [note, setNote] = useState('')
  const [maintenance, setMaintenance] = useState(false)
  const [busy, setBusy] = useState(false)
  const [err, setErr] = useState<string | null>(null)
  const [result, setResult] = useState<Alert | null>(null)

  const submit = async () => {
    setBusy(true)
    setErr(null)
    try {
      const updated =
        kind === 'ack'
          ? await acknowledgeAlert(alert.alert_id, op, note)
          : await resolveAlert(alert.alert_id, op, note, maintenance)
      setResult(updated)
    } catch (e) {
      setErr((e as Error).message)
    } finally {
      setBusy(false)
    }
  }

  return (
    <Modal
      open
      onClose={onClose}
      title={kind === 'ack' ? 'Acknowledge alert' : 'Resolve alert'}
    >
      <div className="space-y-3 text-sm">
        <div className="rounded-lg bg-slate-50 p-3 text-xs text-slate-600">
          <div className="flex items-center gap-2">
            <SeverityBadge severity={alert.severity} />
            <span className="font-medium text-slate-700">
              {alert.asset_id} · {SENSOR_LABELS[alert.sensor_type]}
            </span>
          </div>
          <div className="mt-1">{alert.recommended_action}</div>
        </div>

        {result ? (
          <div className="rounded-lg border border-green-300 bg-green-50 p-3 text-sm text-green-800">
            <div className="flex items-center gap-2 font-medium">
              <CheckCircle2 size={16} /> Alert {kind === 'ack' ? 'acknowledged' : 'resolved'}.
            </div>
            <div className="mt-1 flex items-center gap-2 text-xs">
              New status: <AlertStatusBadge status={result.status} />
              {result.acknowledged_by && <span>· by {result.acknowledged_by}</span>}
            </div>
          </div>
        ) : (
          <>
            <label className="block">
              <span className="mb-1 block text-xs font-medium text-slate-500">Operator</span>
              <input
                value={op}
                onChange={(e) => setOp(e.target.value)}
                className="w-full rounded-md border border-slate-300 px-2 py-1.5 text-sm"
              />
            </label>
            <label className="block">
              <span className="mb-1 block text-xs font-medium text-slate-500">Note</span>
              <textarea
                value={note}
                onChange={(e) => setNote(e.target.value)}
                rows={3}
                placeholder="Optional note…"
                className="w-full rounded-md border border-slate-300 px-2 py-1.5 text-sm"
              />
            </label>
            {kind === 'resolve' && (
              <label className="flex items-center gap-2 text-sm text-slate-600">
                <input
                  type="checkbox"
                  checked={maintenance}
                  onChange={(e) => setMaintenance(e.target.checked)}
                  className="h-4 w-4 rounded border-slate-300"
                />
                Mark asset for maintenance
              </label>
            )}
            {err && <div className="rounded-md bg-red-50 p-2 text-xs text-red-700">{err}</div>}
          </>
        )}
      </div>

      <div className="mt-4 flex justify-end gap-2">
        {result ? (
          <button
            onClick={onDone}
            className="rounded-md bg-sky-600 px-3 py-1.5 text-sm font-medium text-white hover:bg-sky-700"
          >
            Done
          </button>
        ) : (
          <>
            <button
              onClick={onClose}
              className="rounded-md border border-slate-300 px-3 py-1.5 text-sm font-medium text-slate-600 hover:bg-slate-50"
            >
              Cancel
            </button>
            <button
              onClick={submit}
              disabled={busy || !op.trim()}
              className="inline-flex items-center gap-2 rounded-md bg-sky-600 px-3 py-1.5 text-sm font-medium text-white hover:bg-sky-700 disabled:opacity-50"
            >
              {busy && <Spinner />}
              {kind === 'ack' ? 'Acknowledge' : 'Resolve'}
            </button>
          </>
        )}
      </div>
    </Modal>
  )
}
