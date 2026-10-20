import { useCallback, useEffect, useState } from 'react'
import { AlertTriangle, FlaskConical, Play, Square } from 'lucide-react'
import { ApiError, getDemoStatus, setScenario, startDemo, stopDemo } from '../api'
import type { DemoStatus } from '../types'
import { usePolling } from '../hooks/usePolling'
import { ViewHeader } from '../components/ViewHeader'
import { PollStatus } from '../components/PollStatus'
import { Card, ErrorPanel, Spinner } from '../components/Primitives'
import { Modal } from '../components/Modal'
import { fmtAge, fmtInt } from '../lib/format'

const HIGH_IMPACT = new Set(['alarm_burst', 'line_slowdown'])

const SCENARIO_LABELS: Record<string, string> = {
  normal: 'Normal operation',
  temp_excursion: 'Temperature excursion',
  vibration_degradation: 'Vibration degradation',
  pressure_instability: 'Pressure instability',
  line_slowdown: 'Line slowdown (high impact)',
  alarm_burst: 'Alarm burst (high impact)',
  recovery: 'Recovery',
}

export function ScenarioControls({
  interval,
  operator,
}: {
  interval: number
  operator: string
}) {
  const fetcher = useCallback(() => getDemoStatus(), [])
  const { data, error, loading, refreshing, lastUpdated, latencyMs, reload } = usePolling(
    fetcher,
    interval,
  )

  const [scenario, setScenarioSel] = useState<string>('normal')
  const [rate, setRate] = useState(1.0)
  const [anomalySecs, setAnomalySecs] = useState(60)
  const [busy, setBusy] = useState<string | null>(null)
  const [actionErr, setActionErr] = useState<string | null>(null)
  const [confirmScenario, setConfirmScenario] = useState<string | null>(null)
  const [dirty, setDirty] = useState(false)

  // Seed local controls from server state until the user edits them.
  useEffect(() => {
    if (data && !dirty) {
      setScenarioSel(data.scenario)
      setRate(data.rate)
      setAnomalySecs(data.anomaly_secs)
    }
  }, [data, dirty])

  const applyScenario = async (confirm: boolean) => {
    setBusy('scenario')
    setActionErr(null)
    try {
      // NOTE: deliberately do NOT send `seed`. Echoing the current seed back here
      // pins it, which makes every restart re-emit identical source_event_ids that
      // silver dedup drops — freezing Plant Overview / Alert Console. Leaving seed
      // unset lets the backend rotate it per start (its intended anti-dedup design).
      await setScenario({
        scenario,
        rate,
        anomaly_secs: anomalySecs,
        confirm_high_impact: confirm,
        operator: operator || 'operator',
      })
      setConfirmScenario(null)
      setDirty(false)
      reload()
    } catch (e) {
      if (
        e instanceof ApiError &&
        e.status === 409 &&
        typeof e.body === 'object' &&
        e.body !== null &&
        (e.body as { detail?: { error?: string } }).detail?.error === 'confirmation_required'
      ) {
        setConfirmScenario(scenario)
      } else {
        setActionErr((e as Error).message)
      }
    } finally {
      setBusy(null)
    }
  }

  const onApply = () => {
    if (HIGH_IMPACT.has(scenario)) setConfirmScenario(scenario)
    else applyScenario(false)
  }

  const control = async (kind: 'start' | 'stop') => {
    setBusy(kind)
    setActionErr(null)
    try {
      if (kind === 'start') await startDemo(operator || 'operator')
      else await stopDemo(operator || 'operator')
      reload()
    } catch (e) {
      setActionErr((e as Error).message)
    } finally {
      setBusy(null)
    }
  }

  const s: DemoStatus | null = data
  const scenarios = s?.valid_scenarios ?? Object.keys(SCENARIO_LABELS)

  return (
    <div>
      <ViewHeader
        title="Scenario Controls"
        status={
          <PollStatus
            lastUpdated={lastUpdated}
            refreshing={refreshing}
            latencyMs={latencyMs}
            dataTs={s?.last_ingest_ts}
            onReload={reload}
          />
        }
      />

      <div className="mb-4 flex items-start gap-3 rounded-xl border border-amber-300 bg-amber-50 p-3 text-sm text-amber-800">
        <FlaskConical size={18} className="mt-0.5 shrink-0" />
        <p>
          <strong>All generated data is synthetic.</strong> These controls drive a synthetic
          telemetry generator feeding the demo pipeline — they do not affect any real equipment.
        </p>
      </div>

      {loading ? (
        <div className="skeleton h-64 w-full rounded-xl" />
      ) : error ? (
        <ErrorPanel error={error} onRetry={reload} context="demo status" />
      ) : s ? (
        <div className="grid grid-cols-1 gap-4 lg:grid-cols-3">
          {/* Status panel */}
          <Card className="p-4">
            <h3 className="mb-3 text-sm font-semibold text-slate-700">Generator status</h3>
            <div className="flex items-center gap-2">
              <span
                className={`inline-flex h-2.5 w-2.5 rounded-full ${s.running ? 'bg-green-500' : 'bg-slate-300'}`}
              />
              <span className="font-medium text-slate-800">
                {s.running ? 'Running' : 'Stopped'}
              </span>
              <span className="ml-auto text-xs text-slate-400">seed {s.seed}</span>
            </div>

            <dl className="mt-4 space-y-2 text-sm">
              <Row label="Active scenario" value={SCENARIO_LABELS[s.scenario] ?? s.scenario} />
              <Row label="Rate" value={`${s.rate}× events`} />
              <Row label="Anomaly duration" value={`${s.anomaly_secs}s`} />
              <Row label="Total events" value={fmtInt(s.total_events)} />
              <Row label="Last ingest" value={fmtAge(s.last_ingest_ts)} />
              <Row
                label="Ingest available"
                value={
                  <span className={s.ingest_available ? 'text-green-600' : 'text-red-600'}>
                    {s.ingest_available ? 'Yes' : 'No'} · {s.reason}
                  </span>
                }
              />
              <Row
                label="Generator"
                value={
                  <span className={s.generator_ok ? 'text-green-600' : 'text-red-600'}>
                    {s.generator_ok ? 'OK' : 'Error'}
                  </span>
                }
              />
            </dl>

            {s.last_error && (
              <div className="mt-3 rounded-md bg-red-50 p-2 text-xs text-red-700">
                Last error: {s.last_error}
              </div>
            )}

            <div className="mt-4 flex gap-2">
              <button
                onClick={() => control('start')}
                disabled={s.running || busy !== null}
                className="inline-flex flex-1 items-center justify-center gap-2 rounded-md bg-green-600 px-3 py-2 text-sm font-medium text-white hover:bg-green-700 disabled:opacity-40"
              >
                {busy === 'start' ? <Spinner /> : <Play size={15} />} Start
              </button>
              <button
                onClick={() => control('stop')}
                disabled={!s.running || busy !== null}
                className="inline-flex flex-1 items-center justify-center gap-2 rounded-md bg-slate-700 px-3 py-2 text-sm font-medium text-white hover:bg-slate-800 disabled:opacity-40"
              >
                {busy === 'stop' ? <Spinner /> : <Square size={15} />} Stop
              </button>
            </div>
          </Card>

          {/* Scenario configuration */}
          <Card className="p-4 lg:col-span-2">
            <h3 className="mb-3 text-sm font-semibold text-slate-700">Configure scenario</h3>

            <div className="grid grid-cols-1 gap-4 sm:grid-cols-2">
              {scenarios.map((sc) => {
                const active = scenario === sc
                const high = HIGH_IMPACT.has(sc)
                return (
                  <button
                    key={sc}
                    onClick={() => {
                      setScenarioSel(sc)
                      setDirty(true)
                    }}
                    className={`rounded-lg border p-3 text-left transition ${
                      active
                        ? 'border-sky-500 bg-sky-50 ring-1 ring-sky-500'
                        : 'border-slate-200 hover:border-slate-300'
                    }`}
                  >
                    <div className="flex items-center justify-between">
                      <span className="text-sm font-medium text-slate-800">
                        {SCENARIO_LABELS[sc] ?? sc}
                      </span>
                      {high && (
                        <span className="inline-flex items-center gap-1 rounded-full bg-red-100 px-1.5 py-0.5 text-[10px] font-semibold text-red-700">
                          <AlertTriangle size={10} /> HIGH
                        </span>
                      )}
                    </div>
                    <div className="mt-0.5 font-mono text-[11px] text-slate-400">{sc}</div>
                  </button>
                )
              })}
            </div>

            <div className="mt-5 grid grid-cols-1 gap-5 sm:grid-cols-2">
              <div>
                <label htmlFor="rate" className="mb-1 flex items-baseline justify-between text-xs font-medium text-slate-500">
                  <span>Event rate</span>
                  <span className="tabular-nums text-slate-700">{rate.toFixed(1)}×</span>
                </label>
                <input
                  id="rate"
                  type="range"
                  min={0.1}
                  max={20}
                  step={0.1}
                  value={rate}
                  onChange={(e) => {
                    setRate(Number(e.target.value))
                    setDirty(true)
                  }}
                  className="w-full accent-sky-600"
                />
                <div className="flex justify-between text-[10px] text-slate-400">
                  <span>0.1×</span>
                  <span>20×</span>
                </div>
              </div>

              <div>
                <label htmlFor="anomaly" className="mb-1 block text-xs font-medium text-slate-500">
                  Anomaly duration (seconds)
                </label>
                <input
                  id="anomaly"
                  type="number"
                  min={1}
                  max={3600}
                  value={anomalySecs}
                  onChange={(e) => {
                    setAnomalySecs(Number(e.target.value))
                    setDirty(true)
                  }}
                  className="w-full rounded-md border border-slate-300 px-2 py-1.5 text-sm"
                />
              </div>
            </div>

            {actionErr && (
              <div className="mt-3 rounded-md bg-red-50 p-2 text-xs text-red-700">{actionErr}</div>
            )}

            <div className="mt-4 flex items-center gap-3">
              <button
                onClick={onApply}
                disabled={busy !== null}
                className="inline-flex items-center gap-2 rounded-md bg-sky-600 px-4 py-2 text-sm font-medium text-white hover:bg-sky-700 disabled:opacity-50"
              >
                {busy === 'scenario' && <Spinner />} Apply scenario
              </button>
              {HIGH_IMPACT.has(scenario) && (
                <span className="inline-flex items-center gap-1 text-xs text-red-600">
                  <AlertTriangle size={13} /> High-impact — requires confirmation
                </span>
              )}
            </div>
          </Card>
        </div>
      ) : null}

      {/* High-impact confirmation */}
      <Modal
        open={confirmScenario !== null}
        onClose={() => setConfirmScenario(null)}
        title="Confirm high-impact scenario"
      >
        <div className="space-y-3 text-sm text-slate-600">
          <div className="flex items-start gap-2 rounded-lg border border-red-300 bg-red-50 p-3 text-red-800">
            <AlertTriangle size={18} className="mt-0.5 shrink-0" />
            <p>
              <strong>{SCENARIO_LABELS[confirmScenario ?? ''] ?? confirmScenario}</strong> is a
              high-volume / multi-line scenario. It will generate a burst of synthetic alerts across
              the plant. Continue?
            </p>
          </div>
        </div>
        <div className="mt-4 flex justify-end gap-2">
          <button
            onClick={() => setConfirmScenario(null)}
            className="rounded-md border border-slate-300 px-3 py-1.5 text-sm font-medium text-slate-600 hover:bg-slate-50"
          >
            Cancel
          </button>
          <button
            onClick={() => applyScenario(true)}
            disabled={busy !== null}
            className="inline-flex items-center gap-2 rounded-md bg-red-600 px-3 py-1.5 text-sm font-medium text-white hover:bg-red-700 disabled:opacity-50"
          >
            {busy === 'scenario' && <Spinner />} Confirm &amp; apply
          </button>
        </div>
      </Modal>
    </div>
  )
}

function Row({ label, value }: { label: string; value: React.ReactNode }) {
  return (
    <div className="flex items-baseline justify-between gap-2">
      <dt className="text-xs uppercase tracking-wide text-slate-400">{label}</dt>
      <dd className="text-right font-medium text-slate-700">{value}</dd>
    </div>
  )
}
