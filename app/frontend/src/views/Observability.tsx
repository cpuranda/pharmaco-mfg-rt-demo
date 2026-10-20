import { useCallback } from 'react'
import {
  Activity,
  AppWindow,
  ArrowRight,
  BookOpen,
  Boxes,
  Cpu,
  Database,
  GitBranch,
  Layers,
  Network,
  Radio,
  Search,
  Sparkles,
  Wrench,
} from 'lucide-react'
import { getObservability } from '../api'
import type { AgenticStatus, ArchitectureStage } from '../types'
import { usePolling } from '../hooks/usePolling'
import { useNow } from '../hooks/useNow'
import { ViewHeader } from '../components/ViewHeader'
import { PollStatus } from '../components/PollStatus'
import { Card, ErrorPanel, EmptyState } from '../components/Primitives'
import { StatTile } from '../components/Widgets'
import { fmtAge, fmtInt, fmtMs } from '../lib/format'

const STAGE_ICON: Record<string, typeof Activity> = {
  Generator: Cpu,
  'Zerobus Ingest': Radio,
  'Structured Streaming': GitBranch,
  'Gold Delta': Layers,
  Lakebase: Database,
  'Databricks App': AppWindow,
  'SOP Knowledge': BookOpen,
  'Vector Search': Search,
  'Foundation Model': Sparkles,
  'Fix Assistant': Wrench,
  Supervisor: Network,
}

function FlowStrip({ stages, tone = 'sky' }: { stages: ArchitectureStage[]; tone?: 'sky' | 'violet' }) {
  const chip = tone === 'violet' ? 'bg-violet-100 text-violet-700' : 'bg-sky-100 text-sky-700'
  return (
    <div className="scroll-x pb-2">
      <div className="flex min-w-max items-stretch gap-2">
        {stages.map((stage, i) => {
          const Icon = STAGE_ICON[stage.stage] ?? Boxes
          return (
            <div key={stage.stage} className="flex items-stretch gap-2">
              <div className="flex w-52 flex-col rounded-lg border border-slate-200 bg-gradient-to-b from-white to-slate-50 p-3">
                <div className="flex items-center gap-2">
                  <div className={`flex h-7 w-7 items-center justify-center rounded-md ${chip}`}>
                    <Icon size={16} />
                  </div>
                  <span className="text-sm font-semibold text-slate-800">{stage.stage}</span>
                </div>
                <p className="mt-2 text-[11px] leading-snug text-slate-500">{stage.detail}</p>
              </div>
              {i < stages.length - 1 && (
                <div className="flex items-center text-slate-300">
                  <ArrowRight size={18} />
                </div>
              )}
            </div>
          )
        })}
      </div>
    </div>
  )
}

export function Observability({ interval }: { interval: number }) {
  const now = useNow()
  const fetcher = useCallback(() => getObservability(), [])
  const { data, error, loading, refreshing, lastUpdated, latencyMs, reload } = usePolling(
    fetcher,
    interval,
  )

  return (
    <div>
      <ViewHeader
        title="Architecture & Observability"
        status={
          <PollStatus
            lastUpdated={lastUpdated}
            refreshing={refreshing}
            latencyMs={latencyMs}
            dataTs={data?.server_ts}
            onReload={reload}
          />
        }
      />

      {loading ? (
        <div className="space-y-4">
          <div className="skeleton h-28 w-full rounded-xl" />
          <div className="skeleton h-40 w-full rounded-xl" />
        </div>
      ) : error ? (
        <ErrorPanel error={error} onRetry={reload} context="observability" />
      ) : !data ? (
        <EmptyState title="No metrics" />
      ) : (
        <div className="space-y-5">
          {/* Architecture flow — data pipeline */}
          <Card className="p-4">
            <h3 className="mb-3 text-sm font-semibold text-slate-700">Data pipeline</h3>
            <FlowStrip stages={data.architecture} tone="sky" />
          </Card>

          {/* Architecture flow — knowledge & agents (Agent Bricks-style) */}
          {data.agentic_architecture?.length > 0 && (
            <Card className="p-4">
              <div className="mb-3 flex items-center gap-2">
                <Sparkles size={16} className="text-violet-600" />
                <h3 className="text-sm font-semibold text-slate-700">Knowledge &amp; agents</h3>
                <span className="rounded-full bg-violet-100 px-2 py-0.5 text-[11px] font-medium text-violet-700">
                  {data.agentic?.managed_supervisor ? 'managed Agent Bricks supervisor' : 'built-in supervisor'}
                </span>
              </div>
              <FlowStrip stages={data.agentic_architecture} tone="violet" />
              {data.agentic && <AgenticStatusRow a={data.agentic} />}
            </Card>
          )}

          {/* Record counts */}
          <div>
            <h3 className="mb-2 text-sm font-semibold uppercase tracking-wide text-slate-500">
              Delta record counts
            </h3>
            <div className="grid grid-cols-2 gap-3 sm:grid-cols-4">
              <StatTile label="Bronze rows" value={fmtInt(data.delta.bronze_rows)} tone="sky" />
              <StatTile label="Silver rows" value={fmtInt(data.delta.silver_rows)} tone="sky" />
              <StatTile
                label="Quarantine rows"
                value={fmtInt(data.delta.quarantine_rows)}
                tone={Number(data.delta.quarantine_rows) > 0 ? 'amber' : 'slate'}
              />
              <StatTile
                label="Anomaly events"
                value={fmtInt(data.delta.anomaly_events)}
                tone={Number(data.delta.anomaly_events) > 0 ? 'red' : 'slate'}
              />
            </div>
            <div className="mt-1 text-[11px] text-slate-400">
              Last bronze {fmtAge(data.delta.last_bronze_ts, now)} · last silver{' '}
              {fmtAge(data.delta.last_silver_ts, now)}
            </div>
          </div>

          {/* Latencies — kept separate */}
          <div>
            <h3 className="mb-2 text-sm font-semibold uppercase tracking-wide text-slate-500">
              Latency (measured separately)
            </h3>
            <div className="grid grid-cols-1 gap-3 sm:grid-cols-3">
              <StatTile
                label="Ingest events"
                value={fmtInt(data.ingest.total_events)}
                tone="slate"
                sub={`last ingest ${fmtAge(data.ingest.last_ingest_ts, now)}`}
              />
              <StatTile
                label="Pipeline / query latency"
                value={fmtMs(data.delta.query_latency_ms)}
                tone="slate"
                sub="Delta gold read (batch)"
              />
              <StatTile
                label="Lakebase serving latency"
                value={fmtMs(data.lakebase.serving_read_latency_ms)}
                tone="sky"
                sub={`${data.lakebase.assets} assets · sync ${fmtAge(data.lakebase.last_sync_ts, now)}`}
              />
            </div>
          </div>

          {/* Streaming job + data quality */}
          <div className="grid grid-cols-1 gap-4 md:grid-cols-2">
            <Card className="p-4">
              <h3 className="mb-3 text-sm font-semibold text-slate-700">Structured Streaming job</h3>
              <StreamingJob job={data.streaming_job} />
            </Card>

            <Card className="p-4">
              <h3 className="mb-3 text-sm font-semibold text-slate-700">Data quality (last 5 min)</h3>
              <dl className="space-y-2 text-sm">
                <QRow label="Events observed" value={fmtInt(data.quality_5m.n)} />
                <QRow
                  label="Avg ingest latency"
                  value={
                    data.quality_5m.avg_ingest_latency_ms != null
                      ? fmtMs(Number(data.quality_5m.avg_ingest_latency_ms))
                      : '—'
                  }
                />
                <QRow
                  label="Out-of-band readings"
                  value={data.quality_5m.oob != null ? fmtInt(data.quality_5m.oob) : '—'}
                  tone={Number(data.quality_5m.oob) > 0 ? 'amber' : undefined}
                />
              </dl>
              <div className="mt-3 flex items-center gap-2 rounded-md bg-slate-50 p-2 text-xs text-slate-500">
                <Database size={13} />
                Lakebase{' '}
                <span className={data.lakebase.available ? 'text-green-600' : 'text-red-600'}>
                  {data.lakebase.available ? 'available' : 'unavailable'}
                </span>
              </div>
            </Card>
          </div>
        </div>
      )}
    </div>
  )
}

function AgenticStatusRow({ a }: { a: AgenticStatus }) {
  const vs = a.vector_search || {}
  const vsReady = vs.ready === true && !vs.error
  return (
    <div className="mt-3 grid grid-cols-2 gap-3 border-t border-slate-100 pt-3 sm:grid-cols-4">
      <MiniStat
        label="SOP corpus"
        value={a.sop_count != null ? `${a.sop_count} SOPs` : '—'}
        sub={a.sop_chunks != null ? `${a.sop_chunks} chunks indexed` : 'knowledge base'}
      />
      <MiniStat
        label="Vector Search"
        value={vs.error ? 'error' : vsReady ? 'online' : (vs.state ?? 'provisioning')}
        sub={vs.indexed_rows != null ? `${vs.indexed_rows} rows` : 'Delta-Sync index'}
        tone={vs.error ? 'red' : vsReady ? 'green' : 'amber'}
      />
      <MiniStat label="Foundation Model" value={shortEndpoint(a.llm_endpoint)} sub="grounded + cited" />
      <MiniStat
        label="Supervisor agents"
        value="Genie + SOP"
        sub={a.genie_configured ? 'data + knowledge' : 'knowledge only (no Genie)'}
        tone={a.genie_configured ? undefined : 'amber'}
      />
    </div>
  )
}

function shortEndpoint(name: string) {
  return (name || '').replace(/^databricks-/, '')
}

function MiniStat({
  label,
  value,
  sub,
  tone,
}: {
  label: string
  value: React.ReactNode
  sub?: string
  tone?: 'green' | 'amber' | 'red'
}) {
  const color =
    tone === 'green' ? 'text-green-600' : tone === 'amber' ? 'text-amber-600' : tone === 'red' ? 'text-red-600' : 'text-slate-800'
  return (
    <div className="rounded-lg border border-slate-200 bg-white p-2.5">
      <div className="text-[10px] uppercase tracking-wide text-slate-400">{label}</div>
      <div className={`mt-0.5 truncate text-sm font-semibold ${color}`}>{value}</div>
      {sub && <div className="truncate text-[11px] text-slate-400">{sub}</div>}
    </div>
  )
}

function StreamingJob({ job }: { job: import('../types').StreamingJob }) {
  if (!job.configured) {
    return <div className="text-sm text-slate-500">No streaming job configured.</div>
  }
  const lifecycle = job.life_cycle_state
  const active = job.active ?? (lifecycle === 'RUNNING')
  const hasError = Boolean(job.error)
  return (
    <div className="space-y-2 text-sm">
      <div className="flex items-center gap-2">
        <span
          className={`inline-flex h-2.5 w-2.5 rounded-full ${
            hasError ? 'bg-red-500' : active ? 'bg-green-500' : 'bg-slate-300'
          }`}
        />
        <span className="font-medium text-slate-800">
          {hasError ? 'Unreachable' : lifecycle ?? (active ? 'RUNNING' : 'Configured')}
        </span>
        {job.result_state && (
          <span className="text-xs text-slate-400">· {job.result_state}</span>
        )}
      </div>
      {job.job_id && (
        <div className="font-mono text-[11px] text-slate-400">job_id {job.job_id}</div>
      )}
      {hasError && (
        <div className="rounded-md bg-red-50 p-2 text-xs text-red-700">{job.error}</div>
      )}
    </div>
  )
}

function QRow({
  label,
  value,
  tone,
}: {
  label: string
  value: React.ReactNode
  tone?: 'amber'
}) {
  return (
    <div className="flex items-baseline justify-between gap-2">
      <dt className="text-xs uppercase tracking-wide text-slate-400">{label}</dt>
      <dd className={`font-medium tabular-nums ${tone === 'amber' ? 'text-amber-600' : 'text-slate-700'}`}>
        {value}
      </dd>
    </div>
  )
}
