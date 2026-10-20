import { useCallback, useEffect, useRef, useState } from 'react'
import {
  AlertTriangle,
  BookOpen,
  ChevronDown,
  ChevronRight,
  Code2,
  Database,
  FlaskConical,
  Plus,
  Send,
  Sparkles,
} from 'lucide-react'
import { assistantAsk, getAssistantStatus } from '../api'
import type { GenieResult, SupervisorAnswer } from '../types'
import { usePolling } from '../hooks/usePolling'
import { ViewHeader } from '../components/ViewHeader'
import { Card, Spinner } from '../components/Primitives'

interface Turn {
  id: string
  question: string
  answer: SupervisorAnswer | null
  error?: string
}

export function AskAssistant() {
  const statusFetch = useCallback(() => getAssistantStatus(), [])
  const status = usePolling(statusFetch, 0)

  const [turns, setTurns] = useState<Turn[]>([])
  const [input, setInput] = useState('')
  const [busy, setBusy] = useState(false)

  const scrollRef = useRef<HTMLDivElement>(null)
  useEffect(() => {
    scrollRef.current?.scrollTo({ top: scrollRef.current.scrollHeight, behavior: 'smooth' })
  }, [turns])

  const ask = async (question: string) => {
    const q = question.trim()
    if (!q || busy) return
    setInput('')
    const id = `${Date.now()}-${Math.random().toString(36).slice(2, 7)}`
    setTurns((t) => [...t, { id, question: q, answer: null }])
    setBusy(true)
    try {
      const res = await assistantAsk(q)
      setTurns((t) => t.map((turn) => (turn.id === id ? { ...turn, answer: res } : turn)))
    } catch (e) {
      setTurns((t) =>
        t.map((turn) => (turn.id === id ? { ...turn, error: (e as Error).message } : turn)),
      )
    } finally {
      setBusy(false)
    }
  }

  const onKeyDown = (e: React.KeyboardEvent<HTMLTextAreaElement>) => {
    if (e.key === 'Enter' && !e.shiftKey) {
      e.preventDefault()
      ask(input)
    }
  }

  const suggestions = status.data?.suggestions ?? []
  const managed = status.data?.managed_supervisor

  return (
    <div className="flex h-[calc(100vh-8rem)] flex-col">
      <ViewHeader
        title={
          <span className="flex items-center gap-2">
            <Sparkles size={20} className="text-sky-600" /> Ask the Assistant
          </span>
        }
        actions={
          turns.length > 0 ? (
            <button
              onClick={() => setTurns([])}
              className="inline-flex items-center gap-1.5 rounded-md border border-slate-300 bg-white px-2.5 py-1 text-xs font-medium text-slate-600 hover:bg-slate-50"
            >
              <Plus size={14} /> New conversation
            </button>
          ) : undefined
        }
      />
      <p className="-mt-2 mb-3 flex items-center gap-2 text-sm text-slate-500">
        A supervisor routes your question across the <strong>Genie</strong> data agent and the{' '}
        <strong>SOP knowledge base</strong>, then answers with citations.
        <span className="rounded-full bg-slate-100 px-2 py-0.5 text-[11px] text-slate-500">
          {managed ? 'managed Agent Bricks supervisor' : 'built-in supervisor'}
        </span>
      </p>

      <Card className="flex min-h-0 flex-1 flex-col overflow-hidden">
        <div ref={scrollRef} className="scroll-y min-h-0 flex-1 space-y-4 overflow-y-auto p-4">
          {turns.length === 0 ? (
            <div className="flex h-full flex-col items-center justify-center gap-4 text-center">
              <div className="flex h-12 w-12 items-center justify-center rounded-full bg-sky-100 text-sky-600">
                <Sparkles size={24} />
              </div>
              <div>
                <div className="font-medium text-slate-700">Ask about the plant — data + how to fix it</div>
                <div className="mt-1 text-sm text-slate-500">
                  The assistant blends live data and SOP guidance. Try one of these:
                </div>
              </div>
              <div className="flex max-w-2xl flex-wrap justify-center gap-2">
                {suggestions.map((s) => (
                  <button
                    key={s}
                    onClick={() => ask(s)}
                    disabled={busy}
                    className="rounded-full border border-slate-300 bg-white px-3 py-1.5 text-sm text-slate-600 transition hover:border-sky-400 hover:bg-sky-50 hover:text-sky-700 disabled:opacity-50"
                  >
                    {s}
                  </button>
                ))}
              </div>
            </div>
          ) : (
            turns.map((turn) => <TurnView key={turn.id} turn={turn} />)
          )}
          {busy && <ThinkingBubble />}
        </div>

        <div className="border-t border-slate-200 p-3">
          <div className="flex items-end gap-2">
            <textarea
              value={input}
              onChange={(e) => setInput(e.target.value)}
              onKeyDown={onKeyDown}
              rows={1}
              placeholder="Ask about line health, an alert, or how to fix an issue…  (Enter to send)"
              aria-label="Ask the assistant a question"
              disabled={busy}
              className="max-h-32 min-h-[2.5rem] flex-1 resize-none rounded-lg border border-slate-300 px-3 py-2 text-sm text-slate-700 focus:border-sky-500 focus:outline-none focus:ring-1 focus:ring-sky-500 disabled:bg-slate-50"
            />
            <button
              onClick={() => ask(input)}
              disabled={busy || !input.trim()}
              className="inline-flex h-10 items-center gap-2 rounded-lg bg-sky-600 px-4 text-sm font-medium text-white hover:bg-sky-700 disabled:opacity-40"
            >
              {busy ? <Spinner /> : <Send size={15} />} Send
            </button>
          </div>
          <div className="mt-1.5 flex items-center gap-1 text-[11px] text-amber-600">
            <FlaskConical size={11} /> Answers combine synthetic demo data and synthetic SOPs.
          </div>
        </div>
      </Card>
    </div>
  )
}

function TurnView({ turn }: { turn: Turn }) {
  const isError = turn.error || turn.answer?.status === 'LLM_ERROR'
  const ans = turn.answer
  return (
    <div className="space-y-3">
      <div className="flex justify-end">
        <div className="max-w-[80%] rounded-2xl rounded-br-sm bg-sky-600 px-4 py-2 text-sm text-white">
          {turn.question}
        </div>
      </div>

      {ans || turn.error ? (
        <div className="flex justify-start">
          <div
            className={`max-w-[88%] rounded-2xl rounded-bl-sm border px-4 py-3 text-sm ${
              isError ? 'border-red-200 bg-red-50 text-red-800' : 'border-slate-200 bg-slate-50 text-slate-700'
            }`}
          >
            <div className="mb-1.5 flex flex-wrap items-center gap-2 text-xs font-semibold uppercase tracking-wide">
              {isError ? (
                <span className="flex items-center gap-1 text-red-600">
                  <AlertTriangle size={12} /> Assistant
                </span>
              ) : (
                <span className="flex items-center gap-1 text-sky-600">
                  <Sparkles size={12} /> Assistant
                </span>
              )}
              {ans && !isError && <RouteBadges ans={ans} />}
            </div>

            <SimpleMarkdown text={turn.error ?? ans?.answer ?? ''} />

            {ans && !isError && ans.sources?.data && (
              <DataDetails data={ans.sources.data} />
            )}
            {ans && !isError && ans.sources?.sops && ans.sources.sops.length > 0 && (
              <Citations sops={ans.sources.sops} />
            )}
          </div>
        </div>
      ) : null}
    </div>
  )
}

function RouteBadges({ ans }: { ans: SupervisorAnswer }) {
  const tools = ans.tools_used ?? []
  // managed supervisor reports tool calls; code supervisor reports sources
  const usedData = tools.includes('genie_data') || !!ans.sources?.data
  const usedSops = tools.includes('sop_ka') || (ans.sources?.sops?.length ?? 0) > 0
  return (
    <span className="flex items-center gap-1.5 normal-case">
      {usedData && (
        <span className="inline-flex items-center gap-1 rounded-full bg-indigo-100 px-1.5 py-0.5 text-[10px] font-medium text-indigo-700">
          <Database size={10} /> Genie data
        </span>
      )}
      {usedSops && (
        <span className="inline-flex items-center gap-1 rounded-full bg-emerald-100 px-1.5 py-0.5 text-[10px] font-medium text-emerald-700">
          <BookOpen size={10} /> SOP knowledge
        </span>
      )}
    </span>
  )
}

function DataDetails({
  data,
}: {
  data: NonNullable<NonNullable<SupervisorAnswer['sources']>['data']>
}) {
  const [open, setOpen] = useState(false)
  if (!data.sql && !data.result) return null
  return (
    <div className="mt-3">
      <button
        onClick={() => setOpen((o) => !o)}
        className="inline-flex items-center gap-1 text-xs font-medium text-slate-500 hover:text-slate-700"
      >
        {open ? <ChevronDown size={13} /> : <ChevronRight size={13} />}
        <Code2 size={13} /> {open ? 'Hide data agent detail' : 'Show data agent detail'}
      </button>
      {open && (
        <>
          {data.sql && (
            <div className="scroll-x mt-2 rounded-lg border border-slate-200 bg-slate-900 p-3">
              <pre className="min-w-max font-mono text-[12px] leading-relaxed text-slate-100">
                {data.sql}
              </pre>
            </div>
          )}
          {data.result && <ResultTable result={data.result} />}
        </>
      )}
    </div>
  )
}

function Citations({ sops }: { sops: NonNullable<NonNullable<SupervisorAnswer['sources']>['sops']> }) {
  return (
    <div className="mt-3">
      <div className="mb-1 flex items-center gap-1 text-[11px] font-medium uppercase tracking-wide text-slate-400">
        <BookOpen size={12} /> SOP sources ({sops.length})
      </div>
      <ul className="space-y-1">
        {sops.map((c) => (
          <li key={c.sop_id + c.section} className="flex items-center gap-2 text-xs text-slate-600">
            <span className="rounded bg-slate-100 px-1.5 py-0.5 font-mono text-[11px] text-slate-700">
              {c.sop_id}
            </span>
            <span className="font-medium text-slate-700">{c.title}</span>
            <span className="text-slate-400">· {c.section}</span>
          </li>
        ))}
      </ul>
    </div>
  )
}

function ResultTable({ result }: { result: GenieResult }) {
  if (!result.columns?.length) return null
  return (
    <div className="mt-3">
      <div className="mb-1 text-[11px] font-medium uppercase tracking-wide text-slate-400">
        Result · {result.rows.length} row{result.rows.length === 1 ? '' : 's'}
      </div>
      <div className="scroll-y scroll-x max-h-64 overflow-auto rounded-lg border border-slate-200">
        <table className="min-w-max text-xs">
          <thead className="sticky top-0 bg-slate-100">
            <tr className="text-left text-slate-500">
              {result.columns.map((c) => (
                <th key={c} className="whitespace-nowrap px-3 py-1.5 font-semibold">
                  {c}
                </th>
              ))}
            </tr>
          </thead>
          <tbody>
            {result.rows.map((row, ri) => (
              <tr key={ri} className="border-t border-slate-100 odd:bg-white even:bg-slate-50/50">
                {row.map((cell, ci) => (
                  <td key={ci} className="whitespace-nowrap px-3 py-1.5 tabular-nums text-slate-700">
                    {cell}
                  </td>
                ))}
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  )
}

function ThinkingBubble() {
  return (
    <div className="flex justify-start">
      <div className="flex items-center gap-2 rounded-2xl rounded-bl-sm border border-slate-200 bg-slate-50 px-4 py-3 text-sm text-slate-500">
        <span className="flex gap-1">
          <Dot delay="0ms" />
          <Dot delay="150ms" />
          <Dot delay="300ms" />
        </span>
        Routing to data + knowledge agents…
      </div>
    </div>
  )
}

function Dot({ delay }: { delay: string }) {
  return (
    <span
      className="inline-block h-2 w-2 animate-bounce rounded-full bg-sky-400"
      style={{ animationDelay: delay }}
    />
  )
}

function SimpleMarkdown({ text }: { text: string }) {
  return (
    <div className="space-y-1 whitespace-pre-wrap break-words leading-relaxed">
      {text.split('\n').map((line, i) => (
        <p key={i}>{renderInline(line)}</p>
      ))}
    </div>
  )
}

function renderInline(line: string) {
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
        <span key={i} className="mx-0.5 rounded bg-sky-100 px-1 py-0.5 font-mono text-[11px] text-sky-700">
          {part.slice(1, -1)}
        </span>
      )
    }
    return <span key={i}>{part}</span>
  })
}
