import type { ReactNode } from 'react'
import { AlertTriangle, Inbox, Loader2, ServerCrash, ShieldAlert } from 'lucide-react'
import { ApiError } from '../api'

export function Card({
  children,
  className = '',
}: {
  children: ReactNode
  className?: string
}) {
  return (
    <div
      className={`rounded-xl border border-slate-200 bg-white shadow-sm ${className}`}
    >
      {children}
    </div>
  )
}

export function Skeleton({ className = '' }: { className?: string }) {
  return <div className={`skeleton rounded-md ${className}`} aria-hidden="true" />
}

export function SkeletonCard() {
  return (
    <Card className="p-4">
      <Skeleton className="h-4 w-1/3" />
      <Skeleton className="mt-3 h-8 w-2/3" />
      <Skeleton className="mt-4 h-3 w-full" />
      <Skeleton className="mt-2 h-3 w-5/6" />
    </Card>
  )
}

export function EmptyState({
  title,
  hint,
  icon,
}: {
  title: string
  hint?: string
  icon?: ReactNode
}) {
  return (
    <div className="flex flex-col items-center justify-center gap-2 rounded-xl border border-dashed border-slate-300 bg-white/60 p-10 text-center">
      <div className="text-slate-400">{icon ?? <Inbox size={28} />}</div>
      <div className="font-medium text-slate-700">{title}</div>
      {hint && <div className="max-w-md text-sm text-slate-500">{hint}</div>}
    </div>
  )
}

// Friendly error surfacing: 401/403 permission, 503 unavailable, other.
export function ErrorPanel({
  error,
  onRetry,
  context,
}: {
  error: ApiError
  onRetry?: () => void
  context?: string
}) {
  const is401 = error.status === 401 || error.status === 403
  const is503 = error.status === 503 || error.status === 0

  let icon = <AlertTriangle size={28} />
  let title = 'Something went wrong'
  let msg = error.message

  if (is401) {
    icon = <ShieldAlert size={28} />
    title = 'Permission denied'
    msg =
      'You do not have access to this resource (HTTP ' +
      error.status +
      '). Check your Databricks App permissions.'
  } else if (is503) {
    icon = <ServerCrash size={28} />
    title = 'Backend / Lakebase unavailable'
    msg =
      'The service or its Lakebase database is not reachable right now. Data will resume when the backend recovers.'
  }

  return (
    <div className="flex flex-col items-center justify-center gap-3 rounded-xl border border-red-200 bg-red-50 p-8 text-center">
      <div className="text-red-500">{icon}</div>
      <div className="font-semibold text-red-800">{title}</div>
      {context && <div className="text-xs uppercase tracking-wide text-red-400">{context}</div>}
      <div className="max-w-lg text-sm text-red-700">{msg}</div>
      {onRetry && (
        <button
          onClick={onRetry}
          className="mt-1 rounded-md border border-red-300 bg-white px-3 py-1.5 text-sm font-medium text-red-700 hover:bg-red-100"
        >
          Retry
        </button>
      )}
    </div>
  )
}

export function Spinner({ className = '' }: { className?: string }) {
  return <Loader2 className={`animate-spin ${className}`} size={16} />
}
