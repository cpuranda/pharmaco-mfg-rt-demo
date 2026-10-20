import { ChevronRight } from 'lucide-react'
import type { ReactNode } from 'react'
import type { Navigate, Route } from '../App'

export interface Crumb {
  label: string
  route?: Route
}

export function ViewHeader({
  title,
  crumbs,
  navigate,
  status,
  actions,
}: {
  title: ReactNode
  crumbs?: Crumb[]
  navigate?: Navigate
  status?: ReactNode
  actions?: ReactNode
}) {
  return (
    <div className="mb-4">
      {crumbs && crumbs.length > 0 && navigate && (
        <nav aria-label="Breadcrumb" className="mb-1 flex items-center gap-1 text-xs text-slate-400">
          {crumbs.map((c, i) => (
            <span key={i} className="flex items-center gap-1">
              {c.route ? (
                <button
                  onClick={() => navigate(c.route!)}
                  className="hover:text-sky-600 hover:underline"
                >
                  {c.label}
                </button>
              ) : (
                <span className="text-slate-600">{c.label}</span>
              )}
              {i < crumbs.length - 1 && <ChevronRight size={12} />}
            </span>
          ))}
        </nav>
      )}
      <div className="flex flex-wrap items-center justify-between gap-3">
        <h1 className="text-xl font-semibold text-slate-800">{title}</h1>
        <div className="flex items-center gap-3">
          {status}
          {actions}
        </div>
      </div>
    </div>
  )
}
