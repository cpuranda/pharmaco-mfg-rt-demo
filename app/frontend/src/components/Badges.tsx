import type { AlertStatus, HealthStatus, OperatingState, Severity } from '../types'
import {
  alertStatusClasses,
  healthClasses,
  operatingStateClasses,
  severityClasses,
} from '../lib/colors'

const base =
  'inline-flex items-center gap-1 rounded-full border px-2 py-0.5 text-xs font-semibold uppercase tracking-wide whitespace-nowrap'

export function HealthBadge({ status }: { status: HealthStatus }) {
  return (
    <span className={`${base} ${healthClasses(status)}`} aria-label={`Health status ${status}`}>
      {status}
    </span>
  )
}

export function SeverityBadge({ severity }: { severity: Severity }) {
  return (
    <span className={`${base} ${severityClasses(severity)}`} aria-label={`Severity ${severity}`}>
      {severity}
    </span>
  )
}

export function AlertStatusBadge({ status }: { status: AlertStatus }) {
  return (
    <span className={`${base} ${alertStatusClasses(status)}`} aria-label={`Alert status ${status}`}>
      {status}
    </span>
  )
}

export function OperatingStateBadge({ state }: { state: OperatingState }) {
  return (
    <span
      className={`${base} ${operatingStateClasses(state)}`}
      aria-label={`Operating state ${state}`}
    >
      {state}
    </span>
  )
}
