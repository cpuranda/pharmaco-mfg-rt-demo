import type { AlertStatus, HealthStatus, OperatingState, Severity } from '../types'

// Canonical severity / health palette (per spec).
export const SEV = {
  CRITICAL: '#dc2626',
  WARNING: '#d97706',
  HEALTHY: '#16a34a',
  WATCH: '#0284c7',
  DEGRADED: '#ea580c',
} as const

// Tailwind class bundles (bg / text / border / ring) for badges.
export function healthClasses(h: HealthStatus): string {
  switch (h) {
    case 'HEALTHY':
      return 'bg-green-50 text-green-700 border-green-300'
    case 'WATCH':
      return 'bg-sky-50 text-sky-700 border-sky-300'
    case 'DEGRADED':
      return 'bg-orange-50 text-orange-700 border-orange-300'
    case 'CRITICAL':
      return 'bg-red-50 text-red-700 border-red-300'
    default:
      return 'bg-slate-50 text-slate-700 border-slate-300'
  }
}

export function healthColor(h: HealthStatus): string {
  switch (h) {
    case 'HEALTHY':
      return SEV.HEALTHY
    case 'WATCH':
      return SEV.WATCH
    case 'DEGRADED':
      return SEV.DEGRADED
    case 'CRITICAL':
      return SEV.CRITICAL
    default:
      return '#64748b'
  }
}

export function severityClasses(s: Severity): string {
  return s === 'CRITICAL'
    ? 'bg-red-600 text-white border-red-700'
    : 'bg-amber-500 text-white border-amber-600'
}

export function severityColor(s: Severity): string {
  return s === 'CRITICAL' ? SEV.CRITICAL : SEV.WARNING
}

export function alertStatusClasses(st: AlertStatus): string {
  switch (st) {
    case 'NEW':
      return 'bg-red-100 text-red-800 border-red-300'
    case 'ACKNOWLEDGED':
      return 'bg-amber-100 text-amber-800 border-amber-300'
    case 'RESOLVED':
      return 'bg-green-100 text-green-800 border-green-300'
    case 'MAINTENANCE':
      return 'bg-sky-100 text-sky-800 border-sky-300'
    default:
      return 'bg-slate-100 text-slate-700 border-slate-300'
  }
}

export function operatingStateClasses(s: OperatingState): string {
  switch (s) {
    case 'RUNNING':
      return 'bg-green-100 text-green-800 border-green-300'
    case 'IDLE':
      return 'bg-slate-100 text-slate-700 border-slate-300'
    case 'STARVED':
      return 'bg-amber-100 text-amber-800 border-amber-300'
    case 'BLOCKED':
      return 'bg-orange-100 text-orange-800 border-orange-300'
    case 'MAINTENANCE':
      return 'bg-sky-100 text-sky-800 border-sky-300'
    case 'FAULT':
      return 'bg-red-100 text-red-800 border-red-300'
    default:
      return 'bg-slate-100 text-slate-700 border-slate-300'
  }
}
