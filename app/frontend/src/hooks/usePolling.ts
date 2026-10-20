import { useCallback, useEffect, useRef, useState } from 'react'
import { ApiError } from '../api'

export interface PollState<T> {
  data: T | null
  error: ApiError | null
  loading: boolean // true only on the very first load
  refreshing: boolean // true while a background refresh is in flight
  lastUpdated: number | null // ms epoch of last successful fetch
  latencyMs: number | null // API round-trip of last fetch
  reload: () => void
}

/**
 * Poll an async fetcher on an interval. Keeps the previous data visible while
 * refreshing so the UI doesn't flash. `intervalMs = 0` disables auto-polling.
 */
export function usePolling<T>(
  fetcher: () => Promise<T>,
  intervalMs: number,
  deps: unknown[] = [],
): PollState<T> {
  const [data, setData] = useState<T | null>(null)
  const [error, setError] = useState<ApiError | null>(null)
  const [loading, setLoading] = useState(true)
  const [refreshing, setRefreshing] = useState(false)
  const [lastUpdated, setLastUpdated] = useState<number | null>(null)
  const [latencyMs, setLatencyMs] = useState<number | null>(null)

  const mounted = useRef(true)
  const fetcherRef = useRef(fetcher)
  fetcherRef.current = fetcher
  const inFlight = useRef(false)

  const run = useCallback(async () => {
    if (inFlight.current) return
    inFlight.current = true
    setRefreshing(true)
    const t0 = performance.now()
    try {
      const result = await fetcherRef.current()
      if (!mounted.current) return
      setData(result)
      setError(null)
      setLastUpdated(Date.now())
      setLatencyMs(performance.now() - t0)
    } catch (e) {
      if (!mounted.current) return
      setError(e instanceof ApiError ? e : new ApiError(0, String(e)))
      setLatencyMs(performance.now() - t0)
    } finally {
      if (mounted.current) {
        setLoading(false)
        setRefreshing(false)
      }
      inFlight.current = false
    }
  }, [])

  useEffect(() => {
    mounted.current = true
    setLoading(true)
    run()
    let id: number | undefined
    if (intervalMs > 0) {
      id = window.setInterval(run, intervalMs)
    }
    return () => {
      mounted.current = false
      if (id) window.clearInterval(id)
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [intervalMs, run, ...deps])

  return { data, error, loading, refreshing, lastUpdated, latencyMs, reload: run }
}
