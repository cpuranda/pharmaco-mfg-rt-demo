import { useCallback, useEffect, useState } from 'react'

const KEY = 'nvs-mfg-operator'

// Operator name persisted in localStorage, used for ack/resolve/scenario actions.
export function useOperator(): [string, (v: string) => void] {
  const [operator, setOperatorState] = useState<string>(() => {
    try {
      return localStorage.getItem(KEY) || 'operator'
    } catch {
      return 'operator'
    }
  })

  const setOperator = useCallback((v: string) => {
    setOperatorState(v)
    try {
      localStorage.setItem(KEY, v)
    } catch {
      /* ignore */
    }
  }, [])

  useEffect(() => {
    try {
      localStorage.setItem(KEY, operator)
    } catch {
      /* ignore */
    }
  }, [operator])

  return [operator, setOperator]
}

const INTERVAL_KEY = 'nvs-mfg-poll-interval'
export function useRefreshInterval(): [number, (v: number) => void] {
  const [interval, setIntervalState] = useState<number>(() => {
    try {
      const v = Number(localStorage.getItem(INTERVAL_KEY))
      return v && [2000, 5000, 10000, 30000].includes(v) ? v : 5000
    } catch {
      return 5000
    }
  })
  const set = useCallback((v: number) => {
    setIntervalState(v)
    try {
      localStorage.setItem(INTERVAL_KEY, String(v))
    } catch {
      /* ignore */
    }
  }, [])
  return [interval, set]
}
