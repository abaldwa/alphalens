import { useEffect, useState } from 'react'
import { useQuery } from '@tanstack/react-query'

import { apiGet } from '@/shared/api/client'
import type { MomentumStrategy } from './types'

/** Shared rank-band strategy dropdown for the momentum portfolio/rebalance/universe
 * pages — mirrors dashboard/static/momentum/js/api.js's initMomentumStrategyDropdown. */
export function useStrategies() {
  return useQuery({
    queryKey: ['momentum-strategies'],
    queryFn: () => apiGet<MomentumStrategy[]>('/api/v1/momentum/strategies'),
  })
}

const LOCAL_STORAGE_KEY = 'momentum_strategy_id'

/**
 * Tracks the currently-selected rank-band strategy, restoring the last
 * choice from localStorage (falling back to the first strategy) and
 * persisting on change — mirrors dashboard/static/js/api.js's
 * initMomentumStrategyDropdown, whose selection is shared (via
 * localStorage) across the universe/rebalance/portfolio screens.
 */
export function useActiveStrategy(strategies: MomentumStrategy[] | undefined) {
  const [strategyId, setStrategyIdState] = useState<string | null>(null)

  useEffect(() => {
    if (!strategies?.length || strategyId != null) return
    const saved = localStorage.getItem(LOCAL_STORAGE_KEY)
    const initial = strategies.some((s) => s.strategy_id === saved) ? saved! : strategies[0].strategy_id
    setStrategyIdState(initial)
  }, [strategies, strategyId])

  const setStrategyId = (id: string) => {
    localStorage.setItem(LOCAL_STORAGE_KEY, id)
    setStrategyIdState(id)
  }

  return [strategyId, setStrategyId] as const
}

export function StrategyPicker({
  strategies,
  value,
  onChange,
}: {
  strategies: MomentumStrategy[]
  value: string | null
  onChange: (strategyId: string) => void
}) {
  return (
    <select
      className="h-9 rounded-[var(--radius-token)] border border-border bg-transparent px-3 text-sm"
      value={value ?? ''}
      onChange={(e) => onChange(e.target.value)}
    >
      {strategies.map((s) => (
        <option key={s.strategy_id} value={s.strategy_id}>
          {s.label}
        </option>
      ))}
    </select>
  )
}
