/**
 * features/backtest-report/data/useStrategyDefinition.ts
 *
 * Reads one strategy's DEFINITION from strategy_registry (A95), separately
 * from the run metrics useReportData already fetches.
 *
 * The split is the point. A run row says how one execution performed; the
 * registry row says what the strategy is. Deriving the second from the first
 * — parsing a variant id, branching per channel to guess what the setup meant
 * — is what A95 exists to stop, because it makes the frontend a third
 * independent copy of facts the backtest and the API already hold.
 *
 * Disabled (never fetched) when no strategy_key is known, so a run recorded
 * before A89 stamped keys degrades to "no definition available" rather than
 * firing a request for `undefined` and rendering an error.
 */

import { useQuery } from '@tanstack/react-query'

import { getStrategy, listFilters } from '@/shared/api/strategies'
import type { RegistryFilter, RegistryStrategy } from '@/shared/api/strategies'

export interface StrategyDefinitionResult {
  strategy: RegistryStrategy | undefined
  /** Only the filters this strategy declares, resolved to their full rows. */
  filters: RegistryFilter[]
  isLoading: boolean
  error: Error | null
}

export function useStrategyDefinition(
  strategyKey: string | null | undefined,
  version?: number,
): StrategyDefinitionResult {
  const strategy = useQuery({
    // version participates in the key: two runs of the same strategy at
    // different versions are different definitions and must not share a cache
    // entry, or the second run is explained with the first one's rules.
    queryKey: ['registry-strategy', strategyKey, version ?? 'current'],
    queryFn: () => getStrategy(strategyKey as string, { version }),
    enabled: Boolean(strategyKey),
  })

  // The full filter catalogue is small (11 rows) and shared across every
  // strategy on the page, so one cached fetch beats N per-filter lookups.
  const filters = useQuery({
    queryKey: ['registry-filters'],
    queryFn: listFilters,
    enabled: Boolean(strategyKey),
  })

  const declared = new Set(strategy.data?.filter_ids ?? [])

  return {
    strategy: strategy.data,
    filters: (filters.data ?? []).filter((f) => declared.has(f.filter_id)),
    isLoading: strategy.isLoading || filters.isLoading,
    error: (strategy.error as Error | null) ?? (filters.error as Error | null) ?? null,
  }
}
