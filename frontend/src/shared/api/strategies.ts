/**
 * shared/api/strategies.ts
 *
 * Typed client for the strategy/filter registries (A95).
 *
 * WHY THIS FILE EXISTS
 * --------------------
 * The registries answer "what IS this strategy" — its label, its entry
 * predicates, its exit policy, the filters it declares. Until now the
 * frontend answered that question itself, by parsing variant-id strings and
 * branching per channel to decide what a run's setup meant. That made three
 * copies of the same facts (backtest imports, API rows, client-side parsing)
 * which were free to drift, and a deployed strategy's definition could differ
 * from the one that was backtested with nothing to catch it.
 *
 * These rows are the single source. Run METRICS still come from the backtest
 * report endpoints (shared/api/backtest.ts) — that split is deliberate: a
 * registry row describes the strategy, a run describes one execution of it.
 *
 * Point-in-time is preserved by the API: pass `version` (or `asOf`) to read
 * the definition as it stood when a given run executed, because explaining a
 * version-3 run with version-5 rules is exactly the drift this replaces.
 */

import { apiGet } from './client'

/** One predicate in a strategy's ordered entry criterion. */
export interface EntryPredicate {
  feature?: string
  op?: string
  value?: unknown
  feature2?: string
  [k: string]: unknown
}

export interface RegistryStrategy {
  strategy_key: string
  version: number
  channel: string
  name: string
  display_label: string | null
  description: string | null
  category: string | null
  definition: Record<string, unknown>
  /**
   * ORDERED list — order is meaningful and must not be re-sorted for display.
   * Exit is a single policy object rather than a list; that asymmetry is real
   * (entry conditions compose, an exit policy is one choice with parameters).
   */
  entry_criterion: EntryPredicate[]
  exit_criterion: Record<string, unknown>
  filter_ids: string[]
  status: string
  valid_from: string | null
  valid_to: string | null
  source_ref?: string | null
}

export interface RegistryFilter {
  filter_id: string
  version: number
  name: string
  description: string | null
  filter_type: string | null
  params_schema: Record<string, unknown>
  default_params: Record<string, unknown>
  applies_to_channels?: string[]
}

interface StrategyListResponse {
  strategies: RegistryStrategy[]
}

interface FilterListResponse {
  filters: RegistryFilter[]
}

/**
 * Every declared strategy, optionally narrowed by channel/status.
 *
 * `limit` is passed through rather than defaulted here: momentum alone has
 * ~3,100 rows, so a caller that wants a picker must page or filter by channel
 * deliberately instead of silently receiving a truncated list it believes is
 * complete.
 */
export async function listStrategies(params?: {
  channel?: string
  status?: string
  limit?: number
  offset?: number
}): Promise<RegistryStrategy[]> {
  const resp = await apiGet<StrategyListResponse>('/api/v1/strategies', params)
  return resp.strategies ?? []
}

/**
 * One strategy's definition.
 *
 * `version` selects a historical revision; omit it for the current one. Pass
 * the version the RUN recorded when explaining a run — see the module note.
 */
export async function getStrategy(
  strategyKey: string,
  params?: { version?: number; asOf?: string },
): Promise<RegistryStrategy> {
  return apiGet<RegistryStrategy>(`/api/v1/strategies/${encodeURIComponent(strategyKey)}`, {
    version: params?.version,
    as_of: params?.asOf,
  })
}

/** Every declared filter concept, one row per implementation. */
export async function listFilters(): Promise<RegistryFilter[]> {
  const resp = await apiGet<FilterListResponse>('/api/v1/filters')
  return resp.filters ?? []
}
