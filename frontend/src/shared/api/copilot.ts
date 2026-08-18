// Co-Pilot API module — mirrors datastore/api/routers/copilot.py's request/
// response shapes exactly.

import { apiGet, apiPost } from './client'

export interface Condition {
  feature?: string
  op?: string
  value?: number | number[] | string
  feature2?: string
}

export interface UniverseFilter {
  rank_start: number | null
  rank_end: number | null
  mcap_min: number | null
  mcap_max: number | null
}

export interface RebalanceRules {
  lookback_days: number | null
  rebalance_every_n_trading_days: number | null
  top_n: number | null
  // [2026-08-18] grace_cycles/min_momentum deprecated with the other five
  // momentum knobs: momentum is a plain list swap, so there is no grace
  // period and no momentum floor.
}

export interface StrategySpec {
  name: string
  description: string
  source_query: string
  universe: UniverseFilter
  technical: Condition[]
  fundamental: Condition[]
  valuation: Condition[]
  rules: RebalanceRules
  unresolved: string[]
  created_at: string
  created_by: string
}

export interface DedupResult {
  matched: boolean
  matched_name?: string
  matched_source?: 'screener_template' | 'saved_strategy'
  similarity?: number
}

export interface BacktestResult {
  mode: 'backtest' | 'unsupported'
  reason?: string
  start_date?: string
  end_date?: string
  starting_capital?: number
  ending_value?: number
  cagr?: number | null
  total_return?: number | null
  churn_factor?: Record<string, unknown> | null
  n_rebalances?: number
  n_transactions?: number
  universe_size?: number
  caveats: string[]
}

export interface SavedStrategiesResponse {
  strategies: StrategySpec[]
}

export function queryStrategy(text: string): Promise<StrategySpec> {
  return apiPost<StrategySpec>('/api/v1/copilot/query', { text })
}

export function checkDedup(spec: StrategySpec): Promise<DedupResult> {
  return apiPost<DedupResult>('/api/v1/copilot/dedup', spec)
}

export function runBacktest(spec: StrategySpec): Promise<BacktestResult> {
  return apiPost<BacktestResult>('/api/v1/copilot/backtest', spec)
}

export function saveStrategy(spec: StrategySpec): Promise<{ slug: string; name: string }> {
  return apiPost('/api/v1/copilot/save', { spec })
}

export function listSavedStrategies(): Promise<SavedStrategiesResponse> {
  return apiGet<SavedStrategiesResponse>('/api/v1/copilot/strategies')
}
