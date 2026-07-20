// Unified Backtest API module — mirrors datastore/api/routers/backtest_runs.py
// (Phase 3 of the Unified Backtest & Paper Trading Umbrella, see
// BacktestUmbrellaPlan.md at the repo root). Distinct from the existing
// legacy backtest_reports.py passthrough (still used by src/pages/ml/backtest.tsx)
// — this module is the new cross-channel run listing/detail/feature-log API.
import { apiGet } from './client'

export type BacktestChannel = 'technical' | 'fundamental' | 'ml' | 'momentum'
export type BacktestMode = 'backtest' | 'walk_forward' | 'paper'

export interface BacktestRunMetrics {
  cagr: number | null
  cagr_trading_day_legacy: number | null
  xirr: number | null
  final_capital: number
  total_contributed: number
  max_drawdown: number
  win_rate: number | null
  profit_factor: number | null
  sortino: number | null
  calmar: number | null
  n_distinct_tickers_traded: number
  turnover_ratio: number | null
  n_trades: number
  benchmark_cagr: number | null
  excess_return: number | null
  benchmark_status: string
  cash_position_series: { date: string; cash: number }[]
}

export interface BacktestDataGap {
  ticker: string
  as_of_date: string
  reason: string
}

export interface BacktestRunSummary {
  run_id: string
  parent_run_id: string | null
  channel: BacktestChannel
  strategy_id: string
  horizon_bucket: string
  mode: BacktestMode
  start_date: string
  end_date: string
  capital_mode: 'lump' | 'sip'
  initial_capital: number
  created_at: string
  metrics: BacktestRunMetrics | null
  data_gaps: BacktestDataGap[]
  integrity_passed: boolean | null
  live_eligible: boolean
}

export interface BacktestRunListResponse {
  runs: BacktestRunSummary[]
}

export interface BacktestRunLineageResponse {
  run_id: string
  lineage: BacktestRunSummary[]
}

export interface FeatureLogRow {
  ticker: string
  as_of_date: string
  horizon_bucket: string
  feature_vector: Record<string, unknown>
  signal_output: string | null
  decision_taken: string
}

export interface FeatureLogResponse {
  run_id: string
  rows: FeatureLogRow[]
}

export function listBacktestRuns(filters?: { channel?: BacktestChannel; mode?: BacktestMode; strategy_id?: string; limit?: number }) {
  return apiGet<BacktestRunListResponse>('/api/v1/backtest/runs', filters as Record<string, string | number | boolean | undefined>)
}

export function getBacktestRun(runId: string) {
  return apiGet<BacktestRunSummary>(`/api/v1/backtest/runs/${runId}`)
}

export function getBacktestRunLineage(runId: string) {
  return apiGet<BacktestRunLineageResponse>(`/api/v1/backtest/runs/${runId}/lineage`)
}

export function getBacktestRunFeatureLog(runId: string) {
  return apiGet<FeatureLogResponse>(`/api/v1/backtest/runs/${runId}/feature_log`)
}
