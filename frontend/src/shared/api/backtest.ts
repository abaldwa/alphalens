// Unified Backtest API module — mirrors datastore/api/routers/backtest_runs.py
// (Phase 3 of the Unified Backtest & Paper Trading Umbrella, see
// BacktestUmbrellaPlan.md at the repo root). Distinct from the existing
// legacy backtest_reports.py passthrough (still used by src/pages/ml/backtest.tsx)
// — this module is the new cross-channel run listing/detail/feature-log API.
import { apiGet, apiPost } from './client'

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

// Iterative MetaLabeler retrain loop (datastore/api/routers/backtest_runs.py's
// /iterative/trigger + /iterative/status/{job_id}) — the one deliberate
// exception to this API being read-only, see that router's module docstring.
export interface IterativeRetrainTriggerResponse {
  job_id: string
  status: string
}

export interface IterativeRetrainIteration {
  iteration: number
  run_id: string
  hyperparams: Record<string, unknown>
  sharpe_mean: number
  win_rate_mean: number
  dsr: number
  random_feature_accuracy: number | null
  promoted: boolean
  rejection_reason: string | null
  runtime_seconds: number
  dropped_candidates: Record<string, number>
}

export interface IterativeRetrainReport {
  generated_at: string
  loop_run_id: string
  runtime_seconds: number
  stopped_reason: string
  holdout_selection: {
    holdout_start: string
    holdout_end: string
    skipped_fiscal_years: number[]
    explanation: string
  }
  excluded_buffer_rows: number
  iterations: IterativeRetrainIteration[]
  best_iteration_index: number | null
  best_hyperparams: Record<string, unknown> | null
  holdout_run_id: string | null
  holdout_runtime_seconds: number | null
  holdout_aggregate: Record<string, unknown> | null
}

export interface IterativeRetrainStatusResponse {
  job_id: string
  status: 'running' | 'completed' | 'failed' | 'unknown'
  report: IterativeRetrainReport | null
  log_tail: string | null
}

export function triggerIterativeRetrain(params?: { horizon_days?: number; folds?: number; max_iterations?: number }) {
  const query = new URLSearchParams()
  if (params?.horizon_days !== undefined) query.set('horizon_days', String(params.horizon_days))
  if (params?.folds !== undefined) query.set('folds', String(params.folds))
  if (params?.max_iterations !== undefined) query.set('max_iterations', String(params.max_iterations))
  const qs = query.toString()
  return apiPost<IterativeRetrainTriggerResponse>(`/api/v1/backtest/iterative/trigger${qs ? `?${qs}` : ''}`)
}

export function getIterativeRetrainStatus(jobId: string) {
  return apiGet<IterativeRetrainStatusResponse>(`/api/v1/backtest/iterative/status/${jobId}`)
}
