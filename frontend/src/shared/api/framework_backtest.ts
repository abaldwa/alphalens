/**
 * shared/api/framework_backtest.ts
 *
 * Typed client for datastore/api/routers/framework_backtest_runs.py --
 * the new native-engine campaign results table (momentum_framework/
 * scripts/run_full_campaign.py writes here), separate from the legacy
 * backtest_runs table `./backtest.ts` already serves.
 *
 * DECOUPLING (explicit user instruction, 2026-09-04): this is the ONLY
 * file that knows the API's response shape. Pages import the
 * FrameworkRunSummary type and these two functions -- never construct
 * the request URL or parse a response by hand -- so a future frontend
 * rewrite only has to keep this one file's exported contract stable, not
 * every page that displays campaign results.
 */

import { apiGet } from './client'

export interface FrameworkRunSummary {
  run_id: string
  strategy_id: string
  strategy_code: string
  band_id: number
  top_n: number | null
  lookback_months: number | null
  rebalance_cadence_days: number | null
  position_sizing: string | null
  start_date: string | null
  end_date: string | null
  cagr: number | null
  sharpe_ratio: number | null
  sortino_ratio: number | null
  calmar_ratio: number | null
  max_drawdown: number | null
  win_rate: number | null
  volatility_annualized: number | null
  trade_count: number
  run_executed_at: string | null
}

export interface FrameworkRunListResponse {
  runs: FrameworkRunSummary[]
  total: number
}

export function listFrameworkRuns(filters?: {
  strategy_code?: string
  band_id?: number
  limit?: number
  offset?: number
}) {
  return apiGet<FrameworkRunListResponse>(
    '/api/v1/framework-backtest/runs',
    filters as Record<string, string | number | boolean | undefined>,
  )
}

// ---------------------------------------------------------------------------
// Per-run analytics (2026-09-05): the Long Term CAGR / Regular Returns /
// Pre-Tax / Post-Tax / Window / Benchmark / Trade Quality features from
// /backtest-report/metrics, computed on demand from one run's own trades
// (datastore/api/routers/framework_backtest_runs.py's analytics endpoints)
// rather than read from a pre-generated report. Scoped to one run_id at a
// time -- see that router's module docstring for why.
// ---------------------------------------------------------------------------

export interface FrameworkYearlyReturnRow {
  fy_label: string
  fy_end: string
  trade_count: number
  win_rate: number | null
  gross_pnl: number
  tax_paid: number
  opening_capital: number
  pre_tax_return_pct: number
  post_tax_return_pct: number
}

export interface FrameworkYearlyReturnsResponse {
  run_id: string
  initial_capital: number
  rows: FrameworkYearlyReturnRow[]
}

export function getFrameworkRunYearlyReturns(runId: string) {
  return apiGet<FrameworkYearlyReturnsResponse>(
    `/api/v1/framework-backtest/runs/${encodeURIComponent(runId)}/yearly-returns`,
  )
}

export interface FrameworkRollingReturnResponse {
  run_id: string
  from_date: string
  to_date: string
  years: number
  trade_count: number
  win_rate: number | null
  gross_pnl: number
  tax_paid: number
  opening_capital: number
  pre_tax_return_pct: number
  post_tax_return_pct: number
  pre_tax_cagr: number | null
  post_tax_cagr: number | null
}

export function getFrameworkRunRollingReturn(runId: string, fromDate: string, toDate: string) {
  return apiGet<FrameworkRollingReturnResponse>(
    `/api/v1/framework-backtest/runs/${encodeURIComponent(runId)}/rolling-return`,
    { from_date: fromDate, to_date: toDate },
  )
}

export interface FrameworkTradeQualityResponse {
  run_id: string
  trade_count: number
  win_rate: number | null
  avg_win_pct: number | null
  avg_loss_pct: number | null
  avg_holding_days: number | null
  best_trade_pct: number | null
  worst_trade_pct: number | null
  profit_factor: number | null
}

export function getFrameworkRunTradeQuality(runId: string, window?: { fromDate?: string; toDate?: string }) {
  return apiGet<FrameworkTradeQualityResponse>(
    `/api/v1/framework-backtest/runs/${encodeURIComponent(runId)}/trade-quality`,
    { from_date: window?.fromDate, to_date: window?.toDate },
  )
}

export interface FrameworkBenchmarkResponse {
  run_id: string
  band_id: number
  index_name: string | null
  is_fallback_index: boolean
  benchmark_cagr: number | null
  benchmark_sharpe_ratio: number | null
  benchmark_max_drawdown: number | null
  benchmark_period_start: string | null
  benchmark_period_end: string | null
  note: string | null
}

export function getFrameworkRunBenchmark(runId: string) {
  return apiGet<FrameworkBenchmarkResponse>(
    `/api/v1/framework-backtest/runs/${encodeURIComponent(runId)}/benchmark`,
  )
}
