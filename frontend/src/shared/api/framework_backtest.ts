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
  max_drawdown: number | null
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
