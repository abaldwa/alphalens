/**
 * shared/api/momentum_overall_rank.ts
 *
 * Typed client for datastore/api/routers/momentum_overall_rank.py -- the
 * overall (all ~800-stock) momentum rank, sourced from the M13 slice of
 * momentum_rank_snapshots (see that router's docstring for why band_id=13
 * already IS the overall rank, not a new computation).
 *
 * DECOUPLING: same rationale as shared/api/framework_backtest.ts -- this
 * is the only file that knows the endpoint's request/response shape.
 */

import { apiGet } from './client'

export interface OverallRankRow {
  ticker: string
  momentum_return: number
  rank: number
}

export interface OverallRankResponse {
  as_of_date: string
  lookback_months: number
  total_ranked: number
  rows: OverallRankRow[]
}

export function getOverallMomentumRank(params: {
  as_of_date: string
  lookback_months?: 1 | 3 | 6 | 9 | 12
  top_n?: number
}) {
  return apiGet<OverallRankResponse>('/api/v1/momentum-rank/overall', params as Record<string, string | number | boolean | undefined>)
}
