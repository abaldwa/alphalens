/**
 * features/backtest-report/data/useEquityCurve.ts
 *
 * The equity curve for ONE run, fetched lazily from the single-run detail
 * endpoint (A90).
 *
 * Why this is its own hook rather than a field on useReportData's rows: the
 * curve is one point per trading day (~2,500 for a ten-year run) and
 * `list_runs` deliberately strips it, along with `cash_position_series`,
 * because it dominates the list payload and nothing in the Runs table or the
 * five section tables draws it. Fetching it per-strategy on the detail page
 * keeps the list cheap and the chart real.
 *
 * WHAT THIS IS NOT: the cash series. `cash_position_series` tracks undeployed
 * cash, which falls as the strategy invests — plotting it as an equity curve
 * shows a portfolio apparently collapsing on its first buy. This reads
 * `equity_curve_series`, the mark-to-market cash + positions value that
 * StrategyPortfolio.record_equity() has always computed and that the engine
 * measures CAGR, drawdown and Sharpe off, so the drawn line and the reported
 * numbers cannot disagree.
 */

import { useQuery } from '@tanstack/react-query'

import { getBacktestRun } from '@/shared/api/backtest'

import type { EquityPoint } from '../core/types'

export function useEquityCurve(runId: string | null | undefined) {
  const query = useQuery({
    queryKey: ['backtest-run-equity', runId],
    queryFn: () => getBacktestRun(runId as string),
    enabled: !!runId,
  })

  const series: EquityPoint[] = (
    query.data?.metrics?.equity_curve_series ?? []
  )
    .filter((p) => p && p.date != null && Number.isFinite(p.equity))
    .map((p) => ({ date: p.date, value: p.equity }))

  return {
    series,
    isLoading: query.isLoading,
    error: query.error,
    /** True once the fetch succeeded and the run genuinely carries no curve —
     * a run predating A90, not a load still in flight. Lets the caller say
     * which of the two it is instead of showing an empty chart for both. */
    isEmpty: query.isSuccess && series.length === 0,
  }
}
