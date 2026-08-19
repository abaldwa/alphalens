/**
 * features/backtest-report/useReportPage.ts
 *
 * What every section page needs, assembled once: URL params, the merged
 * cross-channel strategy list, the resolved window, and the benchmark options
 * for that window.
 *
 * Window filtering here is FILTERING, not recomputation. A run covers a fixed
 * period; selecting "5 years" picks the runs that cover it and labels the ones
 * that do not, rather than silently truncating a 10-year figure and presenting
 * it as a 5-year one. Recomputing metrics over an arbitrary sub-window needs
 * the engine — that is A96.
 */

import { useMemo } from 'react'

import { baseCapitalFor, regularReturns } from '../core/regularReturns'
import { useBenchmarkOverride } from './useBenchmarkOverride'
import { useReportData } from './useReportData'
import { resolveWindow } from '../core/window'
import { useReportParams } from './useReportParams'

import type { BenchmarkOption, StrategyReport } from '../core/types'

/** True when the strategy's own run spans the requested window. */
export function coversWindow(
  r: StrategyReport,
  startDate: string | null,
  endDate: string | null,
): boolean {
  if (!startDate) return true
  const s = r.setup.window.startDate
  if (!s) return true // unknown coverage is reported, not excluded
  return s <= startDate && (!endDate || (r.setup.window.endDate ?? '9999') >= endDate)
}

export function useReportPage() {
  const [params, setParams] = useReportParams()
  // taxBasis is a DATA input, not a formatting one: the two bases are two
  // separate simulations, so it decides which run each row is drawn from.
  const data = useReportData({
    channel: params.channel,
    taxBasis: params.taxBasis,
  })

  const resolved = useMemo(
    () =>
      resolveWindow(params.window, data.latestDate, {
        startDate: params.startDate,
        endDate: params.endDate,
      }),
    [params.window, params.startDate, params.endDate, data.latestDate],
  )

  // The benchmark dropdown re-scores the rows against the selected index.
  // Applied before window filtering so the caveat travels with the row it
  // belongs to. See useBenchmarkOverride for why this is not a label change.
  const benchmarked = useBenchmarkOverride(
    data.strategies,
    params.benchmark,
    params.taxBasis,
  )

  /**
   * Regular-returns mode fills in `income`, which is null on every row
   * otherwise — no run in the report was executed with the engine's annual
   * reset, so selecting the mode used to add four permanently empty columns
   * and change nothing else. core/regularReturns derives the schedule
   * exactly from the year-on-year series; see that module for what the
   * derivation does and does not claim.
   */
  const withIncome = useMemo(() => {
    if (params.mode !== 'regular_returns') return benchmarked.strategies
    return benchmarked.strategies.map((r) => ({
      ...r,
      income: regularReturns(r.consistency.yoy, {
        baseCapital: baseCapitalFor(r.setup.capitalDeployed),
        topUpAfterLoss: params.topUpAfterLoss,
      }),
    }))
  }, [benchmarked.strategies, params.mode, params.topUpAfterLoss])

  const windowed = useMemo(
    () =>
      withIncome.map((r) => ({
        report: r,
        covers: coversWindow(r, resolved.startDate, resolved.endDate),
      })),
    [withIncome, resolved.startDate, resolved.endDate],
  )

  /** Strategies whose run covers the selected window. Comparing a 10-year
   * CAGR against a 3-year one in the same table is the single easiest way to
   * pick the wrong strategy, so the others are excluded from ranking and
   * surfaced as a count instead. */
  const strategies = useMemo(
    () => windowed.filter((w) => w.covers).map((w) => w.report),
    [windowed],
  )
  const excludedCount = windowed.length - strategies.length

  const benchmarkOptions = useMemo<BenchmarkOption[]>(() => {
    const idx = data.indices.data
    if (!idx) return []
    const live = new Set(idx.live_over_window)
    return idx.indices
      .filter((i) => i.usable_as_benchmark || live.has(i.index_name))
      .map((i) => ({
        indexName: i.index_name,
        live: live.size ? live.has(i.index_name) : i.is_fresh,
        caveat: i.caveat,
      }))
  }, [data.indices.data])

  return {
    params,
    setParams,
    strategies,
    allStrategies: withIncome,
    excludedCount,
    resolved,
    benchmarkOptions,
    recommendedBenchmark: data.indices.data?.recommended_benchmark ?? null,
    fallbackReason: data.indices.data?.fallback_reason ?? null,
    isLoading: data.isLoading,
    errors: [...data.errors, benchmarked.error].filter(Boolean) as Error[],
  }
}
