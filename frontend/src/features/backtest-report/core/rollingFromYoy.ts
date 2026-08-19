/**
 * features/backtest-report/core/rollingFromYoy.ts
 *
 * Rolling multi-year returns built from the financial-year series.
 *
 * WHY NOT THE ENGINE'S `rolling_returns`. The engine slides its window along
 * the daily equity curve, so a 17-year run yields 57 overlapping "3-year
 * windows" — one per rebalance date. That answers "if I had started on an
 * arbitrary Tuesday, what would three years have looked like?", which is a
 * fine question and a terrible summary: the windows overlap almost completely,
 * so the median is dominated by whichever stretch happens to be sampled most
 * often, and "94.7% of windows were positive" describes days, not decisions.
 *
 * What a reader of this table actually means by "3-year window" is three
 * consecutive financial years — the unit the year-on-year matrix directly
 * above it is drawn in, and the unit an investor holds in. Eighteen financial
 * years give sixteen such windows, and "13 of 16 were positive" is a statement
 * you can check against the matrix by eye. The engine's daily figures are
 * still available on the strategy detail page; this is what the section table
 * ranks on.
 *
 * Everything here is a fraction, matching StrategyReport, and every window is
 * ANNUALISED — the geometric mean of its years, never the total over three.
 */

import type { YoyReturn } from './types'

export interface YoyRollingWindow {
  /** Years per window: 3 and 5 on the section table. */
  window: number
  /** Annualised rate, as a fraction. Null when the run is too short. */
  medianCagr: number | null
  minCagr: number | null
  maxCagr: number | null
  /** Absolute counts, not a share.
   *
   * The table used to render 13/16 as "81.3%", which reads as a probability
   * and hides how thin the evidence is — 81.3% from 16 windows and 81.3% from
   * 4 are very different claims, and the percentage form makes them identical
   * on screen. */
  nPositive: number
  nWindows: number
}

/** Median of a numeric list. Even-length lists average the middle pair, which
 * is the ordinary definition and the one the engine uses. */
export function median(values: number[]): number | null {
  if (!values.length) return null
  const sorted = [...values].sort((a, b) => a - b)
  const mid = sorted.length >> 1
  return sorted.length % 2 ? sorted[mid] : (sorted[mid - 1] + sorted[mid]) / 2
}

/**
 * Annualised return of `window` consecutive financial years starting at
 * `start`, or null when the slice runs past the end of the series or contains
 * a year with no data.
 *
 * A missing year is NOT treated as a flat 0%: a strategy that did not exist in
 * FY2011 did not return zero in FY2011, and filling it with zero drags the
 * geometric mean toward the middle and makes a short track record look like a
 * long mediocre one.
 */
export function windowCagr(
  returns: Array<number | null>,
  start: number,
  window: number,
): number | null {
  if (start < 0 || start + window > returns.length) return null
  let growth = 1
  for (let i = start; i < start + window; i += 1) {
    const v = returns[i]
    if (v == null || !Number.isFinite(v)) return null
    growth *= 1 + v
  }
  // A window that wiped the book out has no real geometric mean. Null is
  // honest; NaN would propagate silently into the median.
  if (growth <= 0) return null
  return Math.pow(growth, 1 / window) - 1
}

/**
 * Every rolling `window`-year block over the FY series, summarised.
 *
 * `yoy` is taken in the order the engine emitted it, which is chronological.
 * Partial first/last years (the engine flags them with a trailing `*` on the
 * label) are included: a partial year is a real return over a real period, and
 * dropping it would silently shift every window one year along.
 */
export function rollingFromYoy(
  yoy: YoyReturn[],
  window: number,
): YoyRollingWindow | null {
  const returns = yoy.map((y) => y.returnPct)
  if (returns.length < window) return null
  const windows: number[] = []
  for (let i = 0; i + window <= returns.length; i += 1) {
    const v = windowCagr(returns, i, window)
    if (v != null) windows.push(v)
  }
  if (!windows.length) return null
  return {
    window,
    medianCagr: median(windows),
    minCagr: Math.min(...windows),
    maxCagr: Math.max(...windows),
    nPositive: windows.filter((v) => v > 0).length,
    nWindows: windows.length,
  }
}

/** "13 of 16". Rendered instead of a share — see YoyRollingWindow.nPositive. */
export function countOf(n: number | null, total: number | null): string {
  if (n == null || total == null || !total) return '—'
  return `${n} of ${total}`
}
