/**
 * features/backtest-report/core/fiscalYears.ts
 *
 * The financial-year axis, collected once so every view that draws it agrees.
 *
 * ORDER IS NOT COSMETIC HERE, and the right answer differs by view:
 *
 * - TABLES read newest-first. The reader's question of a year column is nearly
 *   always "what has it done lately?", and the recent years are what a deploy
 *   decision turns on. Oldest-first buries them off the right edge of an
 *   18-column span behind a horizontal scroll.
 * - The CHART reads oldest-first. It is a time series: reversing its x axis
 *   would draw every drawdown as a recovery and every recovery as a
 *   drawdown. A trend that runs right-to-left is not a stylistic preference,
 *   it is a misread waiting to happen.
 *
 * So the two are deliberately opposite, and both come from here rather than
 * from a `.sort()` at each call site — which is how they would quietly drift
 * apart the next time one of them is touched.
 *
 * Labels carry a trailing `*` when the engine marked the year partial (the run
 * opened or closed mid-year). `stripPartialMarker` is used for ordering only,
 * so "FY2027*" sorts beside FY2027 rather than after every unmarked label.
 */

import type { StrategyReport } from './types'

/** "FY2027*" -> "FY2027". Ordering only; the marker stays in what is shown. */
export function stripPartialMarker(label: string): string {
  return label.endsWith('*') ? label.slice(0, -1) : label
}

function byYear(a: string, b: string): number {
  return stripPartialMarker(a).localeCompare(stripPartialMarker(b))
}

/**
 * Every financial year any row covers.
 *
 * The UNION across rows, not the intersection: a strategy that started late
 * must show blanks for the years it did not exist rather than shifting its
 * history along into someone else's year, which would make two strategies
 * with different start dates look like they had the same 2015.
 */
export function collectFiscalYears(
  rows: StrategyReport[],
  order: 'newest-first' | 'oldest-first' = 'newest-first',
): string[] {
  const labels = new Set<string>()
  for (const r of rows) for (const y of r.consistency.yoy) labels.add(y.fyLabel)
  const sorted = [...labels].sort(byYear)
  return order === 'newest-first' ? sorted.reverse() : sorted
}

/**
 * "FY2027" -> "FY27", keeping any partial-year marker: "FY2027*" -> "FY27*".
 *
 * DISPLAY ONLY. The four-digit label is the KEY every year column, matrix
 * column and chart point looks its value up by, and it is what the engine
 * emitted, so it is never rewritten in the data — only in what is drawn. An
 * eighteen-year span is the whole reason: at four digits the year headers set
 * the column width and the grid scrolls horizontally for no other reason.
 *
 * A label that is not in the expected shape is passed through untouched
 * rather than half-parsed, so an unexpected format from the engine shows up
 * as itself instead of as a silently mangled year.
 */
export function shortFyLabel(label: string): string {
  const match = /^FY(\d{2})(\d{2})(\*?)$/.exec(label)
  return match ? `FY${match[2]}${match[3]}` : label
}
