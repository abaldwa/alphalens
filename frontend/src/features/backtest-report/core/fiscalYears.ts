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
 *
 * THE AXIS IS KEYED ON THE BARE YEAR, and that is the whole point of this
 * module. Partial-ness is a property of a ROW, not of a year: FY2020 was a
 * stub for a strategy that started in January 2020 and a full year for every
 * strategy that did not, but it is the same twelve months on the calendar.
 * Unioning the marked label instead (as this did until 2026-08-19) minted a
 * SECOND column — "FY20*" beside "FY20" — populated for the one row that
 * started mid-year and blank for the other 631, which read as "FY20 is empty"
 * and, worse, filed that row's 2020 in a different column from everybody
 * else's 2020. Cross-strategy comparison in exactly those years was
 * misaligned. Use `yoyValueFor` to read a cell so the lookup tolerates the
 * marker the row's own label still carries.
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
  // The BARE year, never the marked label -- see the module docstring.
  for (const r of rows) {
    for (const y of r.consistency.yoy) labels.add(stripPartialMarker(y.fyLabel))
  }
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


/**
 * One row's entry for a financial year, matched on the bare year.
 *
 * The axis is keyed on "FY2020" while a row that opened mid-2019 still labels
 * its own entry "FY2020*", so an === comparison silently misses and renders a
 * populated year as blank. Every year-column value getter goes through here.
 */
export function yoyValueFor(
  yoy: Array<{ fyLabel: string; returnPct: number | null }>,
  label: string,
): { fyLabel: string; returnPct: number | null } | undefined {
  const want = stripPartialMarker(label)
  return yoy.find((y) => stripPartialMarker(y.fyLabel) === want)
}

/** Whether this row's entry for `label` is the engine-flagged partial year. */
export function isPartialFor(
  yoy: Array<{ fyLabel: string; returnPct: number | null }>,
  label: string,
): boolean {
  return yoyValueFor(yoy, label)?.fyLabel.endsWith('*') ?? false
}
