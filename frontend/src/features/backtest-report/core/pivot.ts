/**
 * features/backtest-report/core/pivot.ts
 *
 * The pure half of the configurable pivot: which fields a StrategyReport can be
 * grouped BY, which numbers it can be summarised ON, and how a bucket of
 * strategies collapses into one cell.
 *
 * It is deliberately separate from MatrixTable's fixed strategy x financial-year
 * pivot. That one answers "was this strategy consistent?" and can therefore hard-
 * code a geometric CAGR summary. This one answers "does this KIND of setup do
 * better than that kind?", where the user picks both axes, so the summary rule
 * has to travel with the metric rather than be assumed.
 *
 * Three rules are encoded here rather than left to the component:
 *
 * 1. A CELL AGGREGATES ACROSS STRATEGIES, NEVER ACROSS TIME. Every metric on
 *    StrategyReport is already a whole-run figure, so combining ten strategies'
 *    CAGRs is a cross-sectional summary of ten rates — not a compounding, which
 *    is what AGENTS.md's rate rule forbids. MEDIAN is the default for exactly
 *    that reason: it is the typical member of the bucket, and a single 90%/yr
 *    outlier cannot drag a whole cell up the way a mean can.
 * 2. A NULL METRIC IS EXCLUDED, NOT ZEROED. `n` counts the strategies that
 *    actually supplied the number, and `size` counts everyone in the bucket. A
 *    cell reading 18%/yr from 2 of 9 strategies is a different fact from one
 *    reading it from 9 of 9, and the UI can only say so if both survive here.
 * 3. AN ABSENT DIMENSION VALUE GETS ITS OWN BUCKET. Strategies with no declared
 *    universe are grouped under UNSET rather than dropped, because silently
 *    shrinking the population is how a pivot starts disagreeing with the flat
 *    table it was built from.
 */

import { cagrOn } from './cagrOn.ts'
import type { StrategyKey, StrategyReport, TaxBasis } from './types'

/** Bucket label for a strategy that does not declare the chosen dimension. */
export const UNSET = '(unset)'

// ---------------------------------------------------------------------------
// dimensions — what you can group by
// ---------------------------------------------------------------------------

export interface PivotDimension {
  id: string
  label: string
  /** Bucket this strategy falls in. null becomes UNSET. */
  valueOf: (r: StrategyReport) => string | null
}

/** Window length as a band, not a raw float: `9.7y` and `9.8y` are the same
 * decision, and one column per distinct float is not a pivot. */
export function windowBand(years: number | null | undefined): string | null {
  if (years == null || !Number.isFinite(years)) return null
  if (years < 3) return '< 3y'
  if (years < 5) return '3-5y'
  if (years < 10) return '5-10y'
  return '10y+'
}

/** The channel-specific "what was screened" field, normalised to one label so a
 * momentum Top-N and a technical template can share a column. */
function setupVariant(r: StrategyReport): string | null {
  const s = r.setup as unknown as Record<string, unknown>
  const first = ['templateName', 'preset', 'modelName', 'category'].find(
    (k) => typeof s[k] === 'string' && s[k] !== '',
  )
  if (first) return s[first] as string
  if (typeof s.topN === 'number') return `Top ${s.topN}`
  return null
}

export const PIVOT_DIMENSIONS: PivotDimension[] = [
  { id: 'channel', label: 'Channel', valueOf: (r) => r.channel },
  { id: 'universe', label: 'Universe', valueOf: (r) => r.setup.universe },
  { id: 'variant', label: 'Template / preset / model', valueOf: setupVariant },
  {
    id: 'capitalMode',
    label: 'Capital mode',
    valueOf: (r) => r.setup.capitalMode,
  },
  {
    id: 'benchmark',
    label: 'Benchmark',
    valueOf: (r) => r.setup.benchmarkIndexName ?? r.returns.benchmarkIndexName,
  },
  {
    id: 'windowBand',
    label: 'Window length',
    valueOf: (r) => windowBand(r.setup.window.years),
  },
  {
    id: 'exitVariant',
    label: 'Exit rule',
    valueOf: (r) => r.setup.exitCriterion.variant,
  },
  {
    id: 'rebalance',
    label: 'Rebalance',
    valueOf: (r) => (r.setup as { rebalanceFreq?: string | null }).rebalanceFreq ?? null,
  },
  {
    id: 'status',
    label: 'Registry status',
    valueOf: (r) => r.status ?? null,
  },
  { id: 'strategy', label: 'Strategy', valueOf: (r) => r.label },
]

export function findDimension(id: string | null | undefined): PivotDimension | null {
  return PIVOT_DIMENSIONS.find((d) => d.id === id) ?? null
}

// ---------------------------------------------------------------------------
// metrics — what you can summarise
// ---------------------------------------------------------------------------

/** Which formatter the cell uses. Kept as a tag rather than a function so this
 * module stays free of the display layer and remains testable on its own. */
export type MetricUnit = 'rate' | 'rateDelta' | 'pct' | 'num' | 'int' | 'inr' | 'days'

export interface PivotMetric {
  id: string
  label: string
  unit: MetricUnit
  /** Drives the heat shading: a drawdown of -40% is the BAD end of its scale. */
  higherIsBetter: boolean
  /** Dotted path into StrategyReport.pending, so an empty bucket can still say
   * which backlog item owes the number. */
  pendingPath?: string
  valueOf: (r: StrategyReport, basis: TaxBasis) => number | null
}

export const PIVOT_METRICS: PivotMetric[] = [
  {
    id: 'cagr',
    label: 'CAGR (selected basis)',
    unit: 'rate',
    higherIsBetter: true,
    pendingPath: 'returns.cagrPostTax',
    valueOf: (r, basis) => cagrOn(r, basis),
  },
  {
    id: 'excess',
    label: 'Excess vs benchmark',
    unit: 'rateDelta',
    higherIsBetter: true,
    pendingPath: 'returns.excessReturn',
    valueOf: (r) => r.returns.excessReturn,
  },
  {
    id: 'xirr',
    label: 'XIRR',
    unit: 'rate',
    higherIsBetter: true,
    pendingPath: 'returns.xirr',
    valueOf: (r) => r.returns.xirr,
  },
  {
    id: 'benchmarkCagr',
    label: 'Benchmark CAGR',
    unit: 'rate',
    higherIsBetter: true,
    valueOf: (r) => r.returns.benchmarkCagr,
  },
  {
    id: 'finalCapital',
    label: 'Final capital',
    unit: 'inr',
    higherIsBetter: true,
    valueOf: (r) => r.returns.finalCapital,
  },
  {
    id: 'maxDrawdown',
    label: 'Max drawdown',
    unit: 'pct',
    // A drawdown is carried as a negative fraction, so "higher" really is
    // better here — -0.2 beats -0.5.
    higherIsBetter: true,
    pendingPath: 'risk.maxDrawdown',
    valueOf: (r) => r.risk.maxDrawdown,
  },
  {
    id: 'sharpe',
    label: 'Sharpe',
    unit: 'num',
    higherIsBetter: true,
    valueOf: (r) => r.risk.sharpe,
  },
  {
    id: 'calmar',
    label: 'Calmar',
    unit: 'num',
    higherIsBetter: true,
    valueOf: (r) => r.risk.calmar,
  },
  {
    id: 'volatility',
    label: 'Volatility',
    unit: 'pct',
    higherIsBetter: false,
    valueOf: (r) => r.risk.volatility,
  },
  {
    id: 'winRate',
    label: 'Win rate',
    unit: 'pct',
    higherIsBetter: true,
    valueOf: (r) => r.tradeQuality.winRate,
  },
  {
    id: 'profitFactor',
    label: 'Profit factor',
    unit: 'num',
    higherIsBetter: true,
    valueOf: (r) => r.tradeQuality.profitFactor,
  },
  {
    id: 'churn',
    label: 'Churn / yr',
    unit: 'num',
    higherIsBetter: false,
    pendingPath: 'tradeQuality.churnPerYear',
    valueOf: (r) => r.tradeQuality.churnPerYear,
  },
  {
    id: 'avgHoldDays',
    label: 'Avg hold',
    unit: 'days',
    higherIsBetter: true,
    valueOf: (r) => r.tradeQuality.avgHoldDays,
  },
  {
    id: 'nTrades',
    label: 'Trades',
    unit: 'int',
    higherIsBetter: true,
    valueOf: (r) => r.tradeQuality.nTrades,
  },
  {
    id: 'rolling3y',
    label: '3y rolling median',
    unit: 'rate',
    higherIsBetter: true,
    pendingPath: 'consistency.rolling',
    valueOf: (r) =>
      r.consistency.rolling.find((w) => w.window === 3)?.medianCagr ?? null,
  },
  {
    id: 'positiveYears',
    label: 'Positive years',
    unit: 'pct',
    higherIsBetter: true,
    pendingPath: 'consistency.yoy',
    valueOf: (r) => {
      const yoy = r.consistency.yoy.filter((y) => y.returnPct != null)
      if (yoy.length === 0) return null
      return yoy.filter((y) => (y.returnPct ?? 0) > 0).length / yoy.length
    },
  },
]

export function findMetric(id: string | null | undefined): PivotMetric | null {
  return PIVOT_METRICS.find((m) => m.id === id) ?? null
}

// ---------------------------------------------------------------------------
// aggregation
// ---------------------------------------------------------------------------

export type AggName = 'median' | 'mean' | 'min' | 'max' | 'count'

export const AGG_LABELS: Record<AggName, string> = {
  median: 'Median',
  mean: 'Mean',
  min: 'Worst (min)',
  max: 'Best (max)',
  count: 'Count',
}

export const AGGS = Object.keys(AGG_LABELS) as AggName[]

export function median(values: number[]): number | null {
  if (values.length === 0) return null
  const s = [...values].sort((a, b) => a - b)
  const mid = s.length >> 1
  return s.length % 2 ? s[mid] : (s[mid - 1] + s[mid]) / 2
}

/** Applies the aggregation to the values that exist. `count` is special: it
 * counts strategies rather than summarising a metric, so it is the one
 * aggregation that returns a number when every metric value is null. */
export function aggregate(agg: AggName, values: number[], size: number): number | null {
  if (agg === 'count') return size
  if (values.length === 0) return null
  switch (agg) {
    case 'median':
      return median(values)
    case 'mean':
      return values.reduce((a, b) => a + b, 0) / values.length
    case 'min':
      return Math.min(...values)
    case 'max':
      return Math.max(...values)
  }
}

// ---------------------------------------------------------------------------
// the pivot itself
// ---------------------------------------------------------------------------

export interface PivotCell {
  value: number | null
  /** Strategies in this bucket that supplied the metric. */
  n: number
  /** Strategies in this bucket, supplying it or not. */
  size: number
  members: StrategyKey[]
}

export interface PivotResult {
  rowKeys: string[]
  colKeys: string[]
  /** cells[rowKey][colKey]; a bucket with no strategies at all is absent. */
  cells: Record<string, Record<string, PivotCell>>
  /** One cell per row across every column, aggregated from the raw members —
   * NOT from the row's cells, because an aggregate of medians is not a median. */
  rowTotals: Record<string, PivotCell>
  colTotals: Record<string, PivotCell>
  grandTotal: PivotCell
  /** Strategies present in the input, for the "n of m" caption. */
  total: number
}

export interface PivotSpec {
  row: PivotDimension
  col: PivotDimension
  metric: PivotMetric
  agg: AggName
  basis: TaxBasis
}

function cellOf(
  members: StrategyReport[],
  metric: PivotMetric,
  agg: AggName,
  basis: TaxBasis,
): PivotCell {
  const values: number[] = []
  for (const r of members) {
    const v = metric.valueOf(r, basis)
    if (v != null && Number.isFinite(v)) values.push(v)
  }
  return {
    value: aggregate(agg, values, members.length),
    n: values.length,
    size: members.length,
    members: members.map((r) => r.key),
  }
}

/** Axis labels sort alphabetically, except that UNSET always sinks to the end —
 * a bucket of "we don't know" should not head the table by luck of a bracket
 * sorting before a letter. */
export function sortAxis(keys: Iterable<string>): string[] {
  return [...keys].sort((a, b) => {
    if (a === UNSET) return 1
    if (b === UNSET) return -1
    return a.localeCompare(b, undefined, { numeric: true })
  })
}

export function buildPivot(
  strategies: StrategyReport[],
  spec: PivotSpec,
): PivotResult {
  const { row, col, metric, agg, basis } = spec

  const buckets = new Map<string, Map<string, StrategyReport[]>>()
  const byRow = new Map<string, StrategyReport[]>()
  const byCol = new Map<string, StrategyReport[]>()

  for (const r of strategies) {
    const rk = row.valueOf(r) ?? UNSET
    const ck = col.valueOf(r) ?? UNSET
    let cols = buckets.get(rk)
    if (!cols) buckets.set(rk, (cols = new Map()))
    const list = cols.get(ck)
    if (list) list.push(r)
    else cols.set(ck, [r])

    const rowList = byRow.get(rk)
    if (rowList) rowList.push(r)
    else byRow.set(rk, [r])
    const colList = byCol.get(ck)
    if (colList) colList.push(r)
    else byCol.set(ck, [r])
  }

  const rowKeys = sortAxis(byRow.keys())
  const colKeys = sortAxis(byCol.keys())

  const cells: Record<string, Record<string, PivotCell>> = {}
  for (const rk of rowKeys) {
    cells[rk] = {}
    for (const [ck, members] of buckets.get(rk) ?? []) {
      cells[rk][ck] = cellOf(members, metric, agg, basis)
    }
  }

  const rowTotals: Record<string, PivotCell> = {}
  for (const rk of rowKeys) rowTotals[rk] = cellOf(byRow.get(rk) ?? [], metric, agg, basis)
  const colTotals: Record<string, PivotCell> = {}
  for (const ck of colKeys) colTotals[ck] = cellOf(byCol.get(ck) ?? [], metric, agg, basis)

  return {
    rowKeys,
    colKeys,
    cells,
    rowTotals,
    colTotals,
    grandTotal: cellOf(strategies, metric, agg, basis),
    total: strategies.length,
  }
}

// ---------------------------------------------------------------------------
// heat shading
// ---------------------------------------------------------------------------

/** Every non-null cell value in the grid body, for scaling the heat. Totals are
 * excluded on purpose: a row total is a summary of the same numbers, and letting
 * it set the scale flattens the contrast between the cells being compared. */
export function cellValues(result: PivotResult): number[] {
  const out: number[] = []
  for (const rk of result.rowKeys) {
    for (const ck of result.colKeys) {
      const v = result.cells[rk]?.[ck]?.value
      if (v != null && Number.isFinite(v)) out.push(v)
    }
  }
  return out
}

export type HeatLevel = -2 | -1 | 0 | 1 | 2

/**
 * Places a value on a five-step scale relative to the rest of the grid.
 *
 * This is deliberately RELATIVE, unlike matrix.ts's `classifyRag`, which shades
 * a year's return against absolute boundaries the user chose. There is no
 * absolute "good" for an arbitrary metric — a Sharpe of 1.1 and a churn of 1.1
 * share no scale — so the pivot shades within what is on screen and the legend
 * says so.
 */
export function heatLevel(
  value: number | null,
  values: number[],
  higherIsBetter: boolean,
): HeatLevel {
  if (value == null || !Number.isFinite(value) || values.length < 2) return 0
  const min = Math.min(...values)
  const max = Math.max(...values)
  if (max === min) return 0
  const frac = (value - min) / (max - min)
  const good = higherIsBetter ? frac : 1 - frac
  if (good >= 0.8) return 2
  if (good >= 0.6) return 1
  if (good <= 0.2) return -2
  if (good <= 0.4) return -1
  return 0
}
