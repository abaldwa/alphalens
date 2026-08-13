/**
 * features/backtest-report/matrix.ts
 *
 * Pure pivot-table logic, kept out of MatrixTable.tsx so the self-check can
 * import it — the checker runs under Node's type stripping, which handles .ts
 * but not JSX.
 *
 * Everything here is in fractions, matching StrategyReport.
 */

export type RagBand = 'red' | 'amber' | 'green'

/** Boundaries are fractions, matching the cell values. */
export interface RagBoundaries {
  red: number
  green: number
}

export function classifyRag(v: number, b: RagBoundaries): RagBand {
  if (v < b.red) return 'red'
  if (v >= b.green) return 'green'
  return 'amber'
}

export interface MatrixColumn {
  key: string
  label: string
}

export type MatrixValues = Record<string, number | null | undefined>

/**
 * Geometric mean of the per-period rates present — a rate derived from rates,
 * which is the only composition AGENTS.md's rate rule permits here.
 *
 * Periods with no value are skipped rather than treated as zero: a strategy
 * that did not exist in FY2011 did not return 0% in FY2011, and counting it as
 * such makes a young strategy look worse than it was.
 */
export function periodCagr(
  values: MatrixValues,
  columns: MatrixColumn[],
): number | null {
  const present = columns
    .map((c) => values[c.key])
    .filter((v): v is number => v != null && Number.isFinite(v))
  if (present.length === 0) return null
  const growth = present.reduce((acc, v) => acc * (1 + v), 1)
  // A total wipeout has no real geometric mean; null is honest, NaN is not.
  if (growth <= 0) return null
  return Math.pow(growth, 1 / present.length) - 1
}

export function ragCounts(
  values: MatrixValues,
  columns: MatrixColumn[],
  boundaries: RagBoundaries,
): Record<RagBand, number> {
  const counts: Record<RagBand, number> = { red: 0, amber: 0, green: 0 }
  for (const c of columns) {
    const v = values[c.key]
    if (v != null && Number.isFinite(v)) counts[classifyRag(v, boundaries)] += 1
  }
  return counts
}
