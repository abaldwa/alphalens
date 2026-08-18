/**
 * features/backtest-report/ui/PivotTable.tsx
 *
 * Renders a PivotResult. All of the arithmetic lives in core/pivot.ts; this
 * file only decides how a cell looks and how the axes are picked.
 *
 * It is a sibling of MatrixTable rather than a generalisation of it. MatrixTable
 * is the fixed strategy x financial-year view with absolute RAG boundaries and a
 * geometric CAGR summary — rules that are correct precisely because its axes are
 * known. Folding an arbitrary metric into it would mean deleting those rules for
 * everybody.
 *
 * Two things the shading does NOT do, deliberately:
 *
 * - It is relative to the cells on screen, never absolute, because a Sharpe of
 *   1.1 and a churn of 1.1 share no scale. The caption says so, so nobody reads
 *   a green cell as "good" in the RAG sense.
 * - It never shades on a cell's `n`. A cell backed by one strategy is marked
 *   with its count instead, since a one-member "median" is just that strategy
 *   and should look like a thin cell, not a bad one.
 */

import { EM_DASH, days, inr, int, num, pct, rate, rateDelta } from '../core/format'
import {
  AGGS,
  AGG_LABELS,
  PIVOT_DIMENSIONS,
  PIVOT_METRICS,
  cellValues,
  heatLevel,
  type AggName,
  type HeatLevel,
  type PivotCell,
  type PivotMetric,
  type PivotResult,
  type MetricUnit,
} from '../core/pivot'
import { cn } from '@/lib/utils'

const FORMATTERS: Record<MetricUnit, (v: number | null | undefined) => string> = {
  rate,
  rateDelta,
  pct,
  num: (v) => num(v),
  int,
  inr,
  days,
}

/** Literal class strings: Tailwind extracts statically, so an interpolated
 * `bg-green/${n}` compiles to nothing at all. */
function heatClass(level: HeatLevel): string {
  switch (level) {
    case -2:
      return 'bg-red/20 text-red'
    case -1:
      return 'bg-red/10'
    case 1:
      return 'bg-green/10'
    case 2:
      return 'bg-green/20 text-green'
    default:
      return ''
  }
}

function Picker<T extends string>({
  id,
  label,
  value,
  options,
  onChange,
}: {
  id: string
  label: string
  value: T
  options: Array<{ value: T; label: string }>
  onChange: (v: T) => void
}) {
  return (
    <div className="flex items-center gap-2">
      <label htmlFor={id} className="text-xs font-medium text-muted-foreground">
        {label}
      </label>
      <select
        id={id}
        value={value}
        onChange={(e) => onChange(e.target.value as T)}
        className="h-7 rounded-[var(--radius-token)] border border-border bg-background px-2 text-xs"
      >
        {options.map((o) => (
          <option key={o.value} value={o.value}>
            {o.label}
          </option>
        ))}
      </select>
    </div>
  )
}

export interface PivotControlsProps {
  row: string
  col: string
  metric: string
  agg: AggName
  onChange: (patch: {
    pivotRow?: string
    pivotCol?: string
    pivotMetric?: string
    pivotAgg?: AggName
  }) => void
}

/** The four selectors. Separate from the table so the page can place them in the
 * card header, where a control that changes the whole table belongs. */
export function PivotControls({ row, col, metric, agg, onChange }: PivotControlsProps) {
  const dims = PIVOT_DIMENSIONS.map((d) => ({ value: d.id, label: d.label }))
  return (
    <div className="flex flex-wrap items-center gap-x-6 gap-y-3">
      <Picker
        id="pivot-row"
        label="Rows"
        value={row}
        options={dims}
        onChange={(v) => onChange({ pivotRow: v })}
      />
      <Picker
        id="pivot-col"
        label="Columns"
        value={col}
        options={dims}
        onChange={(v) => onChange({ pivotCol: v })}
      />
      <Picker
        id="pivot-metric"
        label="Value"
        value={metric}
        options={PIVOT_METRICS.map((m) => ({ value: m.id, label: m.label }))}
        onChange={(v) => onChange({ pivotMetric: v })}
      />
      <Picker
        id="pivot-agg"
        label="Summarise by"
        value={agg}
        options={AGGS.map((a) => ({ value: a, label: AGG_LABELS[a] }))}
        onChange={(v) => onChange({ pivotAgg: v })}
      />
      <button
        type="button"
        onClick={() => onChange({ pivotRow: col, pivotCol: row })}
        className="rounded-[var(--radius-token)] border border-border px-2 py-1 text-xs text-muted-foreground hover:text-foreground focus-visible:outline focus-visible:outline-2 focus-visible:outline-offset-2 focus-visible:outline-ring"
      >
        ⇄ Swap axes
      </button>
    </div>
  )
}

export interface PivotTableProps {
  result: PivotResult
  metric: PivotMetric
  agg: AggName
  rowLabel: string
  colLabel: string
  caption?: string
}

export function PivotTable({
  result,
  metric,
  agg,
  rowLabel,
  colLabel,
  caption,
}: PivotTableProps) {
  // `count` is a population, not a measurement, so it formats as an integer
  // whatever the chosen metric's unit is.
  const format = agg === 'count' ? int : FORMATTERS[metric.unit]
  const scale = cellValues(result)
  // Counting rows is never "better when higher" in a way worth shading — every
  // bucket is as legitimate as its size — so the heat is turned off there.
  const shade = agg !== 'count'

  if (result.rowKeys.length === 0) {
    return <p className="text-sm text-muted-foreground">No strategies to pivot yet.</p>
  }

  function Cell({
    cell,
    isTotal,
    className,
  }: {
    cell: PivotCell | undefined
    isTotal?: boolean
    className?: string
  }) {
    if (!cell || cell.size === 0) {
      // No strategy at all fell in this bucket — an empty combination, not a
      // missing measurement.
      return (
        <td className={cn('px-2 py-1 text-right text-muted-foreground', className)}>
          {EM_DASH}
        </td>
      )
    }
    const level = shade && !isTotal ? heatLevel(cell.value, scale, metric.higherIsBetter) : 0
    const partial = agg !== 'count' && cell.n > 0 && cell.n < cell.size
    return (
      <td
        className={cn(
          'px-2 py-1 text-right tabular-nums',
          heatClass(level),
          isTotal && 'font-semibold',
          className,
        )}
        title={
          agg === 'count'
            ? `${cell.size} strateg${cell.size === 1 ? 'y' : 'ies'}`
            : `${cell.n} of ${cell.size} strateg${cell.size === 1 ? 'y' : 'ies'} supplied ${metric.label}`
        }
      >
        {cell.value == null ? (
          <span className="text-muted-foreground">{EM_DASH}</span>
        ) : (
          format(cell.value)
        )}
        {/* The denominator rides with the number. A median over 1 of 9
            strategies and one over 9 of 9 are different claims, and a bare
            figure makes them look identical. */}
        {partial || (cell.n === 1 && agg !== 'count') ? (
          <span className="ml-1 text-[0.65rem] text-muted-foreground">
            n={cell.n}
          </span>
        ) : null}
      </td>
    )
  }

  return (
    // The wrapper scrolls, not the page: a wide pivot must never make the
    // section scroll horizontally as a whole.
    <div className="overflow-x-auto">
      <table className="w-full border-collapse text-xs">
        {caption ? <caption className="sr-only">{caption}</caption> : null}
        <thead>
          <tr>
            <th
              scope="col"
              className="sticky left-0 z-10 bg-card px-2 py-1.5 text-left font-semibold"
            >
              {rowLabel} \ {colLabel}
            </th>
            {result.colKeys.map((ck) => (
              <th key={ck} scope="col" className="px-2 py-1.5 text-right font-semibold">
                {ck}
              </th>
            ))}
            <th scope="col" className="border-l border-border px-2 py-1.5 text-right font-semibold">
              All
            </th>
          </tr>
        </thead>
        <tbody>
          {result.rowKeys.map((rk) => (
            <tr key={rk} className="border-t border-border">
              <th
                scope="row"
                className="sticky left-0 z-10 whitespace-nowrap bg-card px-2 py-1 text-left font-medium"
              >
                {rk}
              </th>
              {result.colKeys.map((ck) => (
                <Cell key={ck} cell={result.cells[rk]?.[ck]} />
              ))}
              <Cell cell={result.rowTotals[rk]} isTotal className="border-l border-border" />
            </tr>
          ))}
          <tr className="border-t-2 border-border">
            <th
              scope="row"
              className="sticky left-0 z-10 bg-card px-2 py-1 text-left font-semibold"
            >
              All
            </th>
            {result.colKeys.map((ck) => (
              <Cell key={ck} cell={result.colTotals[ck]} isTotal />
            ))}
            <Cell cell={result.grandTotal} isTotal className="border-l border-border" />
          </tr>
        </tbody>
      </table>
    </div>
  )
}
