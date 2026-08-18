/**
 * pages/backtest-report/pivot.tsx
 *
 * The Pivot section: the Returns table (and every other metric on
 * StrategyReport) cross-tabulated by two dimensions the user picks.
 *
 * The flat Returns table ranks INDIVIDUAL strategies. This one answers the
 * question that ranking cannot: is momentum beating technical because momentum
 * is better, or because every momentum run happens to sit in a smallcap
 * universe? One row per strategy can never separate those; a channel x universe
 * grid can.
 *
 * It shares the section's window filter, tax basis and benchmark, so the
 * population being pivoted is exactly the population the Returns table ranks —
 * a pivot that quietly used a different set of runs would be worse than none.
 */

import { useMemo } from 'react'

import { Card, CardContent, CardDescription, CardHeader, CardTitle } from '@/lib/ui'
import { DeploySelectionBar } from '@/features/backtest-report/ui/DeploySelection'
import { PivotControls, PivotTable } from '@/features/backtest-report/ui/PivotTable'
import { ReportLayout } from '@/features/backtest-report/ui/ReportLayout'
import { layoutProps } from '@/features/backtest-report/ui/sections'
import { useReportPage } from '@/features/backtest-report/data/useReportPage'
import {
  AGG_LABELS,
  buildPivot,
  findDimension,
  findMetric,
} from '@/features/backtest-report/core/pivot'

export function BacktestPivotPage() {
  const page = useReportPage()
  const { pivotRow, pivotCol, pivotMetric, pivotAgg, taxBasis } = page.params

  // Non-null: useReportParams already falls back to a known id, so an unknown
  // one from a stale link cannot reach here.
  const row = findDimension(pivotRow)!
  const col = findDimension(pivotCol)!
  const metric = findMetric(pivotMetric)!

  const result = useMemo(
    () =>
      buildPivot(page.strategies, {
        row,
        col,
        metric,
        agg: pivotAgg,
        basis: taxBasis,
      }),
    [page.strategies, row, col, metric, pivotAgg, taxBasis],
  )

  const basisNote =
    metric.id === 'cagr'
      ? ` on the ${taxBasis === 'post_tax' ? 'post-tax' : 'pre-tax'} basis`
      : ''

  return (
    <ReportLayout
      title="Backtest Report — Pivot"
      description="Cross-tabulate any metric by two setup dimensions. A ranking tells you which strategy won; a pivot tells you whether the thing that won was the strategy or the setup it happened to run in."
      {...layoutProps(page)}
    >
      <Card>
        <CardHeader>
          <CardTitle>
            {AGG_LABELS[pivotAgg]} {metric.label.toLowerCase()}
            {basisNote} — {row.label} × {col.label}
          </CardTitle>
          <CardDescription>
            Each cell summarises the strategies in that bucket across the
            selected window; it never combines periods, so a cell of CAGRs stays
            a rate per year. Shading is RELATIVE to the cells on screen, not to
            an absolute good/bad boundary — a green cell is the best of what is
            shown, which may still be a bad number.
          </CardDescription>
        </CardHeader>
        <CardContent className="space-y-4">
          <PivotControls
            row={pivotRow}
            col={pivotCol}
            metric={pivotMetric}
            agg={pivotAgg}
            onChange={page.setParams}
          />
          {page.excludedCount > 0 ? (
            <p className="text-xs text-amber">
              {page.excludedCount} strateg
              {page.excludedCount === 1 ? 'y is' : 'ies are'} excluded: the run
              does not cover the selected window. The pivot summarises{' '}
              {result.total} of {result.total + page.excludedCount}.
            </p>
          ) : null}
          {page.isLoading ? (
            <p className="text-sm text-muted-foreground">Loading strategies…</p>
          ) : (
            <PivotTable
              result={result}
              metric={metric}
              agg={pivotAgg}
              rowLabel={row.label}
              colLabel={col.label}
              caption={`${AGG_LABELS[pivotAgg]} ${metric.label} by ${row.label} and ${col.label}`}
            />
          )}
        </CardContent>
      </Card>
      <DeploySelectionBar />
    </ReportLayout>
  )
}
