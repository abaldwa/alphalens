/**
 * pages/backtest-report/sections.tsx
 *
 * The four metric sections — Returns, Consistency, Risk, Trade quality. They
 * are one file because they are one page with a different column group each;
 * splitting four ~25-line components across four files would be four copies of
 * the same wiring.
 *
 * Every table is the shared DataTable fed by the shared column builders, so
 * sorting, search, facet filters and the priority-collapse behaviour are
 * identical across sections without any of them re-implementing it — the "same
 * sortable formatted table everywhere" rule.
 */

import { useMemo } from 'react'

import { Card, CardContent, CardDescription, CardHeader, CardTitle, DataTable } from '@/lib/ui'
import {
  consistencyColumns,
  identityColumns,
  incomeColumns,
  returnsColumns,
  riskColumns,
  setupColumns,
  tradeQualityColumns,
  tradesColumn,
} from '@/features/backtest-report/columns'
import { DeploySelectionBar } from '@/features/backtest-report/components/DeploySelection'
import { useDeployColumn } from '@/features/backtest-report/deploy/deployColumn'
import { MatrixTable } from '@/features/backtest-report/components/MatrixTable'
import { ReportLayout } from '@/features/backtest-report/components/ReportLayout'
import { layoutProps } from '@/features/backtest-report/sections'
import { useReportPage } from '@/features/backtest-report/useReportPage'

/** Rows excluded by the window selector are counted, not hidden silently —
 * "12 strategies aren't shown" is information; a shorter table is not. */
function WindowNote({ excluded }: { excluded: number }) {
  if (excluded === 0) return null
  return (
    <p className="mb-2 text-xs text-amber">
      {excluded} strateg{excluded === 1 ? 'y is' : 'ies are'} hidden: their run
      does not cover the selected window. Comparing a 10-year CAGR with a 3-year
      one ranks by luck of timing rather than by strategy.
    </p>
  )
}

function useSection() {
  return useReportPage()
}

export function BacktestReturnsPage() {
  const page = useSection()
  const deployColumn = useDeployColumn()
  const columns = useMemo(
    () => [
      ...identityColumns('returns'),
      ...returnsColumns(page.params.taxBasis),
      ...(page.params.mode === 'regular_returns' ? incomeColumns() : []),
      ...setupColumns(),
      tradesColumn(),
      deployColumn,
    ],
    [page.params.taxBasis, page.params.mode, deployColumn],
  )

  return (
    <ReportLayout
      title="Backtest Report — Returns"
      description="CAGR and XIRR on the selected basis, against the selected benchmark. Every figure is a rate per year, never a total over the window."
      {...layoutProps(page)}
    >
      <Card>
        <CardHeader>
          <CardTitle>Returns</CardTitle>
          <CardDescription>
            {page.params.taxBasis === 'post_tax'
              ? 'Post-tax: STCG/LTCG paid as a cash outflow each financial year. This is the money you keep.'
              : 'Pre-tax: before STCG/LTCG. High-churn strategies look better here than they will in your account.'}
          </CardDescription>
        </CardHeader>
        <CardContent>
          <WindowNote excluded={page.excludedCount} />
          <DataTable
            columns={columns}
            data={page.strategies}
            isLoading={page.isLoading}
            emptyMessage="No strategy reports available for this window."
          />
        </CardContent>
      </Card>
      <DeploySelectionBar />
    </ReportLayout>
  )
}

export function BacktestConsistencyPage() {
  const page = useSection()
  const deployColumn = useDeployColumn()
  const columns = useMemo(
    () => [
      ...identityColumns('consistency'),
      ...consistencyColumns(),
      tradesColumn(),
      deployColumn,
    ],
    [deployColumn],
  )

  // The YoY matrix: strategies down, financial years across. Its columns are
  // the union of every strategy's years, so a strategy that started late
  // shows em dashes for the years it did not exist rather than shifting its
  // history left into someone else's year.
  const { matrixColumns, matrixRows } = useMemo(() => {
    const yearSet = new Set<string>()
    for (const s of page.strategies) {
      for (const y of s.consistency.yoy) yearSet.add(y.fyLabel)
    }
    const cols = [...yearSet].sort().map((y) => ({ key: y, label: y }))
    const rows = page.strategies
      .filter((s) => s.consistency.yoy.length > 0)
      .map((s) => ({
        key: s.key,
        label: s.label,
        values: Object.fromEntries(
          s.consistency.yoy.map((y) => [y.fyLabel, y.returnPct]),
        ),
      }))
    return { matrixColumns: cols, matrixRows: rows }
  }, [page.strategies])

  return (
    <ReportLayout
      title="Backtest Report — Consistency"
      description="Rolling windows and year-on-year returns: does the strategy work repeatedly, or did one year carry the whole record?"
      {...layoutProps(page)}
    >
      <Card className="mb-4">
        <CardHeader>
          <CardTitle>Rolling windows</CardTitle>
          <CardDescription>
            Median, worst and share-positive across every 3- and 5-year window
            in the run. Each figure is the annualised rate for that window.
          </CardDescription>
        </CardHeader>
        <CardContent>
          <WindowNote excluded={page.excludedCount} />
          <DataTable
            columns={columns}
            data={page.strategies}
            isLoading={page.isLoading}
            emptyMessage="No strategy reports available for this window."
          />
        </CardContent>
      </Card>

      <Card>
        <CardHeader>
          <CardTitle>Year-on-year consistency matrix</CardTitle>
          <CardDescription>
            One row per strategy, one column per financial year. Scan for the
            fewest red cells, not the single best cell.
          </CardDescription>
        </CardHeader>
        <CardContent>
          <MatrixTable
            columns={matrixColumns}
            rows={matrixRows}
            boundaries={{ red: 0, green: 0.18 }}
            selectedKey={page.params.strategy}
            caption="Year-on-year returns per strategy"
          />
        </CardContent>
      </Card>
      <DeploySelectionBar />
    </ReportLayout>
  )
}

export function BacktestRiskPage() {
  const page = useSection()
  const deployColumn = useDeployColumn()
  const columns = useMemo(
    () => [...identityColumns('risk'), ...riskColumns(), tradesColumn(), deployColumn],
    [deployColumn],
  )

  return (
    <ReportLayout
      title="Backtest Report — Risk"
      description="Drawdown and risk-adjusted return. The drawdown is the number that decides whether you would actually have stayed invested."
      {...layoutProps(page)}
    >
      <Card>
        <CardHeader>
          <CardTitle>Risk</CardTitle>
        </CardHeader>
        <CardContent>
          <WindowNote excluded={page.excludedCount} />
          <DataTable
            columns={columns}
            data={page.strategies}
            isLoading={page.isLoading}
            emptyMessage="No strategy reports available for this window."
          />
        </CardContent>
      </Card>
      <DeploySelectionBar />
    </ReportLayout>
  )
}

export function BacktestTradeQualityPage() {
  const page = useSection()
  const deployColumn = useDeployColumn()
  const columns = useMemo(
    () => [
      ...identityColumns('trade-quality'),
      ...tradeQualityColumns(),
      tradesColumn(),
      deployColumn,
    ],
    [deployColumn],
  )

  return (
    <ReportLayout
      title="Backtest Report — Trade quality"
      description="Churn, holding period and the shape of the average win against the average loss — what the strategy costs to run, and how it actually makes money."
      {...layoutProps(page)}
    >
      <Card>
        <CardHeader>
          <CardTitle>Trade quality</CardTitle>
          <CardDescription>
            Win rate and average win/loss are per-trade outcomes, so they are
            plain percentages — a three-day trade has no annual rate.
          </CardDescription>
        </CardHeader>
        <CardContent>
          <WindowNote excluded={page.excludedCount} />
          <DataTable
            columns={columns}
            data={page.strategies}
            isLoading={page.isLoading}
            emptyMessage="No strategy reports available for this window."
          />
        </CardContent>
      </Card>
      <DeploySelectionBar />
    </ReportLayout>
  )
}
