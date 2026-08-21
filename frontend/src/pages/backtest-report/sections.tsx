/**
 * pages/backtest-report/sections.tsx
 *
 * The metric workspace: one tabbed screen over Returns, Consistency, Risk and
 * Trade quality, plus an All-metrics view that puts every group in one grid.
 *
 * [2026-08-19] These were four separate pages, each with its own table and its
 * own copy of the same wiring. They are now ONE component parameterised by tab
 * (features/backtest-report/ui/MetricTabs), because they were never four
 * reports — they are four views of a single comparison, and a reader deciding
 * between strategies crosses between them constantly. Each tab is still a real
 * route, so every link, the Back button and the whole query string keep
 * working exactly as before.
 *
 * Every tab renders the same shared AnalyticsGrid workspace
 * (lib/ui/AnalyticsGrid): AG Grid with grouped, pinned and draggable columns,
 * conditional shading, CSV export and print layout, over a Recharts trend of
 * whichever rows the reader ticks. Identical structure on every tab, so the
 * interaction is learned once.
 *
 * The chart is the same year-on-year series on every tab on purpose: the
 * question a reader has while looking at a drawdown or a churn figure is
 * "which year did that come from?", and the YoY series answers it whichever
 * column prompted it.
 */

import { useMemo } from 'react'

import { AnalyticsGrid, Card, CardContent, CardDescription, CardHeader, CardTitle } from '@/lib/ui'
import type { AnalyticsGridSeries } from '@/lib/ui'
import { DeploySelectionBar } from '@/features/backtest-report/ui/DeploySelection'
import { MatrixTable } from '@/features/backtest-report/ui/MatrixTable'
import { MetricTabs, findMetricTab, type MetricTabId } from '@/features/backtest-report/ui/MetricTabs'
import { ReportLayout } from '@/features/backtest-report/ui/ReportLayout'
import { layoutProps } from '@/features/backtest-report/ui/sections'
import {
  cashFlowYearGroup,
  cashHeatmapFor,
  consistencyGroup,
  fiscalYearGroup,
  identityGroup,
  incomeGroup,
  returnsGroup,
  riskGroup,
  setupGroup,
  tradeQualityGroup,
} from '@/features/backtest-report/ui/gridColumns'
import { collectFiscalYears, shortFyLabel, stripPartialMarker, yoyValueFor } from '@/features/backtest-report/core/fiscalYears'
import { inr, pct } from '@/features/backtest-report/core/format'
import {
  baseCapitalFor,
  regularReturnsByYear,
} from '@/features/backtest-report/core/regularReturns'
import { useReportPage } from '@/features/backtest-report/data/useReportPage'
import type { StrategyReport, TaxBasis } from '@/features/backtest-report/core/types'

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

const rowId = (r: StrategyReport) => r.key

/** Shading thresholds. Full colour at +50% / −35% is roughly where a financial
 * year stops being remarkable and starts being an outlier; both ends are
 * adjustable from the grid toolbar. */
const HEATMAP = { positiveCeiling: 0.5, negativeCeiling: 0.35 }

/**
 * Which column groups each tab shows.
 *
 * Kept as data rather than as four branches so a new tab is one entry, and so
 * the All-metrics view is provably the union of the others rather than a
 * hand-maintained second list that drifts from them.
 */
function groupsFor(
  tab: MetricTabId,
  basis: TaxBasis,
  showIncome: boolean,
): Array<ReturnType<typeof returnsGroup> | null> {
  // In regular-returns mode the income group rides on EVERY tab, not just
  // Returns. The mode is a question about cash — "what did this pay me?" —
  // and a reader who came to Risk to check the drawdown still needs the total
  // cash out next to it to judge whether that drawdown was worth sitting
  // through. Hiding it behind a tab change makes them hold it in their head.
  const income = showIncome ? [incomeGroup()] : []
  switch (tab) {
    case 'returns':
      return [returnsGroup(basis), ...income]
    case 'consistency':
      return [...income, consistencyGroup()]
    case 'risk':
      return [...income, riskGroup()]
    case 'trade-quality':
      return [...income, tradeQualityGroup()]
    case 'all':
    default:
      return [
        returnsGroup(basis),
        ...income,
        consistencyGroup(),
        riskGroup(),
        tradeQualityGroup(),
      ]
  }
}

/**
 * The chart series: each ticked strategy's financial-year returns.
 *
 * Built from the rows on screen so the x axis is the union of every year any
 * of them covers — a strategy that started in FY2015 leaves FY2010..FY2014
 * genuinely empty rather than flat, and AnalyticsTrendChart draws that as a
 * gap rather than a line through zero.
 */
/**
 * The chart beneath the grid, in whichever unit the mode is about.
 *
 * LONG-TERM CAGR -> each ticked strategy's year-on-year return, as a percent.
 * REGULAR RETURNS -> the cash it paid out that year, in rupees, with a
 * deficit year plotted below zero.
 *
 * They are not two skins on one series. In regular-returns mode a +40% year
 * on a book still under water pays nothing at all, so the percentage line and
 * the cash line genuinely diverge — and they diverge hardest in exactly the
 * years a reader is trying to judge. Plotting percent under a mode about cash
 * would answer the wrong question confidently.
 */
function useYearSeries(
  rows: StrategyReport[],
  mode: 'long_term_cagr' | 'regular_returns',
  topUpAfterLoss: boolean,
): AnalyticsGridSeries<StrategyReport> {
  return useMemo(() => {
    const categories = collectFiscalYears(rows, 'oldest-first').map((l) => ({
      // OLDEST first, deliberately opposite to the tables: this is a time
      // series, and a reversed x axis draws every drawdown as a recovery.
      key: l,
      label: shortFyLabel(l),
    }))

    if (mode === 'regular_returns') {
      return {
        categories,
        value: (row, key) =>
          regularReturnsByYear(row.consistency.yoy, {
            baseCapital: baseCapitalFor(row.setup.capitalDeployed),
            topUpAfterLoss,
          })?.get(key)?.netCash ?? null,
        label: (row) => row.label,
        format: (v) => inr(v),
        valueLabel: 'cash out over the financial year',
        chartTitle: 'Cash paid out per year by the selected strategies',
        chartDescription: topUpAfterLoss
          ? 'Above the line is money withdrawn; below it is money you had to put back to restore base capital. A strategy that spends years below the line is not an income source, whatever its CAGR says.'
          : 'Above the line is money withdrawn; below it is how far under base capital the book ended, carried rather than funded. Flat stretches at zero are years that paid nothing at all.',
      }
    }

    return {
      categories,
      value: (row, key) =>
        yoyValueFor(row.consistency.yoy, key)?.returnPct ?? null,
      label: (row) => row.label,
      // A single financial year's return is a return OVER that year, not a
      // rate per year on top of it, so it is a plain percentage.
      format: (v) => pct(v, 1),
      valueLabel: 'return over the financial year',
      chartTitle: 'Year-on-year returns of the selected strategies',
      chartDescription:
        'Tick rows above to compare. Look for the strategy whose bad years are shallow, not the one whose best year is tallest — a single outstanding year is not repeatable.',
    }
  }, [rows, mode, topUpAfterLoss])
}

/** The one screen behind every tab. */
function MetricWorkspace({ tab }: { tab: MetricTabId }) {
  const page = useReportPage()
  const meta = findMetricTab(tab)
  const regular = page.params.mode === 'regular_returns'
  const series = useYearSeries(
    page.strategies,
    page.params.mode,
    page.params.topUpAfterLoss,
  )

  const columns = useMemo(
    () =>
      [
        identityGroup(),
        ...groupsFor(tab, page.params.taxBasis, regular),
        // Regular-returns mode replaces the percent year columns with rupee
        // ones. Showing both would put two numbers under the same year
        // heading that disagree by design.
        regular
          ? cashFlowYearGroup(page.strategies, page.params.topUpAfterLoss)
          : fiscalYearGroup(page.strategies),
        setupGroup(),
      ].filter(Boolean) as never[],
    [tab, page.params.taxBasis, regular, page.params.topUpAfterLoss, page.strategies],
  )

  // Rupee cells need rupee shading ceilings; the fraction ones would paint
  // every payout solid green.
  const heatmap = useMemo(
    () => (regular ? cashHeatmapFor(page.strategies) : HEATMAP),
    [regular, page.strategies],
  )

  const description = regular && (tab === 'returns' || tab === 'all')
    ? `Base capital goes to work every financial year and everything above it is withdrawn each March. ${
        page.params.topUpAfterLoss
          ? 'A losing year is topped back up out of pocket, so every year starts from the same stake — watch "Topped back up", that money is an input.'
          : 'A losing year is carried: the book must earn its way back above base capital before it pays again.'
      } Derived from each run's year-on-year series, not from a separate reset simulation.`
    : tab === 'returns' || tab === 'all'
      ? page.params.taxBasis === 'post_tax'
        ? 'Post-tax: STCG/LTCG paid as a cash outflow each financial year. This is the money you keep.'
        : 'Pre-tax: before STCG/LTCG. High-churn strategies look better here than they will in your account.'
      : tab === 'risk'
        ? 'Volatility is annualised, so it is comparable with the CAGR beside it; drawdown is a point-in-time fall, so it is not.'
        : tab === 'trade-quality'
          ? '“% trades won” is how OFTEN the strategy is right; “avg gain per winning trade” is how MUCH it makes when it is. Both are per-trade outcomes, so they are plain percentages — a three-day trade has no annual rate.'
          : 'Median, worst and count-positive across every rolling 3- and 5-consecutive-financial-year window. Each count can be checked against the year columns beside it.'

  return (
    <ReportLayout
      title={`Backtest Report — ${meta.label}`}
      description={meta.blurb}
      {...layoutProps(page)}
    >
      <MetricTabs active={tab} />

      <Card>
        <CardHeader>
          <CardTitle>{regular && tab === 'returns' ? 'Regular returns' : meta.label}</CardTitle>
          <CardDescription>{description}</CardDescription>
        </CardHeader>
        <CardContent>
          <WindowNote excluded={page.excludedCount} />
          <AnalyticsGrid
            // Per-tab id: column order, widths, filter and chart toggles are
            // remembered separately for each tab, so tuning the Risk layout
            // does not rearrange Returns.
            id={`backtest-report-${tab}`}
            columns={columns}
            rows={page.strategies}
            getRowId={rowId}
            series={series}
            heatmap={heatmap}
            isLoading={page.isLoading}
            csvFileName={`backtest_${tab.replace('-', '_')}`}
            title={`Backtest report — ${meta.label} (${page.params.taxBasis === 'post_tax' ? 'post-tax' : 'pre-tax'}, ${page.params.window})`}
            emptyMessage="No strategy reports available for this window."
          />
        </CardContent>
      </Card>

      {tab === 'consistency' ? <YoyMatrixCard page={page} /> : null}
      <DeploySelectionBar />
    </ReportLayout>
  )
}

/**
 * The compact RAG matrix, kept alongside the grid on the Consistency tab
 * rather than replaced by it.
 *
 * They answer different questions: the grid answers "how did this strategy
 * do", the matrix answers "which years were bad for EVERYONE" — and shading a
 * sortable grid does not make it scannable down a column the way a fixed
 * matrix is.
 */
function YoyMatrixCard({ page }: { page: ReturnType<typeof useReportPage> }) {
  const { matrixColumns, matrixRows } = useMemo(() => {
    // Newest first, matching the year columns in the grid above it — the two
    // are read against each other, and opposite orderings would make that a
    // trap rather than a cross-check.
    const cols = collectFiscalYears(page.strategies, 'newest-first').map((y) => ({
      // key stays the engine's four-digit label — it is what each row's
      // values are keyed by. Only the heading is abbreviated.
      key: y,
      label: shortFyLabel(y),
    }))
    const rows = page.strategies
      .filter((s) => s.consistency.yoy.length > 0)
      .map((s) => ({
        key: s.key,
        label: s.label,
        // Keyed on the BARE year so a row whose own label carries the
        // partial marker still lands in the shared column for that year.
        values: Object.fromEntries(
          s.consistency.yoy.map((y) => [stripPartialMarker(y.fyLabel), y.returnPct]),
        ),
      }))
    return { matrixColumns: cols, matrixRows: rows }
  }, [page.strategies])

  return (
    <Card className="mt-4">
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
  )
}

// One route each, so every existing link keeps working and the Back button
// walks the tabs the reader actually visited.
export function BacktestAllMetricsPage() {
  return <MetricWorkspace tab="all" />
}

export function BacktestReturnsPage() {
  return <MetricWorkspace tab="returns" />
}

export function BacktestConsistencyPage() {
  return <MetricWorkspace tab="consistency" />
}

export function BacktestRiskPage() {
  return <MetricWorkspace tab="risk" />
}

export function BacktestTradeQualityPage() {
  return <MetricWorkspace tab="trade-quality" />
}
