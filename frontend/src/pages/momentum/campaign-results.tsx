/**
 * pages/momentum/campaign-results.tsx
 *
 * Results table for the native-engine full campaign (momentum_framework/
 * scripts/run_full_campaign.py -> framework_backtest_runs), pulled through
 * shared/api/framework_backtest.ts's typed client only -- this page never
 * touches config_json/metrics_json or a DB path directly (see that
 * file's docstring on why: a frontend rewrite should only need to keep
 * FrameworkRunSummary's shape stable, not chase every page that reads it).
 *
 * [2026-09-05, explicit user instruction] Rebuilt to match the look of
 * /backtest-report/metrics: the same AnalyticsGrid workspace (AG Grid,
 * grouped/pinned columns, CAGR/drawdown shading, CSV export, print layout)
 * that page uses, via the same lib/ui/AnalyticsGrid component -- rather
 * than the plain DataTable this page had before. The column set differs
 * because the underlying data differs (one point-in-time run per row, not
 * a multi-year StrategyReport): identity + the sweep's own config columns
 * (band, top_n, lookback, cadence, sizing) are added since they don't
 * exist on the backtest-report side at all.
 */

import { useMemo, useState } from 'react'
import { useQuery } from '@tanstack/react-query'
import type { ColDef, ColGroupDef, ValueFormatterParams } from 'ag-grid-community'
import type { ColumnDef } from '@tanstack/react-table'

import {
  AnalyticsGrid,
  AppShell,
  Card,
  CardContent,
  CardDescription,
  CardHeader,
  CardTitle,
  DataTable,
  HEATMAP_COLUMN,
  Input,
  StatCard,
} from '@/lib/ui'
import { listFrameworkRuns, type FrameworkRunSummary } from '@/shared/api/framework_backtest'
import { getOverallMomentumRank, type OverallRankRow } from '@/shared/api/momentum_overall_rank'
import { CampaignRunAnalysis } from './CampaignRunAnalysis'
import { EM_DASH, fmtPct, fmtNum, signClass } from './campaignFormat'

/** AG Grid value formatters need the null-safe wrapper on a plain fraction so
 * sorting stays numeric (the value getter returns the raw number) while the
 * cell still renders the same "—" the rest of the app uses for missing data. */
function fmt(format: (v: number) => string) {
  return (params: ValueFormatterParams<FrameworkRunSummary>) =>
    params.value == null || !Number.isFinite(Number(params.value)) ? EM_DASH : format(Number(params.value))
}

const NUMERIC: Partial<ColDef<FrameworkRunSummary>> = {
  type: 'numericColumn',
  cellClass: 'tabular-nums',
  width: 110,
}

/** Same shape as backtest-report's identityGroup: pinned so the row keeps
 * its label while the reader scrolls the metric columns sideways. */
function identityGroup(): ColGroupDef<FrameworkRunSummary> {
  return {
    headerName: 'Identity',
    children: [
      { field: 'strategy_code', headerName: 'Strategy', pinned: 'left', width: 100 },
      {
        field: 'band_id',
        headerName: 'Band',
        pinned: 'left',
        width: 80,
        valueFormatter: (p) => `M${String(p.value).padStart(2, '0')}`,
      },
      {
        field: 'run_id',
        headerName: 'Run ID',
        pinned: 'left',
        width: 240,
        cellClass: 'font-mono text-xs',
        suppressSizeToFit: true,
      },
    ],
  }
}

/** The sweep's own dimensions — not present on any backtest-report table
 * because that report compares strategies, not individual grid configs. */
function configGroup(): ColGroupDef<FrameworkRunSummary> {
  return {
    headerName: 'Config',
    children: [
      { field: 'top_n', headerName: 'Top N', width: 90, type: 'numericColumn' },
      { field: 'lookback_months', headerName: 'Lookback (mo)', width: 110, type: 'numericColumn' },
      { field: 'rebalance_cadence_days', headerName: 'Cadence (d)', width: 100, type: 'numericColumn' },
      { field: 'position_sizing', headerName: 'Sizing', width: 140 },
    ],
  }
}

function performanceGroup(): ColGroupDef<FrameworkRunSummary> {
  return {
    headerName: 'Performance',
    children: [
      { field: 'cagr', headerName: 'CAGR', valueFormatter: fmt((v) => fmtPct(v)), context: HEATMAP_COLUMN, ...NUMERIC },
      { field: 'sharpe_ratio', headerName: 'Sharpe', valueFormatter: fmt((v) => fmtNum(v)), ...NUMERIC },
      { field: 'sortino_ratio', headerName: 'Sortino', valueFormatter: fmt((v) => fmtNum(v)), ...NUMERIC },
      { field: 'calmar_ratio', headerName: 'Calmar', valueFormatter: fmt((v) => fmtNum(v)), ...NUMERIC },
      { field: 'max_drawdown', headerName: 'Max DD', valueFormatter: fmt((v) => fmtPct(v)), context: HEATMAP_COLUMN, ...NUMERIC },
      { field: 'volatility_annualized', headerName: 'Volatility', valueFormatter: fmt((v) => fmtPct(v)), ...NUMERIC },
      { field: 'win_rate', headerName: 'Win Rate', valueFormatter: fmt((v) => fmtPct(v, 1)), ...NUMERIC },
    ],
  }
}

function tradeGroup(): ColGroupDef<FrameworkRunSummary> {
  return {
    headerName: 'Trades',
    children: [
      { field: 'trade_count', headerName: 'Trades', width: 90, type: 'numericColumn' },
      { field: 'run_executed_at', headerName: 'Executed', width: 170 },
    ],
  }
}

/** One "Analyze" button per row, opening that run in the drill-down panel
 * below the grid (Long Term CAGR / Regular Returns / tax / rolling window /
 * benchmark / trade quality — see CampaignRunAnalysis.tsx). A plain button
 * rather than row selection: AnalyticsGrid's own row selection is scoped to
 * its trend chart and isn't exposed to the page. */
function analyzeColumn(onAnalyze: (run: FrameworkRunSummary) => void): ColDef<FrameworkRunSummary> {
  return {
    headerName: '',
    pinned: 'right',
    width: 100,
    sortable: false,
    filter: false,
    cellRenderer: (p: { data?: FrameworkRunSummary }) =>
      p.data ? (
        <button
          type="button"
          onClick={() => onAnalyze(p.data as FrameworkRunSummary)}
          className="text-xs text-primary underline-offset-2 hover:underline"
        >
          Analyze
        </button>
      ) : null,
  }
}

function buildColumns(
  onAnalyze: (run: FrameworkRunSummary) => void,
): Array<ColDef<FrameworkRunSummary> | ColGroupDef<FrameworkRunSummary>> {
  return [identityGroup(), configGroup(), performanceGroup(), tradeGroup(), analyzeColumn(onAnalyze)]
}

// Same thresholds as backtest-report's HEATMAP: full colour at +50%/-35% CAGR
// or drawdown is roughly where a run stops being ordinary and starts being
// an outlier.
const HEATMAP = { positiveCeiling: 0.5, negativeCeiling: 0.35 }

const rowId = (r: FrameworkRunSummary) => r.run_id

const OVERALL_RANK_COLUMNS: ColumnDef<OverallRankRow, unknown>[] = [
  { accessorKey: 'rank', header: 'Rank' },
  { accessorKey: 'ticker', header: 'Ticker' },
  {
    accessorKey: 'momentum_return',
    header: 'Momentum Return',
    meta: { align: 'right' },
    cell: ({ row }) => (
      <span className={signClass(row.original.momentum_return)}>{fmtPct(row.original.momentum_return)}</span>
    ),
  },
]

const LOOKBACK_OPTIONS = [1, 3, 6, 9, 12] as const

function OverallMomentumRankSection() {
  const [asOfDate, setAsOfDate] = useState('')
  const [lookbackMonths, setLookbackMonths] = useState<(typeof LOOKBACK_OPTIONS)[number]>(6)

  const { data, isLoading, error } = useQuery({
    queryKey: ['overall-momentum-rank', asOfDate, lookbackMonths],
    queryFn: () => getOverallMomentumRank({ as_of_date: asOfDate, lookback_months: lookbackMonths, top_n: 100 }),
    enabled: !!asOfDate,
  })

  return (
    <Card>
      <CardHeader>
        <CardTitle>Overall Momentum Rank (All ~800 Stocks)</CardTitle>
        <CardDescription>
          The full-universe momentum rank — computed once across every liquid stock (M13), the same source every
          band-scoped rank is sliced from. Not a separate computation; top 100 shown.
        </CardDescription>
      </CardHeader>
      <CardContent className="space-y-3">
        <div className="flex flex-wrap items-end gap-3">
          <div>
            <label className="block text-xs font-semibold uppercase text-muted-foreground mb-1">As-of Date</label>
            <Input type="date" value={asOfDate} onChange={(e) => setAsOfDate(e.target.value)} className="w-40" />
          </div>
          <div>
            <label className="block text-xs font-semibold uppercase text-muted-foreground mb-1">Lookback (months)</label>
            <select
              className="h-9 rounded-[var(--radius-token)] border border-border bg-transparent px-3 text-sm"
              value={lookbackMonths}
              onChange={(e) => setLookbackMonths(Number(e.target.value) as (typeof LOOKBACK_OPTIONS)[number])}
            >
              {LOOKBACK_OPTIONS.map((lb) => (
                <option key={lb} value={lb}>{lb}mo</option>
              ))}
            </select>
          </div>
        </div>
        {error ? (
          <p className="text-sm text-red-600">{(error as Error).message}</p>
        ) : (
          <DataTable
            columns={OVERALL_RANK_COLUMNS}
            data={data?.rows ?? []}
            isLoading={isLoading}
            emptyMessage={asOfDate ? 'No cached rank for this date.' : 'Pick a date to see the overall rank.'}
          />
        )}
      </CardContent>
    </Card>
  )
}

export function MomentumCampaignResultsPage() {
  const { data, isLoading } = useQuery({
    queryKey: ['framework-campaign-runs'],
    queryFn: () => listFrameworkRuns({ limit: 5000 }),
    // Ongoing background campaign — poll so the table fills in live rather
    // than requiring a manual refresh.
    refetchInterval: 30_000,
  })

  const runs = useMemo(() => data?.runs ?? [], [data?.runs])

  const [selectedRun, setSelectedRun] = useState<FrameworkRunSummary | null>(null)
  const columns = useMemo(() => buildColumns(setSelectedRun), [])

  const stats = useMemo(() => {
    const byStrategy = new Set(runs.map((r) => r.strategy_code))
    const cagrs = runs.map((r) => r.cagr).filter((v): v is number => typeof v === 'number')
    const best = cagrs.length ? Math.max(...cagrs) : null
    return {
      total: data?.total ?? 0,
      loaded: runs.length,
      strategies: byStrategy.size,
      bestCagr: best,
    }
  }, [runs, data?.total])

  return (
    <AppShell
      title="Momentum Campaign Results"
      description="Live results from the full native-engine campaign — every (strategy, band, lookback, cadence, position sizing) config. Refreshes every 30s while the campaign is still running."
    >
      <div className="grid grid-cols-2 gap-4 md:grid-cols-4 mb-4">
        <StatCard label="Jobs Persisted" value={String(stats.total)} />
        <StatCard label="Loaded (this page)" value={String(stats.loaded)} />
        <StatCard label="Distinct Strategies" value={String(stats.strategies)} />
        <StatCard label="Best CAGR So Far" value={fmtPct(stats.bestCagr)} />
      </div>

      <Card className="mb-4">
        <CardHeader>
          <CardTitle>All Runs</CardTitle>
          <CardDescription>
            CAGR and max drawdown are shaded by magnitude — scan for shape before reading a single number. Every
            other column sorts, filters and exports the same way as the Backtest Report grids.
          </CardDescription>
        </CardHeader>
        <CardContent>
          <AnalyticsGrid
            id="momentum-campaign-results"
            columns={columns}
            rows={runs}
            getRowId={rowId}
            heatmap={HEATMAP}
            isLoading={isLoading}
            csvFileName="momentum_campaign_results"
            title="Momentum campaign results — full native-engine grid"
            emptyMessage="No campaign runs persisted yet."
          />
        </CardContent>
      </Card>

      <div className="mb-4">
        <CampaignRunAnalysis runId={selectedRun?.run_id ?? null} runLabel={selectedRun?.strategy_id ?? null} />
      </div>

      <OverallMomentumRankSection />
    </AppShell>
  )
}
