// Technical — Batch Backtest: single-place launcher + run history for every
// Technical Indicator backtest strategy (all 42 screener templates + combo
// strategies, run as one optimized batch — backtest/run_strategy_queue.py
// with max_workers>1, defer_db_writes, prefetch_feature_parquets, and a
// prewarmed shared OHLCV snapshot, see scripts/run_technical_recommended_
// strategies.py). Consolidates the Technical pillar's backtest entry points
// (Strategies/Screener, Backtest Sweep, Recommended Strategies, and this
// page) under one Technical menu, per user request.
import { useMemo } from 'react'
import { useQueryClient } from '@tanstack/react-query'
import type { ColumnDef } from '@tanstack/react-table'

import { AppShell, Card, CardContent, CardDescription, CardHeader, CardTitle, DataTable } from '@/lib/ui'
import { listBacktestRuns, type BacktestRunSummary } from '@/shared/api/backtest'
import { useQuery } from '@tanstack/react-query'
import { SweepTriggerButton } from './SweepTriggerButton'

function fmtPct(v: number | null | undefined) {
  return typeof v === 'number' ? `${(v * 100).toFixed(1)}%` : '—'
}
function fmtNum(v: number | null | undefined, digits = 2) {
  return typeof v === 'number' ? v.toFixed(digits) : '—'
}

export function TechnicalBatchBacktestPage() {
  const queryClient = useQueryClient()

  const runs = useQuery({
    queryKey: ['backtest-runs', 'technical'],
    queryFn: () => listBacktestRuns({ channel: 'technical', sort_by: 'cagr', limit: 1000 }),
  })

  const rows = runs.data?.runs ?? []

  const columns = useMemo<ColumnDef<BacktestRunSummary, unknown>[]>(
    () => [
      { accessorKey: 'strategy_id', header: 'Strategy' },
      { accessorKey: 'horizon_bucket', header: 'Horizon' },
      { accessorKey: 'mode', header: 'Mode' },
      {
        id: 'cagr',
        header: 'CAGR',
        meta: { align: 'right' },
        cell: (i) => fmtPct(i.row.original.metrics?.cagr),
      },
      {
        id: 'sharpe',
        header: 'Sharpe',
        meta: { align: 'right' },
        cell: (i) => fmtNum(i.row.original.metrics?.sharpe),
      },
      {
        id: 'sortino',
        header: 'Sortino',
        meta: { align: 'right', priority: 'medium' },
        cell: (i) => fmtNum(i.row.original.metrics?.sortino),
      },
      {
        id: 'win_rate',
        header: 'Win Rate',
        meta: { align: 'right' },
        cell: (i) => fmtPct(i.row.original.metrics?.win_rate ?? null),
      },
      {
        id: 'n_trades',
        header: 'Trades',
        meta: { align: 'right' },
        cell: (i) => i.row.original.metrics?.n_trades ?? '—',
      },
      {
        id: 'integrity',
        header: 'Integrity',
        cell: (i) =>
          i.row.original.integrity_passed === null ? '—' : i.row.original.integrity_passed ? 'Passed' : 'Failed',
      },
      {
        accessorKey: 'created_at',
        header: 'Run At',
        cell: (i) => new Date(i.getValue<string>()).toLocaleString(),
      },
    ],
    [],
  )

  return (
    <AppShell
      title="Technical — Batch Backtest"
      description="Launch every Technical Indicator backtest strategy (all screener templates plus composite/combo filters) as one optimized batch, and review persisted results."
    >
      <Card>
        <CardHeader>
          <CardTitle>Full Technical Sweep</CardTitle>
          <CardDescription>
            Runs backtest/run_strategy_queue.py across every screener template and composite/combo strategy in
            parallel (max_workers), with exit-check Parquet prefetch, deferred DB writes, and a shared OHLCV
            snapshot prewarmed once for the whole batch — instead of each strategy re-fetching the same data
            (scripts/run_technical_recommended_strategies.py).
          </CardDescription>
          <div className="mt-2 flex flex-wrap gap-3">
            <SweepTriggerButton
              label="Run Full Technical Sweep"
              triggerUrl="/api/v1/technical_backtest/recommended_strategies/trigger"
              statusUrlPrefix="/api/v1/technical_backtest/recommended_strategies/trigger/status"
              onCompleted={() => {
                queryClient.invalidateQueries({ queryKey: ['backtest-runs', 'technical'] })
                queryClient.invalidateQueries({ queryKey: ['technical-recommended-strategies'] })
              }}
            />
          </div>
        </CardHeader>
      </Card>

      <div className="mt-4">
        <Card>
          <CardHeader>
            <CardTitle>Technical Backtest Runs</CardTitle>
            <CardDescription>
              {runs.isLoading
                ? 'Loading…'
                : runs.error
                  ? 'Failed to load'
                  : `${rows.length} persisted run${rows.length === 1 ? '' : 's'} (channel = technical)`}
            </CardDescription>
          </CardHeader>
          <CardContent>
            {runs.error ? (
              <p className="text-sm text-red">
                Could not reach GET /api/v1/backtest/runs?channel=technical — {(runs.error as Error).message}
              </p>
            ) : (
              <DataTable
                columns={columns}
                data={rows}
                isLoading={runs.isLoading}
                emptyMessage="No Technical backtest runs yet — trigger the sweep above."
                maxHeight={640}
              />
            )}
          </CardContent>
        </Card>
      </div>
    </AppShell>
  )
}
