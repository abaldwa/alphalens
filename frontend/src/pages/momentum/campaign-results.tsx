/**
 * pages/momentum/campaign-results.tsx
 *
 * Results table for the native-engine full campaign (momentum_framework/
 * scripts/run_full_campaign.py -> framework_backtest_runs), pulled through
 * shared/api/framework_backtest.ts's typed client only -- this page never
 * touches config_json/metrics_json or a DB path directly (see that
 * file's docstring on why: a frontend rewrite should only need to keep
 * FrameworkRunSummary's shape stable, not chase every page that reads it).
 */

import { useMemo, useState } from 'react'
import { useQuery } from '@tanstack/react-query'
import type { ColumnDef } from '@tanstack/react-table'

import { Card, CardContent, CardHeader, CardTitle, DataTable, Input, StatCard } from '@/lib/ui'
import { listFrameworkRuns, type FrameworkRunSummary } from '@/shared/api/framework_backtest'
import { getOverallMomentumRank, type OverallRankRow } from '@/shared/api/momentum_overall_rank'

function fmtPct(v: number | null) {
  return typeof v === 'number' ? `${(v * 100).toFixed(2)}%` : '—'
}
function fmtNum(v: number | null, digits = 2) {
  return typeof v === 'number' ? v.toFixed(digits) : '—'
}

const OVERALL_RANK_COLUMNS: ColumnDef<OverallRankRow, unknown>[] = [
  { accessorKey: 'rank', header: 'Rank' },
  { accessorKey: 'ticker', header: 'Ticker' },
  { accessorKey: 'momentum_return', header: 'Momentum Return', cell: ({ row }) => fmtPct(row.original.momentum_return) },
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
        <p className="text-sm text-gray-600 mt-1">
          The full-universe momentum rank — computed once across every liquid stock (M13), the same source every
          band-scoped rank is sliced from. Not a separate computation; top 100 shown.
        </p>
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

const COLUMNS: ColumnDef<FrameworkRunSummary, unknown>[] = [
  { accessorKey: 'strategy_code', header: 'Strategy' },
  { accessorKey: 'band_id', header: 'Band', cell: ({ row }) => `M${String(row.original.band_id).padStart(2, '0')}` },
  { accessorKey: 'top_n', header: 'Top N' },
  { accessorKey: 'lookback_months', header: 'Lookback (mo)' },
  { accessorKey: 'rebalance_cadence_days', header: 'Cadence (d)' },
  { accessorKey: 'position_sizing', header: 'Sizing' },
  { accessorKey: 'cagr', header: 'CAGR', cell: ({ row }) => fmtPct(row.original.cagr) },
  { accessorKey: 'sharpe_ratio', header: 'Sharpe', cell: ({ row }) => fmtNum(row.original.sharpe_ratio) },
  { accessorKey: 'max_drawdown', header: 'Max DD', cell: ({ row }) => fmtPct(row.original.max_drawdown) },
  { accessorKey: 'trade_count', header: 'Trades' },
  { accessorKey: 'run_executed_at', header: 'Executed' },
]

export function MomentumCampaignResultsPage() {
  const { data, isLoading } = useQuery({
    queryKey: ['framework-campaign-runs'],
    queryFn: () => listFrameworkRuns({ limit: 5000 }),
    // Ongoing background campaign — poll so the table fills in live rather
    // than requiring a manual refresh.
    refetchInterval: 30_000,
  })

  const runs = useMemo(() => data?.runs ?? [], [data?.runs])

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
    <div className="space-y-4 p-4">
      <div>
        <h1 className="text-3xl font-bold mb-2">Momentum Campaign Results</h1>
        <p className="text-gray-600">
          Live results from the full native-engine campaign — every (strategy, band, lookback, cadence,
          position sizing) config. Refreshes every 30s while the campaign is still running.
        </p>
      </div>

      <div className="grid grid-cols-2 gap-4 md:grid-cols-4">
        <StatCard label="Jobs Persisted" value={String(stats.total)} />
        <StatCard label="Loaded (this page)" value={String(stats.loaded)} />
        <StatCard label="Distinct Strategies" value={String(stats.strategies)} />
        <StatCard label="Best CAGR So Far" value={fmtPct(stats.bestCagr)} />
      </div>

      <Card>
        <CardHeader>
          <CardTitle>All Runs</CardTitle>
        </CardHeader>
        <CardContent>
          <DataTable columns={COLUMNS} data={runs} isLoading={isLoading} emptyMessage="No campaign runs persisted yet." />
        </CardContent>
      </Card>

      <OverallMomentumRankSection />
    </div>
  )
}
