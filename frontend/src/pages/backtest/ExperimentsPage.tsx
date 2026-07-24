// Experiments comparison page — one row per backtest_runs entry, so the
// 270-job exit-variant x template/preset matrix (backtest/reports/
// experiment_matrix_45x6.json) can be browsed/compared as it lands. Point
// of this page (per the user's original ask): "distinguish what strategy
// is giving what returns... to make decisions appropriately" — so for
// each strategy_id, the exit_policy_variant with the best Sortino (the
// risk-adjusted ratio metrics.py actually computes; no `sharpe` field
// exists there) is badged "Best" inline.
import { useMemo, useState } from 'react'
import { useQuery } from '@tanstack/react-query'
import type { ColumnDef } from '@tanstack/react-table'

import {
  AppShell,
  Badge,
  Card,
  CardContent,
  CardDescription,
  CardHeader,
  CardTitle,
  DataTable,
} from '@/lib/ui'
import {
  listBacktestExperiments,
  experimentTradeLogUrl,
  type BacktestChannel,
  type ExitPolicyVariant,
  type ExperimentRow,
} from '@/shared/api/backtest'

const CHANNELS: BacktestChannel[] = ['technical', 'fundamental', 'ml', 'momentum']
const EXIT_VARIANTS: ExitPolicyVariant[] = [
  'baseline',
  'condition',
  'combined',
  'trailing',
  'atr_adaptive',
  'regime_conditional',
]

function fmtPct(v: number | null | undefined) {
  return typeof v === 'number' ? `${(v * 100).toFixed(1)}%` : '—'
}
function fmtNum(v: number | null | undefined, digits = 2) {
  return typeof v === 'number' ? v.toFixed(digits) : '—'
}
function fmtDays(v: number | null | undefined) {
  return typeof v === 'number' ? `${v.toFixed(0)}d` : '—'
}

/** run_id -> true iff this run has the best Sortino among all rows sharing
 * its strategy_id. Runs with sortino == null never win (can't compare). */
function bestSortinoByStrategy(rows: ExperimentRow[]): Set<string> {
  const bestByStrategy = new Map<string, { run_id: string; sortino: number }>()
  for (const r of rows) {
    if (typeof r.sortino !== 'number') continue
    const current = bestByStrategy.get(r.strategy_id)
    if (!current || r.sortino > current.sortino) {
      bestByStrategy.set(r.strategy_id, { run_id: r.run_id, sortino: r.sortino })
    }
  }
  return new Set(Array.from(bestByStrategy.values()).map((v) => v.run_id))
}

export function ExperimentsPage() {
  const [strategyId, setStrategyId] = useState('')
  const [channel, setChannel] = useState<BacktestChannel | ''>('')
  const [exitVariant, setExitVariant] = useState<ExitPolicyVariant | ''>('')

  const experiments = useQuery({
    queryKey: ['backtest-experiments', strategyId, channel, exitVariant],
    queryFn: () =>
      listBacktestExperiments({
        strategy_id: strategyId || undefined,
        channel: channel || undefined,
        exit_policy_variant: exitVariant || undefined,
        limit: 1000,
      }),
  })

  const rows = experiments.data?.experiments ?? []
  const bestRunIds = useMemo(() => bestSortinoByStrategy(rows), [rows])

  const strategyOptions = useMemo(
    () => Array.from(new Set(rows.map((r) => r.strategy_id))).sort(),
    [rows],
  )

  const columns = useMemo<ColumnDef<ExperimentRow, unknown>[]>(
    () => [
      {
        accessorKey: 'strategy_id',
        header: 'Strategy',
        cell: (i) => <span className="font-mono-data">{i.getValue<string>()}</span>,
      },
      { accessorKey: 'channel', header: 'Channel' },
      {
        accessorKey: 'exit_policy_variant',
        header: 'Exit Variant',
        cell: (i) => {
          const row = i.row.original
          const isBest = bestRunIds.has(row.run_id)
          return (
            <span className="inline-flex items-center gap-1.5">
              <span className={isBest ? 'font-semibold' : undefined}>{row.exit_policy_variant ?? '—'}</span>
              {isBest ? <Badge variant="success">Best</Badge> : null}
            </span>
          )
        },
      },
      { accessorKey: 'regime_label', header: 'Regime', cell: (i) => i.getValue<string | null>() ?? '—' },
      {
        accessorKey: 'sortino',
        header: 'Sortino',
        meta: { align: 'right' },
        cell: (i) => fmtNum(i.getValue<number | null>()),
      },
      {
        accessorKey: 'cagr',
        header: 'CAGR',
        meta: { align: 'right' },
        cell: (i) => fmtPct(i.getValue<number | null>()),
      },
      {
        accessorKey: 'max_drawdown',
        header: 'Max DD',
        meta: { align: 'right' },
        cell: (i) => fmtPct(i.getValue<number | null>()),
      },
      {
        accessorKey: 'win_rate',
        header: 'Win Rate',
        meta: { align: 'right' },
        cell: (i) => fmtPct(i.getValue<number | null>()),
      },
      {
        accessorKey: 'avg_days_held',
        header: 'Avg Days Held',
        meta: { align: 'right', priority: 'medium' },
        cell: (i) => fmtDays(i.getValue<number | null>()),
      },
      {
        accessorKey: 'n_trades',
        header: 'Trades',
        meta: { align: 'right' },
        cell: (i) => i.getValue<number | null>() ?? '—',
      },
      {
        id: 'trade_log',
        header: 'Trade Log',
        cell: (i) =>
          i.row.original.has_trade_log ? (
            <a
              href={experimentTradeLogUrl(i.row.original.run_id)}
              className="text-sm text-primary underline underline-offset-2"
              download
            >
              CSV
            </a>
          ) : (
            <span className="text-muted-foreground">—</span>
          ),
        meta: { priority: 'low' },
      },
    ],
    [bestRunIds],
  )

  return (
    <AppShell
      title="Backtest — Experiments"
      description="Every backtest_runs row, one per Entry-template x Exit-variant combination — compare which pairing wins per strategy."
    >
      <Card>
        <CardHeader>
          <CardTitle>Experiments</CardTitle>
          <CardDescription>
            {experiments.isLoading
              ? 'Loading…'
              : experiments.error
                ? 'Failed to load'
                : `${rows.length} run${rows.length === 1 ? '' : 's'} — "Best" badges the highest-Sortino exit variant per strategy.`}
          </CardDescription>
        </CardHeader>
        <CardContent>
          <div className="mb-4 flex flex-wrap items-end gap-3">
            <select
              className="h-9 rounded-[var(--radius-token)] border border-border bg-transparent px-3 text-sm"
              value={strategyId}
              onChange={(e) => setStrategyId(e.target.value)}
            >
              <option value="">All strategies</option>
              {strategyOptions.map((s) => (
                <option key={s} value={s}>
                  {s}
                </option>
              ))}
            </select>
            <select
              className="h-9 rounded-[var(--radius-token)] border border-border bg-transparent px-3 text-sm"
              value={channel}
              onChange={(e) => setChannel(e.target.value as BacktestChannel | '')}
            >
              <option value="">All channels</option>
              {CHANNELS.map((c) => (
                <option key={c} value={c}>
                  {c}
                </option>
              ))}
            </select>
            <select
              className="h-9 rounded-[var(--radius-token)] border border-border bg-transparent px-3 text-sm"
              value={exitVariant}
              onChange={(e) => setExitVariant(e.target.value as ExitPolicyVariant | '')}
            >
              <option value="">All exit variants</option>
              {EXIT_VARIANTS.map((v) => (
                <option key={v} value={v}>
                  {v}
                </option>
              ))}
            </select>
          </div>

          {experiments.error ? (
            <p className="text-sm text-red">
              Could not reach GET /api/v1/backtest/experiments — {(experiments.error as Error).message}
            </p>
          ) : (
            <DataTable
              columns={columns}
              data={rows}
              isLoading={experiments.isLoading}
              emptyMessage="No experiment runs yet — the experiment_matrix_45x6.json queue hasn't populated backtest_runs yet."
            />
          )}
        </CardContent>
      </Card>
    </AppShell>
  )
}
