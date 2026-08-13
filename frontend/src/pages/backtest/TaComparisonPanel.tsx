// TA strategy comparison panel — renders one collated comparison report from
// backtest/ta_comparison_report.py (served by datastore/api/routers/
// backtest_reports.py). A report covers a whole queue: up to 65 screener
// templates over 2007-2026.
//
// Basis note, which matters for reading the numbers: every figure here is
// REALIZED (computed from closed trades in the trade books) EXCEPT the
// rolling-return block, which is mark-to-market off the equity curve. The
// header states this so the two are never silently compared.
import { useMemo, useState } from 'react'
import { useQuery } from '@tanstack/react-query'
import type { ColumnDef } from '@tanstack/react-table'

import {
  Badge,
  Card,
  CardContent,
  CardDescription,
  CardHeader,
  CardTitle,
  DataTable,
  Table,
  TableHeader,
  TableBody,
  TableRow,
  TableHead,
  TableCell,
} from '@/lib/ui'
import {
  listTaComparisons,
  getTaComparison,
  type TaComparisonStrategy,
} from '@/shared/api/backtest'

const ROLLING_WINDOWS = ['2y', '3y', '4y', '5y'] as const

const pct = (v: number | null | undefined) =>
  v === null || v === undefined || Number.isNaN(v) ? '—' : `${(v * 100).toFixed(2)}%`

const num = (v: number | null | undefined, dp = 2) =>
  v === null || v === undefined || Number.isNaN(v) ? '—' : v.toFixed(dp)

/** Indian formatting: ₹1,23,45,678 (lakh/crore grouping), not ₹12,345,678. */
const inr = (v: number | null | undefined) =>
  v === null || v === undefined || Number.isNaN(v)
    ? '—'
    : `₹${new Intl.NumberFormat('en-IN', { maximumFractionDigits: 0 }).format(v)}`

const signClass = (v: number | null | undefined) =>
  v === null || v === undefined ? '' : v > 0 ? 'text-emerald-600 dark:text-emerald-400' : v < 0 ? 'text-red-600 dark:text-red-400' : ''

export function TaComparisonPanel() {
  const [selected, setSelected] = useState<string | null>(null)
  const [styleFilter, setStyleFilter] = useState<string>('all')

  const list = useQuery({
    queryKey: ['ta-comparisons'],
    queryFn: listTaComparisons,
    refetchInterval: 60_000, // the autopilot rewrites reports as the queue advances
  })

  const available = list.data?.comparisons ?? []
  const active = selected ?? available[0]?.name ?? null

  const report = useQuery({
    queryKey: ['ta-comparison', active],
    queryFn: () => getTaComparison(active as string),
    enabled: Boolean(active),
    refetchInterval: 60_000,
  })

  // Ordering is DataTable's job now (click any column header). The panel only
  // filters; the hand-rolled comparator and its "Sort by" dropdown are gone,
  // so this table sorts the same way as every other table in the app.
  const strategies = useMemo(() => {
    const rows = report.data?.strategies ?? []
    return styleFilter === 'all' ? rows : rows.filter((r) => r.style === styleFilter)
  }, [report.data, styleFilter])

  const headlineColumns = useMemo<ColumnDef<TaComparisonStrategy, unknown>[]>(
    () => [
      { id: 'template', accessorFn: (s) => s.template_name, header: 'Template', size: 150 },
      {
        id: 'style',
        accessorFn: (s) => s.style,
        header: 'Style',
        size: 110,
        cell: (i) => <Badge variant="secondary">{i.row.original.style}</Badge>,
      },
      {
        id: 'cagr',
        accessorFn: (s) => s.engine_metrics?.cagr ?? null,
        header: 'CAGR',
        size: 90,
        meta: { align: 'right' },
        cell: (i) => (
          <span className={signClass(i.row.original.engine_metrics?.cagr)}>
            {pct(i.row.original.engine_metrics?.cagr)}
          </span>
        ),
      },
      {
        id: 'benchmark',
        accessorFn: (s) => s.engine_metrics?.benchmark_cagr ?? null,
        header: 'Benchmark',
        size: 100,
        meta: { align: 'right', priority: 'medium' },
        cell: (i) => (
          <span className="text-muted-foreground">
            {pct(i.row.original.engine_metrics?.benchmark_cagr)}
          </span>
        ),
      },
      {
        id: 'excess',
        accessorFn: (s) => s.engine_metrics?.excess_return ?? null,
        header: 'Excess',
        size: 90,
        meta: { align: 'right' },
        cell: (i) => (
          <span className={signClass(i.row.original.engine_metrics?.excess_return)}>
            {pct(i.row.original.engine_metrics?.excess_return)}
          </span>
        ),
      },
      {
        id: 'sharpe',
        accessorFn: (s) => s.engine_metrics?.sharpe ?? null,
        header: 'Sharpe',
        size: 80,
        meta: { align: 'right' },
        cell: (i) => num(i.row.original.engine_metrics?.sharpe),
      },
      {
        id: 'sortino',
        accessorFn: (s) => s.engine_metrics?.sortino ?? null,
        header: 'Sortino',
        size: 80,
        meta: { align: 'right', priority: 'medium' },
        cell: (i) => num(i.row.original.engine_metrics?.sortino),
      },
      {
        id: 'calmar',
        accessorFn: (s) => s.engine_metrics?.calmar ?? null,
        header: 'Calmar',
        size: 80,
        meta: { align: 'right', priority: 'medium' },
        cell: (i) => num(i.row.original.engine_metrics?.calmar),
      },
      {
        id: 'maxdd',
        accessorFn: (s) => s.engine_metrics?.max_drawdown ?? null,
        header: 'Max DD',
        size: 90,
        meta: { align: 'right' },
        cell: (i) => (
          <span className="text-red-600 dark:text-red-400">
            {pct(i.row.original.engine_metrics?.max_drawdown)}
          </span>
        ),
      },
      {
        id: 'winrate',
        accessorFn: (s) => s.engine_metrics?.win_rate ?? null,
        header: 'Win rate',
        size: 90,
        meta: { align: 'right' },
        cell: (i) => pct(i.row.original.engine_metrics?.win_rate),
      },
      {
        id: 'trades',
        accessorFn: (s) => s.closed_trades ?? null,
        header: 'Trades',
        size: 80,
        meta: { align: 'right' },
        cell: (i) => i.row.original.closed_trades ?? '—',
      },
      {
        id: 'avghold',
        accessorFn: (s) => s.avg_holding_days ?? null,
        header: 'Avg hold (d)',
        size: 100,
        meta: { align: 'right' },
        cell: (i) => num(i.row.original.avg_holding_days, 1),
      },
      {
        id: 'avgheld',
        accessorFn: (s) => s.holdings?.avg_concurrent_positions_calendar ?? null,
        header: 'Avg held',
        size: 90,
        meta: { align: 'right', priority: 'low', group: 'activity' },
        cell: (i) => num(i.row.original.holdings?.avg_concurrent_positions_calendar, 1),
      },
      {
        id: 'signals',
        accessorFn: (s) => s.entries?.avg_entries_per_month ?? null,
        header: 'Signals/mo',
        size: 95,
        meta: { align: 'right', priority: 'low', group: 'activity' },
        cell: (i) => num(i.row.original.entries?.avg_entries_per_month, 1),
      },
    ],
    [],
  )

  const styles = useMemo(
    () => Array.from(new Set((report.data?.strategies ?? []).map((r) => r.style))).sort(),
    [report.data],
  )

  const years = useMemo(
    () =>
      Array.from(
        new Set((report.data?.strategies ?? []).flatMap((r) => (r.yearly ?? []).map((y) => y.trading_year))),
      ).sort(),
    [report.data],
  )

  if (list.isLoading) return <Card><CardContent className="py-8 text-center text-muted-foreground">Loading comparisons…</CardContent></Card>

  if (!available.length) {
    return (
      <Card>
        <CardHeader>
          <CardTitle>Strategy Comparison</CardTitle>
          <CardDescription>No collated comparison reports yet.</CardDescription>
        </CardHeader>
        <CardContent className="text-sm text-muted-foreground">
          Reports appear here once a strategy queue completes and
          <code className="mx-1 rounded bg-muted px-1 py-0.5">backtest.ta_comparison_report</code>
          has collated it.
        </CardContent>
      </Card>
    )
  }

  const meta = report.data
  const activeRegime = meta?.tax_regime ?? 'ltcg_12_5pct_1_25L'

  return (
    <div className="space-y-4">
      <Card>
        <CardHeader>
          <div className="flex flex-wrap items-start justify-between gap-3">
            <div>
              <CardTitle>Strategy Comparison</CardTitle>
              <CardDescription>
                {meta
                  ? `${meta.n_strategies} strategies · ${meta.queue_suffix} · generated ${new Date(meta.generated_at).toLocaleString('en-IN')}`
                  : 'Loading…'}
              </CardDescription>
            </div>
            <div className="flex flex-wrap gap-2">
              <select
                aria-label="Comparison report"
                className="h-9 rounded-md border bg-background px-2 text-sm"
                value={active ?? ''}
                onChange={(e) => setSelected(e.target.value)}
              >
                {available.map((c) => (
                  <option key={c.name} value={c.name}>
                    {c.queue_suffix} · {c.tax_regime}
                  </option>
                ))}
              </select>
              <select
                aria-label="Filter by style"
                className="h-9 rounded-md border bg-background px-2 text-sm"
                value={styleFilter}
                onChange={(e) => setStyleFilter(e.target.value)}
              >
                <option value="all">All styles</option>
                {styles.map((s) => <option key={s} value={s}>{s}</option>)}
              </select>
            </div>
          </div>
        </CardHeader>
        <CardContent className="space-y-2">
          <p className="text-xs text-muted-foreground">
            Basis: <strong>{meta?.basis ?? 'realized'}</strong> — all figures are realised from closed
            trades, except rolling returns which are mark-to-market off the equity curve. Tax regime:{' '}
            <strong>{meta?.tax_regime}</strong> (STCG 20% under 1 year). Financial years run Apr 1 – Mar 31.
          </p>
          {meta?.failed_reports?.length ? (
            <p className="text-xs text-amber-600 dark:text-amber-400">
              {meta.failed_reports.length} report(s) failed to collate and are excluded:{' '}
              {meta.failed_reports.map((f) => f.report).join(', ')}
            </p>
          ) : null}
        </CardContent>
      </Card>

      {/* Headline metrics */}
      <Card>
        <CardHeader>
          <CardTitle className="text-base">Performance &amp; Risk</CardTitle>
          <CardDescription>Click any column header to sort.</CardDescription>
        </CardHeader>
        <CardContent>
          <DataTable
            columns={headlineColumns}
            data={strategies}
            isLoading={report.isLoading}
            emptyMessage="No strategies in this report."
          />
        </CardContent>
      </Card>

      {/* Rolling returns */}
      <Card>
        <CardHeader>
          <CardTitle className="text-base">Rolling Returns</CardTitle>
          <CardDescription>
            Mark-to-market, annualised median per window, with the share of windows that were positive.
          </CardDescription>
        </CardHeader>
        <CardContent className="overflow-x-auto">
          <Table>
            <TableHeader>
              <TableRow>
                <TableHead>Template</TableHead>
                {ROLLING_WINDOWS.map((w) => (
                  <TableHead key={w} colSpan={2} className="text-center">{w}</TableHead>
                ))}
              </TableRow>
              <TableRow>
                <TableHead />
                {ROLLING_WINDOWS.map((w) => [
                  <TableHead key={`${w}-m`} className="text-right text-xs font-normal">median p.a.</TableHead>,
                  <TableHead key={`${w}-p`} className="text-right text-xs font-normal">% positive</TableHead>,
                ])}
              </TableRow>
            </TableHeader>
            <TableBody>
              {strategies.map((s) => (
                <TableRow key={s.template_name}>
                  <TableCell className="font-medium">{s.template_name}</TableCell>
                  {ROLLING_WINDOWS.map((w) => {
                    const r = s.rolling?.[w] ?? undefined
                    return [
                      <TableCell key={`${w}-m`} className={cnRight(signClass(r?.median_annualized))}>
                        {pct(r?.median_annualized)}
                      </TableCell>,
                      <TableCell key={`${w}-p`} className="text-right text-muted-foreground">
                        {pct(r?.positive_share)}
                      </TableCell>,
                    ]
                  })}
                </TableRow>
              ))}
            </TableBody>
          </Table>
        </CardContent>
      </Card>

      {/* Tax */}
      <Card>
        <CardHeader>
          <CardTitle className="text-base">Pre-tax vs Post-tax</CardTitle>
          <CardDescription>
            Tax is computed per financial year, because the LTCG exemption is a per-year allowance.
          </CardDescription>
        </CardHeader>
        <CardContent className="overflow-x-auto">
          <Table>
            <TableHeader>
              <TableRow>
                <TableHead>Template</TableHead>
                <TableHead className="text-right">Capital</TableHead>
                <TableHead className="text-right">Pre-tax P&amp;L</TableHead>
                <TableHead className="text-right">Total tax</TableHead>
                <TableHead className="text-right">Post-tax P&amp;L</TableHead>
                <TableHead className="text-right">Post-tax return</TableHead>
              </TableRow>
            </TableHeader>
            <TableBody>
              {strategies.map((s) => {
                const preTax =
                  s.post_tax_pnl_inr !== null && s.total_tax_inr !== null
                    ? s.post_tax_pnl_inr + s.total_tax_inr
                    : null
                return (
                  <TableRow key={s.template_name}>
                    <TableCell className="font-medium">{s.template_name}</TableCell>
                    <TableCell className="text-right text-muted-foreground">{inr(s.initial_capital)}</TableCell>
                    <TableCell className={cnRight(signClass(preTax))}>{inr(preTax)}</TableCell>
                    <TableCell className="text-right text-muted-foreground">{inr(s.total_tax_inr)}</TableCell>
                    <TableCell className={cnRight(signClass(s.post_tax_pnl_inr))}>{inr(s.post_tax_pnl_inr)}</TableCell>
                    <TableCell className={cnRight(signClass(s.post_tax_return_on_capital))}>
                      {pct(s.post_tax_return_on_capital)}
                    </TableCell>
                  </TableRow>
                )
              })}
            </TableBody>
          </Table>
        </CardContent>
      </Card>

      {/* Tax by financial year */}
      <Card>
        <CardHeader>
          <CardTitle className="text-base">Tax by Financial Year</CardTitle>
          <CardDescription>
            Assessed per year, not once on the whole period — the LTCG exemption is a
            per-year allowance, so pooling {years.length} years would grant it once instead of{' '}
            {years.length} times and overstate the liability. A loss-making year pays nil and is
            not carried forward.
          </CardDescription>
        </CardHeader>
        <CardContent className="overflow-x-auto">
          {strategies.some((s) => Object.keys(s.taxes?.[activeRegime]?.per_year ?? {}).length) ? (
            <Table>
              <TableHeader>
                <TableRow>
                  <TableHead className="sticky left-0 bg-background">Template</TableHead>
                  {years.map((y) => (
                    <TableHead key={y} className="text-right whitespace-nowrap">{y}</TableHead>
                  ))}
                  <TableHead className="text-right whitespace-nowrap">Total</TableHead>
                </TableRow>
              </TableHeader>
              <TableBody>
                {strategies.map((s) => {
                  const regime = s.taxes?.[activeRegime]
                  const py = regime?.per_year ?? {}
                  return (
                    <TableRow key={s.template_name}>
                      <TableCell className="sticky left-0 bg-background font-medium">
                        {s.template_name}
                      </TableCell>
                      {years.map((y) => (
                        <TableCell key={y} className="text-right text-muted-foreground">
                          {inr(py[y]?.total_tax_inr)}
                        </TableCell>
                      ))}
                      <TableCell className="text-right font-medium">
                        {inr(regime?.total_tax_inr)}
                      </TableCell>
                    </TableRow>
                  )
                })}
              </TableBody>
            </Table>
          ) : (
            <p className="text-sm text-muted-foreground">
              No per-year tax detail in this report. Reports generated before 2026-08-10 omit it —
              <code className="mx-1 rounded bg-muted px-1 py-0.5">tax_liability()</code>
              computed the breakdown but did not return it. Headline totals were unaffected.
            </p>
          )}
        </CardContent>
      </Card>

      {/* Year on year */}
      <Card>
        <CardHeader>
          <CardTitle className="text-base">Year-on-Year Returns</CardTitle>
          <CardDescription>Indian financial years (Apr 1 – Mar 31), return on capital.</CardDescription>
        </CardHeader>
        <CardContent className="overflow-x-auto">
          <Table>
            <TableHeader>
              <TableRow>
                <TableHead className="sticky left-0 bg-background">Template</TableHead>
                {years.map((y) => <TableHead key={y} className="text-right whitespace-nowrap">{y}</TableHead>)}
              </TableRow>
            </TableHeader>
            <TableBody>
              {strategies.map((s) => {
                const byYear = new Map((s.yearly ?? []).map((y) => [y.trading_year, y]))
                return (
                  <TableRow key={s.template_name}>
                    <TableCell className="sticky left-0 bg-background font-medium">{s.template_name}</TableCell>
                    {years.map((y) => {
                      const row = byYear.get(y)
                      return (
                        <TableCell key={y} className={cnRight(signClass(row?.return_pct))}>
                          {pct(row?.return_pct)}
                        </TableCell>
                      )
                    })}
                  </TableRow>
                )
              })}
            </TableBody>
          </Table>
        </CardContent>
      </Card>
    </div>
  )
}

/** Right-aligned cell class, optionally with a sign colour. */
function cnRight(extra?: string) {
  return extra ? `text-right ${extra}` : 'text-right'
}

export default TaComparisonPanel
