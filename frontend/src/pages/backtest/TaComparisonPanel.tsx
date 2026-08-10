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

import {
  Badge,
  Card,
  CardContent,
  CardDescription,
  CardHeader,
  CardTitle,
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

type SortKey = 'sharpe' | 'cagr' | 'excess_return' | 'calmar' | 'max_drawdown' | 'post_tax'

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

function sortValue(s: TaComparisonStrategy, key: SortKey): number {
  const m = s.engine_metrics ?? {}
  switch (key) {
    case 'sharpe': return m.sharpe ?? -Infinity
    case 'cagr': return m.cagr ?? -Infinity
    case 'excess_return': return m.excess_return ?? -Infinity
    case 'calmar': return m.calmar ?? -Infinity
    // Least-negative drawdown is best, so a plain descending sort is correct.
    case 'max_drawdown': return m.max_drawdown ?? -Infinity
    case 'post_tax': return s.post_tax_return_on_capital ?? -Infinity
  }
}

export function TaComparisonPanel() {
  const [selected, setSelected] = useState<string | null>(null)
  const [sortKey, setSortKey] = useState<SortKey>('sharpe')
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

  const strategies = useMemo(() => {
    const rows = report.data?.strategies ?? []
    const filtered = styleFilter === 'all' ? rows : rows.filter((r) => r.style === styleFilter)
    return [...filtered].sort((a, b) => sortValue(b, sortKey) - sortValue(a, sortKey))
  }, [report.data, sortKey, styleFilter])

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
              <select
                aria-label="Sort by"
                className="h-9 rounded-md border bg-background px-2 text-sm"
                value={sortKey}
                onChange={(e) => setSortKey(e.target.value as SortKey)}
              >
                <option value="sharpe">Sharpe</option>
                <option value="cagr">CAGR</option>
                <option value="excess_return">Excess vs benchmark</option>
                <option value="calmar">Calmar</option>
                <option value="max_drawdown">Max drawdown</option>
                <option value="post_tax">Post-tax return</option>
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
        <CardHeader><CardTitle className="text-base">Performance &amp; Risk</CardTitle></CardHeader>
        <CardContent className="overflow-x-auto">
          <Table>
            <TableHeader>
              <TableRow>
                <TableHead>Template</TableHead>
                <TableHead>Style</TableHead>
                <TableHead className="text-right">CAGR</TableHead>
                <TableHead className="text-right">Benchmark</TableHead>
                <TableHead className="text-right">Excess</TableHead>
                <TableHead className="text-right">Sharpe</TableHead>
                <TableHead className="text-right">Sortino</TableHead>
                <TableHead className="text-right">Calmar</TableHead>
                <TableHead className="text-right">Max DD</TableHead>
                <TableHead className="text-right">Win rate</TableHead>
                <TableHead className="text-right">Trades</TableHead>
                <TableHead className="text-right">Avg hold (d)</TableHead>
                <TableHead className="text-right">Avg held</TableHead>
                <TableHead className="text-right">Signals/mo</TableHead>
              </TableRow>
            </TableHeader>
            <TableBody>
              {strategies.map((s) => {
                const m = s.engine_metrics ?? {}
                return (
                  <TableRow key={s.template_name}>
                    <TableCell className="font-medium">{s.template_name}</TableCell>
                    <TableCell><Badge variant="secondary">{s.style}</Badge></TableCell>
                    <TableCell className={cnRight(signClass(m.cagr))}>{pct(m.cagr)}</TableCell>
                    <TableCell className="text-right text-muted-foreground">{pct(m.benchmark_cagr)}</TableCell>
                    <TableCell className={cnRight(signClass(m.excess_return))}>{pct(m.excess_return)}</TableCell>
                    <TableCell className="text-right">{num(m.sharpe)}</TableCell>
                    <TableCell className="text-right">{num(m.sortino)}</TableCell>
                    <TableCell className="text-right">{num(m.calmar)}</TableCell>
                    <TableCell className="text-right text-red-600 dark:text-red-400">{pct(m.max_drawdown)}</TableCell>
                    <TableCell className="text-right">{pct(m.win_rate)}</TableCell>
                    <TableCell className="text-right">{s.closed_trades ?? '—'}</TableCell>
                    <TableCell className="text-right">{num(s.avg_holding_days, 1)}</TableCell>
                    <TableCell className="text-right">{num(s.holdings?.avg_concurrent_positions_calendar, 1)}</TableCell>
                    <TableCell className="text-right">{num(s.entries?.avg_entries_per_month, 1)}</TableCell>
                  </TableRow>
                )
              })}
            </TableBody>
          </Table>
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
