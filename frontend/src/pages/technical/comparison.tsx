// Technical Analysis long-history comparison (2009-04-01 -> 2026-08-10).
//
// backtest/ta_comparison_report.py's dataset, served at
// /api/v1/technical_backtest/comparison: every screener template x exit variant
// with CAGR/Sharpe/Sortino/Calmar, per-FY year-on-year returns, and rolling
// 2/3/4/5-year return distributions.
//
// TWO DELIBERATE PRESENTATION DECISIONS
// 1. Rolling returns are shown as worst/median/best, never as one number. The
//    entire point of a rolling measure is to show how much the answer depends
//    on when you started; collapsing it to a mean throws that away.
// 2. The annual-reset ("income") measure is NOT charted here. Its numbers are
//    provisional — FY tax is reported but never debited from the portfolio, so
//    equity compounds tax-free. Rather than render a caveat next to a number
//    people will screenshot anyway, this page states the status and withholds
//    the figures until the annual-reset sweep is re-run.
import { useMemo, useState } from 'react'
import { useQuery } from '@tanstack/react-query'
import type { ColumnDef } from '@tanstack/react-table'

import {
  AppShell, Card, CardContent, CardDescription, CardHeader, CardTitle, DataTable,
} from '@/lib/ui'
import { apiGet } from '@/shared/api/client'
import type { TAComparisonReport, TAComparisonStrategy } from './types'

function fmtPct(v: number | null | undefined, digits = 2) {
  return typeof v === 'number' ? `${v.toFixed(digits)}%` : '—'
}
function fmtNum(v: number | null | undefined, digits = 2) {
  return typeof v === 'number' ? v.toFixed(digits) : '—'
}
function fmtInr(v: number | null | undefined) {
  return typeof v === 'number' ? `₹${Math.round(v).toLocaleString('en-IN')}` : '—'
}

export function TechnicalComparisonPage() {
  const [variantFilter, setVariantFilter] = useState<string>('')
  const [selected, setSelected] = useState<TAComparisonStrategy | null>(null)

  const report = useQuery({
    queryKey: ['technical-comparison'],
    queryFn: () => apiGet<TAComparisonReport>('/api/v1/technical_backtest/comparison'),
  })

  const rows = report.data?.strategies ?? []
  const benchmark = rows.find((r) => r.lump)?.lump?.benchmark_cagr_pct ?? null

  const variantOptions = useMemo(
    () => Array.from(new Set(rows.map((r) => r.exit_variant))).sort(),
    [rows],
  )
  const filtered = useMemo(
    () => rows.filter((r) => (variantFilter ? r.exit_variant === variantFilter : true) && r.lump),
    [rows, variantFilter],
  )

  const beatBenchmark = useMemo(
    () => (benchmark === null ? 0 : filtered.filter((r) => (r.lump?.cagr_pct ?? 0) > benchmark).length),
    [filtered, benchmark],
  )

  const columns = useMemo<ColumnDef<TAComparisonStrategy>[]>(() => [
    { header: 'Template', accessorFn: (r) => r.template, id: 'template' },
    { header: 'Exit variant', accessorFn: (r) => r.exit_variant, id: 'variant' },
    {
      header: 'CAGR', id: 'cagr',
      accessorFn: (r) => r.lump?.cagr_pct ?? null,
      cell: (c) => {
        const v = c.getValue<number | null>()
        const beats = benchmark !== null && typeof v === 'number' && v > benchmark
        return <span className={beats ? 'font-semibold text-emerald-600 dark:text-emerald-400' : undefined}>{fmtPct(v)}</span>
      },
    },
    { header: 'Sharpe', id: 'sharpe', accessorFn: (r) => r.lump?.sharpe ?? null, cell: (c) => fmtNum(c.getValue<number | null>()) },
    { header: 'Sortino', id: 'sortino', accessorFn: (r) => r.lump?.sortino ?? null, cell: (c) => fmtNum(c.getValue<number | null>()) },
    { header: 'Calmar', id: 'calmar', accessorFn: (r) => r.lump?.calmar ?? null, cell: (c) => fmtNum(c.getValue<number | null>()) },
    {
      header: 'Max DD', id: 'dd',
      accessorFn: (r) => r.lump?.max_drawdown_pct ?? null,
      cell: (c) => <span className="text-rose-600 dark:text-rose-400">{fmtPct(c.getValue<number | null>(), 1)}</span>,
    },
    {
      header: '5y rolling (worst / med / best)', id: 'roll5',
      accessorFn: (r) => r.lump?.rolling_returns?.['5y']?.median_pct ?? null,
      cell: (c) => {
        const roll = c.row.original.lump?.rolling_returns?.['5y']
        if (!roll) return '—'
        return (
          <span className="tabular-nums">
            <span className="text-rose-600 dark:text-rose-400">{roll.worst_pct.toFixed(1)}</span>
            {' / '}<span className="font-medium">{roll.median_pct.toFixed(1)}</span>
            {' / '}<span className="text-emerald-600 dark:text-emerald-400">{roll.best_pct.toFixed(1)}</span>
          </span>
        )
      },
    },
    { header: 'Trades', id: 'trades', accessorFn: (r) => r.lump?.total_trades ?? null, cell: (c) => (c.getValue<number | null>() ?? 0).toLocaleString('en-IN') },
    { header: 'Win %', id: 'win', accessorFn: (r) => r.lump?.win_rate_pct ?? null, cell: (c) => fmtPct(c.getValue<number | null>(), 1) },
  ], [benchmark])

  return (
    <AppShell>
      <div className="flex flex-col gap-4">
        <Card>
          <CardHeader>
            <CardTitle>Technical strategy comparison — 2009 to 2026</CardTitle>
            <CardDescription>
              {report.data
                ? `${report.data.n_strategies} template × exit-variant pairs from ${report.data.n_runs} runs.
                   ₹10,00,000 per strategy, PIT top-800 by ADTV, Indian FY basis.
                   Benchmark (NIFTY) CAGR ${fmtPct(benchmark)}.`
                : 'Loading…'}
            </CardDescription>
          </CardHeader>
          <CardContent>
            <div className="flex flex-wrap items-center gap-3">
              <label className="text-sm" htmlFor="variant">Exit variant</label>
              <select
                id="variant" className="rounded border bg-background px-2 py-1 text-sm"
                value={variantFilter} onChange={(e) => setVariantFilter(e.target.value)}
              >
                <option value="">All</option>
                {variantOptions.map((v) => <option key={v} value={v}>{v}</option>)}
              </select>
              {benchmark !== null && (
                <span className="text-sm text-muted-foreground">
                  {beatBenchmark} of {filtered.length} beat the index
                </span>
              )}
            </div>
          </CardContent>
        </Card>

        {report.data?.measure_3_status && (
          <Card className="border-amber-500/50">
            <CardHeader>
              <CardTitle className="text-base">
                Income mode (annual reset) — {report.data.measure_3_status.status}, figures withheld
              </CardTitle>
              <CardDescription>
                {report.data.measure_3_status.reason}{' '}
                <span className="font-medium">{report.data.measure_3_status.affects}</span>
              </CardDescription>
            </CardHeader>
          </Card>
        )}

        <Card>
          <CardHeader>
            <CardTitle>All strategies</CardTitle>
            <CardDescription>Select a row for its year-by-year and rolling-return detail.</CardDescription>
          </CardHeader>
          <CardContent>
            <DataTable
              columns={columns}
              data={filtered}
              isLoading={report.isLoading}
              emptyMessage="No comparison report yet — run scripts/build_ta_comparison_report.py"
              onRowClick={(row: TAComparisonStrategy) => setSelected(row)}
            />
          </CardContent>
        </Card>

        {selected?.lump && (
          <Card>
            <CardHeader>
              <CardTitle>{selected.template} · {selected.exit_variant}</CardTitle>
              <CardDescription>
                {selected.lump.start_date} → {selected.lump.end_date} · final capital{' '}
                {fmtInr(selected.lump.final_capital)} · avg hold {fmtNum(selected.lump.avg_days_held, 0)} days
              </CardDescription>
            </CardHeader>
            <CardContent className="flex flex-col gap-6">
              <div>
                <h3 className="mb-2 text-sm font-semibold">Rolling returns (annualised)</h3>
                <div className="overflow-x-auto">
                  <table className="w-full text-sm tabular-nums">
                    <thead>
                      <tr className="border-b text-left text-muted-foreground">
                        <th className="py-1 pr-4">Window</th><th className="py-1 pr-4">Windows</th>
                        <th className="py-1 pr-4">Worst</th><th className="py-1 pr-4">Median</th>
                        <th className="py-1 pr-4">Best</th><th className="py-1">Positive</th>
                      </tr>
                    </thead>
                    <tbody>
                      {Object.entries(selected.lump.rolling_returns).map(([w, r]) => (
                        <tr key={w} className="border-b last:border-0">
                          <td className="py-1 pr-4 font-medium">{w}</td>
                          <td className="py-1 pr-4">{r.n_windows}</td>
                          <td className="py-1 pr-4 text-rose-600 dark:text-rose-400">{fmtPct(r.worst_pct, 1)}</td>
                          <td className="py-1 pr-4">{fmtPct(r.median_pct, 1)}</td>
                          <td className="py-1 pr-4 text-emerald-600 dark:text-emerald-400">{fmtPct(r.best_pct, 1)}</td>
                          <td className="py-1">{r.positive_windows} / {r.n_windows}</td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              </div>

              <div>
                <h3 className="mb-2 text-sm font-semibold">Year on year (Indian FY)</h3>
                <div className="overflow-x-auto">
                  <table className="w-full text-sm tabular-nums">
                    <thead>
                      <tr className="border-b text-left text-muted-foreground">
                        <th className="py-1 pr-4">FY</th><th className="py-1 pr-4">Opening</th>
                        <th className="py-1 pr-4">Closing</th><th className="py-1">Return</th>
                      </tr>
                    </thead>
                    <tbody>
                      {selected.lump.fy_returns.map((fy) => (
                        <tr key={fy.fy_end} className="border-b last:border-0">
                          <td className="py-1 pr-4 font-medium">
                            {fy.fy_label}
                            {fy.partial && <span className="ml-1 text-xs text-muted-foreground">(partial)</span>}
                          </td>
                          <td className="py-1 pr-4">{fmtInr(fy.opening_equity)}</td>
                          <td className="py-1 pr-4">{fmtInr(fy.closing_equity)}</td>
                          <td className={`py-1 ${(fy.return_pct ?? 0) < 0 ? 'text-rose-600 dark:text-rose-400' : 'text-emerald-600 dark:text-emerald-400'}`}>
                            {fmtPct(fy.return_pct, 1)}
                          </td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              </div>
            </CardContent>
          </Card>
        )}
      </div>
    </AppShell>
  )
}
