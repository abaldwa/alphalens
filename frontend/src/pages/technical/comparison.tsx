// Technical Analysis long-history comparison (2009-04-01 -> 2026-08-10).
//
// Six views over backtest/ta_comparison_report.py's dataset:
//   Strategies · Compare · Filters · Trade book · Rolling returns · Profit take-out
//
// UNITS, because this has already caused one wrong analysis in this project:
// trade pnl_pct is a FRACTION on the wire (-0.05 == -5%). trade_stats fields
// ending _pct are ALREADY percent (converted server-side, once). The only
// place *100 belongs is the raw trade book.
//
// Where a view needs a backtest we have not run, it says so and names the run
// that would produce it, rather than rendering an empty chart or a plausible
// placeholder number.
import { useMemo, useState } from 'react'
import { useQuery } from '@tanstack/react-query'
import type { ColumnDef } from '@tanstack/react-table'
import {
  CartesianGrid, Legend, Line, LineChart, ResponsiveContainer, Tooltip, XAxis, YAxis,
} from 'recharts'

import {
  AppShell, Card, CardContent, CardDescription, CardHeader, CardTitle, DataTable,
} from '@/lib/ui'
import { apiGet } from '@/shared/api/client'
import type {
  TAComparisonReport, TAComparisonStrategy, TATradeBook,
} from './types'

type View = 'strategies' | 'compare' | 'filters' | 'tradebook' | 'rolling' | 'income'

const VIEWS: { id: View; label: string }[] = [
  { id: 'strategies', label: 'Strategies' },
  { id: 'compare', label: 'Compare' },
  { id: 'filters', label: 'Filters' },
  { id: 'tradebook', label: 'Trade book' },
  { id: 'rolling', label: 'Rolling returns' },
  { id: 'income', label: 'Profit take-out' },
]

// Distinguishable in both themes and for the common colour-vision deficiencies;
// deliberately not a rainbow, which stops being readable past four series.
const SERIES_COLOURS = ['#3f56c9', '#0f7a52', '#b0344a', '#96660b', '#7a3fb0', '#0b7285']

const key = (s: TAComparisonStrategy) => `${s.template}·${s.exit_variant}`

// Profit take-out table. Amounts are right-aligned so digits line up for
// scanning; the losing-FY count is centred because it is a small ratio, not a
// magnitude to compare down the column.
type IncomeSortKey =
  | 'template' | 'regime' | 'withdrawn_pretax_total' | 'withdrawn_post_tax_total'
  | 'tax_paid_total' | 'topped_up_total' | 'net_extracted' | 'losing_years'

const INCOME_COLUMNS: { key: IncomeSortKey; label: string; align: 'left' | 'right' | 'center' }[] = [
  { key: 'template', label: 'Strategy', align: 'left' },
  { key: 'regime', label: 'Regime', align: 'left' },
  { key: 'withdrawn_pretax_total', label: 'Withdrawn pre-tax', align: 'right' },
  { key: 'withdrawn_post_tax_total', label: 'Withdrawn post-tax', align: 'right' },
  { key: 'tax_paid_total', label: 'Tax', align: 'right' },
  { key: 'topped_up_total', label: 'Topped up', align: 'right' },
  { key: 'net_extracted', label: 'Net extracted', align: 'right' },
  { key: 'losing_years', label: 'Losing FYs', align: 'center' },
]

const ALIGN_CLASS = { left: 'text-left', right: 'text-right', center: 'text-center' } as const

function pct(v: number | null | undefined, d = 2) {
  return typeof v === 'number' ? `${v.toFixed(d)}%` : '—'
}
function num(v: number | null | undefined, d = 2) {
  return typeof v === 'number' ? v.toFixed(d) : '—'
}
function inr(v: number | null | undefined) {
  return typeof v === 'number' ? `₹${Math.round(v).toLocaleString('en-IN')}` : '—'
}
function days(v: number | null | undefined) {
  return typeof v === 'number' ? `${Math.round(v)}d` : '—'
}

export function TechnicalComparisonPage() {
  const [view, setView] = useState<View>('strategies')
  const [variantFilter, setVariantFilter] = useState('')
  const [selected, setSelected] = useState<TAComparisonStrategy | null>(null)
  const [compareKeys, setCompareKeys] = useState<string[]>([])
  const [tradeRunKey, setTradeRunKey] = useState<string>('')
  const [tradeOutcome, setTradeOutcome] = useState<string>('')
  const [tradePage, setTradePage] = useState(0)
  const [incomeSort, setIncomeSort] = useState<{ key: IncomeSortKey; dir: 'asc' | 'desc' }>(
    { key: 'net_extracted', dir: 'desc' },
  )

  const report = useQuery({
    queryKey: ['technical-comparison'],
    queryFn: () => apiGet<TAComparisonReport>('/api/v1/technical_backtest/comparison'),
  })

  const rows = report.data?.strategies ?? []
  const withLump = useMemo(() => rows.filter((r) => r.lump), [rows])
  const benchmark = withLump[0]?.lump?.benchmark_cagr_pct ?? null

  const variantOptions = useMemo(
    () => Array.from(new Set(rows.map((r) => r.exit_variant))).sort(), [rows],
  )
  const filtered = useMemo(
    () => withLump.filter((r) => (variantFilter ? r.exit_variant === variantFilter : true)),
    [withLump, variantFilter],
  )

  const tradeRun = useMemo(
    () => withLump.find((r) => key(r) === tradeRunKey) ?? null, [withLump, tradeRunKey],
  )
  const tradeBook = useQuery({
    queryKey: ['ta-trade-book', tradeRun?.lump?.run_id, tradeOutcome, tradePage],
    enabled: !!tradeRun?.lump?.run_id,
    queryFn: () => apiGet<TATradeBook>(
      `/api/v1/technical_backtest/trade_book?run_id=${tradeRun!.lump!.run_id}`
      + `&limit=200&offset=${tradePage * 200}`
      + (tradeOutcome ? `&outcome=${tradeOutcome}` : ''),
    ),
  })

  // ------------------------------------------------------------- strategies
  const columns = useMemo<ColumnDef<TAComparisonStrategy>[]>(() => [
    { header: 'Template', id: 'template', accessorFn: (r) => r.template },
    { header: 'Variant', id: 'variant', accessorFn: (r) => r.exit_variant },
    {
      header: 'CAGR', id: 'cagr', accessorFn: (r) => r.lump?.cagr_pct ?? null,
      cell: (c) => {
        const v = c.getValue<number | null>()
        const beats = benchmark !== null && typeof v === 'number' && v > benchmark
        return <span className={beats ? 'font-semibold text-emerald-600 dark:text-emerald-400' : undefined}>{pct(v)}</span>
      },
    },
    { header: 'Sharpe', id: 'sharpe', accessorFn: (r) => r.lump?.sharpe ?? null, cell: (c) => num(c.getValue<number | null>()) },
    {
      header: 'Max DD', id: 'dd', accessorFn: (r) => r.lump?.max_drawdown_pct ?? null,
      cell: (c) => <span className="text-rose-600 dark:text-rose-400">{pct(c.getValue<number | null>(), 1)}</span>,
    },
    { header: 'Avg hold', id: 'hold', accessorFn: (r) => r.lump?.trade_stats?.avg_hold_days ?? null, cell: (c) => days(c.getValue<number | null>()) },
    { header: 'Win %', id: 'wr', accessorFn: (r) => r.lump?.trade_stats?.win_rate_pct ?? null, cell: (c) => pct(c.getValue<number | null>(), 1) },
    {
      header: 'Avg win', id: 'avgwin', accessorFn: (r) => r.lump?.trade_stats?.avg_win_pct ?? null,
      cell: (c) => <span className="text-emerald-600 dark:text-emerald-400">{pct(c.getValue<number | null>(), 1)}</span>,
    },
    {
      header: 'Avg loss', id: 'avgloss', accessorFn: (r) => r.lump?.trade_stats?.avg_loss_pct ?? null,
      cell: (c) => <span className="text-rose-600 dark:text-rose-400">{pct(c.getValue<number | null>(), 1)}</span>,
    },
    {
      header: 'Payoff', id: 'payoff', accessorFn: (r) => r.lump?.trade_stats?.payoff_ratio ?? null,
      cell: (c) => <span className="font-medium">{num(c.getValue<number | null>())}</span>,
    },
    { header: 'Expectancy', id: 'exp', accessorFn: (r) => r.lump?.trade_stats?.expectancy_pct ?? null, cell: (c) => pct(c.getValue<number | null>(), 2) },
    { header: 'Trades', id: 'n', accessorFn: (r) => r.lump?.trade_stats?.n_closed ?? null, cell: (c) => (c.getValue<number | null>() ?? 0).toLocaleString('en-IN') },
  ], [benchmark])

  // ---------------------------------------------------------------- compare
  const compareSeries = useMemo(
    () => compareKeys.map((k) => withLump.find((r) => key(r) === k)).filter((r): r is TAComparisonStrategy => !!r),
    [compareKeys, withLump],
  )
  const compareData = useMemo(() => {
    if (!compareSeries.length) return []
    const byDate = new Map<string, Record<string, number | string>>()
    compareSeries.forEach((s) => {
      s.lump!.equity_monthly.forEach((p) => {
        const row = byDate.get(p.date) ?? { date: p.date }
        row[key(s)] = p.index
        byDate.set(p.date, row)
      })
    })
    return Array.from(byDate.values()).sort((a, b) => String(a.date).localeCompare(String(b.date)))
  }, [compareSeries])

  // ----------------------------------------------------------------- income
  // Flattened first (one row per strategy x regime) so sorting is over the rows
  // actually rendered — sorting the nested structure would only order
  // strategies and leave each strategy's two regimes in map order.
  const incomeRows = useMemo(
    () => filtered.flatMap((s) =>
      Object.entries(s.annual_reset ?? {}).map(([regime, ar]) => ({
        rowKey: `${key(s)}-${regime}`, template: s.template, regime, ...ar,
      })),
    ),
    [filtered],
  )
  const sortedIncomeRows = useMemo(() => {
    const { key: k, dir } = incomeSort
    return [...incomeRows].sort((a, b) => {
      const av = a[k as keyof typeof a]
      const bv = b[k as keyof typeof b]
      const cmp = typeof av === 'string' && typeof bv === 'string'
        ? av.localeCompare(bv)
        : Number(av ?? 0) - Number(bv ?? 0)
      return dir === 'asc' ? cmp : -cmp
    })
  }, [incomeRows, incomeSort])

  const toggleIncomeSort = (k: IncomeSortKey) =>
    setIncomeSort((prev) => ({
      key: k,
      // New column starts descending for amounts (largest first is what you
      // want from a leaderboard) and ascending for the text columns.
      dir: prev.key === k ? (prev.dir === 'asc' ? 'desc' : 'asc')
        : (k === 'template' || k === 'regime') ? 'asc' : 'desc',
    }))

  const toggleCompare = (k: string) =>
    setCompareKeys((prev) => (prev.includes(k) ? prev.filter((x) => x !== k)
      : prev.length >= SERIES_COLOURS.length ? prev : [...prev, k]))

  return (
    <AppShell>
      <div className="flex flex-col gap-4">
        <Card>
          <CardHeader>
            <CardTitle>Technical strategy comparison — 2009 to 2026</CardTitle>
            <CardDescription>
              {report.data
                ? `${report.data.n_strategies} template × exit-variant pairs from ${report.data.n_runs} runs. `
                  + `₹10,00,000 per strategy, PIT top-800 by ADTV, Indian FY. Benchmark (NIFTY) ${pct(benchmark)}.`
                : 'Loading…'}
            </CardDescription>
          </CardHeader>
          <CardContent className="flex flex-col gap-3">
            <div className="flex flex-wrap gap-1" role="tablist" aria-label="Comparison views">
              {VIEWS.map((v) => (
                <button
                  key={v.id} type="button" role="tab" aria-selected={view === v.id}
                  onClick={() => setView(v.id)}
                  className={`rounded px-3 py-1.5 text-sm transition-colors ${
                    view === v.id
                      ? 'bg-primary text-primary-foreground font-medium'
                      : 'border bg-background hover:bg-muted'}`}
                >
                  {v.label}
                </button>
              ))}
            </div>
            {(view === 'strategies' || view === 'rolling' || view === 'income') && (
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
                    {filtered.filter((r) => (r.lump?.cagr_pct ?? 0) > benchmark).length} of {filtered.length} beat the index
                  </span>
                )}
              </div>
            )}
          </CardContent>
        </Card>

        {/* ------------------------------------------------------ strategies */}
        {view === 'strategies' && (
          <>
            <Card>
              <CardHeader>
                <CardTitle>All strategies</CardTitle>
                <CardDescription>
                  Winners and losers are averaged separately — one blended mean cannot tell a
                  strategy that wins small and loses big from its opposite. Payoff is avg win ÷ avg
                  loss; expectancy is what one trade is worth on average. Select a row for detail.
                </CardDescription>
              </CardHeader>
              <CardContent>
                <DataTable
                  columns={columns} data={filtered} isLoading={report.isLoading}
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
                    {inr(selected.lump.final_capital)}
                  </CardDescription>
                </CardHeader>
                <CardContent className="flex flex-col gap-5">
                  <div className="grid gap-3 sm:grid-cols-2 lg:grid-cols-4">
                    {[
                      ['Avg trade duration', days(selected.lump.trade_stats.avg_hold_days)],
                      ['Winners held', days(selected.lump.trade_stats.avg_win_hold_days)],
                      ['Losers held', days(selected.lump.trade_stats.avg_loss_hold_days)],
                      ['Payoff ratio', num(selected.lump.trade_stats.payoff_ratio)],
                      ['Avg gain on wins', pct(selected.lump.trade_stats.avg_win_pct, 1)],
                      ['Avg loss on losses', pct(selected.lump.trade_stats.avg_loss_pct, 1)],
                      ['Best trade', pct(selected.lump.trade_stats.best_trade_pct, 1)],
                      ['Worst trade', pct(selected.lump.trade_stats.worst_trade_pct, 1)],
                    ].map(([label, value]) => (
                      <div key={label} className="rounded border bg-muted/40 px-3 py-2">
                        <div className="text-xs uppercase tracking-wide text-muted-foreground">{label}</div>
                        <div className="font-mono text-lg tabular-nums">{value}</div>
                      </div>
                    ))}
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
                              <td className="py-1 pr-4">{inr(fy.opening_equity)}</td>
                              <td className="py-1 pr-4">{inr(fy.closing_equity)}</td>
                              <td className={`py-1 ${(fy.return_pct ?? 0) < 0 ? 'text-rose-600 dark:text-rose-400' : 'text-emerald-600 dark:text-emerald-400'}`}>
                                {pct(fy.return_pct, 1)}
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
          </>
        )}

        {/* --------------------------------------------------------- compare */}
        {view === 'compare' && (
          <>
            <Card>
              <CardHeader>
                <CardTitle>Compare strategies</CardTitle>
                <CardDescription>
                  Growth of ₹10,00,000, indexed to 100 at the start so shapes are comparable
                  regardless of ending capital. Pick up to {SERIES_COLOURS.length}.
                </CardDescription>
              </CardHeader>
              <CardContent className="flex flex-wrap gap-1.5">
                {filtered.slice(0, 60).map((s) => {
                  const k = key(s)
                  const on = compareKeys.includes(k)
                  const idx = compareKeys.indexOf(k)
                  return (
                    <button
                      key={k} type="button" onClick={() => toggleCompare(k)} aria-pressed={on}
                      className={`rounded border px-2 py-1 text-xs transition-colors ${on ? 'text-white' : 'bg-background hover:bg-muted'}`}
                      style={on ? { backgroundColor: SERIES_COLOURS[idx], borderColor: SERIES_COLOURS[idx] } : undefined}
                    >
                      {s.template}
                    </button>
                  )
                })}
              </CardContent>
            </Card>
            <Card>
              <CardContent className="pt-6">
                {compareSeries.length === 0 ? (
                  <p className="py-16 text-center text-sm text-muted-foreground">
                    Select one or more strategies above.
                  </p>
                ) : (
                  <div style={{ width: '100%', height: 420 }}>
                    <ResponsiveContainer>
                      <LineChart data={compareData} margin={{ top: 8, right: 16, bottom: 8, left: 8 }}>
                        <CartesianGrid strokeOpacity={0.15} vertical={false} />
                        <XAxis dataKey="date" tick={{ fontSize: 11 }} minTickGap={48} />
                        <YAxis scale="log" domain={['auto', 'auto']} tick={{ fontSize: 11 }}
                               width={64} label={{ value: 'Index (log)', angle: -90, position: 'insideLeft', fontSize: 11 }} />
                        <Tooltip formatter={(v: number) => v.toFixed(1)} />
                        <Legend />
                        {compareSeries.map((s, i) => (
                          <Line key={key(s)} type="monotone" dataKey={key(s)} dot={false}
                                stroke={SERIES_COLOURS[i]} strokeWidth={1.8} />
                        ))}
                      </LineChart>
                    </ResponsiveContainer>
                  </div>
                )}
                <p className="mt-2 text-xs text-muted-foreground">
                  Log scale: over 17 years a linear axis compresses the first decade into the baseline
                  and hides every early drawdown.
                </p>
              </CardContent>
            </Card>
          </>
        )}

        {/* --------------------------------------------------------- filters */}
        {view === 'filters' && (
          <Card className="border-amber-500/50">
            <CardHeader>
              <CardTitle>Filter recommendations — not yet measured</CardTitle>
              <CardDescription>
                This view needs a filter-overlay sweep that has not been run for the 2009–2026 window.
              </CardDescription>
            </CardHeader>
            <CardContent className="flex flex-col gap-3 text-sm text-muted-foreground">
              <p>
                Recommending a filter per strategy means measuring each one&apos;s marginal effect
                while holding everything else fixed. Inferring it from the runs we already have would
                be guesswork dressed as analysis, so this view stays empty until the sweep exists.
              </p>
              <div>
                <div className="mb-1 font-medium text-foreground">Filters that would be swept</div>
                <ul className="list-disc pl-5">
                  <li>Liquidity floor — <code>min_adtv_cr</code></li>
                  <li>Quality gates — <code>quality_gate_min_f_score</code>, <code>max_m_score</code></li>
                  <li>Downtrend and circuit bands — <code>downtrend_filter_pct</code>, <code>circuit_band_pct</code></li>
                  <li>Bear-regime gate — <code>bear_drawdown_pct</code>, <code>disable_buys_in_regime</code></li>
                  <li>Selection breadth — <code>top_n</code></li>
                </ul>
              </div>
              <p>
                Producer: <code>scripts/run_technical_filter_overlays.py</code>, one run per
                (strategy × filter × setting). Cheap once the two-pass split lands, because entry
                signals are computed once and each filter is then replayed over them.
              </p>
            </CardContent>
          </Card>
        )}

        {/* ------------------------------------------------------- trade book */}
        {view === 'tradebook' && (
          <>
            <Card>
              <CardHeader><CardTitle>Trade book</CardTitle></CardHeader>
              <CardContent className="flex flex-wrap items-center gap-3">
                <select
                  className="rounded border bg-background px-2 py-1 text-sm" value={tradeRunKey}
                  onChange={(e) => { setTradeRunKey(e.target.value); setTradePage(0) }}
                  aria-label="Strategy"
                >
                  <option value="">Select a strategy…</option>
                  {withLump.map((s) => <option key={key(s)} value={key(s)}>{key(s)}</option>)}
                </select>
                <select
                  className="rounded border bg-background px-2 py-1 text-sm" value={tradeOutcome}
                  onChange={(e) => { setTradeOutcome(e.target.value); setTradePage(0) }}
                  aria-label="Outcome"
                >
                  <option value="">All trades</option>
                  <option value="win">Winners</option>
                  <option value="loss">Losers</option>
                </select>
                {tradeBook.data && (
                  <span className="text-sm text-muted-foreground">
                    {tradeBook.data.total.toLocaleString('en-IN')} trades ·{' '}
                    {tradeBook.data.wins.toLocaleString('en-IN')} won ·{' '}
                    net {inr(tradeBook.data.net_pnl_inr)}
                  </span>
                )}
              </CardContent>
            </Card>
            {tradeRun && (
              <Card>
                <CardContent className="pt-6">
                  <div className="overflow-x-auto">
                    <table className="w-full text-sm tabular-nums">
                      <thead>
                        <tr className="border-b text-left text-muted-foreground">
                          <th className="py-1 pr-3">Ticker</th><th className="py-1 pr-3">Bought</th>
                          <th className="py-1 pr-3">Sold</th><th className="py-1 pr-3">Held</th>
                          <th className="py-1 pr-3">Buy</th><th className="py-1 pr-3">Sell</th>
                          <th className="py-1 pr-3">P&amp;L</th><th className="py-1 pr-3">Return</th>
                          <th className="py-1">Exit</th>
                        </tr>
                      </thead>
                      <tbody>
                        {(tradeBook.data?.trades ?? []).map((t, i) => (
                          <tr key={`${t.ticker}-${t.sale_date}-${i}`} className="border-b last:border-0">
                            <td className="py-1 pr-3 font-medium">{t.ticker}</td>
                            <td className="py-1 pr-3">{t.buy_date}</td>
                            <td className="py-1 pr-3">{t.sale_date}</td>
                            <td className="py-1 pr-3">{t.holding_days}d</td>
                            <td className="py-1 pr-3">{inr(t.buy_price)}</td>
                            <td className="py-1 pr-3">{inr(t.sale_price)}</td>
                            <td className={`py-1 pr-3 ${t.pnl_inr < 0 ? 'text-rose-600 dark:text-rose-400' : 'text-emerald-600 dark:text-emerald-400'}`}>
                              {inr(t.pnl_inr)}
                            </td>
                            {/* pnl_pct is a fraction on the wire — convert here, once. */}
                            <td className={`py-1 pr-3 ${t.pnl_pct < 0 ? 'text-rose-600 dark:text-rose-400' : 'text-emerald-600 dark:text-emerald-400'}`}>
                              {pct(t.pnl_pct * 100, 1)}
                            </td>
                            <td className="py-1 text-muted-foreground">{t.exit_reason}</td>
                          </tr>
                        ))}
                      </tbody>
                    </table>
                  </div>
                  {tradeBook.data && tradeBook.data.total > tradeBook.data.limit && (
                    <div className="mt-3 flex items-center gap-3 text-sm">
                      <button type="button" className="rounded border px-3 py-1 disabled:opacity-40"
                              disabled={tradePage === 0} onClick={() => setTradePage((p) => p - 1)}>
                        Previous
                      </button>
                      <span className="text-muted-foreground">
                        {tradePage * 200 + 1}–{Math.min((tradePage + 1) * 200, tradeBook.data.total)} of{' '}
                        {tradeBook.data.total.toLocaleString('en-IN')}
                      </span>
                      <button type="button" className="rounded border px-3 py-1 disabled:opacity-40"
                              disabled={(tradePage + 1) * 200 >= tradeBook.data.total}
                              onClick={() => setTradePage((p) => p + 1)}>
                        Next
                      </button>
                    </div>
                  )}
                </CardContent>
              </Card>
            )}
          </>
        )}

        {/* --------------------------------------------------------- rolling */}
        {view === 'rolling' && (
          <Card>
            <CardHeader>
              <CardTitle>Rolling returns, annualised</CardTitle>
              <CardDescription>
                Worst / median / best across every window, not a single average — the point of a
                rolling measure is to show how much the answer depends on when you started.
              </CardDescription>
            </CardHeader>
            <CardContent>
              <div className="overflow-x-auto">
                <table className="w-full text-sm tabular-nums">
                  <thead>
                    <tr className="border-b text-left text-muted-foreground">
                      <th className="py-1 pr-3">Strategy</th>
                      {(report.data?.rolling_windows_years ?? [2, 3, 4, 5]).map((y) => (
                        <th key={y} className="py-1 pr-3">{y}y worst / med / best</th>
                      ))}
                    </tr>
                  </thead>
                  <tbody>
                    {filtered.map((s) => (
                      <tr key={key(s)} className="border-b last:border-0">
                        <td className="py-1 pr-3 font-medium">{s.template}</td>
                        {(report.data?.rolling_windows_years ?? [2, 3, 4, 5]).map((y) => {
                          const r = s.lump?.rolling_returns?.[`${y}y`]
                          return (
                            <td key={y} className="py-1 pr-3">
                              {r ? (
                                <>
                                  <span className="text-rose-600 dark:text-rose-400">{r.worst_pct.toFixed(1)}</span>
                                  {' / '}<span className="font-medium">{r.median_pct.toFixed(1)}</span>
                                  {' / '}<span className="text-emerald-600 dark:text-emerald-400">{r.best_pct.toFixed(1)}</span>
                                </>
                              ) : '—'}
                            </td>
                          )
                        })}
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            </CardContent>
          </Card>
        )}

        {/* ---------------------------------------------------------- income */}
        {view === 'income' && (
          <>
            {report.data?.measure_3_status && (
              <Card className="border-amber-500/50">
                <CardHeader>
                  <CardTitle className="text-base">
                    Provisional — {report.data.measure_3_status.status}
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
                <CardTitle>Returns with profit taken out each year</CardTitle>
                <CardDescription>
                  Start each financial year on ₹10,00,000, withdraw booked profit after tax, top the
                  base back up after a losing year. Shown pre-tax and post-tax; the gap is the tax
                  drag on an income strategy.
                </CardDescription>
              </CardHeader>
              <CardContent>
                <div className="overflow-x-auto">
                  <table className="w-full text-sm tabular-nums">
                    <thead>
                      <tr className="border-b text-muted-foreground">
                        {INCOME_COLUMNS.map((col) => {
                          const active = incomeSort.key === col.key
                          return (
                            <th key={col.key} className={`py-1 pr-3 ${ALIGN_CLASS[col.align]}`}
                                aria-sort={active ? (incomeSort.dir === 'asc' ? 'ascending' : 'descending') : 'none'}>
                              <button
                                type="button" onClick={() => toggleIncomeSort(col.key)}
                                className={`inline-flex items-center gap-1 hover:text-foreground ${
                                  active ? 'font-semibold text-foreground' : ''}`}
                              >
                                {col.label}
                                <span aria-hidden="true" className="text-xs">
                                  {active ? (incomeSort.dir === 'asc' ? '▲' : '▼') : '↕'}
                                </span>
                              </button>
                            </th>
                          )
                        })}
                      </tr>
                    </thead>
                    <tbody>
                      {sortedIncomeRows.map((r) => (
                        <tr key={r.rowKey} className="border-b last:border-0">
                          <td className="py-1 pr-3 font-medium">{r.template}</td>
                          <td className="py-1 pr-3 text-muted-foreground">{r.regime}</td>
                          <td className="py-1 pr-3 text-right">{inr(r.withdrawn_pretax_total)}</td>
                          <td className="py-1 pr-3 text-right">{inr(r.withdrawn_post_tax_total)}</td>
                          <td className="py-1 pr-3 text-right text-rose-600 dark:text-rose-400">{inr(r.tax_paid_total)}</td>
                          <td className="py-1 pr-3 text-right">{inr(r.topped_up_total)}</td>
                          <td className="py-1 pr-3 text-right font-medium text-emerald-600 dark:text-emerald-400">{inr(r.net_extracted)}</td>
                          <td className="py-1 text-center">{r.losing_years} / {r.n_financial_years}</td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              </CardContent>
            </Card>
          </>
        )}
      </div>
    </AppShell>
  )
}
