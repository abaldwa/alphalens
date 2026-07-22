import { useMemo } from 'react'
import { useQuery } from '@tanstack/react-query'
import type { ColumnDef } from '@tanstack/react-table'

import { AppShell, Badge, Button, Card, CardContent, CardHeader, CardTitle, DataTable, InfoTooltip, TickerLink, formatCurrencyINR } from '@/lib/ui'
import { apiGet } from '@/shared/api/client'
import type { TAStrategyWinRateResponse, TAStrategyWinRateRow, TAWatchlistResponse, TAWatchlistRow } from './types'

function fmtLevels(v: number[] | null | undefined): string {
  return v && v.length ? v.map((x) => formatCurrencyINR(x)).join(' / ') : '—'
}

// Real ISO date (YYYY-MM-DD) from the backend, displayed without the year —
// the watchlist only ever spans a few trailing trading days, so the year is
// redundant clutter on this page.
function fmtRecDate(iso: string | null | undefined): string {
  if (!iso) return '—'
  const d = new Date(`${iso}T00:00:00`)
  if (Number.isNaN(d.getTime())) return iso
  return d.toLocaleDateString('en-GB', { day: '2-digit', month: 'short' }).replace(' ', '-')
}

function buildColumns(winRateByTemplate: Map<string, TAStrategyWinRateRow>): ColumnDef<TAWatchlistRow, unknown>[] {
  return [
  {
    // Ahead of the ticker per request — always visible (not low-priority)
    // since it's now a leading identifier column, not a collapsible
    // detail. Rank by market cap over the full universe — null (not a
    // fabricated number) when the backend hasn't sourced a market cap for
    // this ticker yet (see TAWatchlistRow.market_cap_rank).
    accessorKey: 'market_cap_rank',
    header: 'Mcap Rank',
    size: 68,
    meta: { align: 'right' },
    cell: (i) => {
      const v = i.getValue<number | null>()
      return v == null ? '—' : String(v)
    },
  },
  { accessorKey: 'ticker', header: 'Stock', size: 90, cell: (i) => <TickerLink ticker={i.getValue<string>()} /> },
  {
    // "Recommendation" — the actionable call itself (which day it fired,
    // at what price). Leads the row per the app-wide recommendation →
    // expected return → strategy → success rate column convention.
    accessorKey: 'recommendation_date',
    header: 'Rec. Date',
    size: 75,
    cell: (i) => fmtRecDate(i.getValue<string | null>()),
  },
  {
    accessorKey: 'recommended_price',
    header: 'Rec. Price',
    size: 100,
    meta: { align: 'right' },
    cell: (i) => formatCurrencyINR(i.getValue<number | null>()),
  },
  {
    // Expected return block: target price + the % gain it implies from CMP.
    // "Target Price" = nearest computed resistance level (the same real
    // resistance_levels[] the low-priority Resistance column shows in
    // full, just surfaced as a single always-visible upside target).
    id: 'target_price',
    accessorFn: (row) => row.resistance_levels[0] ?? null,
    header: () => (
      <span className="block leading-tight">
        TGT
        <br />
        Price
      </span>
    ),
    size: 80,
    meta: { align: 'right' },
    cell: (i) => formatCurrencyINR(i.getValue<number | null>()),
  },
  {
    // Real, derived arithmetic from the two real fields above it — (Target
    // Price − CMP) / CMP — not a separately modeled/forecast return.
    id: 'pct_gain_expected',
    accessorFn: (row) => {
      const target = row.resistance_levels[0]
      const cmp = row.current_price
      if (target == null || cmp == null || cmp === 0) return null
      return ((target - cmp) / cmp) * 100
    },
    header: () => (
      <span className="inline-flex items-center gap-1">
        Gain Tar%
        <InfoTooltip>(Target Price − CMP) / CMP — the price move needed to reach the nearest resistance level above CMP, not a forecast or probability of getting there.</InfoTooltip>
      </span>
    ),
    size: 65,
    meta: { align: 'right' },
    cell: (i) => {
      const pct = i.getValue<number | null>()
      if (pct == null) return '—'
      return <span className={pct >= 0 ? 'text-green' : 'text-red'}>{pct.toFixed(2)}%</span>
    },
  },
  {
    // Strategy — the template that fired this recommendation.
    id: 'template',
    accessorFn: (row) => row.template_name,
    header: 'Template',
    size: 110,
    cell: ({ row }) => {
      const wr = winRateByTemplate.get(row.original.template_name)
      return (
        <span className="inline-flex items-center gap-1.5">
          <Badge>{row.original.category}</Badge>
          {row.original.template_name}
          <InfoTooltip>
            <div className="flex max-w-xs flex-col gap-1">
              <span className="font-medium">
                {row.original.template_description ?? row.original.template_name}
              </span>
              <span className="text-muted-foreground">
                {row.original.template_strategy_description ?? 'No strategy description available.'}
              </span>
              <span>
                {wr?.win_rate != null
                  ? `Win rate: ${(wr.win_rate * 100).toFixed(0)}% (95% CI ${(wr.wilson_lo! * 100).toFixed(0)}–${(wr.wilson_hi! * 100).toFixed(0)}%) across ${wr.times_recommended} calls`
                  : 'Win rate: not enough recommendation history yet'}
              </span>
            </div>
          </InfoTooltip>
        </span>
      )
    },
  },
  {
    // Success rate — the strategy's own win rate, immediately after the
    // strategy name it belongs to (previously only visible inside the
    // Template column's tooltip).
    id: 'win_rate',
    accessorFn: (row) => winRateByTemplate.get(row.template_name)?.win_rate ?? null,
    header: () => (
      <span className="inline-flex items-center gap-1">
        Win Rate
        <InfoTooltip>Historical win rate for this template across all its past recommendations (95% Wilson CI shown in the Template tooltip).</InfoTooltip>
      </span>
    ),
    size: 85,
    meta: { align: 'right' },
    cell: ({ row }) => {
      const wr = winRateByTemplate.get(row.original.template_name)
      return wr?.win_rate != null ? `${(wr.win_rate * 100).toFixed(0)}%` : '—'
    },
  },
  {
    accessorKey: 'current_price',
    header: 'CMP',
    size: 100,
    meta: { align: 'right' },
    cell: (i) => formatCurrencyINR(i.getValue<number | null>()),
  },
  {
    accessorKey: 'score',
    header: () => (
      <span className="inline-flex items-center gap-1">
        Score
        <InfoTooltip>
          matched_conditions / total_conditions for the template — e.g. 5/5 = 1.00. This screener only surfaces full
          matches (score ≥ 1.0, every one of the template's conditions true), so 1.00 here means "all conditions
          met," not a confidence or strength rating — it isn't a partial-match score you'll see below 1.00 on this
          page.
        </InfoTooltip>
      </span>
    ),
    size: 85,
    meta: { align: 'right', priority: 'medium' },
    cell: ({ row }) => `${row.original.score.toFixed(2)} (${row.original.matched_conditions}/${row.original.total_conditions})`,
  },
  {
    // Kept as a column for layout/column-set uniformity with the ML
    // watchlist (which has a real quantile-regression time horizon), but
    // always blank here — the TA screener templates only produce a match
    // score, not a time-to-target forecast, so there is no real value to
    // show and one is not fabricated.
    id: 'target_days',
    header: () => (
      <span className="inline-flex items-center gap-1">
        Tar Days
        <InfoTooltip>Not available for template-based signals — this screener produces a match score, not a time-to-target forecast.</InfoTooltip>
      </span>
    ),
    size: 60,
    meta: { align: 'center', priority: 'medium' },
    cell: () => '—',
  },
  {
    accessorKey: 'rationale',
    header: 'Rationale',
    meta: { priority: 'low' },
    cell: (i) => <span className="block max-w-[480px] truncate text-xs text-muted-foreground">{i.getValue<string>()}</span>,
  },
  {
    accessorKey: 'resistance_levels',
    header: 'Resistance',
    meta: { priority: 'low', group: 'price', align: 'right' },
    cell: (i) => <span className="text-red">{fmtLevels(i.getValue<number[]>())}</span>,
  },
  {
    accessorKey: 'support_levels',
    header: 'Support',
    meta: { priority: 'low', group: 'price', align: 'right' },
    cell: (i) => <span className="text-green">{fmtLevels(i.getValue<number[]>())}</span>,
  },
  {
    // Grouped with `sector` under 'identity' so both collapse onto one
    // disclosure line.
    accessorKey: 'company_name',
    header: 'Name',
    meta: { priority: 'low', group: 'identity' },
    cell: (i) => <span className="text-xs text-muted-foreground">{i.getValue<string | null>() ?? '—'}</span>,
  },
  { accessorKey: 'sector', header: 'Sector', meta: { priority: 'low', group: 'identity' }, cell: (i) => i.getValue<string | null>() ?? '—' },
  {
    id: 'deep_dive',
    header: () => (
      <span className="block leading-tight">
        Deep
        <br />
        Dive
      </span>
    ),
    size: 42,
    meta: { align: 'center' },
    cell: ({ row }) => (
      <a
        href={`/technical-deep_dive?ticker=${row.original.ticker}&reason=${encodeURIComponent(row.original.rationale || '')}`}
        target="_blank"
        rel="noopener"
        title="Technical Deep Dive"
      >
        🔎
      </a>
    ),
  },
  ]
}

export function TechnicalWatchlistPage() {
  const watchlist = useQuery({
    queryKey: ['ta-watchlist-weekly'],
    queryFn: () => apiGet<TAWatchlistResponse>('/api/v1/ta/watchlist/daily', { limit: 30, lookback_days: 5 }),
  })

  const winRates = useQuery({
    queryKey: ['ta-strategy-win-rates'],
    queryFn: () => apiGet<TAStrategyWinRateResponse>('/api/v1/ta/strategies/win_rates'),
  })

  const winRateByTemplate = useMemo(() => {
    const map = new Map<string, TAStrategyWinRateRow>()
    for (const rows of Object.values(winRates.data?.styles ?? {})) {
      for (const row of rows) map.set(row.template_name, row)
    }
    return map
  }, [winRates.data])

  const columns = useMemo(() => buildColumns(winRateByTemplate), [winRateByTemplate])

  return (
    <AppShell
      title="Technical — Weekly Watchlist"
      description={
        watchlist.data?.date
          ? `Best template match per stock, pooled across the trailing 5 trading days ending ${watchlist.data.date}`
          : 'Weekly TA WatchList'
      }
      actions={
        <Button asChild variant="outline" size="sm">
          <a href="/explain/backtest-guide.html#technical-strategies" target="_blank" rel="noreferrer">
            📖 Strategy reference (all 42 templates)
          </a>
        </Button>
      }
    >
      <Card>
        <CardHeader>
          <CardTitle>Weekly WatchList</CardTitle>
        </CardHeader>
        <CardContent>
          {watchlist.error ? (
            <p className="text-sm text-red">Could not reach GET /api/v1/ta/watchlist/daily — {(watchlist.error as Error).message}</p>
          ) : (
            <DataTable
              columns={columns}
              data={watchlist.data?.rows ?? []}
              isLoading={watchlist.isLoading}
              placeholder="Search ticker, name, template…"
              facetFilters={[
                { columnId: 'category', label: 'Category' },
                { columnId: 'recommendation_date', label: 'Rec. Date', formatValue: fmtRecDate },
              ]}
            />
          )}
        </CardContent>
      </Card>
    </AppShell>
  )
}
