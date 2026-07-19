import { useQuery } from '@tanstack/react-query'
import type { ColumnDef } from '@tanstack/react-table'

import { AppShell, Badge, Card, CardContent, CardDescription, CardHeader, CardTitle, DataTable, InfoTooltip, TickerLink } from '@/lib/ui'
import { apiGet } from '@/shared/api/client'
import type { WatchlistResponse } from './types'

function fmtPct(v: unknown) {
  return typeof v === 'number' ? `${(v * 100).toFixed(1)}%` : '—'
}

const columns: ColumnDef<Record<string, unknown>, unknown>[] = [
  { accessorKey: 'ticker', header: 'Ticker', cell: (i) => <TickerLink ticker={i.getValue<string>()} /> },
  {
    accessorKey: 'mb_probability',
    header: () => (
      <span className="inline-flex items-center gap-1">
        MB Prob
        <InfoTooltip>The MultibaggerModel's probability estimate, carried forward from ml_multibagger's most recent (typically weekly) run. Not a return multiplier prediction.</InfoTooltip>
      </span>
    ),
    cell: (i) => fmtPct(i.getValue()),
  },
  {
    accessorKey: 'mb_tier',
    header: () => (
      <span className="inline-flex items-center gap-1">
        Deterministic Probability Band
        <InfoTooltip>A deterministic probability-band bucket ("10x"/"5x"/"3x"/"2x"/"none", e.g. mb_probability {'>='} 0.80 → "10x") — not a forecast the stock will actually return that multiple.</InfoTooltip>
      </span>
    ),
    cell: (i) => <Badge variant="outline">{String(i.getValue() ?? '—')}</Badge>,
  },
  {
    accessorKey: 'mb_archetype',
    header: () => (
      <span className="inline-flex items-center gap-1">
        Archetype
        <InfoTooltip>A categorical label for the type of setup the model associates with this ticker (e.g. small-cap breakout, turnaround).</InfoTooltip>
      </span>
    ),
    cell: (i) => String(i.getValue() ?? '—'),
  },
  {
    accessorKey: 'survival_6m',
    header: () => (
      <span className="inline-flex items-center gap-1">
        6m
        <InfoTooltip>The model's estimated probability the position survives (doesn't hit a defined failure condition) 6 months out.</InfoTooltip>
      </span>
    ),
    cell: (i) => fmtPct(i.getValue()),
  },
  { accessorKey: 'survival_12m', header: '12m', cell: (i) => fmtPct(i.getValue()) },
  { accessorKey: 'survival_18m', header: '18m', cell: (i) => fmtPct(i.getValue()) },
  { accessorKey: 'survival_24m', header: '24m', cell: (i) => fmtPct(i.getValue()) },
  { accessorKey: 'survival_36m', header: '36m', cell: (i) => fmtPct(i.getValue()) },
]

function asOfNote(tickers: Record<string, unknown>[] | undefined): string | undefined {
  const first = tickers?.[0]
  const date = first && typeof first.date === 'string' ? first.date.slice(0, 10) : null
  return date ? `As of ${date} (scored weekly, Sunday)` : undefined
}

export function MlMultibaggerPage() {
  const watchlist = useQuery({
    queryKey: ['ml-watchlist-current'],
    queryFn: () => apiGet<WatchlistResponse>('/api/v1/watchlist/current'),
  })

  return (
    <AppShell title="ML — Multibagger Watchlist" description="M-08 multibagger-candidate screener, scored weekly (Sunday) — probability band, archetype, and multi-horizon survival curves.">
      <Card>
        <CardHeader>
          <CardTitle>Watchlist</CardTitle>
          <CardDescription>{watchlist.data?.notes ?? (watchlist.error ? 'Failed to load' : 'Loading…')}</CardDescription>
        </CardHeader>
        <CardContent>
          {watchlist.error ? (
            <p className="text-sm text-red">Could not reach GET /api/v1/watchlist/current — {(watchlist.error as Error).message}</p>
          ) : watchlist.data && !watchlist.data.implemented ? (
            <p className="text-sm text-muted-foreground">{watchlist.data.notes || 'No watchlist data yet.'}</p>
          ) : (
            <>
              {asOfNote(watchlist.data?.tickers) && (
                <p className="mb-2 text-sm text-muted-foreground">{asOfNote(watchlist.data?.tickers)}</p>
              )}
              <DataTable columns={columns} data={watchlist.data?.tickers ?? []} isLoading={watchlist.isLoading} emptyMessage="No multibagger scoring data yet." />
            </>
          )}
        </CardContent>
      </Card>

      <div className="mt-4">
        <Card>
          <CardHeader>
            <CardTitle>Below liquidity floor</CardTitle>
            <CardDescription>
              {watchlist.data?.low_liquidity_tickers.length
                ? `${watchlist.data.low_liquidity_tickers.length} picks below the ₹20cr/day ADTV recommendation floor — shown separately, not filtered from the main list above.`
                : undefined}
            </CardDescription>
          </CardHeader>
          <CardContent>
            {asOfNote(watchlist.data?.low_liquidity_tickers) && (
              <p className="mb-2 text-sm text-muted-foreground">{asOfNote(watchlist.data?.low_liquidity_tickers)}</p>
            )}
            <DataTable columns={columns} data={watchlist.data?.low_liquidity_tickers ?? []} isLoading={watchlist.isLoading} emptyMessage="No low-liquidity multibagger candidates." />
          </CardContent>
        </Card>
      </div>
    </AppShell>
  )
}
