import { useQuery } from '@tanstack/react-query'
import type { ColumnDef } from '@tanstack/react-table'

import { AppShell, Badge, Card, CardContent, CardHeader, CardTitle, DataTable, TickerLink } from '@/lib/ui'
import { apiGet } from '@/shared/api/client'
import type { TAWatchlistResponse, TAWatchlistRow } from './types'

function fmtPrice(v: number | null): string {
  return v == null ? '—' : `₹${v.toLocaleString('en-IN')}`
}

function fmtLevels(v: number[] | null | undefined): string {
  return v && v.length ? v.map((x) => fmtPrice(x)).join(' / ') : '—'
}

const columns: ColumnDef<TAWatchlistRow, unknown>[] = [
  { accessorKey: 'ticker', header: 'Stock', cell: (i) => <TickerLink ticker={i.getValue<string>()} /> },
  {
    accessorKey: 'company_name',
    header: 'Name',
    cell: (i) => <span className="text-xs text-muted-foreground">{i.getValue<string | null>() ?? '—'}</span>,
  },
  { accessorKey: 'sector', header: 'Sector', cell: (i) => i.getValue<string | null>() ?? '—' },
  {
    accessorKey: 'recommended_price',
    header: 'Recommended Price',
    cell: (i) => fmtPrice(i.getValue<number | null>()),
  },
  {
    accessorKey: 'current_price',
    header: 'Price',
    cell: (i) => fmtPrice(i.getValue<number | null>()),
  },
  {
    id: 'template',
    header: 'Template',
    cell: ({ row }) => (
      <span className="inline-flex items-center gap-1.5">
        <Badge>{row.original.category}</Badge>
        {row.original.template_name}
      </span>
    ),
  },
  { accessorKey: 'score', header: 'Score', cell: (i) => i.getValue<number>().toFixed(2) },
  {
    accessorKey: 'rationale',
    header: 'Rationale',
    cell: (i) => <span className="max-w-[320px] text-xs text-muted-foreground">{i.getValue<string>()}</span>,
  },
  {
    accessorKey: 'resistance_levels',
    header: 'Next Resistance',
    cell: (i) => <span className="text-red">{fmtLevels(i.getValue<number[]>())}</span>,
  },
  {
    accessorKey: 'support_levels',
    header: 'Support',
    cell: (i) => <span className="text-green">{fmtLevels(i.getValue<number[]>())}</span>,
  },
  {
    id: 'deep_dive',
    header: 'Deep Dive',
    cell: ({ row }) => (
      <a
        href={`/technical-deep_dive.html?ticker=${row.original.ticker}&reason=${encodeURIComponent(row.original.rationale || '')}`}
        target="_blank"
        rel="noopener"
        title="Technical Deep Dive"
      >
        🔎
      </a>
    ),
  },
]

export function TechnicalWatchlistPage() {
  const watchlist = useQuery({
    queryKey: ['ta-watchlist-weekly'],
    queryFn: () => apiGet<TAWatchlistResponse>('/api/v1/ta/watchlist/daily', { limit: 30, lookback_days: 5 }),
  })

  return (
    <AppShell
      title="Technical — Weekly Watchlist"
      description={
        watchlist.data?.date
          ? `Best template match per stock, pooled across the trailing 5 trading days ending ${watchlist.data.date}`
          : 'Weekly TA WatchList'
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
            <DataTable columns={columns} data={watchlist.data?.rows ?? []} isLoading={watchlist.isLoading} />
          )}
        </CardContent>
      </Card>
    </AppShell>
  )
}
