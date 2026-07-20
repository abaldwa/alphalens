import { useQuery } from '@tanstack/react-query'
import type { ColumnDef } from '@tanstack/react-table'

import { AppShell, Card, CardContent, CardHeader, CardTitle, DataTable, StatCard, sectorColumn } from '@/lib/ui'
import { apiGet } from '@/shared/api/client'
import type { TAMarketOverviewResponse, TASectorBreadthRow } from './types'

function fmtPct(v: number | null): string {
  return v == null ? '—' : `${(v * 100).toFixed(2)}%`
}

const columns: ColumnDef<TASectorBreadthRow, unknown>[] = [
  sectorColumn<TASectorBreadthRow>(),
  { accessorKey: 'advances', header: 'Advances', meta: { align: 'right' }, cell: (i) => <span className="text-green">{i.getValue<number>()}</span> },
  { accessorKey: 'declines', header: 'Declines', meta: { align: 'right' }, cell: (i) => <span className="text-red">{i.getValue<number>()}</span> },
  {
    accessorKey: 'avg_change_pct',
    header: 'Avg Change %',
    meta: { align: 'right' },
    cell: (i) => {
      const v = i.getValue<number | null>()
      return <span className={v != null && v >= 0 ? 'text-green' : 'text-red'}>{fmtPct(v)}</span>
    },
  },
]

export function TechnicalOverviewPage() {
  const overview = useQuery({
    queryKey: ['ta-market-overview'],
    queryFn: () => apiGet<TAMarketOverviewResponse>('/api/v1/ta/market_overview'),
  })

  return (
    <AppShell
      title="Technical — Market Overview"
      description={overview.data?.date ? `Market breadth for ${overview.data.date.slice(0, 10)}` : 'TA-E Market Overview'}
    >
      <Card>
        <CardHeader>
          <CardTitle>Market Breadth</CardTitle>
        </CardHeader>
        <CardContent>
          {overview.error ? (
            <p className="text-sm text-red">Could not reach GET /api/v1/ta/market_overview — {(overview.error as Error).message}</p>
          ) : !overview.isLoading && !overview.data?.available ? (
            <p className="text-sm text-muted-foreground">No OHLCV data available</p>
          ) : (
            <div className="grid grid-cols-1 gap-3 sm:grid-cols-3">
              <StatCard label="Advances" value={overview.data?.advances ?? '—'} tone="green" />
              <StatCard label="Declines" value={overview.data?.declines ?? '—'} tone="red" />
              <StatCard label="Unchanged" value={overview.data?.unchanged ?? '—'} />
            </div>
          )}
        </CardContent>
      </Card>

      <div className="mt-4">
        <Card>
          <CardHeader>
            <CardTitle>Sector Breadth</CardTitle>
          </CardHeader>
          <CardContent>
            {!overview.isLoading && overview.data?.available && !overview.data.sector_breadth.length ? (
              <p className="text-sm text-muted-foreground">No sector data</p>
            ) : (
              <DataTable
                columns={columns}
                data={overview.data?.available ? overview.data.sector_breadth : []}
                isLoading={overview.isLoading}
              />
            )}
          </CardContent>
        </Card>
      </div>
    </AppShell>
  )
}
