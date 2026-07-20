import { useQuery } from '@tanstack/react-query'
import type { ColumnDef } from '@tanstack/react-table'

import { AppShell, Badge, Card, CardContent, CardHeader, CardTitle, DataTable, InfoTooltip, formatCurrencyINR, tickerColumn } from '@/lib/ui'
import { apiGet } from '@/shared/api/client'
import { StrategyPicker, useActiveStrategy, useStrategies } from './StrategyPicker'
import type { MomentumRankingRow } from './types'

const fmtMoney = formatCurrencyINR
function fmtPct(v: number | null | undefined): string {
  return v == null ? '—' : `${(v * 100).toFixed(1)}%`
}

/** Minimal inline sparkline SVG, mirroring dashboard/static/js/api.js's sparklineSvg. */
function Sparkline({ series }: { series: number[] | null | undefined }) {
  if (!Array.isArray(series) || series.length < 2) return <span>—</span>
  const width = 80
  const height = 24
  const min = Math.min(...series)
  const max = Math.max(...series)
  const range = max - min || 1
  const stepX = width / (series.length - 1)
  const points = series
    .map((v, i) => `${(i * stepX).toFixed(2)},${(height - ((v - min) / range) * height).toFixed(2)}`)
    .join(' ')
  const lastUp = series[series.length - 1] >= series[0]
  const color = lastUp ? '#16a34a' : '#dc2626'
  return (
    <svg width={width} height={height} viewBox={`0 0 ${width} ${height}`} preserveAspectRatio="none">
      <polyline points={points} fill="none" stroke={color} strokeWidth={1.5} />
    </svg>
  )
}

const columns: ColumnDef<MomentumRankingRow, unknown>[] = [
  {
    accessorKey: 'momentum_rank',
    header: () => (
      <span className="inline-flex items-center gap-1">
        Rank
        <InfoTooltip>Rank within this rank-band's universe by trailing return (1 = highest momentum).</InfoTooltip>
      </span>
    ),
    meta: { align: 'right' },
  },
  tickerColumn<MomentumRankingRow>(),
  { accessorKey: 'company_name', header: 'Name', meta: { priority: 'low' }, cell: (i) => i.getValue<string | null>() ?? '—' },
  { accessorKey: 'price', header: 'Price', meta: { priority: 'low', align: 'right' }, cell: (i) => fmtMoney(i.getValue<number | null>()) },
  {
    accessorKey: 'momentum_return',
    header: () => (
      <span className="inline-flex items-center gap-1">
        Trailing 6mo Return
        <InfoTooltip>Trailing 6-month return, the ranking metric used to rank tickers by momentum.</InfoTooltip>
      </span>
    ),
    meta: { align: 'right' },
    cell: (i) => fmtPct(i.getValue<number>()),
  },
  { accessorKey: 'return_20d', header: '20d Return', meta: { priority: 'low', align: 'right' }, cell: (i) => fmtPct(i.getValue<number | null>()) },
  {
    accessorKey: 'sparkline',
    header: '30d Trend',
    cell: (i) => <Sparkline series={i.getValue<number[] | null | undefined>()} />,
  },
  {
    accessorKey: 'in_top_n',
    header: () => (
      <span className="inline-flex items-center gap-1">
        In Top 15
        <InfoTooltip>Whether this ticker is currently within the strategy's top-15 rank cutoff for holding.</InfoTooltip>
      </span>
    ),
    cell: (i) => <Badge variant={i.getValue<boolean>() ? 'success' : 'secondary'}>{i.getValue<boolean>() ? 'Yes' : 'No'}</Badge>,
  },
]

export function MomentumUniversePage() {
  const strategies = useStrategies()
  const [activeStrategyId, setStrategyId] = useActiveStrategy(strategies.data)

  const universe = useQuery({
    queryKey: ['momentum-universe', activeStrategyId],
    queryFn: () => apiGet<MomentumRankingRow[]>('/api/v1/momentum/universe', { strategy_id: activeStrategyId! }),
    enabled: !!activeStrategyId,
  })

  return (
    <AppShell title="Momentum — Universe" description="ML38 live momentum ranking for the selected rank-band strategy.">
      <div className="mb-4">
        <StrategyPicker strategies={strategies.data ?? []} value={activeStrategyId} onChange={setStrategyId} />
      </div>

      <Card>
        <CardHeader>
          <CardTitle>{universe.data ? `${universe.data.length} ticker(s) ranked` : 'Ranking'}</CardTitle>
        </CardHeader>
        <CardContent>
          {universe.error ? (
            <p className="text-sm text-red">Could not reach GET /api/v1/momentum/universe — {(universe.error as Error).message}</p>
          ) : (
            <DataTable
              columns={columns}
              data={universe.data ?? []}
              isLoading={universe.isLoading}
              emptyMessage="No ranking available yet for today — the daily pipeline's compute_momentum step may not have run yet."
            />
          )}
        </CardContent>
      </Card>
    </AppShell>
  )
}
