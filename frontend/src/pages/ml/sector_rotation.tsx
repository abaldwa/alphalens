import { useState } from 'react'
import { useQuery } from '@tanstack/react-query'
import type { ColumnDef } from '@tanstack/react-table'
import { Bar, BarChart, CartesianGrid, Tooltip, XAxis, YAxis } from 'recharts'

import { AppShell, Card, CardContent, CardDescription, CardHeader, CardTitle, DataTable, ResponsiveChartCard, TickerLink } from '@/lib/ui'
import { apiGet } from '@/shared/api/client'
import type {
  SectorAccumulationDrilldownRow,
  SectorAccumulationRow,
  SectorRotationReport,
  SectorRotationRow,
} from './types'

function fmtScore(v: number | null | undefined) {
  return v == null ? '—' : v.toExponential(2)
}
function fmtNum(v: number | null | undefined) {
  return v == null ? '—' : Math.round(v).toLocaleString('en-IN')
}

function fmtPct(v: number | null | undefined) {
  return v == null ? '—' : `${(v * 100).toFixed(2)}%`
}
function tone(v: number | null | undefined) {
  if (v == null) return undefined
  return v > 0 ? 'text-green' : v < 0 ? 'text-red' : undefined
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

const columns: ColumnDef<SectorRotationRow, unknown>[] = [
  { accessorKey: 'rank', header: 'Rank' },
  { accessorKey: 'sector', header: 'Sector' },
  { accessorKey: 'index_name', header: 'Index' },
  { accessorKey: 'sector_market_cap_cr', header: 'Market Cap (₹ cr)', cell: (i) => i.getValue<number | null>()?.toLocaleString('en-IN') ?? '—' },
  { accessorKey: 'sparkline', header: 'Trend (63d)', cell: (i) => <Sparkline series={i.getValue<number[] | null | undefined>()} /> },
  { accessorKey: 'rs_1d', header: 'RS 1d', cell: (i) => <span className={tone(i.getValue<number | null>())}>{fmtPct(i.getValue<number | null>())}</span> },
  { accessorKey: 'rs_5d', header: 'RS 5d', cell: (i) => <span className={tone(i.getValue<number | null>())}>{fmtPct(i.getValue<number | null>())}</span> },
  {
    accessorKey: 'rs_21d',
    header: 'RS 21d',
    cell: ({ row }) => {
      const v = row.original.rs_21d ?? row.original.relative_strength
      return <span className={tone(v)}>{fmtPct(v)}</span>
    },
  },
  { accessorKey: 'rs_63d', header: 'RS 63d', cell: (i) => <span className={tone(i.getValue<number | null>())}>{fmtPct(i.getValue<number | null>())}</span> },
  {
    id: 'top_stocks',
    header: 'Top Stocks',
    cell: ({ row }) => {
      const stocks = row.original.top_stocks ?? []
      if (!stocks.length) return '—'
      return stocks.map((t, idx) => (
        <span key={t.ticker}>
          <TickerLink ticker={t.ticker} />
          {t.buy_prob != null ? ` (${(t.buy_prob * 100).toFixed(0)}%)` : ''}
          {idx < stocks.length - 1 ? ', ' : ''}
        </span>
      ))
    },
  },
]

export function MlSectorRotationPage() {
  const report = useQuery({
    queryKey: ['sector-rotation-report'],
    queryFn: () => apiGet<SectorRotationReport>('/api/v1/sector_rotation/report', { top_n_stocks: 5 }),
  })

  const accumulation = useQuery({
    queryKey: ['sector-accumulation-daily'],
    queryFn: () => apiGet<SectorAccumulationRow[]>('/api/v1/sector_accumulation/daily', {}),
  })

  const [drilldownKey, setDrilldownKey] = useState<{ sector: string; date: string } | null>(null)
  const drilldown = useQuery({
    queryKey: ['sector-accumulation-drilldown', drilldownKey?.sector, drilldownKey?.date],
    queryFn: () =>
      apiGet<SectorAccumulationDrilldownRow[]>('/api/v1/sector_accumulation/drilldown', {
        sector: drilldownKey!.sector,
        date: drilldownKey!.date,
      }),
    enabled: !!drilldownKey,
  })

  const accumulationColumns: ColumnDef<SectorAccumulationRow, unknown>[] = [
    { accessorKey: 'date', header: 'Date' },
    { accessorKey: 'sector', header: 'Sector' },
    {
      accessorKey: 'accumulation_score',
      header: 'Accumulation Score',
      cell: ({ row }) => (
        <button
          type="button"
          className="font-mono-data underline decoration-dotted"
          title="Click to see the per-stock breakdown"
          onClick={() => setDrilldownKey({ sector: row.original.sector, date: row.original.date })}
        >
          {fmtScore(row.original.accumulation_score)}
        </button>
      ),
    },
    { accessorKey: 'delivery_volume', header: 'Delivery Volume', cell: (i) => <span className="font-mono-data">{fmtNum(i.getValue<number>())}</span> },
    { accessorKey: 'sector_shares_outstanding', header: 'Sector Shares Outstanding', cell: (i) => <span className="font-mono-data">{fmtNum(i.getValue<number>())}</span> },
    { accessorKey: 'n_stocks_included', header: '# Stocks' },
  ]

  const drilldownColumns: ColumnDef<SectorAccumulationDrilldownRow, unknown>[] = [
    { accessorKey: 'ticker', header: 'Stock', cell: (i) => <TickerLink ticker={i.getValue<string>()} /> },
    { accessorKey: 'volume', header: 'Volume', cell: (i) => <span className="font-mono-data">{fmtNum(i.getValue<number>())}</span> },
    { accessorKey: 'delivery_pct', header: 'Delivery %', cell: (i) => <span className="font-mono-data">{fmtPct((i.getValue<number>() ?? 0) / 100)}</span> },
    { accessorKey: 'delivery_volume', header: 'Delivery Volume', cell: (i) => <span className="font-mono-data">{fmtNum(i.getValue<number>())}</span> },
    { accessorKey: 'shares_outstanding', header: 'Shares Outstanding', cell: (i) => <span className="font-mono-data">{fmtNum(i.getValue<number>())}</span> },
    { accessorKey: 'contribution_pct', header: 'Contribution %', cell: (i) => <span className="font-mono-data">{fmtPct((i.getValue<number>() ?? 0) / 100)}</span> },
  ]

  const chartData = (report.data?.sectors ?? []).slice(0, 15).map((s) => ({ sector: s.sector, rs21d: (s.rs_21d ?? 0) * 100 }))

  return (
    <AppShell title="ML — Sector Rotation" description="Sectors ranked by trailing 21-trading-day relative strength vs Nifty 500, with 1d/5d/21d/63d horizons and top in-favor stocks.">
      <Card>
        <CardHeader>
          <CardTitle>Sector rotation report</CardTitle>
          <CardDescription>
            {report.data?.as_of_date ? `As of ${report.data.as_of_date}` : report.error ? 'Failed to load' : 'Loading…'}
          </CardDescription>
        </CardHeader>
        <CardContent>
          {report.error ? (
            <p className="text-sm text-red">Could not reach GET /api/v1/sector_rotation/report — {(report.error as Error).message}</p>
          ) : (
            <DataTable columns={columns} data={report.data?.sectors ?? []} isLoading={report.isLoading} emptyMessage="No ranked sectors available — needs 21+ trading days of index_ohlcv history." />
          )}
        </CardContent>
      </Card>

      <div className="mt-4">
        <ResponsiveChartCard title="Relative strength (21d) by sector" height={280}>
          <BarChart data={chartData}>
            <CartesianGrid strokeDasharray="3 3" stroke="var(--border)" />
            <XAxis dataKey="sector" tick={{ fontSize: 10 }} interval={0} angle={-30} textAnchor="end" height={70} />
            <YAxis tick={{ fontSize: 11 }} />
            <Tooltip />
            <Bar dataKey="rs21d" fill="var(--teal)" radius={[3, 3, 0, 0]} />
          </BarChart>
        </ResponsiveChartCard>
      </div>

      <div className="mt-4">
        <Card>
          <CardHeader>
            <CardTitle>Sector accumulation</CardTitle>
            <CardDescription>
              (sum of each constituent stock&apos;s delivery% x volume) / sector&apos;s total outstanding shares, tracked daily. Click a score to see the per-stock breakdown.
            </CardDescription>
          </CardHeader>
          <CardContent>
            {accumulation.error ? (
              <p className="text-sm text-red">
                Could not reach GET /api/v1/sector_accumulation/daily — {(accumulation.error as Error).message}
              </p>
            ) : (
              <DataTable
                columns={accumulationColumns}
                data={accumulation.data ?? []}
                isLoading={accumulation.isLoading}
                emptyMessage="No sector accumulation data available — needs both ohlcv_adjusted (volume/delivery_pct) and fundamentals (shares_outstanding) rows for at least one full sector's constituents on a given date."
              />
            )}
          </CardContent>
        </Card>
      </div>

      {drilldownKey && (
        <div className="mt-4">
          <Card>
            <CardHeader>
              <CardTitle>
                Breakdown — {drilldownKey.sector}, {drilldownKey.date}
              </CardTitle>
            </CardHeader>
            <CardContent>
              <DataTable
                columns={drilldownColumns}
                data={drilldown.data ?? []}
                isLoading={drilldown.isLoading}
                emptyMessage={`No per-stock breakdown available for ${drilldownKey.sector} on ${drilldownKey.date}`}
              />
            </CardContent>
          </Card>
        </div>
      )}
    </AppShell>
  )
}
