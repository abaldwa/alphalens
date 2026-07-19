import { useState } from 'react'
import { useQuery } from '@tanstack/react-query'
import type { ColumnDef } from '@tanstack/react-table'

import { AppShell, Button, Card, CardContent, CardHeader, CardTitle, DataTable, InfoTooltip, StatCard } from '@/lib/ui'
import { apiGet } from '@/shared/api/client'
import type { FARatiosResponse, FAScoresResponse, FundamentalsResponse, FundamentalsRow } from './types'

const TRAFFIC_LIGHT_RATIOS: [string, string][] = [
  ['roe', 'ROE'],
  ['roce', 'ROCE'],
  ['net_margin', 'Net Margin'],
  ['debt_to_equity', 'Debt/Equity (lower better)'],
  ['revenue_growth_yoy', 'Revenue Growth YoY'],
]

// debt_to_equity: lower z (less leverage than sector) is better, so the sign flips.
function trafficLightClass(key: string, z: number | null | undefined): string {
  if (z === null || z === undefined) return 'bg-transparent'
  const signed = key === 'debt_to_equity' ? -z : z
  if (signed > 0.5) return 'bg-green/20 text-green'
  if (signed < -0.5) return 'bg-red/20 text-red'
  return 'bg-yellow-500/20 text-yellow-600'
}

const HISTORY_COLS: [string, string, boolean][] = [
  ['revenue', 'Revenue', false],
  ['ebitda', 'EBITDA', false],
  ['pat', 'PAT', false],
  ['eps', 'EPS', false],
  ['operating_margin', 'Op Margin', true],
  ['net_margin', 'Net Margin', true],
  ['roe', 'ROE', true],
  ['roce', 'ROCE', true],
  ['debt_to_equity', 'D/E', false],
]

const fmtNum = (v: number | null | undefined, digits = 2) => (v === null || v === undefined ? '—' : v.toFixed(digits))
const fmtPct = (v: number | null | undefined, digits = 1) => (v === null || v === undefined ? '—' : `${(v * 100).toFixed(digits)}%`)

const columns: ColumnDef<FundamentalsRow, unknown>[] = [
  {
    id: 'quarter',
    header: 'Quarter',
    cell: (i) => `${i.row.original.fiscal_year} Q${i.row.original.quarter}`,
  },
  ...HISTORY_COLS.map(
    ([key, label, isPct]): ColumnDef<FundamentalsRow, unknown> => ({
      accessorKey: key,
      header: label,
      cell: (i) => {
        const v = i.getValue<number | null>()
        return isPct ? fmtPct(v) : fmtNum(v, 2)
      },
    }),
  ),
]

export function FundamentalPage() {
  const [input, setInput] = useState('RELIANCE')
  const [ticker, setTicker] = useState('RELIANCE')

  const ratios = useQuery({
    queryKey: ['fa-ratios-dash', ticker],
    queryFn: () => apiGet<FARatiosResponse>(`/api/v1/fundamentals/${ticker}/ratios`),
    enabled: !!ticker,
  })

  const scores = useQuery({
    queryKey: ['fa-scores-dash', ticker],
    queryFn: () => apiGet<FAScoresResponse>(`/api/v1/fundamentals/${ticker}/scores`),
    enabled: !!ticker,
  })

  const history = useQuery({
    queryKey: ['fa-history', ticker],
    queryFn: () => apiGet<FundamentalsResponse>(`/api/v1/fundamentals/${ticker}/history`),
    enabled: !!ticker,
  })

  const historyRows = [...(history.data?.data ?? [])].sort(
    (a, b) => new Date(a.quarter_end_date).getTime() - new Date(b.quarter_end_date).getTime(),
  )

  return (
    <AppShell title="Fundamental — Dashboard" description="FA-A Financial Dashboard: quarterly fundamentals, sector-relative traffic-light ratios, and quality/growth composite scores.">
      <Card>
        <CardHeader>
          <CardTitle>Ticker</CardTitle>
        </CardHeader>
        <CardContent>
          <div className="flex items-center gap-2">
            <input
              className="h-9 rounded-[var(--radius-token)] border border-border bg-transparent px-3 text-sm"
              value={input}
              onChange={(e) => setInput(e.target.value.toUpperCase())}
              placeholder="Ticker (e.g. RELIANCE)"
            />
            <Button onClick={() => setTicker(input.trim())}>Load</Button>
          </div>
        </CardContent>
      </Card>

      <div className="mt-4">
        <Card>
          <CardHeader>
            <CardTitle>Traffic Light</CardTitle>
          </CardHeader>
          <CardContent>
            {ratios.error ? (
              <p className="text-sm text-red">Could not reach GET /api/v1/fundamentals/{'{ticker}'}/ratios — {(ratios.error as Error).message}</p>
            ) : ratios.isLoading ? (
              <p className="text-sm text-muted-foreground">Loading…</p>
            ) : !ratios.data?.available ? (
              <p className="text-sm text-muted-foreground">No ratio data for {ticker} yet</p>
            ) : (
              <>
                <div className="grid grid-cols-2 gap-4">
                  <StatCard label="Quality Score" value={scores.data?.quality_score != null ? fmtNum(scores.data.quality_score, 0) : '—'} />
                  <StatCard label="Growth Score" value={scores.data?.growth_score != null ? fmtNum(scores.data.growth_score, 0) : '—'} />
                </div>
                <p className="mt-4 mb-2 text-sm text-muted-foreground inline-flex items-center gap-1">
                  Sector-relative z-score (vs sector peers, not an absolute threshold)
                  <InfoTooltip>Sector-relative z-score: how many standard deviations this stock's ratio is from the sector average — 0 means average, positive/negative means above/below peers.</InfoTooltip>
                </p>
                <table className="w-full text-sm">
                  <thead>
                    <tr>
                      {TRAFFIC_LIGHT_RATIOS.map(([key, label]) => (
                        <th key={key} className="p-2 text-left font-medium">
                          {label}
                        </th>
                      ))}
                    </tr>
                  </thead>
                  <tbody>
                    <tr>
                      {TRAFFIC_LIGHT_RATIOS.map(([key]) => {
                        const z = ratios.data?.ratios[key]
                        return (
                          <td key={key} className={`p-2 rounded ${trafficLightClass(key, z)}`}>
                            {z === null || z === undefined ? '—' : fmtNum(z, 2)}
                          </td>
                        )
                      })}
                    </tr>
                  </tbody>
                </table>
              </>
            )}
          </CardContent>
        </Card>
      </div>

      <div className="mt-4">
        <Card>
          <CardHeader>
            <CardTitle>Quarterly History</CardTitle>
          </CardHeader>
          <CardContent>
            {history.error ? (
              <p className="text-sm text-red">Could not reach GET /api/v1/fundamentals/{'{ticker}'}/history — {(history.error as Error).message}</p>
            ) : (
              <DataTable columns={columns} data={historyRows} isLoading={history.isLoading} emptyMessage={`No quarterly fundamentals for ${ticker}`} />
            )}
          </CardContent>
        </Card>
      </div>
    </AppShell>
  )
}
