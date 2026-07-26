import { useState } from 'react'
import { useQuery } from '@tanstack/react-query'
import { useSearchParams } from 'react-router-dom'
import type { ColumnDef } from '@tanstack/react-table'

import { AppShell, Card, CardContent, CardHeader, CardTitle, DataTable, InfoTooltip, tickerColumn } from '@/lib/ui'
import { apiGet } from '@/shared/api/client'
import type { FAScreenerResponse } from './types'

const PRESETS: [string, string][] = [
  ['quality_compounder', 'Quality Compounder'],
  ['garp', 'GARP'],
  ['turnaround', 'Turnaround'],
  ['magic_formula', 'Magic Formula'],
  ['quality_value', 'Quality Value'],
  ['fcf_low_debt', 'FCF + Low Debt'],
  ['deep_value_solvency', 'Deep Value + Solvency'],
  ['cash_flow_backed_earnings', 'Cash-Flow-Backed Earnings'],
  ['turnaround_recovery', 'Turnaround Recovery'],
  ['piotroski_on_value', 'Piotroski-on-Value'],
  ['margin_of_safety', 'Margin of Safety'],
  ['net_net', 'Net-Net'],
]

const PRESET_TOOLTIP: Record<string, string> = {
  garp: 'GARP (Growth At a Reasonable Price): a strategy that looks for companies with above-average growth that aren\'t trading at an excessive valuation premium.',
}

interface TickerRow {
  rank: number
  ticker: string
}

const columns: ColumnDef<TickerRow, unknown>[] = [
  { accessorKey: 'rank', header: '#', meta: { align: 'right' } },
  tickerColumn<TickerRow>('fundamental'),
]

export function FundamentalScreenerPage() {
  const [searchParams] = useSearchParams()
  const presetParam = searchParams.get('preset')
  const [preset, setPreset] = useState(
    presetParam && PRESETS.some(([key]) => key === presetParam) ? presetParam : PRESETS[0][0]
  )

  const screener = useQuery({
    queryKey: ['fa-screener', preset],
    queryFn: () => apiGet<FAScreenerResponse>('/api/v1/fundamentals/screener', { preset }),
  })

  const rows: TickerRow[] = (screener.data?.tickers ?? []).map((t, i) => ({ rank: i + 1, ticker: t }))

  return (
    <AppShell title="Fundamental — Screener" description="FA-D Fundamental Screener: presets computed against the latest day's sector-relative z-scored ratios.">
      <Card>
        <CardHeader>
          <CardTitle>Preset</CardTitle>
        </CardHeader>
        <CardContent>
          <div className="flex items-center gap-2">
            <select
              className="h-9 rounded-[var(--radius-token)] border border-border bg-transparent px-3 text-sm"
              value={preset}
              onChange={(e) => setPreset(e.target.value)}
            >
              {PRESETS.map(([key, label]) => (
                <option key={key} value={key}>
                  {label}
                </option>
              ))}
            </select>
            {PRESET_TOOLTIP[preset] && <InfoTooltip>{PRESET_TOOLTIP[preset]}</InfoTooltip>}
          </div>
        </CardContent>
      </Card>

      <div className="mt-4">
        <Card>
          <CardHeader>
            <CardTitle>
              Matches {screener.data ? `(${screener.data.tickers.length} as of ${screener.data.date ?? '—'})` : ''}
            </CardTitle>
          </CardHeader>
          <CardContent>
            {screener.error ? (
              <p className="text-sm text-red">Could not reach GET /api/v1/fundamentals/screener — {(screener.error as Error).message}</p>
            ) : (
              <DataTable columns={columns} data={rows} isLoading={screener.isLoading} emptyMessage={`No tickers match "${preset}" today`} />
            )}
          </CardContent>
        </Card>
      </div>
    </AppShell>
  )
}
