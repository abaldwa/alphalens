import { useState } from 'react'
import { useQuery } from '@tanstack/react-query'
import type { ColumnDef } from '@tanstack/react-table'

import { AppShell, Button, Card, CardContent, CardHeader, CardTitle, DataTable, InfoTooltip, tickerColumn } from '@/lib/ui'
import { apiGet } from '@/shared/api/client'
import type { FAScreenerResponse } from './types'

const PRESETS: [string, string][] = [
  ['quality_compounder', 'Quality Compounder'],
  ['garp', 'GARP'],
  ['turnaround', 'Turnaround'],
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
  const [preset, setPreset] = useState(PRESETS[0][0])

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
          <div className="flex gap-2">
            {PRESETS.map(([key, label]) => (
              <span key={key} className="inline-flex items-center gap-1">
                <Button variant={preset === key ? 'default' : 'outline'} onClick={() => setPreset(key)}>
                  {label}
                </Button>
                {PRESET_TOOLTIP[key] && <InfoTooltip>{PRESET_TOOLTIP[key]}</InfoTooltip>}
              </span>
            ))}
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
