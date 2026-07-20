import { useState } from 'react'
import { useQuery } from '@tanstack/react-query'
import type { ColumnDef } from '@tanstack/react-table'

import { AppShell, Button, Card, CardContent, CardHeader, CardTitle, DataTable, InfoTooltip, tickerColumn } from '@/lib/ui'
import { apiGet } from '@/shared/api/client'
import type { FAPeerRow, FAPeersResponse } from './types'

function zHeader(label: string, extra?: string) {
  return () => (
    <span className="inline-flex items-center gap-1">
      {label}
      <InfoTooltip>
        {extra ? `${extra} ` : ''}
        "(z)" is the sector-relative z-score: how many standard deviations this stock's value is from the sector average — 0 means average, positive/negative means above/below peers.
      </InfoTooltip>
    </span>
  )
}

const columns: ColumnDef<FAPeerRow, unknown>[] = [
  tickerColumn<FAPeerRow>(),
  { accessorKey: 'roe', header: zHeader('ROE (z)', 'ROE (Return on Equity): net profit as a % of shareholder equity.'), meta: { align: 'right' }, cell: (i) => i.getValue<number | null>()?.toFixed(2) ?? '—' },
  { accessorKey: 'roce', header: zHeader('ROCE (z)', 'ROCE (Return on Capital Employed): operating profit as a % of total capital employed (equity + debt).'), meta: { align: 'right' }, cell: (i) => i.getValue<number | null>()?.toFixed(2) ?? '—' },
  { accessorKey: 'debt_to_equity', header: zHeader('D/E (z)', 'D/E (Debt-to-Equity): total debt divided by shareholder equity — a leverage measure.'), meta: { align: 'right' }, cell: (i) => i.getValue<number | null>()?.toFixed(2) ?? '—' },
  { accessorKey: 'pe_ratio', header: zHeader('PE (z)', 'PE (Price-to-Earnings): share price divided by earnings per share.'), meta: { align: 'right' }, cell: (i) => i.getValue<number | null>()?.toFixed(2) ?? '—' },
]

export function FundamentalPeersPage() {
  const [input, setInput] = useState('RELIANCE')
  const [ticker, setTicker] = useState('RELIANCE')

  const peers = useQuery({
    queryKey: ['fa-peers', ticker],
    queryFn: () => apiGet<FAPeersResponse>(`/api/v1/fundamentals/${ticker}/peers`, { k: 8 }),
    enabled: !!ticker,
  })

  return (
    <AppShell title="Fundamental — Peers" description="FA-B Peer Comparison: same-sector peers ranked by market-cap proximity, with sector-relative z-scored ratios.">
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
            <CardTitle>Peers {peers.data?.sector ? `— Sector: ${peers.data.sector}` : ''}</CardTitle>
          </CardHeader>
          <CardContent>
            {peers.error ? (
              <p className="text-sm text-red">Could not reach GET /api/v1/fundamentals/{'{ticker}'}/peers — {(peers.error as Error).message}</p>
            ) : (
              <DataTable
                columns={columns}
                data={peers.data?.peers ?? []}
                isLoading={peers.isLoading}
                emptyMessage="No peers found — either this ticker's sector is unknown, or no other ticker in its sector has a computed feature row for today"
              />
            )}
          </CardContent>
        </Card>
      </div>
    </AppShell>
  )
}
