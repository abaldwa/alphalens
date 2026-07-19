import { useState } from 'react'
import { useQuery } from '@tanstack/react-query'

import { AppShell, Button, Card, CardContent, CardHeader, CardTitle, InfoTooltip, StatCard } from '@/lib/ui'
import { apiGet } from '@/shared/api/client'
import type { FASectorResponse } from './types'

const KEY_RATIOS: [string, string][] = [
  ['roe', 'ROE'],
  ['roce', 'ROCE'],
  ['net_margin', 'Net Margin'],
  ['debt_to_equity', 'Debt/Equity'],
  ['revenue_growth_yoy', 'Revenue Growth YoY'],
]

const RATIO_TOOLTIP: Record<string, string> = {
  roe: 'ROE (Return on Equity): net profit as a % of shareholder equity.',
  roce: 'ROCE (Return on Capital Employed): operating profit as a % of total capital employed (equity + debt).',
}

export function FundamentalSectorPage() {
  const [input, setInput] = useState('IT')
  const [sector, setSector] = useState('IT')

  const sectorData = useQuery({
    queryKey: ['fa-sector', sector],
    queryFn: () => apiGet<FASectorResponse>(`/api/v1/fundamentals/sector/${encodeURIComponent(sector)}`),
    enabled: !!sector,
  })

  return (
    <AppShell title="Fundamental — Sector" description="FA-C Sector Deep-Dive: sector-average of the standard ratio set (z-scores are mean-centered — ~0 by construction).">
      <Card>
        <CardHeader>
          <CardTitle>Sector</CardTitle>
        </CardHeader>
        <CardContent>
          <div className="flex items-center gap-2">
            <input
              className="h-9 rounded-[var(--radius-token)] border border-border bg-transparent px-3 text-sm"
              value={input}
              onChange={(e) => setInput(e.target.value)}
              placeholder="Sector (e.g. IT, Banking, Pharma)"
            />
            <Button onClick={() => setSector(input.trim())}>Load</Button>
          </div>
        </CardContent>
      </Card>

      <div className="mt-4">
        <Card>
          <CardHeader>
            <CardTitle>
              {sector} averages {sectorData.data ? `(${sectorData.data.ticker_count} tickers)` : ''}
            </CardTitle>
          </CardHeader>
          <CardContent>
            {sectorData.error ? (
              <p className="text-sm text-red">Could not reach GET /api/v1/fundamentals/sector/{'{sector}'} — {(sectorData.error as Error).message}</p>
            ) : !sectorData.data?.ticker_count ? (
              <p className="text-sm text-muted-foreground">{sectorData.isLoading ? 'Loading…' : `No tickers found for sector "${sector}"`}</p>
            ) : (
              <div className="grid grid-cols-2 gap-4 sm:grid-cols-5">
                {KEY_RATIOS.map(([key, label]) => (
                  <StatCard
                    key={key}
                    label={
                      RATIO_TOOLTIP[key] ? (
                        <span className="inline-flex items-center gap-1">
                          {label}
                          <InfoTooltip>{RATIO_TOOLTIP[key]}</InfoTooltip>
                        </span>
                      ) : (
                        label
                      )
                    }
                    value={sectorData.data?.avg_ratios[key]?.toFixed(3) ?? '—'}
                  />
                ))}
              </div>
            )}
          </CardContent>
        </Card>
      </div>

      <div className="mt-4">
        <Card>
          <CardHeader>
            <CardTitle>Sector-unique metrics</CardTitle>
          </CardHeader>
          <CardContent>
            <p className="text-sm text-muted-foreground">
              Sector-unique metrics (GNPA for banks, ANDA approvals for pharma, etc.) are not computed anywhere in this
              codebase yet — only the standard ratio set's sector aggregate, above, is real.
            </p>
          </CardContent>
        </Card>
      </div>
    </AppShell>
  )
}
