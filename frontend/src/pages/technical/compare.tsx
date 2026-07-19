import { useState } from 'react'
import { useQuery } from '@tanstack/react-query'
import type { ColumnDef } from '@tanstack/react-table'

import { AppShell, Button, Card, CardContent, CardHeader, CardTitle, DataTable, InfoTooltip, TickerLink } from '@/lib/ui'
import { apiGet } from '@/shared/api/client'
import type { TACompareResponse, TACompareTickerRow } from './types'

const columns: ColumnDef<TACompareTickerRow, unknown>[] = [
  { accessorKey: 'ticker', header: 'Ticker', cell: (i) => <TickerLink ticker={i.getValue<string>()} /> },
  {
    accessorKey: 'rs_vs_nifty500_21d',
    header: () => (
      <span className="inline-flex items-center gap-1">
        RS vs Nifty500 (21d)
        <InfoTooltip>RS (Relative Strength): trailing 21-day return relative to the Nifty 500 index — positive means the stock has outperformed the broader market.</InfoTooltip>
      </span>
    ),
    cell: (i) => {
      const v = i.getValue<number | null>()
      return v == null ? '—' : `${(v * 100).toFixed(2)}%`
    },
  },
  {
    accessorKey: 'beta_63d',
    header: () => (
      <span className="inline-flex items-center gap-1">
        Beta (63d)
        <InfoTooltip>Beta: sensitivity of the stock's trailing 63-day returns to the market index's returns — greater than 1 means it tends to move more than the market, less than 1 means less.</InfoTooltip>
      </span>
    ),
    cell: (i) => i.getValue<number | null>()?.toFixed(2) ?? '—',
  },
  {
    accessorKey: 'alpha_21d',
    header: () => (
      <span className="inline-flex items-center gap-1">
        Alpha (21d)
        <InfoTooltip>Alpha: trailing 21-day return in excess of what Beta alone would predict from the market's move — a rough measure of stock-specific performance.</InfoTooltip>
      </span>
    ),
    cell: (i) => {
      const v = i.getValue<number | null>()
      return v == null ? '—' : `${(v * 100).toFixed(2)}%`
    },
  },
]

function corrTone(v: number | null | undefined) {
  if (v == null) return 'text-muted-foreground'
  if (v > 0.5) return 'text-green'
  if (v < 0) return 'text-red'
  return 'text-amber'
}

export function TechnicalComparePage() {
  const [input, setInput] = useState('RELIANCE,TCS,INFY')
  const [tickers, setTickers] = useState('RELIANCE,TCS,INFY')

  const compare = useQuery({
    queryKey: ['ta-compare', tickers],
    queryFn: () => apiGet<TACompareResponse>('/api/v1/ta/compare', { tickers, days: 60 }),
    enabled: !!tickers,
  })

  const corrTickers = Object.keys(compare.data?.correlation ?? {})

  return (
    <AppShell title="Technical — Compare" description="TA-C Multi-Stock Compare: relative strength, beta/alpha, and pairwise return correlation.">
      <Card>
        <CardHeader>
          <CardTitle>Tickers</CardTitle>
        </CardHeader>
        <CardContent>
          <div className="flex items-center gap-2">
            <input
              className="h-9 w-80 rounded-[var(--radius-token)] border border-border bg-transparent px-3 text-sm"
              placeholder="Comma-separated tickers, e.g. RELIANCE,TCS,INFY"
              value={input}
              onChange={(e) => setInput(e.target.value.toUpperCase())}
            />
            <Button onClick={() => setTickers(input.trim())}>Load</Button>
          </div>
        </CardContent>
      </Card>

      <div className="mt-4">
        <Card>
          <CardHeader>
            <CardTitle>Relative strength / beta / alpha</CardTitle>
          </CardHeader>
          <CardContent>
            {compare.error ? (
              <p className="text-sm text-red">Could not reach GET /api/v1/ta/compare — {(compare.error as Error).message}</p>
            ) : (
              <DataTable columns={columns} data={compare.data?.rows ?? []} isLoading={compare.isLoading} />
            )}
          </CardContent>
        </Card>
      </div>

      <div className="mt-4">
        <Card>
          <CardHeader>
            <CardTitle>Correlation matrix</CardTitle>
          </CardHeader>
          <CardContent>
            {!corrTickers.length ? (
              <p className="text-sm text-muted-foreground">Not enough overlapping OHLCV history to compute correlation (need at least 2 tickers).</p>
            ) : (
              <div className="overflow-x-auto">
                <table className="w-full text-sm">
                  <thead>
                    <tr>
                      <th className="p-2 text-left"></th>
                      {corrTickers.map((t) => (
                        <th key={t} className="p-2 text-left font-mono-data">
                          {t}
                        </th>
                      ))}
                    </tr>
                  </thead>
                  <tbody>
                    {corrTickers.map((t1) => (
                      <tr key={t1}>
                        <td className="p-2 font-semibold">{t1}</td>
                        {corrTickers.map((t2) => {
                          const v = compare.data?.correlation[t1]?.[t2]
                          return (
                            <td key={t2} className={`p-2 font-mono-data ${corrTone(v)}`}>
                              {v == null ? '—' : v.toFixed(2)}
                            </td>
                          )
                        })}
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            )}
          </CardContent>
        </Card>
      </div>
    </AppShell>
  )
}
