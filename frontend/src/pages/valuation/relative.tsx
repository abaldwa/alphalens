import { useState } from 'react'
import { useQuery } from '@tanstack/react-query'

import { AppShell, Badge, Button, InfoTooltip, StatCard } from '@/lib/ui'
import { apiGet } from '@/shared/api/client'

interface RelativeValuationResponse {
  ticker: string
  sector: string | null
  as_of_date: string
  actual_pe: number | null
  predicted_pe: number | null
  gap_pct: number | null
  is_overvalued: boolean | null
  r_squared: number | null
  n_peers: number
  current_price: number | null
  implied_price: number | null
}

function fmtMoney(v: number | null | undefined): string {
  return v == null ? '—' : `₹${v.toLocaleString('en-IN', { maximumFractionDigits: 2 })}`
}

function fmtNum(v: number | null | undefined, digits = 1): string {
  return v == null ? '—' : v.toFixed(digits)
}

function relativeTone(gapPct: number | null | undefined): 'success' | 'destructive' | 'warning' | 'default' {
  if (gapPct == null) return 'default'
  if (gapPct < -0.1) return 'success'
  if (gapPct > 0.1) return 'destructive'
  return 'warning'
}

function relativeLabel(gapPct: number | null | undefined): string {
  if (gapPct == null) return 'N/A'
  if (gapPct < -0.1) return 'Cheap vs Peers'
  if (gapPct > 0.1) return 'Expensive vs Peers'
  return 'In-line vs Peers'
}

export function RelativePage() {
  const [tickerInput, setTickerInput] = useState('RELIANCE')
  const [ticker, setTicker] = useState('RELIANCE')

  const query = useQuery({
    queryKey: ['valuation-relative', ticker],
    queryFn: () => apiGet<RelativeValuationResponse>(`/api/v1/valuation/${ticker}/relative`),
  })

  const r = query.data

  return (
    <AppShell
      title="Valuation — Relative"
      description="Sector-peer P/E regression valuation (SPEC-VAL-002 Model 5)."
      actions={
        <div className="flex items-center gap-2">
          <input
            className="h-9 rounded-[var(--radius-token)] border border-border bg-transparent px-3 text-sm"
            value={tickerInput}
            onChange={(e) => setTickerInput(e.target.value.toUpperCase())}
            placeholder="Ticker (e.g. RELIANCE)"
          />
          <Button onClick={() => setTicker(tickerInput.trim().toUpperCase())}>Load</Button>
        </div>
      }
    >
      {query.error ? (
        <p className="text-sm text-red">
          Could not reach GET /api/v1/valuation/{ticker}/relative — {(query.error as Error).message}
        </p>
      ) : (
        <>
          <div className="grid grid-cols-1 gap-4 sm:grid-cols-4">
            <StatCard label="vs Peers" value={<Badge variant={relativeTone(r?.gap_pct)}>{relativeLabel(r?.gap_pct)}</Badge>} />
            <StatCard label="Actual P/E" value={fmtNum(r?.actual_pe)} />
            <StatCard
              label={
                <span className="inline-flex items-center gap-1">
                  Peer-Implied Fair P/E <InfoTooltip>P/E predicted by a regression fit across sector peers — what this stock's P/E "should" be given its fundamentals and peer relationships.</InfoTooltip>
                </span>
              }
              value={fmtNum(r?.predicted_pe)}
            />
            <StatCard
              label={
                <span className="inline-flex items-center gap-1">
                  Gap <InfoTooltip>Difference between the actual P/E and the peer-implied fair P/E, as a percentage of the fair P/E.</InfoTooltip>
                </span>
              }
              value={r?.gap_pct != null ? `${(-r.gap_pct * 100).toFixed(1)}%` : '—'}
              tone={r?.gap_pct != null ? (r.gap_pct < 0 ? 'green' : 'red') : 'default'}
            />
          </div>

          <div className="mt-4 grid grid-cols-1 gap-4 sm:grid-cols-4">
            <StatCard label="CMP" value={fmtMoney(r?.current_price)} />
            <StatCard
              label={
                <span className="inline-flex items-center gap-1">
                  Peer-Implied Price <InfoTooltip>Current price scaled to what it would be if the stock traded at its peer-implied fair P/E instead of its actual P/E.</InfoTooltip>
                </span>
              }
              value={fmtMoney(r?.implied_price)}
            />
            <StatCard label="Sector" value={r?.sector ?? '—'} />
            <StatCard
              label={
                <span className="inline-flex items-center gap-1">
                  Peers Used / R² <InfoTooltip>Number of sector peers included in the P/E regression, and the R² (goodness of fit) of that regression — low R² means the peer-implied P/E is less reliable.</InfoTooltip>
                </span>
              }
              value={r ? `${r.n_peers} / ${fmtNum(r.r_squared, 2)}` : '—'}
            />
          </div>
        </>
      )}
    </AppShell>
  )
}
