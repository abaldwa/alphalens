import { useState } from 'react'
import { useQuery } from '@tanstack/react-query'

import { AppShell, Button, Card, CardContent, CardHeader, CardTitle, InfoTooltip } from '@/lib/ui'
import { apiGet } from '@/shared/api/client'
import type { FAPeersResponse, FARatiosResponse, FAScoresResponse } from './types'

function initialTickerFromUrl(): string {
  return (new URLSearchParams(window.location.search).get('ticker') ?? 'RELIANCE').toUpperCase()
}

function Stat({ label, value, tone, tooltip }: { label: string; value: string; tone?: string; tooltip?: string }) {
  return (
    <Card>
      <CardContent className="pt-4">
        <div className="inline-flex items-center gap-1 text-xs uppercase text-muted-foreground">
          {label}
          {tooltip && <InfoTooltip>{tooltip}</InfoTooltip>}
        </div>
        <div className={`font-mono-data text-lg font-semibold ${tone ?? ''}`}>{value}</div>
      </CardContent>
    </Card>
  )
}

const num = (v: number | null | undefined, d = 2) => (v == null ? '—' : v.toFixed(d))
const zTone = (v: number | null | undefined) => (v == null ? '' : v >= 0 ? 'text-green' : 'text-red')

/**
 * Fundamental pillar's Deep Dive — the microscope destination for any
 * fundamental-pillar table (mirrors technical/deep_dive.tsx's structure).
 * Sourced entirely from the already-computed sector-relative z-scored
 * ratios/composite scores/peer set (fundamentals.py's /ratios, /scores,
 * /peers) — no new backend endpoints, no fabricated numbers.
 */
export function FundamentalDeepDivePage() {
  const [tickerInput, setTickerInput] = useState(initialTickerFromUrl)
  const [ticker, setTicker] = useState(initialTickerFromUrl)

  const ratios = useQuery({
    queryKey: ['fa-ratios-dd', ticker],
    queryFn: () => apiGet<FARatiosResponse>(`/api/v1/fundamentals/${ticker}/ratios`),
    enabled: !!ticker,
  })
  const scores = useQuery({
    queryKey: ['fa-scores-dd', ticker],
    queryFn: () => apiGet<FAScoresResponse>(`/api/v1/fundamentals/${ticker}/scores`),
    enabled: !!ticker,
  })
  const peers = useQuery({
    queryKey: ['fa-peers-dd', ticker],
    queryFn: () => apiGet<FAPeersResponse>(`/api/v1/fundamentals/${ticker}/peers`),
    enabled: !!ticker,
  })

  const load = () => setTicker(tickerInput.trim().toUpperCase())

  const r = ratios.data?.ratios ?? {}

  return (
    <AppShell
      title="Fundamental — Deep Dive"
      description="Sector-relative z-scored ratios, quality/growth composites, and peer comparison for one ticker."
    >
      <Card>
        <CardHeader>
          <CardTitle>Ticker</CardTitle>
        </CardHeader>
        <CardContent>
          <div className="flex flex-wrap items-center gap-2">
            <input
              className="h-9 rounded-[var(--radius-token)] border border-border bg-transparent px-3 text-sm"
              value={tickerInput}
              onChange={(e) => setTickerInput(e.target.value.toUpperCase())}
              placeholder="Ticker (e.g. RELIANCE)"
              onKeyDown={(e) => e.key === 'Enter' && load()}
            />
            <Button onClick={load}>Load</Button>
          </div>
        </CardContent>
      </Card>

      {!ratios.data?.available ? (
        <div className="mt-4">
          <Card>
            <CardContent className="pt-4 text-sm text-muted-foreground">
              {ratios.isLoading ? 'Loading…' : `No fundamental ratio data for ${ticker}`}
            </CardContent>
          </Card>
        </div>
      ) : (
        <>
          <div className="mt-4 grid grid-cols-2 gap-4 sm:grid-cols-3 lg:grid-cols-4">
            <Stat
              label="Quality Score"
              value={num(scores.data?.quality_score, 0)}
              tooltip="Composite of profitability/leverage ratios vs. sector peers (features/fundamental_composites.py)."
            />
            <Stat label="Growth Score" value={num(scores.data?.growth_score, 0)} />
            <Stat label="Management Quality" value={num(scores.data?.management_quality_score, 0)} />
            <Stat label="Sector" value={peers.data?.sector ?? '—'} />
          </div>

          <div className="mt-4 grid grid-cols-2 gap-4 sm:grid-cols-3 lg:grid-cols-6">
            <Stat label="ROE (z)" value={num(r.roe)} tone={zTone(r.roe)} tooltip="Sector-relative z-score — positive means above sector average." />
            <Stat label="ROCE (z)" value={num(r.roce)} tone={zTone(r.roce)} />
            <Stat label="Net Margin (z)" value={num(r.net_margin)} tone={zTone(r.net_margin)} />
            <Stat label="Debt/Equity (z)" value={num(r.debt_to_equity)} tone={zTone(-1 * (r.debt_to_equity ?? 0))} tooltip="Lower is better — tone reflects that (negative z on this metric shown green)." />
            <Stat label="Revenue Growth YoY (z)" value={num(r.revenue_growth_yoy)} tone={zTone(r.revenue_growth_yoy)} />
            <Stat label="EPS Growth YoY (z)" value={num(r.eps_growth_yoy)} tone={zTone(r.eps_growth_yoy)} />
          </div>
        </>
      )}

      <div className="mt-4">
        <Card>
          <CardHeader>
            <CardTitle>Peers ({peers.data?.sector ?? '—'})</CardTitle>
          </CardHeader>
          <CardContent>
            {!peers.data?.peers.length ? (
              <p className="text-sm text-muted-foreground">No peer set available for {ticker}</p>
            ) : (
              <div className="flex flex-col gap-2 text-sm">
                {peers.data.peers.map((p) => (
                  <div key={p.ticker} className="flex items-center justify-between border-b border-border py-1 last:border-0">
                    <span className="font-medium">{p.ticker}</span>
                    <span className="font-mono-data tabular-nums text-muted-foreground">
                      ROE {num(p.roe)} · ROCE {num(p.roce)} · D/E {num(p.debt_to_equity)}
                    </span>
                  </div>
                ))}
              </div>
            )}
          </CardContent>
        </Card>
      </div>
    </AppShell>
  )
}
