import { useMemo, useState } from 'react'
import { useQuery } from '@tanstack/react-query'

import { AppShell, Badge, Button, Card, CardContent, CardHeader, CardTitle } from '@/lib/ui'
import { API_BASE_URL, apiGet } from '@/shared/api/client'
import type { FARatiosResponse, FAScoresResponse } from './types'

const RATIO_LABELS: Record<string, string> = {
  roe: 'ROE (Return on Equity)',
  roce: 'ROCE (Return on Capital Employed)',
  net_margin: 'Net margin',
  revenue_growth_yoy: 'Revenue growth (YoY)',
  eps_growth_yoy: 'EPS (Earnings Per Share) growth (YoY)',
  debt_to_equity: 'Debt/Equity',
}
const LOWER_IS_BETTER = new Set(['debt_to_equity', 'pe_ratio'])

export function FundamentalThesisPage() {
  const [input, setInput] = useState('RELIANCE')
  const [ticker, setTicker] = useState('RELIANCE')

  const ratios = useQuery({
    queryKey: ['fa-ratios', ticker],
    queryFn: () => apiGet<FARatiosResponse>(`/api/v1/fundamentals/${ticker}/ratios`),
    enabled: !!ticker,
  })

  const scores = useQuery({
    queryKey: ['fa-scores-thesis', ticker],
    queryFn: () => apiGet<FAScoresResponse>(`/api/v1/fundamentals/${ticker}/scores`),
    enabled: !!ticker,
  })

  const { strengths, risks } = useMemo(() => {
    const s: string[] = []
    const r: string[] = []
    if (ratios.data?.available) {
      for (const [key, label] of Object.entries(RATIO_LABELS)) {
        const raw = ratios.data.ratios[key]
        if (raw == null) continue
        const z = LOWER_IS_BETTER.has(key) ? -raw : raw
        if (z > 0.5) s.push(`${label} is ${z.toFixed(1)} sector-std above peers`)
        else if (z < -0.5) r.push(`${label} is ${Math.abs(z).toFixed(1)} sector-std below peers`)
      }
    }
    return { strengths: s, risks: r }
  }, [ratios.data])

  return (
    <AppShell title="Fundamental — Thesis" description="FA-E Thesis Builder: templated Strengths/Risks synthesis from real sector-relative z-scores crossing a +/-0.5 threshold — no generative text.">
      <Card>
        <CardHeader>
          <CardTitle>Ticker</CardTitle>
        </CardHeader>
        <CardContent>
          <div className="flex flex-wrap items-center gap-2">
            <input
              className="h-9 rounded-[var(--radius-token)] border border-border bg-transparent px-3 text-sm"
              value={input}
              onChange={(e) => setInput(e.target.value.toUpperCase())}
              placeholder="Ticker (e.g. RELIANCE)"
            />
            <Button onClick={() => setTicker(input.trim())}>Load</Button>
            <a
              className="inline-flex h-9 items-center justify-center rounded-[var(--radius-token)] border border-border px-4 text-sm hover:bg-accent"
              href={`${API_BASE_URL}/api/v1/fundamentals/${ticker}/thesis/pdf`}
            >
              Export PDF
            </a>
          </div>
        </CardContent>
      </Card>

      {!ratios.data?.available ? (
        <div className="mt-4">
          <Card>
            <CardContent className="pt-4 text-sm text-muted-foreground">
              {ratios.error ? `Could not reach GET /api/v1/fundamentals/{ticker}/ratios — ${(ratios.error as Error).message}` : ratios.isLoading ? 'Loading…' : `No ratio data for ${ticker} yet`}
            </CardContent>
          </Card>
        </div>
      ) : (
        <>
          <div className="mt-4 flex items-center gap-2">
            <span className="text-lg font-semibold">{ticker}</span>
            <Badge>Quality {scores.data?.quality_score != null ? scores.data.quality_score.toFixed(0) : '—'}</Badge>
            <Badge variant="secondary">Growth {scores.data?.growth_score != null ? scores.data.growth_score.toFixed(0) : '—'}</Badge>
          </div>

          <div className="mt-4 grid grid-cols-1 gap-4 lg:grid-cols-2">
            <Card className="border-l-4" style={{ borderLeftColor: 'var(--green)' }}>
              <CardHeader>
                <CardTitle className="text-green">Strengths</CardTitle>
              </CardHeader>
              <CardContent>
                {strengths.length ? (
                  <ul className="flex flex-col gap-1 text-sm">
                    {strengths.map((s) => (
                      <li key={s}>• {s}</li>
                    ))}
                  </ul>
                ) : (
                  <p className="text-sm text-muted-foreground">No ratio is meaningfully above sector peers</p>
                )}
              </CardContent>
            </Card>

            <Card className="border-l-4" style={{ borderLeftColor: 'var(--red)' }}>
              <CardHeader>
                <CardTitle className="text-red">Risks</CardTitle>
              </CardHeader>
              <CardContent>
                {risks.length ? (
                  <ul className="flex flex-col gap-1 text-sm">
                    {risks.map((s) => (
                      <li key={s}>• {s}</li>
                    ))}
                  </ul>
                ) : (
                  <p className="text-sm text-muted-foreground">No ratio is meaningfully below sector peers</p>
                )}
              </CardContent>
            </Card>
          </div>
        </>
      )}
    </AppShell>
  )
}
