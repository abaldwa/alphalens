import { useEffect, useState } from 'react'
import { useQuery } from '@tanstack/react-query'
import { useSearchParams } from 'react-router-dom'

import { AppShell, Card, CardContent, ConfidenceMatrix, SymbolPageLayout, TradingViewWidget } from '@/lib/ui'
import { useTickerStore } from '@/app/tickerStore'
import { apiGet } from '@/shared/api/client'
import { getValuation } from '@/shared/api/valuation'
import type { TAStrategyWinRateResponse } from '@/pages/technical/types'

/**
 * The /charts route: Signal → Rationale → Proof → Validation for whatever
 * ticker is currently in the global ticker store. TickerLink anywhere in
 * the app opens this route with `?ticker=` in a new tab, so on mount we
 * seed the store from the URL param — this keeps the page linkable/
 * shareable (a fresh tab has no store state yet) while staying reactive to
 * store updates for the case where a chart tab is already open.
 */
export function SymbolOverviewPage() {
  const [searchParams] = useSearchParams()
  const ticker = useTickerStore((s) => s.ticker)
  const setTicker = useTickerStore((s) => s.setTicker)
  const [input, setInput] = useState(ticker ?? '')

  useEffect(() => {
    const paramTicker = searchParams.get('ticker')
    if (paramTicker) setTicker(paramTicker.toUpperCase())
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [])

  const valuation = useQuery({
    queryKey: ['symbol-overview', 'valuation', ticker],
    queryFn: () => getValuation(ticker!),
    enabled: !!ticker,
  })

  const winRates = useQuery({
    queryKey: ['symbol-overview', 'win-rates'],
    queryFn: () => apiGet<TAStrategyWinRateResponse>('/api/v1/ta/strategies/win_rates'),
  })

  const direction =
    valuation.data?.margin_of_safety != null
      ? valuation.data.margin_of_safety >= 0.15
        ? 'buy'
        : valuation.data.margin_of_safety <= -0.05
          ? 'sell'
          : 'hold'
      : null

  if (!ticker) {
    return (
      <AppShell title="Symbol Overview" description="Search a ticker to open its overview.">
        <Card>
          <CardContent className="flex gap-2 p-6">
            <input
              value={input}
              onChange={(e) => setInput(e.target.value.toUpperCase())}
              placeholder="e.g. RELIANCE"
              className="w-64 rounded-[var(--radius-token)] border border-border bg-background px-3 py-2 text-sm"
              onKeyDown={(e) => {
                if (e.key === 'Enter' && input.trim()) setTicker(input.trim())
              }}
            />
          </CardContent>
        </Card>
      </AppShell>
    )
  }

  const allWinRates = Object.values(winRates.data?.styles ?? {}).flat()

  return (
    <AppShell title={`Symbol Overview — ${ticker}`} description="Signal → Rationale → Proof → Validation, one symbol at a time.">
      <SymbolPageLayout>
        <SymbolPageLayout.Signal
          ticker={ticker}
          direction={direction}
          currentPrice={valuation.data?.current_price ?? null}
          targetPrice={valuation.data?.intrinsic_value ?? null}
          chart={<TradingViewWidget ticker={ticker} />}
        />
        <SymbolPageLayout.Rationale>
          {valuation.data?.lifecycle_stage
            ? `${ticker} is in its ${valuation.data.lifecycle_stage} lifecycle stage. Margin of safety vs intrinsic value: ${
                valuation.data.margin_of_safety != null ? `${(valuation.data.margin_of_safety * 100).toFixed(1)}%` : '—'
              }.`
            : 'No valuation thesis available for this symbol yet.'}
        </SymbolPageLayout.Rationale>
        <SymbolPageLayout.Proof>
          <ConfidenceMatrix ticker={ticker} />
        </SymbolPageLayout.Proof>
        <SymbolPageLayout.Validation>
          <p className="mb-2 text-xs text-muted-foreground">
            Technical screener win-rates are pooled across the universe, not specific to {ticker} — no per-symbol backtest
            endpoint exists yet.
          </p>
          <div className="flex flex-col gap-1 text-sm">
            {allWinRates.slice(0, 5).map((r) => (
              <div key={r.template_name} className="flex items-center justify-between border-b border-border py-1 last:border-0">
                <span>{r.template_name}</span>
                <span className="font-mono-data tabular-nums">
                  {r.win_rate != null ? `${(r.win_rate * 100).toFixed(0)}% win rate` : '—'}
                </span>
              </div>
            ))}
          </div>
        </SymbolPageLayout.Validation>
      </SymbolPageLayout>
    </AppShell>
  )
}
