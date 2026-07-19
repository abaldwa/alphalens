import { useEffect, useState } from 'react'
import { useQuery } from '@tanstack/react-query'

import { AppShell, Button, Card, CardContent, CardHeader, CardTitle, InfoTooltip } from '@/lib/ui'
import { apiGet } from '@/shared/api/client'
import type { TAPatternsResponse, TASummaryResponse, TAWatchlistResponse } from './types'

interface OpsFreshnessRow {
  source: string
  latest_data_date?: string | null
}
interface OpsFreshnessResponse {
  sources: OpsFreshnessRow[]
}

function initialTickerFromUrl(): string {
  return (new URLSearchParams(window.location.search).get('ticker') ?? 'RELIANCE').toUpperCase()
}
function reasonFromUrl(): string | null {
  return new URLSearchParams(window.location.search).get('reason')
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

const money = (v: number | null | undefined) => (v == null ? '—' : `₹${v.toLocaleString('en-IN')}`)
const pct = (v: number | null | undefined) => (v == null ? '—' : `${(v * 100).toFixed(2)}%`)
const num = (v: number | null | undefined, d = 2) => (v == null ? '—' : v.toFixed(d))

export function TechnicalDeepDivePage() {
  const [tickerInput, setTickerInput] = useState(initialTickerFromUrl)
  const [dateInput, setDateInput] = useState('')
  const [ticker, setTicker] = useState(initialTickerFromUrl)
  const [date, setDate] = useState<string | undefined>(undefined)
  const reason = reasonFromUrl()

  // The last trading date we actually have OHLCV for — "today" is unsafe
  // as a default (18:00 IST pipeline cutoff, weekends/holidays), so mirror
  // the old dashboard/static/technical/js/deep_dive.js's loadDefaultDate().
  const freshness = useQuery({
    queryKey: ['ops-freshness-for-deep-dive'],
    queryFn: () => apiGet<OpsFreshnessResponse>('/api/v1/ops/freshness'),
  })
  useEffect(() => {
    if (dateInput || date) return
    const row = freshness.data?.sources.find((s) => s.source === 'ohlcv_adjusted')
    if (row?.latest_data_date) setDate(row.latest_data_date)
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [freshness.data])

  const summary = useQuery({
    queryKey: ['ta-summary', ticker, date],
    queryFn: () => apiGet<TASummaryResponse>(`/api/v1/ta/${ticker}/summary`, { date }),
    enabled: !!ticker,
  })

  const patterns = useQuery({
    queryKey: ['ta-patterns-dd', ticker, date],
    queryFn: () => apiGet<TAPatternsResponse>(`/api/v1/ta/${ticker}/patterns`, { date }),
    enabled: !!ticker,
  })

  // Support/resistance reuses the Daily WatchList's already-computed levels
  // per the old deep_dive.js — over-fetches and filters client-side since
  // /api/v1/ta/watchlist/daily has no per-ticker filter.
  const watchlist = useQuery({
    queryKey: ['ta-watchlist-for-levels'],
    queryFn: () => apiGet<TAWatchlistResponse>('/api/v1/ta/watchlist/daily', { limit: 100 }),
  })
  const levelsRow = watchlist.data?.rows.find((r) => r.ticker === ticker)

  const load = () => {
    setTicker(tickerInput.trim().toUpperCase())
    setDate(dateInput.trim() || undefined)
  }

  return (
    <AppShell title="Technical — Deep Dive" description="T6 Technical Deep Dive: DMA ratios, 52wk hi/lo, support/resistance, delivery volume.">
      <Card>
        <CardHeader>
          <CardTitle>Ticker &amp; date</CardTitle>
        </CardHeader>
        <CardContent>
          <div className="flex flex-wrap items-center gap-2">
            <input
              className="h-9 rounded-[var(--radius-token)] border border-border bg-transparent px-3 text-sm"
              value={tickerInput}
              onChange={(e) => setTickerInput(e.target.value.toUpperCase())}
              placeholder="Ticker (e.g. RELIANCE)"
            />
            <input
              className="h-9 rounded-[var(--radius-token)] border border-border bg-transparent px-3 text-sm"
              value={dateInput}
              onChange={(e) => setDateInput(e.target.value)}
              placeholder="YYYY-MM-DD (optional, defaults to latest)"
            />
            <Button onClick={load}>Load</Button>
          </div>
        </CardContent>
      </Card>

      {reason && (
        <div className="mt-3">
          <Card>
            <CardContent className="pt-4 text-sm text-muted-foreground">
              <span className="font-semibold text-foreground">Why on WatchList: </span>
              {reason}
            </CardContent>
          </Card>
        </div>
      )}

      {!summary.data?.available ? (
        <div className="mt-4">
          <Card>
            <CardContent className="pt-4 text-sm text-muted-foreground">
              {summary.isLoading ? 'Loading…' : `No price data for ${ticker}${date ? ` on ${date}` : ''}`}
            </CardContent>
          </Card>
        </div>
      ) : (
        <>
          <div className="mt-4 grid grid-cols-2 gap-4 sm:grid-cols-3 lg:grid-cols-6">
            <Stat label="CMP" value={money(summary.data.cmp)} tooltip="CMP (Current Market Price): the latest traded price for this ticker." />
            <Stat label="52 Wk High" value={money(summary.data.week52_high)} />
            <Stat label="52 Wk Low" value={money(summary.data.week52_low)} />
            <Stat
              label="Delivery %"
              value={summary.data.delivery_pct == null ? '—' : `${summary.data.delivery_pct.toFixed(1)}%`}
              tooltip="Delivery %: share of the day's traded volume settled by actual delivery (not squared off intraday) — an NSE/BSE-specific proxy for genuine investment demand vs. speculative trading."
            />
            <Stat label="Avg Delivery (21d)" value={summary.data.avg_delivery_pct_21d == null ? '—' : `${summary.data.avg_delivery_pct_21d.toFixed(1)}%`} />
            <Stat
              label="Delivery Z-Score"
              value={num(summary.data.delivery_pct_zscore_21d)}
              tooltip="Delivery Z-Score: how many standard deviations today's delivery % is from its trailing 21-day average — a spike can flag unusual investor accumulation/distribution."
            />
          </div>

          <div className="mt-4 grid grid-cols-2 gap-4 sm:grid-cols-3 lg:grid-cols-6">
            <Stat label="SMA 20" value={money(summary.data.sma_20)} />
            <Stat label="SMA 50" value={money(summary.data.sma_50)} />
            <Stat label="SMA 100" value={money(summary.data.sma_100)} />
            <Stat label="SMA 200" value={money(summary.data.sma_200)} />
            <Stat label="9 EMA" value={money(summary.data.ema_9)} />
            <Stat label="21 EMA" value={money(summary.data.ema_21)} />
            <Stat
              label="RSI"
              value={num(summary.data.rsi_14, 1)}
              tooltip="RSI (Relative Strength Index): momentum oscillator (0-100) measuring the speed/magnitude of recent price changes; conventionally >70 = overbought, <30 = oversold."
            />
            <Stat
              label="SuperTrend"
              value={summary.data.supertrend_value == null ? '—' : `${money(summary.data.supertrend_value)} (${(summary.data.supertrend_dir ?? 0) > 0 ? 'Up' : 'Down'})`}
              tone={summary.data.supertrend_value == null ? '' : (summary.data.supertrend_dir ?? 0) > 0 ? 'text-green' : 'text-red'}
              tooltip="SuperTrend: a trend-following indicator built off ATR-based bands around price; flips Up/Down as price crosses the band, used as a trailing-stop / trend-direction signal."
            />
            <Stat
              label="MACD"
              value={summary.data.macd == null ? '—' : `${num(summary.data.macd)} / ${num(summary.data.macd_signal)}`}
              tooltip="MACD (Moving Average Convergence Divergence): trend-following momentum indicator showing the relationship between two moving averages of price (value / signal line); used to spot trend shifts via crossovers."
            />
          </div>

          <div className="mt-4 grid grid-cols-2 gap-4 sm:grid-cols-4">
            <Stat label="SMA Ratio (50/200)" value={num(summary.data.sma_50_200_ratio, 3)} />
            <Stat label="Dist. from 52 Wk High" value={pct(summary.data.dist_from_52w_high)} tone={(summary.data.dist_from_52w_high ?? 0) >= 0 ? 'text-green' : 'text-red'} />
            <Stat label="Dist. from 52 Wk Low" value={pct(summary.data.dist_from_52w_low)} tone={(summary.data.dist_from_52w_low ?? 0) >= 0 ? 'text-green' : 'text-red'} />
            <Stat
              label="VWAP (20d)"
              value={money(summary.data.vwap_20d)}
              tooltip="VWAP (Volume-Weighted Average Price): average traded price over the last 20 days, weighted by volume at each price — a common benchmark for whether current price is cheap/rich relative to recent trading."
            />
          </div>
        </>
      )}

      <div className="mt-4 grid grid-cols-1 gap-4 lg:grid-cols-2">
        <Card>
          <CardHeader>
            <CardTitle>Support / Resistance</CardTitle>
          </CardHeader>
          <CardContent>
            {!levelsRow ? (
              <p className="text-sm text-muted-foreground">
                {ticker} not in today's WatchList template matches — no computed levels available
              </p>
            ) : (
              <div className="flex flex-col gap-3 text-sm">
                <div>
                  <span className="text-red">Next Resistance: </span>
                  {levelsRow.resistance_levels?.length ? levelsRow.resistance_levels.map(money).join(' / ') : '—'}
                </div>
                <div>
                  <span className="text-green">Support: </span>
                  {levelsRow.support_levels?.length ? levelsRow.support_levels.map(money).join(' / ') : '—'}
                </div>
              </div>
            )}
          </CardContent>
        </Card>

        <Card>
          <CardHeader>
            <CardTitle>Chart patterns</CardTitle>
          </CardHeader>
          <CardContent>
            {!patterns.data?.available || !Object.keys(patterns.data.patterns ?? {}).length ? (
              <p className="text-sm text-muted-foreground">No chart-pattern scores for {ticker}{date ? ` on ${date}` : ''}</p>
            ) : (
              <div className="flex flex-col gap-2">
                {Object.entries(patterns.data.patterns).map(([name, v]) => (
                  <div key={name} className="flex items-center justify-between text-sm">
                    <span>{name}</span>
                    <span className={`font-mono-data ${(v ?? 0) >= 0 ? 'text-green' : 'text-red'}`}>{num(v, 3)}</span>
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
