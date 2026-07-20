import { useState } from 'react'
import { useQuery } from '@tanstack/react-query'

import { AppShell, Badge, Button, Card, CardContent, CardHeader, CardTitle, InfoTooltip, PriceChart, type PriceChartMarker, Table, TableBody, TableRow, TableCell } from '@/lib/ui'
import { apiGet } from '@/shared/api/client'
import type {
  EventRow,
  OHLCVResponse,
  TAIndicatorsResponse,
  TAPatternsResponse,
  TARecommendationResponse,
  TATickerProfileResponse,
} from './types'

// A72's 3 real event types, mirroring dashboard/static/technical/js/chart.js's
// colorByType palette (tokens swapped in for the vendored hex values).
// Canvas fillStyle can't resolve CSS var() without cascade context, so
// these are read from computed style at render time rather than passed
// as raw "var(--x)" strings.
const EVENT_COLOR_VARS: Record<string, string> = {
  corporate_action: '--purple',
  bulk_deal: '--amber',
  recommendation_trigger: '--green',
}
function eventColor(eventType: string): string {
  const varName = EVENT_COLOR_VARS[eventType] ?? '--muted-foreground'
  return getComputedStyle(document.documentElement).getPropertyValue(varName).trim() || '#64748b'
}
const EVENT_SHAPE: Record<string, 'arrowUp' | 'circle' | 'square'> = {
  corporate_action: 'square',
  bulk_deal: 'circle',
  recommendation_trigger: 'arrowUp',
}

const CURATED_INDICATORS: [string, string, 'ratio' | 'num' | 'pct'][] = [
  ['sma_50_ratio', 'Price / SMA50', 'ratio'],
  ['sma_200_ratio', 'Price / SMA200', 'ratio'],
  ['ema_21_ratio', 'Price / EMA21', 'ratio'],
  ['rsi_14', 'RSI (14)', 'num'],
  ['macd_hist', 'MACD Histogram', 'num'],
  ['adx_14', 'ADX (14)', 'num'],
  ['bb_position', 'Bollinger Band Position', 'num'],
  ['atr_14_pct', 'ATR % (14)', 'pct'],
  ['supertrend_dir', 'Supertrend Direction', 'num'],
  ['ichimoku_cloud_position', 'Ichimoku Cloud Position', 'num'],
  ['composite_momentum_21d', 'Composite Momentum (21d)', 'num'],
  ['rs_vs_nifty500_21d', 'RS vs Nifty 500 (21d)', 'pct'],
]

const PATTERN_LABELS: Record<string, string> = {
  head_shoulders_score: 'Head & Shoulders',
  double_bottom_score: 'Double Bottom',
  cup_handle_score: 'Cup & Handle',
  flag_pattern_score: 'Flag Pattern',
  wedge_score: 'Wedge',
  base_breakout_score: 'Base Breakout',
}

const INDICATOR_TOOLTIPS: Record<string, string> = {
  rsi_14: 'RSI (Relative Strength Index): momentum oscillator (0-100) measuring the speed/magnitude of recent price changes; conventionally >70 = overbought, <30 = oversold.',
  macd_hist: 'MACD (Moving Average Convergence Divergence) Histogram: difference between the MACD line and its signal line — trend-following momentum indicator; used to spot trend shifts via signal-line crossovers.',
  adx_14: 'ADX (Average Directional Index): measures trend strength (not direction), 0-100; above ~25 is often read as a strong trend.',
  atr_14_pct: 'ATR (Average True Range) as % of price: measures recent price volatility as an average of the true trading range; used for stop-loss/position-sizing, not direction.',
  rs_vs_nifty500_21d: 'RS (Relative Strength) vs Nifty 500: this ticker\'s trailing 21-day return relative to the Nifty 500 index — positive means it has outperformed the broader market.',
}

const PATTERN_TOOLTIPS: Record<string, string> = {
  head_shoulders_score: 'Heuristic pattern-detector score (not a certainty): a Head & Shoulders formation is conventionally read as a bearish reversal signal.',
  double_bottom_score: 'Heuristic pattern-detector score (not a certainty): a Double Bottom is conventionally read as a bullish reversal signal.',
  cup_handle_score: 'Heuristic pattern-detector score (not a certainty): a Cup & Handle is conventionally read as a bullish continuation signal.',
  flag_pattern_score: 'Heuristic pattern-detector score (not a certainty): a Flag is conventionally read as a continuation of the preceding trend.',
  wedge_score: 'Heuristic pattern-detector score (not a certainty): a Wedge can signal either a reversal or continuation depending on prior trend and breakout direction.',
  base_breakout_score: 'Heuristic pattern-detector score (not a certainty): a Base Breakout is conventionally read as a bullish continuation signal once price clears the consolidation range.',
}

function fmtIndicator(v: number | null | undefined, kind: string): string {
  if (v == null) return '—'
  if (kind === 'pct') return `${(v * 100).toFixed(2)}%`
  return v.toFixed(3)
}

function initialTickerFromUrl(): string {
  const fromQuery = new URLSearchParams(window.location.search).get('ticker')
  return (fromQuery ?? 'RELIANCE').toUpperCase()
}

export function TechnicalChartPage() {
  const [input, setInput] = useState(initialTickerFromUrl)
  const [ticker, setTicker] = useState(initialTickerFromUrl)
  // Empty recDate means "let the backend pick the latest date it has
  // recommendations for" — once that response comes back its own `date`
  // is shown in the picker, so the user always sees a concrete date even
  // before touching the field.
  const [recDate, setRecDate] = useState('')

  const to = new Date()
  const from = new Date()
  from.setDate(from.getDate() - 400)
  const fmt = (d: Date) => d.toISOString().slice(0, 10)

  const profile = useQuery({
    queryKey: ['ta-ticker-profile', ticker],
    queryFn: () => apiGet<TATickerProfileResponse>(`/api/v1/ta/${ticker}/profile`),
    enabled: !!ticker,
  })

  const recommendations = useQuery({
    queryKey: ['ta-ticker-recommendations', ticker, recDate],
    queryFn: () =>
      apiGet<TARecommendationResponse>(`/api/v1/ta/${ticker}/recommendations`, recDate ? { date: recDate } : {}),
    enabled: !!ticker,
  })

  const ohlcv = useQuery({
    queryKey: ['ohlcv', ticker],
    queryFn: () => apiGet<OHLCVResponse>(`/api/v1/ohlcv/${ticker}`, { from: fmt(from), to: fmt(to) }),
    enabled: !!ticker,
  })

  const indicators = useQuery({
    queryKey: ['ta-indicators', ticker],
    queryFn: () => apiGet<TAIndicatorsResponse>(`/api/v1/ta/${ticker}/indicators`),
    enabled: !!ticker,
  })

  const patterns = useQuery({
    queryKey: ['ta-patterns', ticker],
    queryFn: () => apiGet<TAPatternsResponse>(`/api/v1/ta/${ticker}/patterns`),
    enabled: !!ticker,
  })

  const events = useQuery({
    queryKey: ['events', ticker],
    queryFn: () => apiGet<EventRow[]>(`/api/v1/events/${ticker}`, { from_date: fmt(from), to_date: fmt(to) }),
    enabled: !!ticker,
  })

  const rows = (ohlcv.data?.data ?? []).slice().sort((a, b) => (a.date < b.date ? -1 : 1))
  const chartData = rows.map((r) => ({
    time: r.date.slice(0, 10),
    open: r.open,
    high: r.high,
    low: r.low,
    close: r.close,
    volume: r.volume,
  }))
  const latest = rows.at(-1)

  const rowDates = new Set(rows.map((r) => r.date.slice(0, 10)))
  const markers: PriceChartMarker[] = (events.data ?? [])
    .filter((ev) => rowDates.has(ev.date))
    .map((ev) => ({
      time: ev.date,
      position: 'aboveBar',
      color: eventColor(ev.event_type),
      shape: EVENT_SHAPE[ev.event_type] ?? 'circle',
      text: ev.description.length > 24 ? `${ev.description.slice(0, 24)}…` : ev.description,
    }))

  return (
    <AppShell title="Technical — Chart" description="TA-A price chart with curated indicator and chart-pattern panels (Recharts area/volume view in place of the vendored candlestick chart).">
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
            {profile.data && (profile.data.company_name || profile.data.sector) && (
              <span className="ml-2 inline-flex items-center gap-2 text-sm text-muted-foreground">
                {profile.data.company_name && <span>{profile.data.company_name}</span>}
                {profile.data.sector && <Badge variant="outline">{profile.data.sector}</Badge>}
              </span>
            )}
          </div>
        </CardContent>
      </Card>

      <div className="mt-4">
        <Card>
          <CardHeader>
            <CardTitle>Recommendations for this day</CardTitle>
          </CardHeader>
          <CardContent>
            <div className="mb-3 flex items-center gap-2">
              <label className="text-sm text-muted-foreground" htmlFor="rec-date">Date</label>
              <input
                id="rec-date"
                type="date"
                className="h-9 rounded-[var(--radius-token)] border border-border bg-transparent px-3 text-sm"
                value={recDate || recommendations.data?.date || ''}
                onChange={(e) => setRecDate(e.target.value)}
              />
            </div>
            {recommendations.error ? (
              <p className="text-sm text-red">
                Could not reach GET /api/v1/ta/{ticker}/recommendations — {(recommendations.error as Error).message}
              </p>
            ) : !recommendations.data?.rows.length ? (
              <p className="text-sm text-muted-foreground">
                {recommendations.isLoading ? 'Loading…' : `No template matched ${ticker} on ${recommendations.data?.date ?? recDate}`}
              </p>
            ) : (
              <div className="flex flex-col gap-2">
                {recommendations.data.rows.map((r) => (
                  <div key={r.template_name} className="flex flex-col gap-1 rounded-[var(--radius-token)] border border-border p-2 text-sm">
                    <div className="flex flex-wrap items-center gap-2">
                      <Badge>{r.category}</Badge>
                      <span className="font-medium">{r.template_name}</span>
                      {r.style && <Badge variant="outline">{r.style}</Badge>}
                      <span className="text-xs text-muted-foreground">Score {r.score.toFixed(2)}</span>
                      {r.outcome && (
                        <Badge variant={r.outcome === 'win' ? 'success' : r.outcome === 'loss' ? 'destructive' : 'outline'}>
                          {r.outcome}
                        </Badge>
                      )}
                    </div>
                    <span className="text-xs text-muted-foreground">{r.rationale}</span>
                  </div>
                ))}
              </div>
            )}
          </CardContent>
        </Card>
      </div>

      {latest && (
        <div className="mt-4 grid grid-cols-2 gap-4 sm:grid-cols-4">
          <Card>
            <CardContent className="pt-4">
              <div className="text-xs uppercase text-muted-foreground">Close</div>
              <div className="font-mono-data text-xl font-semibold">₹{latest.close.toLocaleString('en-IN')}</div>
            </CardContent>
          </Card>
          <Card>
            <CardContent className="pt-4">
              <div className="text-xs uppercase text-muted-foreground">High</div>
              <div className="font-mono-data text-xl font-semibold">₹{latest.high.toLocaleString('en-IN')}</div>
            </CardContent>
          </Card>
          <Card>
            <CardContent className="pt-4">
              <div className="text-xs uppercase text-muted-foreground">Low</div>
              <div className="font-mono-data text-xl font-semibold">₹{latest.low.toLocaleString('en-IN')}</div>
            </CardContent>
          </Card>
          <Card>
            <CardContent className="pt-4">
              <div className="text-xs uppercase text-muted-foreground">Volume</div>
              <div className="font-mono-data text-xl font-semibold">{latest.volume.toLocaleString('en-IN')}</div>
            </CardContent>
          </Card>
        </div>
      )}

      <div className="mt-4">
        <Card>
          <CardHeader>
            <CardTitle>Price &amp; volume</CardTitle>
          </CardHeader>
          <CardContent>
            {chartData.length === 0 ? (
              <p className="text-sm text-muted-foreground">
                {ohlcv.isLoading ? 'Loading…' : `No OHLCV data for ${ticker}`}
              </p>
            ) : (
              <PriceChart data={chartData} markers={markers} height={420} />
            )}
          </CardContent>
        </Card>
      </div>

      <div className="mt-4 grid grid-cols-1 gap-4 lg:grid-cols-2">
        <Card>
          <CardHeader>
            <CardTitle>Indicators {indicators.data?.date ? `(${indicators.data.date})` : ''}</CardTitle>
          </CardHeader>
          <CardContent>
            {!indicators.data?.available ? (
              <p className="text-sm text-muted-foreground">No indicator data for {ticker}</p>
            ) : (
              <Table>
                <TableBody>
                  {CURATED_INDICATORS.map(([key, label, kind]) => (
                    <TableRow key={key}>
                      <TableCell className="text-muted-foreground">
                        <span className="inline-flex items-center gap-1">
                          {label}
                          {INDICATOR_TOOLTIPS[key] && <InfoTooltip>{INDICATOR_TOOLTIPS[key]}</InfoTooltip>}
                        </span>
                      </TableCell>
                      <TableCell className="text-right font-mono-data">{fmtIndicator(indicators.data?.indicators[key], kind)}</TableCell>
                    </TableRow>
                  ))}
                </TableBody>
              </Table>
            )}
          </CardContent>
        </Card>

        <Card>
          <CardHeader>
            <CardTitle>Chart patterns</CardTitle>
          </CardHeader>
          <CardContent>
            {!patterns.data?.available ? (
              <p className="text-sm text-muted-foreground">No pattern data for {ticker}</p>
            ) : (
              <div className="flex flex-col gap-2">
                {Object.entries(patterns.data.patterns).map(([key, v]) => (
                  <div key={key} className="flex items-center justify-between text-sm">
                    <span className="inline-flex items-center gap-1">
                      {PATTERN_LABELS[key] ?? key}
                      {PATTERN_TOOLTIPS[key] && <InfoTooltip>{PATTERN_TOOLTIPS[key]}</InfoTooltip>}
                    </span>
                    <span className="font-mono-data">{v == null ? '—' : `${Math.round(v * 100)}%`}</span>
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
