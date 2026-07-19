import { useQuery } from '@tanstack/react-query'
import type { ColumnDef } from '@tanstack/react-table'

import { AppShell, Badge, Card, CardContent, CardDescription, CardHeader, CardTitle, DataTable, InfoTooltip, StatCard, TickerLink } from '@/lib/ui'
import { apiGet } from '@/shared/api/client'
import type { AlertsResponse, DailyWatchlistResponse, DailyWatchlistRow, MLSignalRow, MultibaggerRow, PaperTradingStateResponse, RegimeResponse } from './types'

function todayStr() {
  return new Date().toISOString().slice(0, 10)
}
function fmtPct(v: number | null | undefined) {
  return v == null ? '—' : `${(v * 100).toFixed(1)}%`
}
function fmtMoney(v: number | null | undefined) {
  return v == null ? '—' : `₹${v.toLocaleString('en-IN')}`
}
function pnlTone(v: number | null | undefined) {
  if (v == null) return undefined
  return v >= 0 ? 'text-green' : 'text-red'
}
function horizonLabel(modelName: string | null) {
  const m = /(\d+)d$/.exec(modelName ?? '')
  return m ? `${m[1]} days` : '—'
}
function severityVariant(sev: string) {
  if (sev === 'high') return 'destructive' as const
  if (sev === 'medium') return 'warning' as const
  return 'outline' as const
}
function regimeVariant(regime: string | null) {
  if (regime === 'bull') return 'success' as const
  if (regime === 'bear') return 'destructive' as const
  return 'outline' as const
}

const topBuysColumns: ColumnDef<MLSignalRow, unknown>[] = [
  { accessorKey: 'ticker', header: 'Stock', cell: (i) => <TickerLink ticker={i.getValue<string>()} /> },
  {
    id: 'signal',
    header: 'Signal',
    cell: ({ row }) => (
      <Badge variant={row.original.signal_direction === 'sell' ? 'destructive' : 'success'}>
        {(row.original.signal_direction ?? '—').toUpperCase()} {horizonLabel(row.original.model_name)}
      </Badge>
    ),
  },
  { accessorKey: 'buy_prob', header: 'Prob', cell: (i) => fmtPct(i.getValue<number | null>()) },
  { accessorKey: 'meta_label', header: 'Meta', cell: (i) => i.getValue<string | null>() ?? '—' },
  {
    id: 'interval',
    header: 'Interval',
    cell: ({ row }) => `${fmtPct(row.original.conformal_lower)} to ${fmtPct(row.original.conformal_upper)}`,
  },
  { accessorKey: 'pnd_score', header: 'P&D', cell: (i) => i.getValue<number | null>()?.toFixed(0) ?? '—' },
  { accessorKey: 'hmm_regime', header: 'Regime', cell: (i) => i.getValue<string | null>() ?? '—' },
]

const watchlistColumns: ColumnDef<DailyWatchlistRow, unknown>[] = [
  { accessorKey: 'ticker', header: 'Stock', cell: (i) => <TickerLink ticker={i.getValue<string>()} /> },
  { accessorKey: 'company_name', header: 'Name', cell: (i) => i.getValue<string | null>() ?? '—' },
  { accessorKey: 'sector', header: 'Sector', cell: (i) => i.getValue<string | null>() ?? '—' },
  {
    accessorKey: 'buy_prob',
    header: () => (
      <span className="inline-flex items-center gap-1">
        Buy Prob*
        <InfoTooltip>Buy/hold/sell classifier's own probability — a separate model head from Target/Expected Return below.</InfoTooltip>
      </span>
    ),
    cell: (i) => fmtPct(i.getValue<number | null>()),
  },
  { accessorKey: 'current_price', header: 'Price', cell: (i) => fmtMoney(i.getValue<number | null>()) },
  {
    accessorKey: 'target_price',
    header: () => (
      <span className="inline-flex items-center gap-1">
        Target*
        <InfoTooltip>Median (q50) of the quantile-regressor's forward-return distribution — independent of Buy Prob*.</InfoTooltip>
      </span>
    ),
    cell: (i) => fmtMoney(i.getValue<number | null>()),
  },
  {
    accessorKey: 'expected_return_pct',
    header: () => (
      <span className="inline-flex items-center gap-1">
        Expected Return*
        <InfoTooltip>Median (q50) of the quantile-regressor's forward-return distribution — independent of Buy Prob*.</InfoTooltip>
      </span>
    ),
    cell: (i) => <span className={pnlTone(i.getValue<number | null>())}>{i.getValue<number | null>() != null ? `${(i.getValue<number>() > 0 ? '+' : '')}${i.getValue<number>().toFixed(1)}%` : '—'}</span>,
  },
  {
    id: 'range',
    header: 'Range (low-high)',
    cell: ({ row }) => (row.original.target_low != null && row.original.target_high != null ? `${fmtMoney(row.original.target_low)} – ${fmtMoney(row.original.target_high)}` : '—'),
  },
  {
    accessorKey: 'target_basis',
    header: 'Basis',
    cell: (i) => <Badge variant="outline">{i.getValue<string>() === 'quantile' ? 'model' : i.getValue<string>() === 'atr' ? 'ATR-est.' : '—'}</Badge>,
  },
]

const multibaggerColumns: ColumnDef<MultibaggerRow, unknown>[] = [
  { accessorKey: 'ticker', header: 'Ticker', cell: (i) => <TickerLink ticker={i.getValue<string>()} /> },
  { accessorKey: 'mb_probability', header: 'MB Prob', cell: (i) => fmtPct(i.getValue<number | null>()) },
  { accessorKey: 'mb_tier', header: 'Deterministic Probability Band', cell: (i) => <Badge variant="outline">{i.getValue<string | null>() ?? '—'}</Badge> },
  { accessorKey: 'mb_archetype', header: 'Archetype', cell: (i) => i.getValue<string | null>() ?? '—' },
  { accessorKey: 'survival_6m', header: '6m', cell: (i) => fmtPct(i.getValue<number | null>()) },
  { accessorKey: 'survival_12m', header: '12m', cell: (i) => fmtPct(i.getValue<number | null>()) },
  { accessorKey: 'survival_18m', header: '18m', cell: (i) => fmtPct(i.getValue<number | null>()) },
  { accessorKey: 'survival_24m', header: '24m', cell: (i) => fmtPct(i.getValue<number | null>()) },
  { accessorKey: 'survival_36m', header: '36m', cell: (i) => fmtPct(i.getValue<number | null>()) },
]

function HorizonSection({ title, rows }: { title: string; rows: DailyWatchlistRow[] }) {
  return (
    <Card className="mt-4">
      <CardHeader>
        <CardTitle>{title}</CardTitle>
      </CardHeader>
      <CardContent>
        <DataTable columns={watchlistColumns} data={rows} emptyMessage="No buy signals for this horizon on the latest available date." />
      </CardContent>
    </Card>
  )
}

export function MlPage() {
  const today = todayStr()

  const regime = useQuery({
    queryKey: ['macro-regime'],
    queryFn: () => apiGet<RegimeResponse>('/api/v1/macro/regime'),
  })
  const alerts = useQuery({
    queryKey: ['alerts-today'],
    queryFn: () => apiGet<AlertsResponse>('/api/v1/alerts/today'),
  })
  const topBuys = useQuery({
    queryKey: ['ml-top-buys', today],
    queryFn: () => apiGet<MLSignalRow[]>(`/api/v1/signals/ml/top_buys/${today}`, { n: 100, carry_forward: true }),
  })
  const positions = useQuery({
    queryKey: ['paper-trading-state-mini'],
    queryFn: () => apiGet<PaperTradingStateResponse>('/api/v1/paper_trading/state'),
  })
  const watchlist = useQuery({
    queryKey: ['watchlist-daily-hub'],
    queryFn: () => apiGet<DailyWatchlistResponse>('/api/v1/watchlist/daily', { n_per_horizon: 10 }),
  })
  const top2Tickers = (topBuys.data ?? []).slice(0, 2).map((r) => r.ticker)
  const latestPrices = useQuery({
    queryKey: ['ml-hub-latest-prices', top2Tickers.join(',')],
    queryFn: async () => {
      const results = await Promise.all(
        top2Tickers.map((t) =>
          apiGet<{ close: number | null } | null>(`/api/v1/ohlcv/${t}/latest`).catch(() => null),
        ),
      )
      return results.map((r) => r?.close ?? null)
    },
    enabled: top2Tickers.length > 0,
  })

  const realPositions = (positions.data?.positions ?? []).filter((p) => p.ticker !== '_HEARTBEAT_').slice(0, 3)
  const signalDate = topBuys.data?.[0]?.date?.slice(0, 10) ?? null
  const top2 = topBuys.data?.slice(0, 2) ?? []

  const byHorizon: Record<string, DailyWatchlistRow[]> = { '5d': [], '21d': [], '63d': [] }
  for (const row of watchlist.data?.rows ?? []) {
    if (byHorizon[row.horizon]) byHorizon[row.horizon].push(row)
  }
  const multibagger = (watchlist.data?.multibagger ?? []) as unknown as MultibaggerRow[]
  const lowLiqMultibagger = (watchlist.data?.low_liquidity_multibagger ?? []) as unknown as MultibaggerRow[]

  return (
    <AppShell title="ML — Daily Insight Hub" description="Today's ML signal alerts, top buy signals, open positions, and the daily multi-horizon watchlist — all models in one place.">
      <Card>
        <CardHeader>
          <CardTitle>Market regime</CardTitle>
        </CardHeader>
        <CardContent>
          {regime.error ? (
            <p className="text-sm text-red">Could not reach GET /api/v1/macro/regime — {(regime.error as Error).message}</p>
          ) : !regime.data?.available ? (
            <p className="text-sm text-muted-foreground">No regime data yet.</p>
          ) : (
            <div className="flex items-center gap-4">
              <Badge variant={regimeVariant(regime.data.hmm_regime)}>● {regime.data.hmm_regime ?? '—'}</Badge>
              <span className="text-xs text-muted-foreground">
                confidence {fmtPct(regime.data.hmm_regime_prob)} · stability {regime.data.hmm_stability?.toFixed(2) ?? '—'} · as of {regime.data.date?.slice(0, 10) ?? '—'}
              </span>
            </div>
          )}
        </CardContent>
      </Card>

      <div className="mt-4">
        <Card>
          <CardHeader>
            <CardTitle>Alerts</CardTitle>
          </CardHeader>
          <CardContent>
            {alerts.error ? (
              <p className="text-sm text-red">Could not reach GET /api/v1/alerts/today — {(alerts.error as Error).message}</p>
            ) : !alerts.data?.alerts.length ? (
              <p className="text-sm text-muted-foreground">No alerts today.</p>
            ) : (
              <div className="flex flex-col gap-2">
                {alerts.data.alerts.slice(0, 5).map((a, i) => (
                  <div key={i} className="flex items-center gap-2 text-sm">
                    <Badge variant={severityVariant(a.severity)}>{a.alert_type}</Badge>
                    <span>{a.message}</span>
                  </div>
                ))}
                {alerts.data.alerts.length > 5 ? <p className="text-xs text-muted-foreground">+{alerts.data.alerts.length - 5} more</p> : null}
              </div>
            )}
          </CardContent>
        </Card>
      </div>

      <div className="mt-4 grid grid-cols-2 gap-4 sm:grid-cols-4">
        <StatCard label="Top buy signals" value={topBuys.data?.length ?? '—'} />
        <StatCard label="Open positions" value={realPositions.length} />
        <StatCard label="Total equity" value={fmtMoney(positions.data?.total_equity)} />
        <StatCard label="5d/21d/63d watchlist rows" value={watchlist.data?.count ?? '—'} />
      </div>

      {top2.length > 0 ? (
        <div className="mt-4 grid grid-cols-1 gap-4 sm:grid-cols-2">
          {top2.map((r, i) => (
            <Card key={r.ticker}>
              <CardHeader>
                <div className="flex items-center gap-2">
                  <TickerLink ticker={r.ticker} />
                  <Badge variant={r.signal_direction === 'sell' ? 'destructive' : 'success'}>
                    {(r.signal_direction ?? '—').toUpperCase()} {horizonLabel(r.model_name)}
                  </Badge>
                  {r.meta_label ? <Badge variant="secondary">Meta: {r.meta_label}</Badge> : null}
                </div>
              </CardHeader>
              <CardContent>
                <div className="grid grid-cols-2 gap-3 sm:grid-cols-4">
                  <div>
                    <p className="text-xs text-muted-foreground">Entry Point</p>
                    <p className="font-semibold">{fmtMoney(latestPrices.data?.[i] ?? null)}</p>
                    <p className="text-xs text-muted-foreground">Latest close</p>
                  </div>
                  <div>
                    <p className="text-xs text-muted-foreground">Expected Return</p>
                    <p className="font-semibold">{fmtPct(r.q50_return)}</p>
                    <p className="text-xs text-muted-foreground">Range: {fmtPct(r.conformal_lower)} to {fmtPct(r.conformal_upper)}</p>
                  </div>
                  <div>
                    <p className="text-xs text-muted-foreground">Duration</p>
                    <p className="font-semibold">{horizonLabel(r.model_name)}</p>
                    <p className="text-xs text-muted-foreground">Signal horizon</p>
                  </div>
                  <div>
                    <p className="text-xs text-muted-foreground">Confidence</p>
                    <p className="font-semibold">{fmtPct(r.buy_prob)}</p>
                    <p className="text-xs text-muted-foreground">Buy probability</p>
                  </div>
                </div>
              </CardContent>
            </Card>
          ))}
        </div>
      ) : null}

      <div className="mt-4">
        <Card>
          <CardHeader>
            <CardTitle>Top buy signals</CardTitle>
            <CardDescription>
              {topBuys.error
                ? 'Failed to load'
                : signalDate && signalDate !== today
                  ? `Showing the last generated signals, from ${signalDate} — today's run hasn't produced signals yet.`
                  : undefined}
            </CardDescription>
          </CardHeader>
          <CardContent>
            <DataTable columns={topBuysColumns} data={topBuys.data ?? []} isLoading={topBuys.isLoading} emptyMessage="No buy signals generated yet." />
          </CardContent>
        </Card>
      </div>

      <div className="mt-4">
        <Card>
          <CardHeader>
            <div className="flex items-center justify-between">
              <CardTitle>Open positions (top 3)</CardTitle>
              <a href="/ml-positions.html" className="text-xs text-muted-foreground hover:underline">View All &rarr;</a>
            </div>
          </CardHeader>
          <CardContent>
            {positions.error ? (
              <p className="text-sm text-red">Could not reach GET /api/v1/paper_trading/state — {(positions.error as Error).message}</p>
            ) : !realPositions.length ? (
              <p className="text-sm text-muted-foreground">No open positions.</p>
            ) : (
              <div className="flex flex-col gap-2">
                {realPositions.map((p) => (
                  <div key={p.ticker} className="flex items-center gap-4 text-sm">
                    <span className="w-24 font-semibold">{p.ticker}</span>
                    <span className={pnlTone(p.unrealised_pnl_pct)}>{fmtPct(p.unrealised_pnl_pct)}</span>
                    <span className="text-muted-foreground">{p.entry_date}</span>
                  </div>
                ))}
              </div>
            )}
          </CardContent>
        </Card>
      </div>

      <div className="mt-4">
        <Card>
          <CardHeader>
            <CardTitle>Daily WatchList</CardTitle>
            <CardDescription>
              {watchlist.error
                ? 'Failed to load'
                : watchlist.data?.date
                  ? `Signals for ${watchlist.data.date} — targets from the model's own quantile-regression forward-return distribution (or a volatility/ATR-scaled band when unavailable), never a fixed %. *Buy Prob and Target/Expected Return are independent model outputs (classifier vs. quantile regressor) and can disagree.`
                  : 'No signal data available yet'}
            </CardDescription>
          </CardHeader>
        </Card>
      </div>

      <HorizonSection title="5-Day Horizon" rows={byHorizon['5d']} />
      <HorizonSection title="21-Day Horizon" rows={byHorizon['21d']} />
      <HorizonSection title="63-Day Horizon" rows={byHorizon['63d']} />

      <div className="mt-4">
        <Card>
          <CardHeader>
            <div className="flex items-center justify-between">
              <CardTitle>MultiBagger Candidates</CardTitle>
              <a href="/ml-multibagger.html" className="text-xs text-muted-foreground hover:underline">View All &rarr;</a>
            </div>
            <CardDescription>{multibagger.length ? `Top ${multibagger.length} by multibagger probability` : undefined}</CardDescription>
          </CardHeader>
          <CardContent>
            <DataTable columns={multibaggerColumns} data={multibagger} emptyMessage="No multibagger scoring data yet." />
          </CardContent>
        </Card>
      </div>

      <div className="mt-4">
        <Card>
          <CardHeader>
            <CardTitle>Stocks with ADTV &lt; 20Cr</CardTitle>
            <CardDescription>
              {lowLiqMultibagger.length
                ? `${lowLiqMultibagger.length} picks below the Rs20cr/day ADTV recommendation floor — shown separately, not filtered into the main list above`
                : undefined}
            </CardDescription>
          </CardHeader>
          <CardContent>
            <DataTable columns={multibaggerColumns} data={lowLiqMultibagger} emptyMessage="No multibagger scoring data yet." />
          </CardContent>
        </Card>
      </div>
    </AppShell>
  )
}
