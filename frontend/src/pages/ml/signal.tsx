import { useState } from 'react'
import { useQuery } from '@tanstack/react-query'
import type { ColumnDef } from '@tanstack/react-table'
import { CartesianGrid, Line, LineChart, Tooltip, XAxis, YAxis } from 'recharts'

import { AppShell, Badge, Button, Card, CardContent, CardHeader, CardTitle, DataTable, InfoTooltip, ResponsiveChartCard, StatCard } from '@/lib/ui'
import { apiGet } from '@/shared/api/client'
import type {
  ForensicRow,
  MLSignalRow,
  MultibaggerRow,
  OHLCVRow,
  RegimeHistoryResponse,
} from './types'

function todayStr() {
  return new Date().toISOString().slice(0, 10)
}

function fmtPct(v: number | null | undefined) {
  return v == null ? '—' : `${(v * 100).toFixed(1)}%`
}

function fmtMoney(v: number | null | undefined) {
  return v == null ? '—' : `₹${v.toLocaleString('en-IN')}`
}

function directionVariant(dir: string | null) {
  if (dir === 'sell') return 'destructive' as const
  if (dir === 'buy') return 'success' as const
  return 'outline' as const
}

const EXIT_TYPE_TEXT: Record<string, string> = {
  thesis_broken: 'Thesis broken — stop-loss hit; the original entry thesis no longer holds.',
  momentum_exhaustion: 'Momentum exhaustion — the move has stalled well before target or stop.',
  risk_management: 'Risk management — position sized/trimmed to control portfolio risk.',
  target_achieved: 'Target achieved — the position hit its profit target.',
  opportunity_cost: 'Opportunity cost — max hold period reached without target or stop; capital reallocated.',
  pnd_exit: 'Pump-and-dump exit — a P&D risk pattern was detected after entry; exiting defensively.',
}

function pnlTone(v: number | null | undefined) {
  if (v == null) return undefined
  return v >= 0 ? 'text-green' : 'text-red'
}

function Sparkline({ series }: { series: number[] }) {
  if (series.length < 2) return <span>—</span>
  const width = 80
  const height = 24
  const min = Math.min(...series)
  const max = Math.max(...series)
  const range = max - min || 1
  const stepX = width / (series.length - 1)
  const points = series.map((v, i) => `${(i * stepX).toFixed(2)},${(height - ((v - min) / range) * height).toFixed(2)}`).join(' ')
  const lastUp = series[series.length - 1] >= series[0]
  const color = lastUp ? '#16a34a' : '#dc2626'
  return (
    <svg width={width} height={height} viewBox={`0 0 ${width} ${height}`} preserveAspectRatio="none">
      <polyline points={points} fill="none" stroke={color} strokeWidth={1.5} />
    </svg>
  )
}

interface PairedHistoryRow {
  buyDate: string
  buyRow: MLSignalRow
  sellDate: string | null
  sellRow: MLSignalRow | null
}

// Mirrors dashboard/static/ml/js/signal.js's pairBuySellHistory: collapses a
// Buy signal that persists across N consecutive days into one paired
// Buy-date/Sell-date row instead of showing every individual daily call.
// `rowsAscending` must be sorted oldest-first.
function pairBuySellHistory(rowsAscending: MLSignalRow[]): PairedHistoryRow[] {
  const pairs: PairedHistoryRow[] = []
  let open: { buyDate: string; buyRow: MLSignalRow } | null = null
  for (const r of rowsAscending) {
    const dir = r.signal_direction
    if (dir === 'buy') {
      if (!open) open = { buyDate: r.date.slice(0, 10), buyRow: r }
    } else if (dir === 'sell') {
      if (open) {
        pairs.push({ buyDate: open.buyDate, buyRow: open.buyRow, sellDate: r.date.slice(0, 10), sellRow: r })
        open = null
      }
    }
  }
  if (open) pairs.push({ buyDate: open.buyDate, buyRow: open.buyRow, sellDate: null, sellRow: null })
  return pairs
}

const modelColumns: ColumnDef<MLSignalRow, unknown>[] = [
  { accessorKey: 'model_name', header: 'Model' },
  {
    id: 'direction',
    header: 'Direction',
    cell: ({ row }) => <Badge variant={directionVariant(row.original.signal_direction)}>{row.original.signal_direction ?? '—'}</Badge>,
  },
  {
    accessorKey: 'buy_prob',
    header: () => (
      <span className="inline-flex items-center gap-1">
        Buy Prob
        <InfoTooltip>signal_5d's own probability that its call is "buy" (0-1). The only model AlphaLens actually trades paper positions off of.</InfoTooltip>
      </span>
    ),
    cell: (i) => fmtPct(i.getValue<number | null>()),
  },
  {
    accessorKey: 'meta_label',
    header: () => (
      <span className="inline-flex items-center gap-1">
        Meta
        <InfoTooltip>meta_labeler's Act/Don't-Act decision at its tuned threshold — a secondary filter on signal_5d's call.</InfoTooltip>
      </span>
    ),
    cell: (i) => i.getValue<string | null>() ?? '—',
  },
  {
    accessorKey: 'pnd_score',
    header: () => (
      <span className="inline-flex items-center gap-1">
        P&amp;D
        <InfoTooltip>pnd_detector's 0-100 pump-and-dump risk score from volume/price anomaly features.</InfoTooltip>
      </span>
    ),
    cell: (i) => i.getValue<number | null>()?.toFixed(0) ?? '—',
  },
  {
    accessorKey: 'exit_urgency',
    header: () => (
      <span className="inline-flex items-center gap-1">
        Exit Urgency
        <InfoTooltip>rule_based_exit_policy's 0-100 urgency score for exiting an existing position.</InfoTooltip>
      </span>
    ),
    cell: (i) => i.getValue<number | null>()?.toFixed(0) ?? '—',
  },
  {
    accessorKey: 'q50_return',
    header: () => (
      <span className="inline-flex items-center gap-1">
        Q50 Return
        <InfoTooltip>signal_5d's median (50th percentile) forecast forward return over its holding horizon, from its quantile-regression head.</InfoTooltip>
      </span>
    ),
    cell: (i) => fmtPct(i.getValue<number | null>()),
  },
  {
    id: 'interval',
    header: () => (
      <span className="inline-flex items-center gap-1">
        Interval
        <InfoTooltip>signal_5d's conformal-prediction bounds around its point forecast — a distinct uncertainty-quantification method from the Q10/Q90 quantile bounds.</InfoTooltip>
      </span>
    ),
    cell: ({ row }) => `${fmtPct(row.original.conformal_lower)} to ${fmtPct(row.original.conformal_upper)}`,
  },
]

interface HistoryDisplayRow {
  signal: MLSignalRow
  recDate: string
  recPrice: number | null
  cmp: number | null
  currentReturn: number | null
  trend: number[]
}

function buildHistoryColumns(): ColumnDef<HistoryDisplayRow, unknown>[] {
  return [
    { accessorKey: 'recDate', header: 'Recommended Date' },
    { accessorKey: 'recPrice', header: 'Recommended Price', cell: (i) => fmtMoney(i.getValue<number | null>()) },
    {
      id: 'q50_return',
      header: () => (
        <span className="inline-flex items-center gap-1">
          Expected Return (q50)
          <InfoTooltip>signal_5d's median (50th percentile) forecast forward return over its holding horizon, from its quantile-regression head.</InfoTooltip>
        </span>
      ),
      cell: ({ row }) => fmtPct(row.original.signal.q50_return),
    },
    { accessorKey: 'cmp', header: 'CMP', cell: (i) => fmtMoney(i.getValue<number | null>()) },
    {
      accessorKey: 'currentReturn',
      header: 'Current Return',
      cell: (i) => <span className={pnlTone(i.getValue<number | null>())}>{fmtPct(i.getValue<number | null>())}</span>,
    },
    {
      id: 'direction',
      header: 'Direction',
      cell: ({ row }) => <Badge variant={directionVariant(row.original.signal.signal_direction)}>{row.original.signal.signal_direction ?? '—'}</Badge>,
    },
    { id: 'trend', header: 'Trend', cell: ({ row }) => <Sparkline series={row.original.trend} /> },
  ]
}

const pairedHistoryColumns: ColumnDef<PairedHistoryRow & { buyPrice: number | null; sellPrice: number | null; cmp: number | null; ret: number | null; rationale: string }, unknown>[] = [
  { accessorKey: 'buyDate', header: 'Buy Date' },
  { accessorKey: 'buyPrice', header: 'Buy Price', cell: (i) => fmtMoney(i.getValue<number | null>()) },
  { accessorKey: 'sellDate', header: 'Sell Date', cell: (i) => i.getValue<string | null>() ?? '—' },
  { accessorKey: 'sellPrice', header: 'Sell Price', cell: (i) => fmtMoney(i.getValue<number | null>()) },
  { accessorKey: 'cmp', header: 'CMP', cell: (i) => fmtMoney(i.getValue<number | null>()) },
  { accessorKey: 'ret', header: 'Return', cell: (i) => <span className={pnlTone(i.getValue<number | null>())}>{fmtPct(i.getValue<number | null>())}</span> },
  { accessorKey: 'rationale', header: 'Rationale' },
]

function shapEntries(shapJson: string | null | undefined): { feature: string; value: number }[] {
  if (!shapJson) return []
  try {
    const parsed = JSON.parse(shapJson)
    const entries = Array.isArray(parsed) ? parsed : Object.entries(parsed).map(([k, v]) => ({ feature: k, value: v }))
    return entries.map((e: { feature?: string; value?: number; 0?: string; 1?: number }) => ({
      feature: e.feature ?? String(e[0] ?? '—'),
      value: Number(e.value ?? e[1] ?? 0),
    }))
  } catch {
    return []
  }
}

export function MlSignalPage() {
  const [tickerInput, setTickerInput] = useState('RELIANCE')
  const [dateInput, setDateInput] = useState(todayStr())
  const [ticker, setTicker] = useState('RELIANCE')
  const [date, setDate] = useState(todayStr())

  const forensic = useQuery({
    queryKey: ['ml-forensic', ticker],
    queryFn: () => apiGet<ForensicRow | null>(`/api/v1/signals/ml/forensic/${ticker}`),
  })
  const multibagger = useQuery({
    queryKey: ['ml-multibagger', ticker],
    queryFn: () => apiGet<MultibaggerRow | null>(`/api/v1/signals/ml/multibagger/${ticker}`),
  })
  const price = useQuery({
    queryKey: ['ohlcv-latest', ticker],
    queryFn: () => apiGet<OHLCVRow | null>(`/api/v1/ohlcv/${ticker}/latest`),
  })
  const signals = useQuery({
    queryKey: ['ml-signals-ticker-date', ticker, date],
    queryFn: () => apiGet<MLSignalRow[]>(`/api/v1/signals/ml/${ticker}/${date}`, { carry_forward: true }),
  })
  const history = useQuery({
    queryKey: ['ml-signal-history', ticker],
    queryFn: () => apiGet<MLSignalRow[]>(`/api/v1/signals/ml/history/${ticker}`, { model_name: 'signal_5d', n: 10 }),
  })
  const regimeHistory = useQuery({
    queryKey: ['macro-regime-history'],
    queryFn: () => apiGet<RegimeHistoryResponse>('/api/v1/macro/regime/history', { days: 30 }),
  })
  const ohlcvRange = useQuery({
    queryKey: ['ml-signal-ohlcv-range', ticker, history.data?.[0]?.date, history.data?.[history.data.length - 1]?.date],
    queryFn: () =>
      apiGet<{ data: OHLCVRow[] }>(`/api/v1/ohlcv/${ticker}`, {
        from_date: history.data![history.data!.length - 1].date.slice(0, 10),
        to_date: history.data![0].date.slice(0, 10),
      }),
    enabled: (history.data?.length ?? 0) > 0,
  })

  const shapRow = signals.data?.find((r) => r.shap_top5_json)
  const shap = shapEntries(shapRow?.shap_top5_json)
  const regimeChartData = (regimeHistory.data?.days ?? []).map((d) => ({
    date: d.date?.slice(0, 10) ?? '',
    prob: d.hmm_regime_prob,
  }))

  const sortedOhlcv = [...(ohlcvRange.data?.data ?? [])].sort((a, b) => String(a.date).localeCompare(String(b.date)))
  const closeByDate: Record<string, number> = {}
  for (const r of sortedOhlcv) closeByDate[String(r.date).slice(0, 10)] = r.close
  const cmp = price.data?.close ?? null

  const historyRows: HistoryDisplayRow[] = (history.data ?? []).map((r) => {
    const recDate = r.date.slice(0, 10)
    const recPrice = closeByDate[recDate] ?? null
    const currentReturn = recPrice && cmp ? cmp / recPrice - 1 : null
    const trend = sortedOhlcv.filter((o) => String(o.date).slice(0, 10) >= recDate).map((o) => o.close)
    return { signal: r, recDate, recPrice, cmp, currentReturn, trend }
  })

  const latestSignal = history.data?.[0] ?? null
  const carryForwardDate = signals.data?.[0]?.date?.slice(0, 10) ?? null

  const ascendingHistory = [...(history.data ?? [])].reverse()
  const pairs = pairBuySellHistory(ascendingHistory)
  const pairedRows = pairs
    .slice()
    .reverse()
    .map((p) => {
      const buyPrice = closeByDate[p.buyDate] ?? null
      const sellPrice = p.sellDate ? closeByDate[p.sellDate] ?? null : null
      const isOpen = p.sellDate === null
      const refPrice = isOpen ? cmp : sellPrice
      const ret = buyPrice && refPrice ? refPrice / buyPrice - 1 : null
      const rationale = p.sellRow?.exit_type
        ? EXIT_TYPE_TEXT[p.sellRow.exit_type] ?? p.sellRow.exit_type
        : isOpen
          ? 'Position still open — no Sell call yet'
          : '—'
      return { ...p, buyPrice, sellPrice: isOpen ? null : sellPrice, cmp: isOpen ? cmp : null, ret, rationale }
    })

  return (
    <AppShell title="ML — Signal Deep Dive" description="Per-ticker model scores, SHAP drivers, and buy/sell call history across all ML models.">
      <Card>
        <CardHeader>
          <CardTitle>Ticker &amp; date</CardTitle>
        </CardHeader>
        <CardContent>
          <div className="flex flex-wrap items-center gap-2">
            <input
              className="h-9 w-40 rounded-[var(--radius-token)] border border-border bg-transparent px-3 text-sm"
              value={tickerInput}
              onChange={(e) => setTickerInput(e.target.value.toUpperCase())}
              placeholder="Ticker"
            />
            <input
              className="h-9 w-40 rounded-[var(--radius-token)] border border-border bg-transparent px-3 text-sm"
              type="date"
              value={dateInput}
              onChange={(e) => setDateInput(e.target.value)}
            />
            <Button
              onClick={() => {
                setTicker(tickerInput.trim().toUpperCase())
                setDate(dateInput)
              }}
            >
              Load
            </Button>
            {forensic.data ? (
              <Badge variant={forensic.data.forensic_flag_label === 'green' ? 'success' : ['red', 'black'].includes(forensic.data.forensic_flag_label ?? '') ? 'destructive' : 'warning'}>
                Forensic: {forensic.data.forensic_flag_label ?? '—'} ({forensic.data.forensic_composite?.toFixed(0) ?? '—'})
              </Badge>
            ) : null}
            {forensic.data ? (
              <InfoTooltip>forensic_ml's 5-level taxonomy (green/yellow/orange/red/black), carried forward from ml_forensic's most recent weekly scoring run — can be several days stale.</InfoTooltip>
            ) : null}
            {multibagger.data?.mb_probability != null ? <Badge variant="outline">Multibagger: {fmtPct(multibagger.data.mb_probability)}</Badge> : null}
            {multibagger.data?.mb_probability != null ? (
              <InfoTooltip>The MultibaggerModel's probability estimate, carried forward from ml_multibagger's most recent (typically weekly) run. Not a return multiplier prediction.</InfoTooltip>
            ) : null}
          </div>
        </CardContent>
      </Card>

      <div className="mt-4 grid grid-cols-2 gap-4 sm:grid-cols-4">
        <StatCard label="Close" value={price.data ? fmtMoney(price.data.close) : '—'} />
        <StatCard label="High" value={price.data ? fmtMoney(price.data.high) : '—'} />
        <StatCard label="Low" value={price.data ? fmtMoney(price.data.low) : '—'} />
        <StatCard label="Volume" value={price.data ? price.data.volume.toLocaleString('en-IN') : '—'} />
      </div>

      <div className="mt-4">
        <Card>
          <CardHeader>
            <CardTitle>Model scores</CardTitle>
          </CardHeader>
          <CardContent>
            {signals.error ? (
              <p className="text-sm text-red">
                Could not reach GET /api/v1/signals/ml/{'{ticker}'}/{'{date}'} — {(signals.error as Error).message}
              </p>
            ) : (
              <>
                {carryForwardDate && carryForwardDate !== date ? (
                  <p className="mb-2 text-sm text-muted-foreground">
                    Showing the last generated signal, from {carryForwardDate} — none for {date} yet.
                  </p>
                ) : null}
                <DataTable columns={modelColumns} data={signals.data ?? []} isLoading={signals.isLoading} emptyMessage="No signal has ever been generated for this ticker on or before this date." />
              </>
            )}
          </CardContent>
        </Card>
      </div>

      <div className="mt-4">
        <Card>
          <CardHeader>
            <CardTitle className="inline-flex items-center gap-1">
              SHAP top features (most recent scored model)
              <InfoTooltip>The two largest-magnitude contributors (by absolute SHAP value) among signal_5d's top-5 feature attributions for that call.</InfoTooltip>
            </CardTitle>
          </CardHeader>
          <CardContent>
            {shap.length ? (
              <div className="flex flex-col gap-2">
                {shap.map((e) => (
                  <div key={e.feature} className="flex items-center gap-2 text-sm">
                    <span className="w-40 truncate">{e.feature}</span>
                    <div className="h-2 flex-1 rounded bg-muted">
                      <div
                        className={`h-2 rounded ${e.value >= 0 ? 'bg-green' : 'bg-red'}`}
                        style={{ width: `${Math.min(Math.abs(e.value) * 100, 100)}%` }}
                      />
                    </div>
                    <span className="w-16 text-right font-mono-data">{e.value.toFixed(4)}</span>
                  </div>
                ))}
              </div>
            ) : (
              <p className="text-sm text-muted-foreground">No SHAP data for this ticker/date.</p>
            )}
          </CardContent>
        </Card>
      </div>

      <div className="mt-4">
        <ResponsiveChartCard title="Market regime probability (30d)" description={regimeHistory.error ? 'Failed to load' : undefined} height={220}>
          <LineChart data={regimeChartData}>
            <CartesianGrid strokeDasharray="3 3" stroke="var(--border)" />
            <XAxis dataKey="date" tick={{ fontSize: 10 }} />
            <YAxis tick={{ fontSize: 11 }} domain={[0, 1]} />
            <Tooltip />
            <Line type="monotone" dataKey="prob" stroke="var(--teal)" dot={false} strokeWidth={2} />
          </LineChart>
        </ResponsiveChartCard>
      </div>

      <div className="mt-4">
        <Card>
          <CardHeader>
            <CardTitle>signal_5d call history (last 10)</CardTitle>
          </CardHeader>
          <CardContent>
            {history.error ? (
              <p className="text-sm text-red">
                Could not reach GET /api/v1/signals/ml/history/{'{ticker}'} — {(history.error as Error).message}
              </p>
            ) : (
              <DataTable columns={buildHistoryColumns()} data={historyRows} isLoading={history.isLoading} emptyMessage="No signal_5d history for this ticker." />
            )}
          </CardContent>
        </Card>
      </div>

      <div className="mt-4">
        <Card>
          <CardHeader>
            <CardTitle>Sell rationale</CardTitle>
          </CardHeader>
          <CardContent>
            {!latestSignal || latestSignal.exit_urgency == null ? (
              <p className="text-sm text-muted-foreground">No exit signal on the latest call — nothing to act on.</p>
            ) : (
              <div className={`rounded-md border-l-4 p-3 ${latestSignal.exit_urgency >= 50 ? 'border-red' : 'border-amber'}`}>
                <div className="mb-1 flex items-center gap-2 font-semibold">
                  {latestSignal.exit_urgency >= 50 ? 'Sell Recommendation' : 'Watch — Not Yet a Sell'}
                  <Badge variant={latestSignal.exit_urgency >= 50 ? 'destructive' : 'warning'}>urgency {latestSignal.exit_urgency.toFixed(0)}</Badge>
                </div>
                <p className="text-sm text-muted-foreground">
                  {latestSignal.exit_type ? EXIT_TYPE_TEXT[latestSignal.exit_type] ?? latestSignal.exit_type : 'No exit_type recorded on the latest call.'}
                </p>
              </div>
            )}
          </CardContent>
        </Card>
      </div>

      <div className="mt-4">
        <Card>
          <CardHeader>
            <CardTitle>Buy/sell-paired call history</CardTitle>
          </CardHeader>
          <CardContent>
            <DataTable columns={pairedHistoryColumns} data={pairedRows} emptyMessage="No buy/sell-paired calls for this ticker in the last 10 signal_5d calls." />
          </CardContent>
        </Card>
      </div>
    </AppShell>
  )
}
