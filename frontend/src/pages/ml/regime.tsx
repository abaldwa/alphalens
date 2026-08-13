import { useState } from 'react'
import { useQuery } from '@tanstack/react-query'
import type { ColumnDef } from '@tanstack/react-table'

import { AppShell, Badge, Card, CardContent, CardDescription, CardHeader, CardTitle, DataTable, InfoTooltip, tickerColumn } from '@/lib/ui'
import { apiGet } from '@/shared/api/client'

function todayStr() {
  return new Date().toISOString().slice(0, 10)
}

interface PerTickerRegimeRow {
  ticker: string
  hmm_regime: number | null
  hmm_regime_name: string | null
  hmm_regime_prob_bullish: number | null
  hmm_regime_prob_bearish: number | null
  hmm_regime_duration: number | null
  hmm_regime_transition: boolean | null
  hmm_regime_stability: number | null
}

interface PerTickerRegimeResponse {
  date: string
  tickers: PerTickerRegimeRow[]
}

function fmtPct(v: number | null | undefined) {
  if (v == null) return '--'
  return `${(v * 100).toFixed(1)}%`
}

function regimeVariant(name: string | null) {
  if (name === 'bullish') return 'success' as const
  if (name === 'bearish') return 'destructive' as const
  return 'outline' as const
}

const columns: ColumnDef<PerTickerRegimeRow, unknown>[] = [
  tickerColumn<PerTickerRegimeRow>('ml'),
  {
    accessorKey: 'hmm_regime_name',
    header: 'Regime',
    cell: (i) => {
      const name = i.getValue<string | null>()
      return <Badge variant={regimeVariant(name)}>{name ?? '—'}</Badge>
    },
  },
  {
    accessorKey: 'hmm_regime_prob_bullish',
    header: () => (
      <span className="inline-flex items-center gap-1">
        Bullish %
        <InfoTooltip>Probability of the bullish state (rank 3) for this ticker's HMM.</InfoTooltip>
      </span>
    ),
    meta: { align: 'right' as const },
    cell: (i) => fmtPct(i.getValue<number | null>()),
  },
  {
    accessorKey: 'hmm_regime_prob_bearish',
    header: () => (
      <span className="inline-flex items-center gap-1">
        Bearish %
        <InfoTooltip>Probability of the bearish state (rank 0) for this ticker's HMM.</InfoTooltip>
      </span>
    ),
    meta: { align: 'right' as const },
    cell: (i) => fmtPct(i.getValue<number | null>()),
  },
  {
    accessorKey: 'hmm_regime_stability',
    header: () => (
      <span className="inline-flex items-center gap-1">
        Stability
        <InfoTooltip>Max state probability — how confident the HMM is about the current regime. Higher = clearer regime.</InfoTooltip>
      </span>
    ),
    meta: { align: 'right' as const },
    cell: (i) => fmtPct(i.getValue<number | null>()),
  },
  {
    accessorKey: 'hmm_regime_duration',
    header: 'Duration',
    meta: { align: 'right' as const },
    cell: (i) => {
      const v = i.getValue<number | null>()
      return v != null ? `${Math.round(v)}d` : '--'
    },
  },
  {
    accessorKey: 'hmm_regime_transition',
    header: () => (
      <span className="inline-flex items-center gap-1">
        Flip
        <InfoTooltip>True = regime changed vs prior day (the "hidden pattern arriving" signal).</InfoTooltip>
      </span>
    ),
    cell: (i) => i.getValue<boolean | null>() ? <Badge variant="warning">NEW</Badge> : <span className="text-muted-foreground">—</span>,
  },
]

export function MlRegimePage() {
  const [dateInput, setDateInput] = useState(todayStr())
  const [date, setDate] = useState<string | null>(todayStr())

  const { data, isLoading, error } = useQuery({
    queryKey: ['ml-regime-per-ticker', date],
    // `date` is nullable but the query is gated on `enabled` below, so it is
    // never actually null here; apiGet's params reject null, and coercing to
    // undefined keeps the key out of the query string rather than sending
    // "as_of=null".
    queryFn: () =>
      apiGet<PerTickerRegimeResponse>('/api/v1/macro/regime/per-ticker', {
        as_of: date ?? undefined,
      }),
    enabled: !!date,
  })

  const tickers = data?.tickers ?? []
  const transitioning = tickers.filter((t) => t.hmm_regime_transition)
  const bullish = tickers.filter((t) => t.hmm_regime_name === 'bullish')
  const bearish = tickers.filter((t) => t.hmm_regime_name === 'bearish')

  return (
    <AppShell
      title="HMM Regime — Per-Ticker"
      description="Hidden Markov Model regime detector: each stock's own latent state (bearish/sideways/bullish) from its price/volume history."
    >
      <div className="mb-4 flex items-end gap-3">
        <div>
          <label className="text-sm text-muted-foreground">Date</label>
          <input
            type="date"
            className="block w-40 rounded border bg-background px-2 py-1 text-sm"
            value={dateInput}
            onChange={(e) => setDateInput(e.target.value)}
          />
        </div>
        <button
          className="rounded bg-primary px-3 py-1 text-sm text-primary-foreground hover:bg-primary/90"
          onClick={() => setDate(dateInput)}
        >
          Load
        </button>
        {data?.date && (
          <span className="text-sm text-muted-foreground">
            Showing {tickers.length} tickers for {data.date}
          </span>
        )}
      </div>

      <div className="mb-4 grid grid-cols-2 gap-3 sm:grid-cols-4">
        <Card>
          <CardHeader className="pb-1"><CardTitle className="text-lg">{tickers.length}</CardTitle></CardHeader>
          <CardContent className="text-xs text-muted-foreground">Total regime-scored</CardContent>
        </Card>
        <Card>
          <CardHeader className="pb-1"><CardTitle className="text-lg text-green">{bullish.length}</CardTitle></CardHeader>
          <CardContent className="text-xs text-muted-foreground">Bullish</CardContent>
        </Card>
        <Card>
          <CardHeader className="pb-1"><CardTitle className="text-lg text-red">{bearish.length}</CardTitle></CardHeader>
          <CardContent className="text-xs text-muted-foreground">Bearish</CardContent>
        </Card>
        <Card>
          <CardHeader className="pb-1"><CardTitle className="text-lg text-yellow">{transitioning.length}</CardTitle></CardHeader>
          <CardContent className="text-xs text-muted-foreground">Regime flips today</CardContent>
        </Card>
      </div>

      {transitioning.length > 0 && (
        <Card className="mb-4 border-yellow/50">
          <CardHeader>
            <CardTitle className="text-yellow">Regime Transitions Today ({transitioning.length})</CardTitle>
            <CardDescription>Stocks whose hidden state just changed — the "pattern arriving" signal.</CardDescription>
          </CardHeader>
          <CardContent>
            <DataTable columns={columns} data={transitioning} emptyMessage="No transitions." />
          </CardContent>
        </Card>
      )}

      <Card>
        <CardHeader>
          <CardTitle>All Tickers — {data?.date ?? '—'}</CardTitle>
          <CardDescription>
            Sorted by regime stability (highest confidence first).
            {isLoading && ' Loading...'}
            {error && <span className="text-red"> Failed to load.</span>}
          </CardDescription>
        </CardHeader>
        <CardContent>
          <DataTable columns={columns} data={tickers} emptyMessage="No regime data for this date." />
        </CardContent>
      </Card>
    </AppShell>
  )
}
