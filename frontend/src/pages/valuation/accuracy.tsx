import { useState } from 'react'
import { useQuery } from '@tanstack/react-query'
import type { ColumnDef } from '@tanstack/react-table'

import { AppShell, Badge, Button, Card, CardContent, CardHeader, CardTitle, DataTable, InfoTooltip, StatCard, formatCurrencyINR, tickerColumn } from '@/lib/ui'
import { apiGet } from '@/shared/api/client'

interface AccuracyRow {
  ticker: string
  signal_date: string
  lifecycle_stage: string | null
  margin_of_safety: number | null
  predicted_undervalued: boolean | null
  entry_price: number | null
  realized_date: string
  realized_price: number | null
  realized_return_pct: number
  hit: boolean | null
}

interface AccuracyResponse {
  horizon_days: number
  count: number
  scored: number
  hits: number
  hit_rate: number | null
  avg_return_undervalued_pct: number | null
  avg_return_overvalued_pct: number | null
  rows: AccuracyRow[]
}

const fmtMoney = formatCurrencyINR

function fmtNum(v: number | null | undefined, digits = 2): string {
  return v == null ? '—' : v.toFixed(digits)
}

const columns: ColumnDef<AccuracyRow, unknown>[] = [
  tickerColumn<AccuracyRow>(),
  { accessorKey: 'signal_date', header: 'Signal Date', meta: { priority: 'low' } },
  { accessorKey: 'lifecycle_stage', header: 'Lifecycle', meta: { priority: 'low' }, cell: (i) => i.getValue<string | null>() ?? '—' },
  {
    accessorKey: 'margin_of_safety',
    header: () => (
      <span className="inline-flex items-center gap-1">
        MoS
        <InfoTooltip>Margin of Safety — the gap between intrinsic value and price at the time of the signal, as a fraction. Positive means the model called it undervalued.</InfoTooltip>
      </span>
    ),
    meta: { priority: 'low', align: 'right' },
    cell: (i) => fmtNum(i.getValue<number | null>()),
  },
  {
    accessorKey: 'predicted_undervalued',
    header: () => (
      <span className="inline-flex items-center gap-1">
        Predicted
        <InfoTooltip>What the valuation model predicted at signal time, based on the sign of the margin of safety.</InfoTooltip>
      </span>
    ),
    cell: (i) => {
      const v = i.getValue<boolean | null>()
      if (v == null) return '—'
      return <Badge variant={v ? 'success' : 'destructive'}>{v ? 'Undervalued' : 'Overvalued'}</Badge>
    },
  },
  { accessorKey: 'entry_price', header: 'Entry Price', meta: { priority: 'low', align: 'right' }, cell: (i) => fmtMoney(i.getValue<number | null>()) },
  { accessorKey: 'realized_date', header: 'Realized Date', meta: { priority: 'low' } },
  { accessorKey: 'realized_price', header: 'Realized Price', meta: { priority: 'low', align: 'right' }, cell: (i) => fmtMoney(i.getValue<number | null>()) },
  {
    accessorKey: 'realized_return_pct',
    header: 'Realized Return',
    meta: { align: 'right' },
    cell: (i) => `${i.getValue<number>().toFixed(2)}%`,
  },
  {
    accessorKey: 'hit',
    header: () => (
      <span className="inline-flex items-center gap-1">
        Hit?
        <InfoTooltip>Whether the model's undervalued/overvalued call matched the direction of the realized price move over the chosen horizon.</InfoTooltip>
      </span>
    ),
    cell: (i) => {
      const v = i.getValue<boolean | null>()
      if (v == null) return '—'
      return <Badge variant={v ? 'success' : 'destructive'}>{v ? 'Hit' : 'Miss'}</Badge>
    },
  },
]

export function AccuracyPage() {
  const [horizonInput, setHorizonInput] = useState(5)
  const [horizon, setHorizon] = useState(5)

  const query = useQuery({
    queryKey: ['valuation-accuracy', horizon],
    queryFn: () => apiGet<AccuracyResponse>('/api/v1/valuation/accuracy/backtest', { horizon_days: horizon }),
  })

  const r = query.data

  return (
    <AppShell
      title="Valuation — Accuracy"
      description="F6 — backtest of past valuation_signals predictions against realized price outcomes."
      actions={
        <div className="flex items-center gap-2">
          <input
            type="number"
            min={1}
            max={252}
            className="h-9 w-24 rounded-[var(--radius-token)] border border-border bg-transparent px-3 text-sm"
            value={horizonInput}
            onChange={(e) => setHorizonInput(Number(e.target.value))}
          />
          <Button onClick={() => setHorizon(horizonInput || 5)}>Run</Button>
        </div>
      }
    >
      {query.error ? (
        <p className="text-sm text-red">
          Could not reach GET /api/v1/valuation/accuracy/backtest — {(query.error as Error).message}
        </p>
      ) : r && r.scored ? (
        <div className="grid grid-cols-1 gap-4 sm:grid-cols-4">
          <StatCard
            label={
              <span className="inline-flex items-center gap-1">
                Scored predictions <InfoTooltip>How many past valuation_signals rows are old enough (past the chosen horizon) to compare against a realized price outcome, out of the total pulled.</InfoTooltip>
              </span>
            }
            value={`${r.scored} / ${r.count}`}
          />
          <StatCard
            label={
              <span className="inline-flex items-center gap-1">
                Hit rate (direction) <InfoTooltip>Share of scored predictions where the undervalued/overvalued call matched the direction of the realized price move.</InfoTooltip>
              </span>
            }
            value={r.hit_rate != null ? `${(r.hit_rate * 100).toFixed(1)}%` : '—'}
          />
          <StatCard
            label={
              <span className="inline-flex items-center gap-1">
                Avg return — undervalued calls <InfoTooltip>Average realized return over the chosen horizon for tickers the model flagged as undervalued.</InfoTooltip>
              </span>
            }
            value={r.avg_return_undervalued_pct != null ? `${fmtNum(r.avg_return_undervalued_pct)}%` : '—'}
          />
          <StatCard
            label={
              <span className="inline-flex items-center gap-1">
                Avg return — overvalued calls <InfoTooltip>Average realized return over the chosen horizon for tickers the model flagged as overvalued.</InfoTooltip>
              </span>
            }
            value={r.avg_return_overvalued_pct != null ? `${fmtNum(r.avg_return_overvalued_pct)}%` : '—'}
          />
        </div>
      ) : null}

      <div className="mt-4">
        <Card>
          <CardHeader>
            <CardTitle>Prediction Accuracy</CardTitle>
          </CardHeader>
          <CardContent>
            {query.isLoading ? (
              <DataTable columns={columns} data={[]} isLoading />
            ) : !r || !r.rows.length ? (
              <p className="text-sm text-muted-foreground">
                No valuation_signals rows old enough to score yet at this horizon — try a shorter horizon or wait for
                more history to accumulate.
              </p>
            ) : (
              <DataTable columns={columns} data={r.rows} />
            )}
          </CardContent>
        </Card>
      </div>
    </AppShell>
  )
}
