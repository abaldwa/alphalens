import { useState } from 'react'
import { useMutation } from '@tanstack/react-query'
import type { ColumnDef } from '@tanstack/react-table'

import { AppShell, Badge, Button, Card, CardContent, CardHeader, CardTitle, DataTable, InfoTooltip, cmpColumn, formatCurrencyINR, tickerColumn } from '@/lib/ui'
import { apiGet } from '@/shared/api/client'

interface ValuationResult {
  ticker: string
  lifecycle_stage: string | null
  intrinsic_value: number | null
  current_price: number | null
  valuation_gap_pct: number | null
  margin_of_safety: number | null
  dcf_model_type: string | null
  data_quality: string | null
}

interface BatchRankedResponse {
  count: number
  as_of_date: string | null
  results: ValuationResult[]
}

const fmtMoney = formatCurrencyINR

function valuationBadge(mos: number | null | undefined) {
  if (mos == null) return <Badge variant="outline">N/A</Badge>
  if (mos > 0.15) return <Badge variant="success">Undervalued</Badge>
  if (mos < -0.15) return <Badge variant="destructive">Overvalued</Badge>
  return <Badge variant="warning">Fairly Valued</Badge>
}

const columns: ColumnDef<ValuationResult, unknown>[] = [
  tickerColumn<ValuationResult>(),
  { id: 'valuation', header: 'Overall Valuation', cell: ({ row }) => valuationBadge(row.original.margin_of_safety) },
  cmpColumn<ValuationResult>('current_price'),
  {
    accessorKey: 'intrinsic_value',
    header: () => (
      <span className="inline-flex items-center gap-1">
        Price/Share (Valuation)
        <InfoTooltip>DCF-estimated intrinsic value per share, discounting projected future cash flows back at the model's WACC.</InfoTooltip>
      </span>
    ),
    meta: { align: 'right' },
    cell: (i) => fmtMoney(i.getValue<number | null>()),
  },
  {
    accessorKey: 'valuation_gap_pct',
    header: () => (
      <span className="inline-flex items-center gap-1">
        % Difference
        <InfoTooltip>Gap between current market price and DCF intrinsic value, as a percentage of intrinsic value.</InfoTooltip>
      </span>
    ),
    meta: { align: 'right' },
    cell: (i) => {
      const v = i.getValue<number | null>()
      return v == null ? '—' : `${(v * 100).toFixed(1)}%`
    },
  },
  { accessorKey: 'lifecycle_stage', header: 'Lifecycle Stage', meta: { priority: 'low' }, cell: (i) => i.getValue<string | null>() ?? '—' },
  {
    accessorKey: 'dcf_model_type',
    header: () => (
      <span className="inline-flex items-center gap-1">
        Model
        <InfoTooltip>Which DCF variant was used for this ticker (e.g. standard, distressed, excess-return) — chosen based on the company's lifecycle stage and financial health.</InfoTooltip>
      </span>
    ),
    meta: { priority: 'low' },
    cell: (i) => i.getValue<string | null>() ?? '—',
  },
  {
    accessorKey: 'data_quality',
    header: () => (
      <span className="inline-flex items-center gap-1">
        Data Quality
        <InfoTooltip>How complete the underlying fundamentals data was for this valuation — "full" vs "partial" inputs, affecting confidence in the result.</InfoTooltip>
      </span>
    ),
    meta: { priority: 'low' },
    cell: (i) => {
      const v = i.getValue<string | null>()
      return <Badge variant={v === 'full' ? 'success' : v === 'partial' ? 'warning' : 'outline'}>{v ?? '—'}</Badge>
    },
  },
]

export function BatchPage() {
  const [scope, setScope] = useState<'all' | '1' | '2' | '4'>('2')

  const run = useMutation({
    mutationFn: () => {
      const params: Record<string, string | number> = { limit: 500, n_workers: 16 }
      if (scope !== 'all') params.max_tier = scope
      return apiGet<BatchRankedResponse>('/api/v1/valuation/batch/ranked', params)
    },
  })

  return (
    <AppShell
      title="Valuation — Batch"
      description="Run DCF valuation across a universe scope, ranked by margin of safety."
      actions={
        <div className="flex items-center gap-2">
          <select
            className="h-9 rounded-[var(--radius-token)] border border-border bg-transparent px-3 text-sm"
            value={scope}
            onChange={(e) => setScope(e.target.value as typeof scope)}
          >
            <option value="1">Nifty 50 (~50 stocks, fast)</option>
            <option value="2">Nifty 100 (~100 stocks, fast)</option>
            <option value="4">Nifty 500 (~500 stocks, a few minutes)</option>
            <option value="all">Full Universe (~2000+ stocks, can take 10-15 minutes)</option>
          </select>
          <Button onClick={() => run.mutate()} disabled={run.isPending}>
            {run.isPending ? 'Running…' : 'Run Batch'}
          </Button>
        </div>
      }
    >
      <Card>
        <CardHeader>
          <CardTitle>Batch Valuation Results</CardTitle>
        </CardHeader>
        <CardContent>
          {run.isPending ? (
            <p className="text-sm text-muted-foreground">Running DCF valuation — this can take a while for larger scopes…</p>
          ) : run.isError ? (
            <p className="text-sm text-red">
              Could not reach GET /api/v1/valuation/batch/ranked — {(run.error as Error).message}
            </p>
          ) : run.data ? (
            <>
              <p className="mb-3 text-sm text-muted-foreground">
                {run.data.count} stocks valued as of {run.data.as_of_date ?? 'latest available data'}
              </p>
              <DataTable columns={columns} data={run.data.results} />
            </>
          ) : (
            <p className="text-sm text-muted-foreground">Choose a scope and click Run Batch.</p>
          )}
        </CardContent>
      </Card>
    </AppShell>
  )
}
