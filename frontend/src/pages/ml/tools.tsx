import { useState } from 'react'
import { useMutation, useQuery } from '@tanstack/react-query'
import type { ColumnDef } from '@tanstack/react-table'

import { AppShell, Badge, Button, Card, CardContent, CardHeader, CardTitle, DataTable, InfoTooltip, tickerColumn } from '@/lib/ui'
import { apiGet, apiPost } from '@/shared/api/client'
import type { MLSignalRow } from './types'

function todayStr() {
  return new Date().toISOString().slice(0, 10)
}
function fmtPct(v: number | null | undefined) {
  return v == null ? '—' : `${(v * 100).toFixed(1)}%`
}

interface BackdatedBuyResult {
  executed: boolean
  quantity?: number
  entry_price?: number
  detail?: string
}

export function MlToolsPage() {
  const [dateInput, setDateInput] = useState(todayStr())
  const [date, setDate] = useState<string | null>(null)
  const [results, setResults] = useState<Record<string, BackdatedBuyResult>>({})

  const topBuys = useQuery({
    queryKey: ['ml-top-buys-backdate', date],
    queryFn: () => apiGet<MLSignalRow[]>(`/api/v1/signals/ml/top_buys/${date}`, { n: 10 }),
    enabled: !!date,
  })

  const buyMutation = useMutation({
    mutationFn: ({ ticker }: { ticker: string }) => apiPost<BackdatedBuyResult>('/api/v1/paper_trading/backdated_buy', { ticker, date }),
    onSuccess: (result, variables) => setResults((prev) => ({ ...prev, [variables.ticker]: result })),
    onError: (err: Error, variables) => setResults((prev) => ({ ...prev, [variables.ticker]: { executed: false, detail: err.message } })),
  })

  const columns: ColumnDef<MLSignalRow, unknown>[] = [
    tickerColumn<MLSignalRow>('ml'),
    {
      accessorKey: 'signal_direction',
      header: 'Direction',
      cell: (i) => <Badge variant={i.getValue<string | null>() === 'sell' ? 'destructive' : 'success'}>{(i.getValue<string | null>() ?? '—').toUpperCase()}</Badge>,
    },
    {
      accessorKey: 'buy_prob',
      header: () => (
        <span className="inline-flex items-center gap-1">
          Buy Prob
          <InfoTooltip>signal_5d's own probability that its call is "buy" (0-1). The only model AlphaLens actually trades paper positions off of.</InfoTooltip>
        </span>
      ),
      meta: { align: 'right' },
      cell: (i) => fmtPct(i.getValue<number | null>()),
    },
    {
      id: 'action',
      header: 'Action',
      cell: ({ row }) => {
        const result = results[row.original.ticker]
        if (result) {
          return (
            <Badge variant={result.executed ? 'success' : 'outline'}>
              {result.executed ? `Bought ${result.quantity} @ ₹${result.entry_price?.toLocaleString('en-IN')}` : result.detail || 'Not executed'}
            </Badge>
          )
        }
        return (
          <Button size="sm" disabled={buyMutation.isPending} onClick={() => buyMutation.mutate({ ticker: row.original.ticker })}>
            Buy
          </Button>
        )
      },
    },
  ]

  return (
    <AppShell title="ML — Tools" description="Historical review: backdated-entry tool for testing what the paper-trading bot would have bought on a past date.">
      <Card>
        <CardHeader>
          <CardTitle>Backdated recommendations</CardTitle>
        </CardHeader>
        <CardContent>
          <div className="flex items-center gap-2">
            <input
              className="h-9 w-40 rounded-[var(--radius-token)] border border-border bg-transparent px-3 text-sm"
              type="date"
              value={dateInput}
              onChange={(e) => setDateInput(e.target.value)}
            />
            <Button
              onClick={() => {
                setResults({})
                setDate(dateInput)
              }}
            >
              Load
            </Button>
          </div>

          <div className="mt-4">
            {topBuys.error ? (
              <p className="text-sm text-red">
                Could not reach GET /api/v1/signals/ml/top_buys/{'{date}'} — {(topBuys.error as Error).message}
              </p>
            ) : date ? (
              <DataTable columns={columns} data={topBuys.data ?? []} isLoading={topBuys.isLoading} emptyMessage="No signals were written for this date — the pipeline may not have run that day." />
            ) : (
              <p className="text-sm text-muted-foreground">Pick a date and click Load.</p>
            )}
          </div>
        </CardContent>
      </Card>
    </AppShell>
  )
}
