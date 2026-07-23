import { useState } from 'react'
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query'
import type { ColumnDef } from '@tanstack/react-table'

import { AppShell, Badge, Button, Card, CardContent, CardHeader, CardTitle, DataTable, InfoTooltip, StatCard, tickerColumn } from '@/lib/ui'
import { apiGet, apiPost, apiPut } from '@/shared/api/client'
import { StrategyPicker, useActiveStrategy, useStrategies } from './StrategyPicker'
import type { MomentumRebalanceNext, MomentumSuggestion, MomentumTrade } from './types'

function actionVariant(action: string): 'success' | 'destructive' | 'warning' {
  if (action === 'add') return 'success'
  if (action === 'exit') return 'destructive'
  return 'warning'
}

export function MomentumRebalancePage() {
  const queryClient = useQueryClient()
  const strategies = useStrategies()
  const [activeStrategyId, setStrategyId] = useActiveStrategy(strategies.data)

  const next = useQuery({
    queryKey: ['momentum-rebalance-next', activeStrategyId],
    queryFn: () => apiGet<MomentumRebalanceNext>('/api/v1/momentum/rebalance/next', { strategy_id: activeStrategyId! }),
    enabled: !!activeStrategyId,
  })

  const suggestions = useQuery({
    queryKey: ['momentum-rebalance-suggestions', activeStrategyId],
    queryFn: () => apiGet<MomentumSuggestion[]>('/api/v1/momentum/rebalance/suggestions', { strategy_id: activeStrategyId! }),
    enabled: !!activeStrategyId,
  })

  const [modalSuggestion, setModalSuggestion] = useState<MomentumSuggestion | null>(null)
  const [modalDate, setModalDate] = useState(new Date().toISOString().slice(0, 10))
  const [modalQty, setModalQty] = useState('')
  const [modalPrice, setModalPrice] = useState('')

  const openModal = (s: MomentumSuggestion) => {
    setModalSuggestion(s)
    setModalDate(new Date().toISOString().slice(0, 10))
    setModalQty('')
    setModalPrice('')
  }

  const saveTrade = useMutation({
    mutationFn: async () => {
      if (!modalSuggestion || !activeStrategyId) return
      const qty = Number(modalQty)
      const price = Number(modalPrice)
      if (modalSuggestion.action === 'exit') {
        const trades = await apiGet<MomentumTrade[]>('/api/v1/momentum/trades/', {
          strategy_id: activeStrategyId,
          open_only: true,
        })
        const match = trades.find((t) => t.ticker === modalSuggestion.ticker)
        if (!match) throw new Error(`No open trade found for ${modalSuggestion.ticker}`)
        return apiPut(`/api/v1/momentum/trades/${match.id}`, {
          sale_date: modalDate,
          sell_price: price,
          exit_rank: modalSuggestion.momentum_rank,
        })
      }
      return apiPost('/api/v1/momentum/trades/', {
        strategy_id: activeStrategyId,
        ticker: modalSuggestion.ticker,
        purchase_date: modalDate,
        qty,
        purchase_price: price,
        entry_rank: modalSuggestion.momentum_rank,
        suggestion_id: modalSuggestion.id,
      })
    },
    onSuccess: () => {
      setModalSuggestion(null)
      queryClient.invalidateQueries({ queryKey: ['momentum-rebalance-suggestions', activeStrategyId] })
    },
  })

  const dismiss = useMutation({
    mutationFn: (id: number) => apiPost(`/api/v1/momentum/rebalance/suggestions/${id}/dismiss`),
    onSuccess: () => queryClient.invalidateQueries({ queryKey: ['momentum-rebalance-suggestions', activeStrategyId] }),
  })

  const columns: ColumnDef<MomentumSuggestion, unknown>[] = [
    {
      accessorKey: 'action',
      header: () => (
        <span className="inline-flex items-center gap-1">
          Action
          <InfoTooltip>
            "add" = new entry into the top-N ranks. "exit" = the position's grace period has fully elapsed, forced
            sell. "grace_hold" = out of the top-N but still within its grace countdown, no action needed yet.
          </InfoTooltip>
        </span>
      ),
      cell: (i) => <Badge variant={actionVariant(i.getValue<string>())}>{i.getValue<string>().toUpperCase()}</Badge>,
    },
    tickerColumn<MomentumSuggestion>('momentum'),
    {
      accessorKey: 'momentum_rank',
      header: () => (
        <span className="inline-flex items-center gap-1">
          Momentum Rank
          <InfoTooltip>Rank among the strategy's rank-band universe by trailing return (1 = highest momentum).</InfoTooltip>
        </span>
      ),
      meta: { align: 'right' },
      cell: (i) => i.getValue<number | null>() ?? '—',
    },
    {
      accessorKey: 'grace_remaining',
      header: () => (
        <span className="inline-flex items-center gap-1">
          Grace Remaining
          <InfoTooltip>
            Rebalance cycles left before this position is force-sold, now that it has dropped out of the top-N
            ranks. Empty means it hasn't dropped out since entry.
          </InfoTooltip>
        </span>
      ),
      meta: { align: 'right' },
      cell: (i) => i.getValue<number | null>() ?? '—',
    },
    { accessorKey: 'status', header: 'Status' },
    {
      id: 'do',
      header: 'Do',
      cell: ({ row }) => {
        const s = row.original
        if (s.status !== 'pending') return null
        if (s.action === 'grace_hold') return <span className="text-xs text-muted-foreground">still within grace — no action needed</span>
        return (
          <div className="flex gap-2">
            <Button size="sm" onClick={() => openModal(s)}>
              {s.action === 'add' ? 'Record Buy' : 'Record Sell'}
            </Button>
            <Button size="sm" variant="secondary" onClick={() => dismiss.mutate(s.id)} disabled={dismiss.isPending}>
              Dismiss
            </Button>
          </div>
        )
      },
    },
  ]

  const inputClass = 'h-9 rounded-[var(--radius-token)] border border-border bg-transparent px-3 text-sm'

  return (
    <AppShell title="Momentum — Rebalance" description="ML38 rebalance suggestions: add/exit/grace-hold actions for the selected rank-band strategy.">
      <div className="mb-4">
        <StrategyPicker strategies={strategies.data ?? []} value={activeStrategyId} onChange={setStrategyId} />
      </div>

      <div className="grid grid-cols-1 gap-4 sm:grid-cols-2">
        <StatCard label="Last Rebalance" value={next.data?.last_rebalance_date ?? '—'} />
        <StatCard label="Next Rebalance" value={next.data?.next_rebalance_date ?? '—'} />
      </div>

      <div className="mt-4">
        <Card>
          <CardHeader>
            <CardTitle>Suggestions</CardTitle>
          </CardHeader>
          <CardContent>
            {suggestions.error ? (
              <p className="text-sm text-red">
                Could not reach GET /api/v1/momentum/rebalance/suggestions — {(suggestions.error as Error).message}
              </p>
            ) : (
              <DataTable
                columns={columns}
                data={suggestions.data ?? []}
                isLoading={suggestions.isLoading}
                emptyMessage="No pending rebalance suggestions — either not a rebalance day yet, or everything's already been actioned."
              />
            )}
          </CardContent>
        </Card>
      </div>

      {modalSuggestion && (
        <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/50" onClick={() => setModalSuggestion(null)}>
          <div className="w-[360px] rounded-[var(--radius-token)] border border-border bg-background p-4" onClick={(e) => e.stopPropagation()}>
            <h3 className="mb-3 text-sm font-semibold">
              {modalSuggestion.action === 'add' ? 'Record Buy' : 'Record Sell'} — {modalSuggestion.ticker}
            </h3>
            <div className="flex flex-col gap-2">
              <input className={inputClass} type="date" value={modalDate} onChange={(e) => setModalDate(e.target.value)} />
              <input className={inputClass} type="number" placeholder="Qty" value={modalQty} onChange={(e) => setModalQty(e.target.value)} />
              <input className={inputClass} type="number" placeholder="Price" value={modalPrice} onChange={(e) => setModalPrice(e.target.value)} />
              {saveTrade.isError && <span className="text-sm text-red">{(saveTrade.error as Error).message}</span>}
              <div className="mt-2 flex justify-end gap-2">
                <Button variant="secondary" onClick={() => setModalSuggestion(null)}>
                  Cancel
                </Button>
                <Button disabled={!modalQty || !modalPrice || !modalDate || saveTrade.isPending} onClick={() => saveTrade.mutate()}>
                  Save
                </Button>
              </div>
            </div>
          </div>
        </div>
      )}
    </AppShell>
  )
}
