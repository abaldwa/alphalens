import { useState } from 'react'
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query'
import type { ColumnDef } from '@tanstack/react-table'

import { AppShell, Button, Card, CardContent, CardHeader, CardTitle, DataTable, InfoTooltip, StatCard, TickerLink } from '@/lib/ui'
import { apiGet, apiPost } from '@/shared/api/client'
import { StrategyPicker, useActiveStrategy, useStrategies } from './StrategyPicker'
import type { MomentumSummary, MomentumTrade, MomentumContribution } from './types'

function fmtMoney(v: number | null | undefined): string {
  return v == null ? '—' : `₹${v.toLocaleString('en-IN', { maximumFractionDigits: 0 })}`
}
function fmtPct(v: number | null | undefined): string {
  return v == null ? '—' : `${(v * 100).toFixed(1)}%`
}

const positionColumns: ColumnDef<MomentumTrade, unknown>[] = [
  { accessorKey: 'ticker', header: 'Ticker', cell: (i) => <TickerLink ticker={i.getValue<string>()} /> },
  { accessorKey: 'purchase_date', header: 'Purchase Date' },
  { accessorKey: 'qty', header: 'Qty' },
  { accessorKey: 'purchase_price', header: 'Purchase Price', cell: (i) => fmtMoney(i.getValue<number | null>()) },
  {
    accessorKey: 'grace_remaining',
    header: () => (
      <span className="inline-flex items-center gap-1">
        Grace Remaining
        <InfoTooltip>
          Rebalance cycles left before this position is force-sold, now that it has dropped out of the strategy's
          top-N momentum ranks. Empty means it's still in the top-N and hasn't started its grace countdown.
        </InfoTooltip>
      </span>
    ),
    cell: (i) => i.getValue<number | null>() ?? '—',
  },
]

const contributionColumns: ColumnDef<MomentumContribution, unknown>[] = [
  { accessorKey: 'contribution_date', header: 'Date' },
  { accessorKey: 'amount', header: 'Amount', cell: (i) => fmtMoney(i.getValue<number>()) },
  { accessorKey: 'note', header: 'Note', cell: (i) => i.getValue<string | null>() ?? '—' },
]

export function MomentumPortfolioPage() {
  const queryClient = useQueryClient()
  const strategies = useStrategies()
  const [activeStrategyId, setStrategyId] = useActiveStrategy(strategies.data)

  const summary = useQuery({
    queryKey: ['momentum-summary', activeStrategyId],
    queryFn: () => apiGet<MomentumSummary>('/api/v1/momentum/summary', { strategy_id: activeStrategyId! }),
    enabled: !!activeStrategyId,
  })

  const positions = useQuery({
    queryKey: ['momentum-trades', activeStrategyId],
    queryFn: () =>
      apiGet<MomentumTrade[]>('/api/v1/momentum/trades/', { strategy_id: activeStrategyId!, open_only: true }),
    enabled: !!activeStrategyId,
  })

  const contributions = useQuery({
    queryKey: ['momentum-contributions', activeStrategyId],
    queryFn: () => apiGet<MomentumContribution[]>('/api/v1/momentum/contributions/', { strategy_id: activeStrategyId! }),
    enabled: !!activeStrategyId,
  })

  const [addTicker, setAddTicker] = useState('')
  const [addDate, setAddDate] = useState(new Date().toISOString().slice(0, 10))
  const [addQty, setAddQty] = useState('')
  const [addPrice, setAddPrice] = useState('')

  const addTrade = useMutation({
    mutationFn: () =>
      apiPost('/api/v1/momentum/trades/', {
        strategy_id: activeStrategyId,
        ticker: addTicker.trim().toUpperCase(),
        purchase_date: addDate,
        qty: Number(addQty),
        purchase_price: addPrice ? Number(addPrice) : null,
      }),
    onSuccess: () => {
      setAddTicker('')
      setAddQty('')
      setAddPrice('')
      queryClient.invalidateQueries({ queryKey: ['momentum-trades', activeStrategyId] })
      queryClient.invalidateQueries({ queryKey: ['momentum-summary', activeStrategyId] })
    },
  })

  const [contribDate, setContribDate] = useState(new Date().toISOString().slice(0, 10))
  const [contribAmount, setContribAmount] = useState('')
  const [contribNote, setContribNote] = useState('')

  const addContribution = useMutation({
    mutationFn: () =>
      apiPost('/api/v1/momentum/contributions/', {
        strategy_id: activeStrategyId,
        contribution_date: contribDate,
        amount: Number(contribAmount),
        note: contribNote.trim() || null,
      }),
    onSuccess: () => {
      setContribAmount('')
      setContribNote('')
      queryClient.invalidateQueries({ queryKey: ['momentum-contributions', activeStrategyId] })
      queryClient.invalidateQueries({ queryKey: ['momentum-summary', activeStrategyId] })
    },
  })

  const inputClass = 'h-9 rounded-[var(--radius-token)] border border-border bg-transparent px-3 text-sm'

  return (
    <AppShell title="Momentum — Portfolio" description="ML38 Holding Dashboard: open positions, contributions, and manually-logged trades for the selected rank-band strategy.">
      <div className="mb-4">
        <StrategyPicker strategies={strategies.data ?? []} value={activeStrategyId} onChange={setStrategyId} />
      </div>

      <div className="grid grid-cols-2 gap-4 sm:grid-cols-4">
        <StatCard label="Capital Invested" value={fmtMoney(summary.data?.capital_invested)} />
        <StatCard label="Current Holdings Value" value={fmtMoney(summary.data?.current_holdings_value)} />
        <StatCard
          label={
            <span className="inline-flex items-center gap-1">
              CAGR <InfoTooltip>Compound Annual Growth Rate of this strategy's portfolio since inception.</InfoTooltip>
            </span>
          }
          value={fmtPct(summary.data?.cagr)}
          tone={summary.data?.cagr != null && summary.data.cagr >= 0 ? 'green' : 'red'}
        />
        <StatCard
          label={
            <span className="inline-flex items-center gap-1">
              XIRR{' '}
              <InfoTooltip>
                Extended Internal Rate of Return — annualized return accounting for the actual timing and size of
                each contribution and trade, unlike CAGR which assumes a single lump-sum investment.
              </InfoTooltip>
            </span>
          }
          value={fmtPct(summary.data?.xirr)}
          tone={summary.data?.xirr != null && summary.data.xirr >= 0 ? 'green' : 'red'}
        />
        <StatCard label="Idle Cash" value={fmtMoney(summary.data?.idle_cash)} />
        <StatCard label="Total Net Worth" value={fmtMoney(summary.data?.total_net_worth)} />
        <StatCard
          label={
            <span className="inline-flex items-center gap-1">
              Total Tax Due <InfoTooltip>Estimated capital-gains tax owed on realized and unrealized gains across open and closed positions.</InfoTooltip>
            </span>
          }
          value={fmtMoney(summary.data?.total_tax_due)}
        />
        <StatCard
          label={
            <span className="inline-flex items-center gap-1">
              Post-Tax Value <InfoTooltip>Total net worth after subtracting the estimated tax due.</InfoTooltip>
            </span>
          }
          value={fmtMoney(summary.data?.post_tax_value)}
        />
        <StatCard label="Total Contributed" value={fmtMoney(summary.data?.total_contributed)} />
        <StatCard label="As Of" value={summary.data?.as_of_date ?? '—'} />
      </div>

      <div className="mt-4">
        <Card>
          <CardHeader>
            <CardTitle>Record a buy</CardTitle>
          </CardHeader>
          <CardContent>
            <div className="flex flex-wrap items-center gap-2">
              <input className={inputClass} placeholder="Ticker" value={addTicker} onChange={(e) => setAddTicker(e.target.value)} />
              <input className={inputClass} type="date" value={addDate} onChange={(e) => setAddDate(e.target.value)} />
              <input className={inputClass} type="number" placeholder="Qty" value={addQty} onChange={(e) => setAddQty(e.target.value)} />
              <input className={inputClass} type="number" placeholder="Price" value={addPrice} onChange={(e) => setAddPrice(e.target.value)} />
              <Button
                disabled={!activeStrategyId || !addTicker.trim() || !addDate || !addQty || addTrade.isPending}
                onClick={() => addTrade.mutate()}
              >
                Record Buy
              </Button>
              {addTrade.isError && <span className="text-sm text-red">{(addTrade.error as Error).message}</span>}
            </div>
          </CardContent>
        </Card>
      </div>

      <div className="mt-4">
        <Card>
          <CardHeader>
            <CardTitle>Open Positions</CardTitle>
          </CardHeader>
          <CardContent>
            {positions.error ? (
              <p className="text-sm text-red">Could not reach GET /api/v1/momentum/trades/ — {(positions.error as Error).message}</p>
            ) : (
              <DataTable
                columns={positionColumns}
                data={positions.data ?? []}
                isLoading={positions.isLoading}
                emptyMessage="No open positions for this strategy — record a buy above."
              />
            )}
          </CardContent>
        </Card>
      </div>

      <div className="mt-4">
        <Card>
          <CardHeader>
            <CardTitle>Add contribution</CardTitle>
          </CardHeader>
          <CardContent>
            <div className="flex flex-wrap items-center gap-2">
              <input className={inputClass} type="date" value={contribDate} onChange={(e) => setContribDate(e.target.value)} />
              <input className={inputClass} type="number" placeholder="Amount" value={contribAmount} onChange={(e) => setContribAmount(e.target.value)} />
              <input className={inputClass} placeholder="Note (optional)" value={contribNote} onChange={(e) => setContribNote(e.target.value)} />
              <Button
                disabled={!activeStrategyId || !contribDate || !contribAmount || addContribution.isPending}
                onClick={() => addContribution.mutate()}
              >
                Add Contribution
              </Button>
              {addContribution.isError && <span className="text-sm text-red">{(addContribution.error as Error).message}</span>}
            </div>
          </CardContent>
        </Card>
      </div>

      <div className="mt-4">
        <Card>
          <CardHeader>
            <CardTitle>Contributions</CardTitle>
          </CardHeader>
          <CardContent>
            {contributions.error ? (
              <p className="text-sm text-red">Could not reach GET /api/v1/momentum/contributions/ — {(contributions.error as Error).message}</p>
            ) : (
              <DataTable
                columns={contributionColumns}
                data={contributions.data ?? []}
                isLoading={contributions.isLoading}
                emptyMessage="No contributions recorded yet for this strategy."
              />
            )}
          </CardContent>
        </Card>
      </div>
    </AppShell>
  )
}
