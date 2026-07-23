import { useState } from 'react'
import { useMutation, useQueryClient } from '@tanstack/react-query'
import type { ColumnDef } from '@tanstack/react-table'
import { CartesianGrid, Line, LineChart, Tooltip, XAxis, YAxis } from 'recharts'

import {
  AppShell,
  Badge,
  Button,
  Card,
  CardContent,
  CardHeader,
  CardTitle,
  DataTable,
  InfoTooltip,
  ResponsiveChartCard,
  StatCard,
  exitTodayCagrColumn,
  formatCurrencyINR,
  sectorColumn,
  tickerColumn,
  tradeDurationColumn,
} from '@/lib/ui'
import { usePaperTrading } from '@/shared/api/paperTrading'
import { apiPost } from '@/shared/api/client'
import type { PaperTradingPosition } from '@/pages/ml/types'

function fmtPct(v: number | null | undefined) {
  return v == null ? '—' : `${(v * 100).toFixed(1)}%`
}
const fmtMoney = formatCurrencyINR
function pnlTone(v: number | null | undefined) {
  if (v == null) return undefined
  return v >= 0 ? 'text-green' : 'text-red'
}

function initialActionFromUrl(): 'view' | 'buy' | 'sell' {
  const a = new URLSearchParams(window.location.search).get('action')
  return a === 'buy' || a === 'sell' ? a : 'view'
}

interface BackdatedBuyResult {
  ticker: string
  date: string
  entry_price: number | null
  quantity: number | null
  executed: boolean
  detail: string | null
}

/**
 * Technical > Portfolio — Stocks/Cash view, Buy, and Sell, all against the
 * app's one real paper-trading account (usePaperTrading, same data
 * ml/positions.tsx uses). Positions opened via this page's Buy form are
 * tagged pillar="technical" in position_meta (backdated_buy request body);
 * positions opened by the ML pending-actions flow are tagged pillar="ml".
 * We filter the shared account down to technical-only positions client-side
 * here; untagged legacy positions (opened before pillar tagging existed)
 * are still shown so nothing silently disappears from the table.
 */
export function TechnicalPortfolioPage() {
  const [tab, setTab] = useState<'view' | 'buy' | 'sell'>(initialActionFromUrl)
  const { state, equity, sell, realPositions: allPositions } = usePaperTrading()
  const queryClient = useQueryClient()
  const realPositions = allPositions.filter((p) => p.pillar == null || p.pillar === 'technical')

  const positionColumns: ColumnDef<PaperTradingPosition, unknown>[] = [
    tickerColumn<PaperTradingPosition>('technical'),
    { accessorKey: 'current_price', header: 'Current', meta: { align: 'right' }, cell: (i) => fmtMoney(i.getValue<number | null>()) },
    {
      accessorKey: 'unrealised_pnl_pct',
      header: () => (
        <span className="inline-flex items-center gap-1">
          Win/Loss (if exited today)
          <InfoTooltip>Unrealised P&L using current_price as a stand-in exit price.</InfoTooltip>
        </span>
      ),
      meta: { align: 'right' },
      cell: (i) => <span className={pnlTone(i.getValue<number | null>())}>{fmtPct(i.getValue<number | null>())}</span>,
    },
    { accessorKey: 'exit_criterion', header: 'Exit Reason', cell: (i) => i.getValue<string | null>() ?? '—' },
    tradeDurationColumn<PaperTradingPosition>(),
    exitTodayCagrColumn<PaperTradingPosition>(),
    { accessorKey: 'entry_price', header: 'Entry Price', meta: { priority: 'low', align: 'right' }, cell: (i) => fmtMoney(i.getValue<number>()) },
    { accessorKey: 'entry_date', header: 'Entry Date', meta: { priority: 'low' } },
    { accessorKey: 'quantity', header: 'Qty', meta: { priority: 'low', align: 'right' } },
    sectorColumn<PaperTradingPosition>(),
    { accessorKey: 'company_name', header: 'Name', meta: { priority: 'low' }, cell: (i) => i.getValue<string | null>() ?? '—' },
    {
      id: 'action',
      header: 'Action',
      cell: ({ row }) => (
        <Button variant="destructive" size="sm" onClick={() => sell.mutate(row.original.ticker)} disabled={sell.isPending}>
          Sell
        </Button>
      ),
    },
  ]

  const pnl = (state.data?.total_equity ?? 0) - (state.data?.initial_capital ?? 0)

  return (
    <AppShell
      title="Technical — Portfolio"
      description="Stocks & cash, buy, and sell against the app's paper-trading account."
      actions={
        <div className="flex gap-1 rounded-[var(--radius-token)] bg-muted p-1">
          {(['view', 'buy', 'sell'] as const).map((t) => (
            <Button key={t} size="sm" variant={tab === t ? 'default' : 'ghost'} onClick={() => setTab(t)}>
              {t === 'view' ? 'Portfolio View' : t === 'buy' ? 'Buy' : 'Sell'}
            </Button>
          ))}
        </div>
      }
    >
      {tab === 'view' && (
        <>
          <div className="grid grid-cols-2 gap-4 sm:grid-cols-4">
            <StatCard label="Total Equity" value={fmtMoney(state.data?.total_equity)} />
            <StatCard label="Cash" value={fmtMoney(state.data?.cash)} />
            <StatCard label="P&L" value={fmtMoney(pnl)} tone={pnl >= 0 ? 'green' : 'red'} />
            <StatCard label="P&L %" value={fmtPct(state.data?.initial_capital ? pnl / state.data.initial_capital : null)} tone={pnl >= 0 ? 'green' : 'red'} />
          </div>

          <div className="mt-4">
            <Card>
              <CardHeader>
                <CardTitle>Holdings</CardTitle>
              </CardHeader>
              <CardContent>
                {state.error ? (
                  <p className="text-sm text-red">Could not reach GET /api/v1/paper_trading/state — {(state.error as Error).message}</p>
                ) : (
                  <DataTable columns={positionColumns} data={realPositions} isLoading={state.isLoading} emptyMessage="No open positions." />
                )}
              </CardContent>
            </Card>
          </div>

          <div className="mt-4">
            <ResponsiveChartCard title="Equity curve" description={equity.error ? 'Failed to load' : undefined} height={260}>
              <LineChart data={equity.data?.points ?? []}>
                <CartesianGrid strokeDasharray="3 3" stroke="var(--border)" />
                <XAxis dataKey="date" tick={{ fontSize: 10 }} />
                <YAxis tick={{ fontSize: 11 }} domain={['auto', 'auto']} />
                <Tooltip />
                <Line type="monotone" dataKey="equity" stroke="var(--teal)" dot={false} strokeWidth={2} />
              </LineChart>
            </ResponsiveChartCard>
          </div>
        </>
      )}

      {tab === 'buy' && <BuyForm onBought={() => queryClient.invalidateQueries({ queryKey: ['paper-trading-state'] })} />}

      {tab === 'sell' && (
        <Card>
          <CardHeader>
            <CardTitle>Sell an open position</CardTitle>
          </CardHeader>
          <CardContent>
            {!realPositions.length ? (
              <p className="text-sm text-muted-foreground">No open positions to sell.</p>
            ) : (
              <div className="flex flex-col gap-2">
                {realPositions.map((p) => (
                  <div key={p.ticker} className="flex items-center justify-between gap-3 rounded-[var(--radius-token)] border border-border p-3 text-sm">
                    <span className="font-semibold">{p.ticker}</span>
                    <span className="text-muted-foreground">{p.company_name ?? '—'}</span>
                    <span className="font-mono-data">{fmtMoney(p.current_price)}</span>
                    <span className={`font-mono-data ${pnlTone(p.unrealised_pnl_pct)}`}>{fmtPct(p.unrealised_pnl_pct)}</span>
                    <Button variant="destructive" size="sm" onClick={() => sell.mutate(p.ticker)} disabled={sell.isPending} className="ml-auto">
                      Sell
                    </Button>
                  </div>
                ))}
              </div>
            )}
          </CardContent>
        </Card>
      )}
    </AppShell>
  )
}

function BuyForm({ onBought }: { onBought: () => void }) {
  const [ticker, setTicker] = useState('')
  const [date, setDate] = useState('')
  const [quantity, setQuantity] = useState('')

  const buy = useMutation({
    mutationFn: () =>
      apiPost<BackdatedBuyResult>('/api/v1/paper_trading/backdated_buy', {
        ticker: ticker.trim().toUpperCase(),
        date: date.trim(),
        quantity: quantity.trim() ? Number(quantity.trim()) : undefined,
        pillar: 'technical',
      }),
    onSuccess: () => onBought(),
  })

  return (
    <Card>
      <CardHeader>
        <CardTitle>Buy</CardTitle>
      </CardHeader>
      <CardContent>
        <div className="flex flex-wrap items-end gap-2">
          <div className="flex flex-col gap-1">
            <label className="text-xs text-muted-foreground">Ticker</label>
            <input
              className="h-9 rounded-[var(--radius-token)] border border-border bg-transparent px-3 text-sm"
              value={ticker}
              onChange={(e) => setTicker(e.target.value.toUpperCase())}
              placeholder="e.g. RELIANCE"
            />
          </div>
          <div className="flex flex-col gap-1">
            <label className="text-xs text-muted-foreground">Date</label>
            <input
              className="h-9 rounded-[var(--radius-token)] border border-border bg-transparent px-3 text-sm"
              value={date}
              onChange={(e) => setDate(e.target.value)}
              placeholder="YYYY-MM-DD"
            />
          </div>
          <div className="flex flex-col gap-1">
            <label className="text-xs text-muted-foreground">Quantity (optional)</label>
            <input
              className="h-9 w-32 rounded-[var(--radius-token)] border border-border bg-transparent px-3 text-sm"
              value={quantity}
              onChange={(e) => setQuantity(e.target.value)}
              placeholder="auto-sized"
            />
          </div>
          <Button onClick={() => buy.mutate()} disabled={!ticker.trim() || !date.trim() || buy.isPending}>
            Buy
          </Button>
        </div>

        {buy.data ? (
          <div className="mt-3">
            <Badge variant={buy.data.executed ? 'success' : 'secondary'}>
              {buy.data.executed
                ? `Bought ${buy.data.quantity} ${buy.data.ticker} @ ${fmtMoney(buy.data.entry_price)}`
                : `Not executed${buy.data.detail ? ` — ${buy.data.detail}` : ''}`}
            </Badge>
          </div>
        ) : null}
        {buy.error ? <p className="mt-3 text-sm text-red">{(buy.error as Error).message}</p> : null}
      </CardContent>
    </Card>
  )
}
