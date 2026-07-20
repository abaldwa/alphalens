import { useState } from 'react'
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query'
import type { ColumnDef } from '@tanstack/react-table'
import { CartesianGrid, Line, LineChart, Tooltip, XAxis, YAxis } from 'recharts'

import { AppShell, Badge, Button, Card, CardContent, CardHeader, CardTitle, DataTable, InfoTooltip, ResponsiveChartCard, StatCard, Table, TableHeader, TableBody, TableRow, TableHead, TableCell, formatCurrencyINR, numericCellClass, sectorColumn, tickerColumn } from '@/lib/ui'
import { apiGet, apiPost } from '@/shared/api/client'
import type {
  EquityCurveResponse,
  GateStatusResponse,
  PaperTradingPosition,
  PaperTradingStateResponse,
  PaperTradingTradesResponse,
  PendingActionsResponse,
} from './types'

function fmtPct(v: number | null | undefined) {
  return v == null ? '—' : `${(v * 100).toFixed(1)}%`
}
const fmtMoney = formatCurrencyINR
function pnlTone(v: number | null | undefined) {
  if (v == null) return undefined
  return v >= 0 ? 'text-green' : 'text-red'
}
function actionVariant(actionType: string) {
  if (actionType === 'buy') return 'success' as const
  if (actionType === 'sell') return 'destructive' as const
  return 'warning' as const
}

export function MlPositionsPage() {
  const queryClient = useQueryClient()

  const state = useQuery({
    queryKey: ['paper-trading-state'],
    queryFn: () => apiGet<PaperTradingStateResponse>('/api/v1/paper_trading/state'),
  })
  const gate = useQuery({
    queryKey: ['paper-trading-gate'],
    queryFn: () => apiGet<GateStatusResponse>('/api/v1/paper_trading/gate_status'),
  })
  const pending = useQuery({
    queryKey: ['paper-trading-pending'],
    queryFn: () => apiGet<PendingActionsResponse>('/api/v1/paper_trading/pending'),
  })
  const equity = useQuery({
    queryKey: ['paper-trading-equity'],
    queryFn: () => apiGet<EquityCurveResponse>('/api/v1/paper_trading/equity_curve'),
  })
  const trades = useQuery({
    queryKey: ['paper-trading-trades'],
    queryFn: () => apiGet<PaperTradingTradesResponse>('/api/v1/paper_trading/trades'),
  })

  const decide = useMutation({
    mutationFn: ({ actionId, decision }: { actionId: string; decision: 'accept' | 'reject' }) =>
      apiPost<{ executed: boolean; status: string; detail?: string }>(`/api/v1/paper_trading/pending/${actionId}/${decision}`),
    onSuccess: (result, variables) => {
      setDecisionResults((prev) => ({ ...prev, [variables.actionId]: result }))
      queryClient.invalidateQueries({ queryKey: ['paper-trading-pending'] })
      queryClient.invalidateQueries({ queryKey: ['paper-trading-state'] })
    },
  })
  const [decisionResults, setDecisionResults] = useState<Record<string, { executed: boolean; status: string; detail?: string }>>({})

  const sell = useMutation({
    mutationFn: (ticker: string) => apiPost(`/api/v1/paper_trading/positions/${ticker}/sell`),
    onSuccess: () => queryClient.invalidateQueries({ queryKey: ['paper-trading-state'] }),
  })

  const positionColumns: ColumnDef<PaperTradingPosition, unknown>[] = [
    tickerColumn<PaperTradingPosition>(),
    { accessorKey: 'company_name', header: 'Name', meta: { priority: 'low' }, cell: (i) => i.getValue<string | null>() ?? '—' },
    sectorColumn<PaperTradingPosition>(),
    { accessorKey: 'entry_date', header: 'Entry Date', meta: { priority: 'low' } },
    { accessorKey: 'entry_price', header: 'Entry Price', meta: { priority: 'low', align: 'right' }, cell: (i) => fmtMoney(i.getValue<number>()) },
    { accessorKey: 'quantity', header: 'Qty', meta: { priority: 'low', align: 'right' } },
    { accessorKey: 'current_price', header: 'Current', meta: { align: 'right' }, cell: (i) => fmtMoney(i.getValue<number | null>()) },
    {
      accessorKey: 'unrealised_pnl_pct',
      header: 'Unrealised P&L',
      meta: { align: 'right' },
      cell: (i) => <span className={pnlTone(i.getValue<number | null>())}>{fmtPct(i.getValue<number | null>())}</span>,
    },
    { accessorKey: 'buy_prob_entry', header: 'Buy Prob (Entry)', meta: { priority: 'low', align: 'right' }, cell: (i) => fmtPct(i.getValue<number | null>()) },
    { accessorKey: 'buy_prob_current', header: 'Buy Prob (Now)', meta: { priority: 'low', align: 'right' }, cell: (i) => fmtPct(i.getValue<number | null>()) },
    { accessorKey: 'target_price', header: 'Target Price', meta: { priority: 'low', align: 'right' }, cell: (i) => fmtMoney(i.getValue<number | null>()) },
    { accessorKey: 'target_date', header: 'Target Date', meta: { priority: 'low' }, cell: (i) => i.getValue<string | null>() ?? '—' },
    {
      accessorKey: 'stock_gain_pct',
      header: 'Stock Gain',
      meta: { priority: 'low', align: 'right' },
      cell: (i) => <span className={pnlTone(i.getValue<number | null>())}>{fmtPct(i.getValue<number | null>())}</span>,
    },
    {
      accessorKey: 'nifty_gain_pct',
      header: 'Nifty Gain',
      meta: { priority: 'low', align: 'right' },
      cell: (i) => <span className={pnlTone(i.getValue<number | null>())}>{fmtPct(i.getValue<number | null>())}</span>,
    },
    { accessorKey: 'exit_criterion', header: 'Exit Criterion', meta: { priority: 'low' }, cell: (i) => i.getValue<string | null>() ?? '—' },
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

  const realPositions = (state.data?.positions ?? []).filter((p) => p.ticker !== '_HEARTBEAT_')
  const pnl = (state.data?.total_equity ?? 0) - (state.data?.initial_capital ?? 0)
  const gatePct = gate.data ? Math.min(100, (gate.data.days_count / gate.data.gate_threshold) * 100) : 0

  return (
    <AppShell title="ML — Position Monitor" description="Live paper-trading portfolio, pending actions, and closed trades — folds in the former standalone Paper Trading screen.">
      <Card>
        <CardHeader>
          <CardTitle>Phase 3 Gate 7 status</CardTitle>
        </CardHeader>
        <CardContent>
          {gate.error ? (
            <p className="text-sm text-red">Could not reach GET /api/v1/paper_trading/gate_status — {(gate.error as Error).message}</p>
          ) : (
            <div className="grid grid-cols-1 gap-3 sm:grid-cols-3">
              <StatCard label="Days Logged" value={gate.data?.days_count ?? '—'} />
              <StatCard label="Threshold" value={gate.data?.gate_threshold ?? '—'} />
              <StatCard label="Status" value={<Badge variant={gate.data?.gate_cleared ? 'success' : 'warning'}>{gate.data?.gate_cleared ? 'CLEARED' : 'in progress'}</Badge>} />
            </div>
          )}
          <div className="mt-3 h-2 overflow-hidden rounded-full bg-muted">
            <div className="h-full bg-teal" style={{ width: `${gatePct}%` }} />
          </div>
        </CardContent>
      </Card>

      <div className="mt-4 grid grid-cols-2 gap-4 sm:grid-cols-4">
        <StatCard label="Total Equity" value={fmtMoney(state.data?.total_equity)} />
        <StatCard label="Cash" value={fmtMoney(state.data?.cash)} />
        <StatCard label="P&L" value={fmtMoney(pnl)} tone={pnl >= 0 ? 'green' : 'red'} />
        <StatCard label="P&L %" value={fmtPct(state.data?.initial_capital ? pnl / state.data.initial_capital : null)} tone={pnl >= 0 ? 'green' : 'red'} />
      </div>

      <div className="mt-4">
        <Card>
          <CardHeader>
            <CardTitle>Pending actions</CardTitle>
          </CardHeader>
          <CardContent>
            {pending.error ? (
              <p className="text-sm text-red">Could not reach GET /api/v1/paper_trading/pending — {(pending.error as Error).message}</p>
            ) : !pending.data?.actions.length ? (
              <p className="text-sm text-muted-foreground">No pending actions{pending.data?.date ? ` for ${pending.data.date}` : ''}.</p>
            ) : (
              <div className="flex flex-col gap-2">
                {pending.data.actions.map((a) => {
                  const targetPct = a.target_price != null && a.price ? (a.target_price / a.price - 1) * 100 : null
                  const result = decisionResults[a.action_id]
                  return (
                    <div key={a.action_id} className="flex flex-wrap items-center gap-3 rounded-[var(--radius-token)] border border-border p-3 text-sm">
                      <Badge variant={actionVariant(a.action_type)}>{a.action_type.toUpperCase()}</Badge>
                      <span className="font-semibold">{a.ticker}</span>
                      <span className="text-muted-foreground">{a.company_name ?? '—'}</span>
                      <span className="font-mono-data">{fmtMoney(a.price)}</span>
                      <span className="font-mono-data">{a.target_price != null ? fmtMoney(a.target_price) : '—'}</span>
                      <span className="font-mono-data text-green">{targetPct != null ? `+${targetPct.toFixed(1)}%` : '—'}</span>
                      <span className="font-mono-data">{a.duration_days != null ? `${a.duration_days}d` : '—'}</span>
                      <span className="max-w-md text-xs text-muted-foreground">{a.reason}</span>
                      {result ? (
                        <Badge variant={result.executed ? 'success' : 'secondary'} className="ml-auto">
                          {result.executed ? 'Executed' : result.status === 'rejected' ? 'Rejected' : 'Not executed'}
                          {result.detail ? ` — ${result.detail}` : ''}
                        </Badge>
                      ) : (
                        <div className="ml-auto flex gap-2">
                          <Button size="sm" disabled={decide.isPending} onClick={() => decide.mutate({ actionId: a.action_id, decision: 'accept' })}>
                            Accept
                          </Button>
                          <Button size="sm" variant="destructive" disabled={decide.isPending} onClick={() => decide.mutate({ actionId: a.action_id, decision: 'reject' })}>
                            Reject
                          </Button>
                        </div>
                      )}
                    </div>
                  )
                })}
              </div>
            )}
          </CardContent>
        </Card>
      </div>

      <div className="mt-4">
        <Card>
          <CardHeader>
            <CardTitle>Open positions</CardTitle>
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

      <div className="mt-4">
        <Card>
          <CardHeader>
            <CardTitle>Closed trades</CardTitle>
          </CardHeader>
          <CardContent>
            {trades.error ? (
              <p className="text-sm text-red">Could not reach GET /api/v1/paper_trading/trades — {(trades.error as Error).message}</p>
            ) : !trades.data?.trades.length ? (
              <p className="text-sm text-muted-foreground">No closed trades yet.</p>
            ) : (
              <Table>
                <TableHeader>
                  <TableRow>
                    {['Ticker', 'Entry', 'Exit', 'Exit Type', 'P&L', 'P&L %'].map((h) => (
                      <TableHead key={h}>
                        {h === 'Exit Type' ? (
                          <span className="inline-flex items-center gap-1 normal-case">
                            {h}
                            <InfoTooltip>The specific rule that triggered the exit (thesis_broken / momentum_exhaustion / risk_management / target_achieved / opportunity_cost / pnd_exit).</InfoTooltip>
                          </span>
                        ) : (
                          h
                        )}
                      </TableHead>
                    ))}
                  </TableRow>
                </TableHeader>
                <TableBody>
                  {trades.data.trades.slice(0, 50).map((t, i) => (
                    <TableRow key={i}>
                      <TableCell className="font-semibold">{t.ticker}</TableCell>
                      <TableCell className={numericCellClass}>
                        {fmtMoney(t.entry_price)} ({t.date})
                      </TableCell>
                      <TableCell className={numericCellClass}>
                        {t.exit_price != null ? fmtMoney(t.exit_price) : '—'} ({t.exit_date ?? '—'})
                      </TableCell>
                      <TableCell>
                        <Badge variant="outline">{t.exit_type ?? '—'}</Badge>
                      </TableCell>
                      <TableCell className={`${numericCellClass} ${pnlTone(t.pnl)}`}>{t.pnl != null ? fmtMoney(t.pnl) : '—'}</TableCell>
                      <TableCell className={`${numericCellClass} ${pnlTone(t.pnl_pct)}`}>{fmtPct(t.pnl_pct)}</TableCell>
                    </TableRow>
                  ))}
                </TableBody>
              </Table>
            )}
          </CardContent>
        </Card>
      </div>
    </AppShell>
  )
}
