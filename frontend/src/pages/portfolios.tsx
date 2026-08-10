// frontend/src/pages/portfolios.tsx
//
// Phase: FeatureBacklog.md ML38 — momentum strategy consolidation
// (2026-08-09, Phase 0: Generic Portfolio module)
//
// Cross-channel Portfolio management — NOT under pages/momentum/, since
// datastore/api/routers/portfolios.py is deliberately channel-agnostic
// (momentum/technical/ml can all tag trades to a portfolio here). Lists
// portfolios, creates new ones, and shows NAV/XIRR/contributed-capital
// plus tagged trade history for a selected portfolio.
import { useState } from 'react'
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query'
import type { ColumnDef } from '@tanstack/react-table'

import { AppShell, Button, Card, CardContent, CardDescription, CardHeader, CardTitle, DataTable, InfoTooltip, StatCard, formatCurrencyINR } from '@/lib/ui'
import { apiGet, apiPatch, apiPost } from '@/shared/api/client'

interface Portfolio {
  portfolio_id: number
  name: string
  description: string | null
  channel: string | null
  base_capital: number
  is_active: boolean
  created_at: string
  updated_at: string
}

interface CashFlow {
  id: number
  portfolio_id: number
  date: string
  amount: number
  kind: string
  note: string | null
  created_at: string
}

interface NavResponse {
  portfolio_id: number
  as_of: string
  nav: number
  cash_balance: number
  holdings_value: number
  total_contributed: number
  total_withdrawn: number
  xirr: number | null
}

interface PortfolioTrade {
  id: number
  strategy_id: string
  ticker: string
  purchase_date: string
  qty: number
  purchase_price: number | null
  sale_date: string | null
  sell_price: number | null
}

const CASH_FLOW_KINDS = ['sip', 'withdrawal', 'dividend', 'tax', 'fee'] as const

function fmtPct(v: number | null | undefined): string {
  return v == null ? '—' : `${(v * 100).toFixed(1)}%`
}

const tradeColumns: ColumnDef<PortfolioTrade, unknown>[] = [
  { accessorKey: 'strategy_id', header: 'Strategy' },
  { accessorKey: 'ticker', header: 'Ticker' },
  { accessorKey: 'purchase_date', header: 'Buy Date' },
  { accessorKey: 'qty', header: 'Qty', meta: { align: 'right' } },
  { accessorKey: 'purchase_price', header: 'Buy Price', meta: { align: 'right' }, cell: (i) => formatCurrencyINR(i.getValue<number | null>()) },
  { accessorKey: 'sale_date', header: 'Sale Date', cell: (i) => i.getValue<string | null>() ?? '—' },
  { accessorKey: 'sell_price', header: 'Sale Price', meta: { align: 'right' }, cell: (i) => formatCurrencyINR(i.getValue<number | null>()) },
]

const cashFlowColumns: ColumnDef<CashFlow, unknown>[] = [
  { accessorKey: 'date', header: 'Date' },
  { accessorKey: 'kind', header: 'Kind' },
  { accessorKey: 'amount', header: 'Amount', meta: { align: 'right' }, cell: (i) => formatCurrencyINR(i.getValue<number>()) },
  { accessorKey: 'note', header: 'Note', cell: (i) => i.getValue<string | null>() ?? '—' },
]

const inputClass = 'h-9 rounded-[var(--radius-token)] border border-border bg-transparent px-3 text-sm'

export function PortfoliosPage() {
  const queryClient = useQueryClient()
  const [selectedId, setSelectedId] = useState<number | null>(null)

  const portfolios = useQuery({
    queryKey: ['portfolios'],
    queryFn: () => apiGet<Portfolio[]>('/api/v1/portfolios/'),
  })

  const [newName, setNewName] = useState('')
  const [newDescription, setNewDescription] = useState('')
  const [newChannel, setNewChannel] = useState('')
  const [newBaseCapital, setNewBaseCapital] = useState('')

  const createPortfolio = useMutation({
    mutationFn: () =>
      apiPost<Portfolio>('/api/v1/portfolios/', {
        name: newName.trim(),
        description: newDescription.trim() || null,
        channel: newChannel || null,
        base_capital: newBaseCapital ? Number(newBaseCapital) : 0,
      }),
    onSuccess: (created) => {
      setNewName('')
      setNewDescription('')
      setNewChannel('')
      setNewBaseCapital('')
      queryClient.invalidateQueries({ queryKey: ['portfolios'] })
      setSelectedId(created.portfolio_id)
    },
  })

  const nav = useQuery({
    queryKey: ['portfolio-nav', selectedId],
    queryFn: () => apiGet<NavResponse>(`/api/v1/portfolios/${selectedId}/nav`),
    enabled: selectedId != null,
  })

  const trades = useQuery({
    queryKey: ['portfolio-trades', selectedId],
    queryFn: () => apiGet<PortfolioTrade[]>(`/api/v1/portfolios/${selectedId}/trades`),
    enabled: selectedId != null,
  })

  const cashFlows = useQuery({
    queryKey: ['portfolio-cash-flows', selectedId],
    queryFn: () => apiGet<CashFlow[]>(`/api/v1/portfolios/${selectedId}/cash_flows`),
    enabled: selectedId != null,
  })

  const [cfDate, setCfDate] = useState(new Date().toISOString().slice(0, 10))
  const [cfAmount, setCfAmount] = useState('')
  const [cfKind, setCfKind] = useState<(typeof CASH_FLOW_KINDS)[number]>('sip')
  const [cfNote, setCfNote] = useState('')

  const addCashFlow = useMutation({
    mutationFn: () =>
      apiPost(`/api/v1/portfolios/${selectedId}/cash_flows`, {
        date: cfDate,
        amount: cfKind === 'withdrawal' || cfKind === 'tax' || cfKind === 'fee' ? -Math.abs(Number(cfAmount)) : Math.abs(Number(cfAmount)),
        kind: cfKind,
        note: cfNote.trim() || null,
      }),
    onSuccess: () => {
      setCfAmount('')
      setCfNote('')
      queryClient.invalidateQueries({ queryKey: ['portfolio-cash-flows', selectedId] })
      queryClient.invalidateQueries({ queryKey: ['portfolio-nav', selectedId] })
    },
  })

  const toggleActive = useMutation({
    mutationFn: (p: Portfolio) => apiPatch(`/api/v1/portfolios/${p.portfolio_id}`, { is_active: !p.is_active }),
    onSuccess: () => queryClient.invalidateQueries({ queryKey: ['portfolios'] }),
  })

  const selected = portfolios.data?.find((p) => p.portfolio_id === selectedId) ?? null

  return (
    <AppShell
      title="Portfolios"
      description="Generic, cross-channel capital pools — assign momentum/technical/ML strategies here to track real NAV, contributed capital, and XIRR, independent of any single strategy's backtest CAGR."
    >
      <Card className="mb-6">
        <CardHeader>
          <CardTitle>Create Portfolio</CardTitle>
        </CardHeader>
        <CardContent>
          <div className="flex flex-wrap items-center gap-2">
            <input className={inputClass} placeholder="Name" value={newName} onChange={(e) => setNewName(e.target.value)} />
            <input className={inputClass} placeholder="Description (optional)" value={newDescription} onChange={(e) => setNewDescription(e.target.value)} />
            <select className={inputClass} value={newChannel} onChange={(e) => setNewChannel(e.target.value)}>
              <option value="">Any channel</option>
              <option value="momentum">Momentum</option>
              <option value="technical">Technical</option>
              <option value="ml">ML</option>
            </select>
            <input className={inputClass} type="number" placeholder="Initial capital" value={newBaseCapital} onChange={(e) => setNewBaseCapital(e.target.value)} />
            <Button disabled={!newName.trim() || createPortfolio.isPending} onClick={() => createPortfolio.mutate()}>
              Create
            </Button>
            {createPortfolio.isError && <span className="text-sm text-red">{(createPortfolio.error as Error).message}</span>}
          </div>
        </CardContent>
      </Card>

      <Card className="mb-6">
        <CardHeader>
          <CardTitle>All Portfolios{portfolios.data ? ` (${portfolios.data.length})` : ''}</CardTitle>
        </CardHeader>
        <CardContent>
          {portfolios.error ? (
            <p className="text-sm text-red">Could not reach GET /api/v1/portfolios/ — {(portfolios.error as Error).message}</p>
          ) : portfolios.isLoading ? (
            <p className="text-sm text-muted-foreground">Loading…</p>
          ) : !portfolios.data?.length ? (
            <p className="text-sm text-muted-foreground">No portfolios yet — create one above.</p>
          ) : (
            <div className="flex flex-wrap gap-2">
              {portfolios.data.map((p) => (
                <button
                  key={p.portfolio_id}
                  type="button"
                  onClick={() => setSelectedId(p.portfolio_id)}
                  className={`rounded-[var(--radius-token)] border px-3 py-1.5 text-sm ${
                    selectedId === p.portfolio_id ? 'border-primary bg-primary/10 font-medium' : 'border-border'
                  } ${p.is_active ? '' : 'opacity-50'}`}
                >
                  {p.name}
                  {p.channel ? ` · ${p.channel}` : ''}
                  {!p.is_active ? ' (inactive)' : ''}
                </button>
              ))}
            </div>
          )}
        </CardContent>
      </Card>

      {selected && (
        <>
          <Card className="mb-6">
            <CardHeader>
              <CardTitle>{selected.name}</CardTitle>
              <CardDescription>
                {selected.description ?? 'No description.'}{' '}
                <button
                  type="button"
                  className="text-primary underline"
                  onClick={() => toggleActive.mutate(selected)}
                  disabled={toggleActive.isPending}
                >
                  {selected.is_active ? 'Mark inactive' : 'Mark active'}
                </button>
              </CardDescription>
            </CardHeader>
            <CardContent>
              {nav.error ? (
                <p className="text-sm text-red">Could not reach GET /api/v1/portfolios/{selectedId}/nav — {(nav.error as Error).message}</p>
              ) : (
                <div className="grid grid-cols-2 gap-4 sm:grid-cols-4">
                  <StatCard label="NAV" value={formatCurrencyINR(nav.data?.nav)} />
                  <StatCard label="Cash Balance" value={formatCurrencyINR(nav.data?.cash_balance)} />
                  <StatCard label="Holdings Value" value={formatCurrencyINR(nav.data?.holdings_value)} />
                  <StatCard label="Total Contributed" value={formatCurrencyINR(nav.data?.total_contributed)} />
                  <StatCard label="Total Withdrawn" value={formatCurrencyINR(nav.data?.total_withdrawn)} />
                  <StatCard
                    label={
                      <span className="inline-flex items-center gap-1">
                        XIRR{' '}
                        <InfoTooltip>
                          Annualized return accounting for the actual timing and size of every contribution/withdrawal
                          against current NAV — the honest money-weighted return, not a strategy's backtest CAGR.
                        </InfoTooltip>
                      </span>
                    }
                    value={fmtPct(nav.data?.xirr)}
                    tone={nav.data?.xirr != null && nav.data.xirr >= 0 ? 'green' : 'red'}
                  />
                  <StatCard label="As Of" value={nav.data?.as_of ?? '—'} />
                </div>
              )}
            </CardContent>
          </Card>

          <Card className="mb-6">
            <CardHeader>
              <CardTitle>Record Cash Flow</CardTitle>
              <CardDescription>SIP contributions, withdrawals, dividends, tax, or fees against this portfolio.</CardDescription>
            </CardHeader>
            <CardContent>
              <div className="flex flex-wrap items-center gap-2">
                <input className={inputClass} type="date" value={cfDate} onChange={(e) => setCfDate(e.target.value)} />
                <select className={inputClass} value={cfKind} onChange={(e) => setCfKind(e.target.value as (typeof CASH_FLOW_KINDS)[number])}>
                  {CASH_FLOW_KINDS.map((k) => (
                    <option key={k} value={k}>
                      {k}
                    </option>
                  ))}
                </select>
                <input className={inputClass} type="number" placeholder="Amount" value={cfAmount} onChange={(e) => setCfAmount(e.target.value)} />
                <input className={inputClass} placeholder="Note (optional)" value={cfNote} onChange={(e) => setCfNote(e.target.value)} />
                <Button disabled={!cfAmount || addCashFlow.isPending} onClick={() => addCashFlow.mutate()}>
                  Record
                </Button>
                {addCashFlow.isError && <span className="text-sm text-red">{(addCashFlow.error as Error).message}</span>}
              </div>
            </CardContent>
          </Card>

          <Card className="mb-6">
            <CardHeader>
              <CardTitle>Cash Flows{cashFlows.data ? ` (${cashFlows.data.length})` : ''}</CardTitle>
            </CardHeader>
            <CardContent>
              <DataTable
                columns={cashFlowColumns}
                data={cashFlows.data ?? []}
                isLoading={cashFlows.isLoading}
                emptyMessage="No cash flows recorded yet — the initial capital (if any) was recorded when this portfolio was created."
              />
            </CardContent>
          </Card>

          <Card>
            <CardHeader>
              <CardTitle>Tagged Trades{trades.data ? ` (${trades.data.length})` : ''}</CardTitle>
              <CardDescription>Every momentum_trades row assigned to this portfolio, across every strategy.</CardDescription>
            </CardHeader>
            <CardContent>
              <DataTable
                columns={tradeColumns}
                data={trades.data ?? []}
                isLoading={trades.isLoading}
                emptyMessage="No trades tagged to this portfolio yet — assign it via Strategy Deploy's Portfolio field."
              />
            </CardContent>
          </Card>
        </>
      )}
    </AppShell>
  )
}
