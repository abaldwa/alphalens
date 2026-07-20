import { useState } from 'react'
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query'
import type { ColumnDef } from '@tanstack/react-table'

import { AppShell, Badge, Card, CardContent, CardHeader, CardTitle, DataTable, cmpColumn, formatCurrencyINR, tickerColumn } from '@/lib/ui'
import { apiGet, apiPost } from '@/shared/api/client'

interface ReconciliationRow {
  id: number
  family_id: string
  ticker: string
  quarter_end_date: string
  estimated_position_pre_correction: number | null
  reported_shares_est: number | null
  discrepancy_pct: number | null
  status: string
}
interface ReconciliationResponse {
  data: ReconciliationRow[]
  record_count: number
}

interface FamilyRow {
  family_id: string
  ticker: string
  company_name: string | null
  cap_band: string
  market_cap_cr: number | null
  family_display_name: string
  trade_date: string | null
  net_transaction_type: string | null
  net_quantity: number | null
  avg_price: number | null
  wac: number | null
  cmp: number | null
  price_diff: number | null
  price_diff_pct: number | null
  cumulative_position_est: number | null
  holding_pct_of_company: number | null
  entry_status: string | null
}
interface FamilyResponse {
  data: FamilyRow[]
  record_count: number
}

interface DealRow {
  trade_date: string
  exchange: string
  deal_type: string
  ticker: string
  company_name: string | null
  cap_band: string
  market_cap_cr: number | null
  client_name: string | null
  transaction_type: string | null
  quantity: number | null
  price: number | null
}
interface DealsResponse {
  date: string | null
  data: DealRow[]
  record_count: number
}

const CAP_BAND_VARIANT: Record<string, 'default' | 'secondary' | 'outline' | 'success' | 'warning' | 'destructive'> = {
  large: 'secondary',
  mid: 'default',
  small: 'warning',
  micro: 'destructive',
  unknown: 'secondary',
}

const selectClass = 'h-9 rounded-[var(--radius-token)] border border-border bg-transparent px-3 text-sm'
const inputClass = selectClass

function isoToday(): string {
  return new Date().toISOString().slice(0, 10)
}

function fmtNum(v: number | null | undefined): string {
  return v != null ? v.toLocaleString('en-IN') : '—'
}

const fmtPrice = formatCurrencyINR

export function BigInvestorsPage() {
  const queryClient = useQueryClient()

  // --- Reconciliation ---
  const reconciliation = useQuery({
    queryKey: ['bi-reconciliation'],
    queryFn: () => apiGet<ReconciliationResponse>('/api/v1/big-investors/reconciliation'),
  })

  const resolveMutation = useMutation({
    mutationFn: ({ id, reviewedBy }: { id: number; reviewedBy: string }) =>
      apiPost(`/api/v1/big-investors/reconciliation/${id}/resolve`, { reviewed_by: reviewedBy }),
    onSuccess: () => queryClient.invalidateQueries({ queryKey: ['bi-reconciliation'] }),
  })

  const reconciliationColumns: ColumnDef<ReconciliationRow, unknown>[] = [
    { accessorKey: 'family_id', header: 'Family' },
    tickerColumn<ReconciliationRow>(),
    { accessorKey: 'quarter_end_date', header: 'Quarter End' },
    { accessorKey: 'estimated_position_pre_correction', header: 'Est. Position', meta: { align: 'right' }, cell: (i) => fmtNum(i.getValue<number | null>()) },
    { accessorKey: 'reported_shares_est', header: 'Reported Est.', meta: { align: 'right' }, cell: (i) => fmtNum(i.getValue<number | null>()) },
    {
      accessorKey: 'discrepancy_pct',
      header: 'Discrepancy',
      meta: { align: 'right' },
      cell: (i) => {
        const v = i.getValue<number | null>()
        return v != null ? `${(v * 100).toFixed(1)}%` : '—'
      },
    },
    {
      accessorKey: 'status',
      header: 'Status',
      cell: (i) => {
        const s = i.getValue<string>()
        return <Badge variant={s === 'flagged_for_review' ? 'destructive' : 'success'}>{s}</Badge>
      },
    },
    {
      id: 'action',
      header: 'Action',
      cell: ({ row }) =>
        row.original.status === 'flagged_for_review' ? (
          <button
            className="h-8 rounded-[var(--radius-token)] border border-border px-2 text-xs"
            disabled={resolveMutation.isPending}
            onClick={() => {
              const reviewedBy = window.prompt('Reviewer name:')
              if (!reviewedBy) return
              resolveMutation.mutate({ id: row.original.id, reviewedBy })
            }}
          >
            Mark Reviewed
          </button>
        ) : null,
    },
  ]

  const reconciliationRows = reconciliation.data?.data ?? []
  const flaggedCount = reconciliationRows.filter((r) => r.status === 'flagged_for_review').length

  // --- Family entries/exits ---
  const family = useQuery({
    queryKey: ['bi-family-entries-exits'],
    queryFn: () => apiGet<FamilyResponse>('/api/v1/big-investors/bulk-deals/families/entries-exits'),
  })

  const familyColumns: ColumnDef<FamilyRow, unknown>[] = [
    tickerColumn<FamilyRow>(),
    { accessorKey: 'company_name', header: 'Company', cell: (i) => i.getValue<string | null>() ?? '—' },
    {
      accessorKey: 'cap_band',
      header: 'Cap Band',
      cell: (i) => <Badge variant={CAP_BAND_VARIANT[i.getValue<string>()] ?? 'secondary'}>{i.getValue<string>()}</Badge>,
    },
    { accessorKey: 'market_cap_cr', header: 'Market Cap (cr)', meta: { align: 'right' }, cell: (i) => fmtNum(i.getValue<number | null>()) },
    {
      accessorKey: 'family_display_name',
      header: 'Investor Family',
      cell: ({ row }) =>
        row.original.family_id.startsWith('unmapped:') ? (
          <span className="text-muted-foreground">{row.original.family_display_name}</span>
        ) : (
          row.original.family_display_name
        ),
    },
    { accessorKey: 'trade_date', header: 'Txn Date', cell: (i) => i.getValue<string | null>() ?? '—' },
    {
      accessorKey: 'net_transaction_type',
      header: 'Net Txn',
      cell: (i) => {
        const v = i.getValue<string | null>()
        return v ? <Badge variant={v === 'BUY' ? 'success' : 'destructive'}>{v}</Badge> : '—'
      },
    },
    { accessorKey: 'net_quantity', header: 'Net Qty', meta: { align: 'right' }, cell: (i) => fmtNum(i.getValue<number | null>()) },
    { accessorKey: 'avg_price', header: 'Entry Price', meta: { align: 'right' }, cell: (i) => fmtPrice(i.getValue<number | null>()) },
    { accessorKey: 'wac', header: 'WAC', meta: { align: 'right' }, cell: (i) => fmtPrice(i.getValue<number | null>()) },
    cmpColumn<FamilyRow>('cmp'),
    {
      id: 'price_diff',
      header: 'CMP vs Entry',
      meta: { align: 'right' },
      cell: ({ row }) => {
        const { price_diff, price_diff_pct } = row.original
        if (price_diff == null || price_diff_pct == null) return '—'
        return `${price_diff >= 0 ? '+' : ''}${price_diff.toFixed(2)} (${price_diff_pct.toFixed(1)}%)`
      },
    },
    { accessorKey: 'cumulative_position_est', header: 'Position Est.', meta: { align: 'right' }, cell: (i) => fmtNum(i.getValue<number | null>()) },
    {
      accessorKey: 'holding_pct_of_company',
      header: '% of Company',
      meta: { align: 'right' },
      cell: (i) => {
        const v = i.getValue<number | null>()
        return v != null ? `${v.toFixed(2)}%` : '—'
      },
    },
    {
      accessorKey: 'entry_status',
      header: 'Status',
      cell: (i) => {
        const v = i.getValue<string | null>()
        if (v === 'new_entry') return <Badge variant="success">NEW ENTRY</Badge>
        if (v === 'old_entry') return <Badge variant="default">OLD ENTRY</Badge>
        return '—'
      },
    },
  ]

  // --- Raw deals ---
  const [dealDate, setDealDate] = useState(isoToday())
  const [dealCapBand, setDealCapBand] = useState('')
  const [dealType, setDealType] = useState('')
  const [dealQueryParams, setDealQueryParams] = useState({ date: isoToday(), capBand: '', dealType: '' })

  const deals = useQuery({
    queryKey: ['bi-deals', dealQueryParams],
    queryFn: () =>
      apiGet<DealsResponse>('/api/v1/big-investors/bulk-deals/entries-exits', {
        date: dealQueryParams.date,
        cap_band: dealQueryParams.capBand || undefined,
        deal_type: dealQueryParams.dealType || undefined,
      }),
  })

  const dealsColumns: ColumnDef<DealRow, unknown>[] = [
    tickerColumn<DealRow>(),
    { accessorKey: 'company_name', header: 'Company', cell: (i) => i.getValue<string | null>() ?? '—' },
    {
      accessorKey: 'cap_band',
      header: 'Cap Band',
      cell: (i) => <Badge variant={CAP_BAND_VARIANT[i.getValue<string>()] ?? 'secondary'}>{i.getValue<string>()}</Badge>,
    },
    { accessorKey: 'market_cap_cr', header: 'Market Cap (cr)', meta: { align: 'right' }, cell: (i) => fmtNum(i.getValue<number | null>()) },
    { accessorKey: 'exchange', header: 'Exchange' },
    { accessorKey: 'deal_type', header: 'Deal' },
    { accessorKey: 'client_name', header: 'Client', cell: (i) => i.getValue<string | null>() ?? '—' },
    {
      accessorKey: 'transaction_type',
      header: 'Txn',
      cell: (i) => {
        const v = i.getValue<string | null>()
        return v ? <Badge variant={v === 'BUY' ? 'success' : 'destructive'}>{v}</Badge> : '—'
      },
    },
    { accessorKey: 'quantity', header: 'Quantity', meta: { align: 'right' }, cell: (i) => fmtNum(i.getValue<number | null>()) },
    { accessorKey: 'price', header: 'Price', meta: { align: 'right' }, cell: (i) => fmtPrice(i.getValue<number | null>()) },
  ]

  return (
    <AppShell title="Big Investors" description="Bulk/block deals, family-attributed positions, and shareholding reconciliation.">
      <Card>
        <CardHeader>
          <CardTitle>
            Reconciliation {flaggedCount > 0 && <span className="ml-2 text-sm text-red">{flaggedCount} flagged for review</span>}
          </CardTitle>
        </CardHeader>
        <CardContent>
          {reconciliation.error ? (
            <p className="text-sm text-red">
              Could not reach GET /api/v1/big-investors/reconciliation — {(reconciliation.error as Error).message}
            </p>
          ) : (
            <DataTable
              columns={reconciliationColumns}
              data={reconciliationRows}
              isLoading={reconciliation.isLoading}
              emptyMessage="No reconciliation rows."
            />
          )}
        </CardContent>
      </Card>

      <div className="mt-4">
        <Card>
          <CardHeader>
            <CardTitle>Big Investor Entries/Exits</CardTitle>
          </CardHeader>
          <CardContent>
            {family.error ? (
              <p className="text-sm text-red">
                Could not reach GET /api/v1/big-investors/bulk-deals/families/entries-exits — {(family.error as Error).message}
              </p>
            ) : (
              <DataTable
                columns={familyColumns}
                data={family.data?.data ?? []}
                isLoading={family.isLoading}
                emptyMessage="No family-attributed positions for this date/filter combination."
              />
            )}
          </CardContent>
        </Card>
      </div>

      <div className="mt-4">
        <Card>
          <CardHeader>
            <CardTitle>Raw Bulk/Block Deals</CardTitle>
            <p className="text-sm text-muted-foreground">Every NSE/BSE client_name row as-is, before family attribution or netting.</p>
          </CardHeader>
          <CardContent>
            <div className="mb-3 flex flex-wrap items-center gap-2">
              <label className="text-sm text-muted-foreground" htmlFor="deal-date-input">
                Date:
              </label>
              <input id="deal-date-input" className={inputClass} type="date" value={dealDate} onChange={(e) => setDealDate(e.target.value)} />
              <label className="text-sm text-muted-foreground" htmlFor="deal-cap-select">
                Cap band:
              </label>
              <select id="deal-cap-select" className={selectClass} value={dealCapBand} onChange={(e) => setDealCapBand(e.target.value)}>
                <option value="">All cap bands</option>
                <option value="large">Large</option>
                <option value="mid">Mid</option>
                <option value="small">Small</option>
                <option value="micro">Micro</option>
              </select>
              <label className="text-sm text-muted-foreground" htmlFor="deal-type-select">
                Deal type:
              </label>
              <select id="deal-type-select" className={selectClass} value={dealType} onChange={(e) => setDealType(e.target.value)}>
                <option value="">Bulk + Block</option>
                <option value="bulk">Bulk only</option>
                <option value="block">Block only</option>
              </select>
              <button
                className="h-9 rounded-[var(--radius-token)] border border-border px-3 text-sm"
                onClick={() => setDealQueryParams({ date: dealDate, capBand: dealCapBand, dealType })}
              >
                Load
              </button>
            </div>
            {deals.error ? (
              <p className="text-sm text-red">
                Could not reach GET /api/v1/big-investors/bulk-deals/entries-exits — {(deals.error as Error).message}
              </p>
            ) : (
              <DataTable
                columns={dealsColumns}
                data={deals.data?.data ?? []}
                isLoading={deals.isLoading}
                emptyMessage="No bulk/block deals for this date/filter combination."
              />
            )}
          </CardContent>
        </Card>
      </div>
    </AppShell>
  )
}
