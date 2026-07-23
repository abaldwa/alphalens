import { useState } from 'react'
import { useQuery } from '@tanstack/react-query'
import type { ColumnDef } from '@tanstack/react-table'

import { AppShell, Badge, Card, CardContent, CardHeader, CardTitle, DataTable, InfoTooltip, Sheet, SheetContent, SheetHeader, SheetTitle, Table, TableHeader, TableBody, TableRow, TableHead, TableCell, numericCellClass, tickerColumn } from '@/lib/ui'
import { apiGet } from '@/shared/api/client'

interface MfMoverRow {
  ticker: string
  company_name: string | null
  market_cap_cr: number | null
  cap_band: string
  curr_month: string | null
  prev_month: string | null
  curr_qty: number
  prev_qty: number
  qty_change: number
  qty_change_pct: number | null
  curr_scheme_count: number | null
  scheme_count_change: number
  direction: string
}
interface MfMoversResponse {
  as_of: string
  data: MfMoverRow[]
  record_count: number
}
interface MfSchemeRow {
  ticker: string
  month: string
  scheme_name: string
  isin: string | null
  quantity: number
  value_inr: number | null
  availability_date: string
}
interface MfSchemeResponse {
  ticker: string
  data: MfSchemeRow[]
  record_count: number
}

const CAP_BAND_VARIANT: Record<string, 'default' | 'secondary' | 'outline' | 'success' | 'warning' | 'destructive'> = {
  large: 'secondary',
  mid: 'default',
  small: 'warning',
  micro: 'destructive',
  unknown: 'secondary',
}
const DIRECTION_VARIANT: Record<string, 'success' | 'destructive' | 'secondary'> = {
  new_entry: 'success',
  full_exit: 'destructive',
  increasing: 'success',
  decreasing: 'destructive',
  unchanged: 'secondary',
}
const DIRECTION_LABEL: Record<string, string> = {
  new_entry: 'NEW ENTRY',
  full_exit: 'FULL EXIT',
  increasing: 'Increasing',
  decreasing: 'Decreasing',
  unchanged: 'Unchanged',
}

const selectClass = 'h-9 rounded-[var(--radius-token)] border border-border bg-transparent px-3 text-sm'

export function BigInvestorsMfHoldingsPage() {
  const [capBand, setCapBand] = useState('')
  const [direction, setDirection] = useState('')
  const [schemeTicker, setSchemeTicker] = useState<string | null>(null)
  const [schemeMonth, setSchemeMonth] = useState<string | null>(null)

  const movers = useQuery({
    queryKey: ['mf-movers', capBand, direction],
    queryFn: () =>
      apiGet<MfMoversResponse>('/api/v1/big-investors/mf-holdings/movers', {
        cap_band: capBand || undefined,
        direction: direction || undefined,
      }),
  })

  const schemeDetail = useQuery({
    queryKey: ['mf-scheme-detail', schemeTicker, schemeMonth],
    queryFn: () =>
      apiGet<MfSchemeResponse>(`/api/v1/big-investors/mf-holdings/${schemeTicker}`, {
        start_month: schemeMonth?.slice(0, 7),
        end_month: schemeMonth?.slice(0, 7),
      }),
    enabled: !!schemeTicker,
  })

  const columns: ColumnDef<MfMoverRow, unknown>[] = [
    tickerColumn<MfMoverRow>(),
    { accessorKey: 'company_name', header: 'Company', meta: { priority: 'low' }, cell: (i) => i.getValue<string | null>() ?? '—' },
    {
      accessorKey: 'cap_band',
      header: 'Cap Band',
      meta: { priority: 'low' },
      cell: (i) => <Badge variant={CAP_BAND_VARIANT[i.getValue<string>()] ?? 'secondary'}>{i.getValue<string>()}</Badge>,
    },
    {
      accessorKey: 'market_cap_cr',
      header: 'Market Cap (cr)',
      meta: { priority: 'low', align: 'right' },
      cell: (i) => i.getValue<number | null>()?.toLocaleString('en-IN') ?? '—',
    },
    { accessorKey: 'prev_qty', header: 'Prev Qty', meta: { priority: 'low', align: 'right' }, cell: (i) => i.getValue<number>().toLocaleString('en-IN') },
    { accessorKey: 'curr_qty', header: 'Curr Qty', meta: { priority: 'low', align: 'right' }, cell: (i) => i.getValue<number>().toLocaleString('en-IN') },
    {
      accessorKey: 'qty_change_pct',
      header: 'Change %',
      meta: { align: 'right' },
      cell: (i) => {
        const v = i.getValue<number | null>()
        return v == null ? '—' : `${v.toFixed(1)}%`
      },
    },
    {
      id: 'schemes',
      header: 'Schemes',
      cell: ({ row }) => (
        <button
          className="cursor-pointer underline decoration-dotted"
          title="View the list of mutual funds holding this stock"
          onClick={() => {
            setSchemeTicker(row.original.ticker)
            setSchemeMonth(row.original.curr_month)
          }}
        >
          {row.original.curr_scheme_count ?? '—'}
        </button>
      ),
    },
    {
      accessorKey: 'scheme_count_change',
      header: () => (
        <span className="inline-flex items-center gap-1">
          Scheme Δ
          <InfoTooltip>Net change in the number of distinct mutual fund schemes holding this stock between the two most recent reporting months.</InfoTooltip>
        </span>
      ),
      meta: { priority: 'low', align: 'right' },
      cell: (i) => {
        const v = i.getValue<number>()
        return v > 0 ? `+${v}` : String(v)
      },
    },
    {
      accessorKey: 'direction',
      header: 'Status',
      cell: (i) => <Badge variant={DIRECTION_VARIANT[i.getValue<string>()] ?? 'secondary'}>{DIRECTION_LABEL[i.getValue<string>()] ?? i.getValue<string>()}</Badge>,
    },
  ]

  return (
    <AppShell title="Big Investors — MF Holdings" description="Mutual fund holding movers between the two most recent PIT-available months.">
      <Card>
        <CardHeader>
          <CardTitle>{movers.data ? `Comparing ${movers.data.data[0]?.prev_month ?? '—'} → ${movers.data.data[0]?.curr_month ?? '—'} (as of ${movers.data.as_of})` : 'Movers'}</CardTitle>
        </CardHeader>
        <CardContent>
          <div className="mb-3 flex flex-wrap items-center gap-2">
            <select className={selectClass} value={capBand} onChange={(e) => setCapBand(e.target.value)}>
              <option value="">All cap bands</option>
              <option value="large">Large</option>
              <option value="mid">Mid</option>
              <option value="small">Small</option>
              <option value="micro">Micro</option>
            </select>
            <select className={selectClass} value={direction} onChange={(e) => setDirection(e.target.value)}>
              <option value="">All movers</option>
              <option value="new_entry">New entries</option>
              <option value="full_exit">Full exits</option>
              <option value="increasing">Increasing</option>
              <option value="decreasing">Decreasing</option>
            </select>
          </div>
          {movers.error ? (
            <p className="text-sm text-red">
              Could not reach GET /api/v1/big-investors/mf-holdings/movers — {(movers.error as Error).message}
            </p>
          ) : (
            <DataTable
              columns={columns}
              data={movers.data?.data ?? []}
              isLoading={movers.isLoading}
              emptyMessage="No MF holdings data available yet (twice-monthly ingestion — see Ops for last run status)."
            />
          )}
        </CardContent>
      </Card>

      <Sheet open={!!schemeTicker} onOpenChange={(open) => !open && setSchemeTicker(null)}>
        <SheetContent>
          <SheetHeader>
            <SheetTitle>
              {schemeTicker} — Schemes holding ({schemeMonth ?? '—'})
            </SheetTitle>
          </SheetHeader>
          <div className="p-4">
            {schemeDetail.isLoading ? (
              <p className="text-sm text-muted-foreground">Loading…</p>
            ) : schemeDetail.error ? (
              <p className="text-sm text-red">Failed to load: {(schemeDetail.error as Error).message}</p>
            ) : !schemeDetail.data?.data.length ? (
              <p className="text-sm text-muted-foreground">No scheme-level detail found for this month.</p>
            ) : (
              <Table>
                <TableHeader>
                  <TableRow>
                    <TableHead>Scheme</TableHead>
                    <TableHead>Quantity</TableHead>
                  </TableRow>
                </TableHeader>
                <TableBody>
                  {schemeDetail.data.data.map((s) => (
                    <TableRow key={s.scheme_name}>
                      <TableCell>{s.scheme_name}</TableCell>
                      <TableCell className={numericCellClass}>{s.quantity.toLocaleString('en-IN')}</TableCell>
                    </TableRow>
                  ))}
                </TableBody>
              </Table>
            )}
          </div>
        </SheetContent>
      </Sheet>
    </AppShell>
  )
}
