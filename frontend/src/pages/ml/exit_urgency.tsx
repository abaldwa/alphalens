import { useQuery } from '@tanstack/react-query'
import type { ColumnDef } from '@tanstack/react-table'

import { AppShell, Badge, Card, CardContent, CardDescription, CardHeader, CardTitle, DataTable, InfoTooltip, formatCurrencyINR, tickerColumn } from '@/lib/ui'
import { apiGet } from '@/shared/api/client'
import type { ExitUrgencyResponse, ExitUrgencyRow } from './types'

function fmtPct(v: number | null | undefined) {
  return v == null ? '—' : `${(v * 100).toFixed(1)}%`
}
const fmtMoney = formatCurrencyINR
function urgencyVariant(u: number | null | undefined) {
  if (u == null) return 'outline' as const
  if (u >= 70) return 'destructive' as const
  if (u >= 40) return 'warning' as const
  return 'success' as const
}
function pnlTone(v: number | null | undefined) {
  if (v == null) return undefined
  return v >= 0 ? 'text-green' : 'text-red'
}

const columns: ColumnDef<ExitUrgencyRow, unknown>[] = [
  tickerColumn<ExitUrgencyRow>(),
  { accessorKey: 'company_name', header: 'Name', meta: { priority: 'low' }, cell: (i) => i.getValue<string | null>() ?? '—' },
  { accessorKey: 'entry_date', header: 'Entry Date', meta: { priority: 'low' } },
  { accessorKey: 'entry_price', header: 'Entry Price', meta: { priority: 'low', align: 'right' }, cell: (i) => fmtMoney(i.getValue<number>()) },
  { accessorKey: 'current_price', header: 'Current', meta: { align: 'right' }, cell: (i) => fmtMoney(i.getValue<number | null>()) },
  {
    accessorKey: 'unrealised_pnl_pct',
    header: 'Unrealised P&L',
    meta: { align: 'right' },
    cell: (i) => <span className={pnlTone(i.getValue<number | null>())}>{fmtPct(i.getValue<number | null>())}</span>,
  },
  {
    accessorKey: 'exit_urgency',
    header: () => (
      <span className="inline-flex items-center gap-1">
        Exit Urgency
        <InfoTooltip>rule_based_exit_policy's 0-100 urgency score for exiting an existing position.</InfoTooltip>
      </span>
    ),
    meta: { align: 'right' },
    cell: (i) => <Badge variant={urgencyVariant(i.getValue<number | null>())}>{i.getValue<number | null>()?.toFixed(0) ?? '—'}</Badge>,
  },
  {
    accessorKey: 'exit_type',
    header: () => (
      <span className="inline-flex items-center gap-1">
        Exit Type (reason)
        <InfoTooltip>The specific rule that triggered the exit urgency score (thesis_broken / momentum_exhaustion / risk_management / target_achieved / opportunity_cost / pnd_exit).</InfoTooltip>
      </span>
    ),
    cell: (i) => i.getValue<string | null>() ?? '—',
  },
]

export function MlExitUrgencyPage() {
  const exitUrgency = useQuery({
    queryKey: ['paper-trading-exit-urgency'],
    queryFn: () => apiGet<ExitUrgencyResponse>('/api/v1/paper_trading/exit_urgency'),
  })

  return (
    <AppShell title="ML — Exit Urgency" description="All open paper-trading positions ranked by exit urgency, sourced from the latest signal_5d call for each ticker.">
      <Card>
        <CardHeader>
          <CardTitle>Exit urgency</CardTitle>
          <CardDescription>
            {exitUrgency.data?.as_of_date ? `All open positions, ranked by exit_urgency — as of ${exitUrgency.data.as_of_date}` : exitUrgency.error ? 'Failed to load' : 'Loading…'}
          </CardDescription>
        </CardHeader>
        <CardContent>
          {exitUrgency.error ? (
            <p className="text-sm text-red">Could not reach GET /api/v1/paper_trading/exit_urgency — {(exitUrgency.error as Error).message}</p>
          ) : (
            <DataTable columns={columns} data={exitUrgency.data?.rows ?? []} isLoading={exitUrgency.isLoading} emptyMessage="No open positions." />
          )}
        </CardContent>
      </Card>
    </AppShell>
  )
}
