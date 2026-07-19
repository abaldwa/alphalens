import { useState } from 'react'
import { useQuery } from '@tanstack/react-query'
import type { ColumnDef } from '@tanstack/react-table'

import { AppShell, Button, Card, CardContent, CardHeader, CardTitle, DataTable, InfoTooltip, StatCard } from '@/lib/ui'
import { apiGet } from '@/shared/api/client'
import type { ForensicRow } from './types'

interface FundamentalsRow {
  fiscal_year: number
  quarter: number
  quarter_end_date: string
  pat: number | null
  fcf: number | null
  capex: number | null
}

interface FundamentalsHistoryResponse {
  ticker: string
  count: number
  data: FundamentalsRow[]
}

function fmtMoney(v: number | null | undefined): string {
  return v == null ? '—' : `₹${v.toLocaleString('en-IN', { maximumFractionDigits: 2 })}`
}

const columns: ColumnDef<FundamentalsRow, unknown>[] = [
  { id: 'quarter', header: 'Quarter', cell: ({ row }) => `${row.original.fiscal_year} Q${row.original.quarter}` },
  { accessorKey: 'pat', header: 'PAT', cell: (i) => fmtMoney(i.getValue<number | null>()) },
  {
    accessorKey: 'fcf',
    header: () => (
      <span className="inline-flex items-center gap-1">
        FCF
        <InfoTooltip>
          Free cash flow. Used here as the real-data proxy for CFO (operating cash flow), since this schema's
          FundamentalsRow has no raw CFO line item — the standard identity FCF = CFO - Capex is used in reverse
          elsewhere to derive CFO from FCF and Capex.
        </InfoTooltip>
      </span>
    ),
    cell: (i) => fmtMoney(i.getValue<number | null>()),
  },
  { accessorKey: 'capex', header: 'Capex', cell: (i) => fmtMoney(i.getValue<number | null>()) },
]

export function CashflowPage() {
  const [tickerInput, setTickerInput] = useState('RELIANCE')
  const [ticker, setTicker] = useState('RELIANCE')

  const forensic = useQuery({
    queryKey: ['forensic-cashflow-forensic', ticker],
    queryFn: () => apiGet<ForensicRow | null>(`/api/v1/signals/ml/forensic/${ticker}`),
  })

  const history = useQuery({
    queryKey: ['forensic-cashflow-history', ticker],
    queryFn: () => apiGet<FundamentalsHistoryResponse>(`/api/v1/fundamentals/${ticker}/history`),
  })

  const row = forensic.data
  const rows = [...(history.data?.data ?? [])].sort(
    (a, b) => new Date(a.quarter_end_date).getTime() - new Date(b.quarter_end_date).getTime(),
  )

  return (
    <AppShell
      title="Forensic — Cash Flow"
      description="Cash flow quality: accrual metrics and quarterly FCF vs PAT (FundamentalsRow has no raw CFO line item — FCF and PAT are the real series shown)."
      actions={
        <div className="flex items-center gap-2">
          <input
            className="h-9 rounded-[var(--radius-token)] border border-border bg-transparent px-3 text-sm"
            value={tickerInput}
            onChange={(e) => setTickerInput(e.target.value.toUpperCase())}
            placeholder="Ticker (e.g. RELIANCE)"
          />
          <Button onClick={() => setTicker(tickerInput.trim().toUpperCase())}>Load</Button>
        </div>
      }
    >
      <Card className="mb-4">
        <CardHeader>
          <CardTitle>Accrual Quality Summary</CardTitle>
        </CardHeader>
        <CardContent>
          {forensic.error ? (
            <p className="text-sm text-red">
              Could not reach GET /api/v1/signals/ml/forensic/{ticker} — {(forensic.error as Error).message}
            </p>
          ) : row && (row.sloan_accrual != null || row.dechow_f != null) ? (
            <div className="grid grid-cols-1 gap-4 sm:grid-cols-2">
              <StatCard
                label={
                  <span className="inline-flex items-center gap-1">
                    Sloan Accrual Ratio
                    <InfoTooltip>
                      Ratio of accounting accruals to total assets. Large positive values are historically
                      associated with lower forward returns (the "accrual anomaly").
                    </InfoTooltip>
                  </span>
                }
                value={row.sloan_accrual != null ? row.sloan_accrual.toFixed(3) : '—'}
              />
              <StatCard
                label={
                  <span className="inline-flex items-center gap-1">
                    Dechow F-Score
                    <InfoTooltip>
                      Earnings-quality / manipulation-risk score derived from the Dechow-Ge-Larson-Sloan model.
                    </InfoTooltip>
                  </span>
                }
                value={row.dechow_f != null ? row.dechow_f.toFixed(2) : '—'}
              />
            </div>
          ) : (
            <p className="text-sm text-muted-foreground">No accrual-quality scores for this ticker</p>
          )}
        </CardContent>
      </Card>

      <Card>
        <CardHeader>
          <CardTitle>Free Cash Flow vs PAT — Quarterly</CardTitle>
        </CardHeader>
        <CardContent>
          {history.error ? (
            <p className="text-sm text-red">
              Could not reach GET /api/v1/fundamentals/{ticker}/history — {(history.error as Error).message}
            </p>
          ) : (
            <DataTable
              columns={columns}
              data={rows}
              isLoading={history.isLoading}
              emptyMessage="No quarterly fundamentals history for this ticker"
            />
          )}
        </CardContent>
      </Card>
    </AppShell>
  )
}
