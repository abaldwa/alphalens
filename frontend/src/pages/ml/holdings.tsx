import { useRef, useState } from 'react'
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query'
import type { ColumnDef } from '@tanstack/react-table'

import { AppShell, Badge, Button, Card, CardContent, CardHeader, CardTitle, DataTable, InfoTooltip, TickerLink } from '@/lib/ui'
import { API_BASE_URL, apiDelete, apiGet, apiPost, ApiError } from '@/shared/api/client'
import type { HoldingRow, MLSignalRow } from './types'

function todayStr() {
  return new Date().toISOString().slice(0, 10)
}
function fmtPct(v: number | null | undefined) {
  return v == null ? '—' : `${(v * 100).toFixed(1)}%`
}
function fmtMoney(v: number | null | undefined) {
  return v == null ? '—' : `₹${v.toLocaleString('en-IN')}`
}

export function MlHoldingsPage() {
  const queryClient = useQueryClient()
  const [addTicker, setAddTicker] = useState('')
  const [addQty, setAddQty] = useState('')
  const [addPrice, setAddPrice] = useState('')
  const [addDate, setAddDate] = useState(todayStr())
  const [addRationale, setAddRationale] = useState('')
  const [csvError, setCsvError] = useState<string | null>(null)
  const fileInputRef = useRef<HTMLInputElement>(null)

  const holdings = useQuery({
    queryKey: ['holdings'],
    queryFn: () => apiGet<HoldingRow[]>('/api/v1/holdings/'),
  })

  const tickers = holdings.data ?? []
  const signals = useQuery({
    queryKey: ['holdings-signals', tickers.map((h) => h.ticker).join(',')],
    queryFn: async () => {
      const today = todayStr()
      const entries = await Promise.all(
        tickers.map(async (h) => {
          try {
            const rows = await apiGet<MLSignalRow[]>(`/api/v1/signals/ml/${h.ticker}/${today}`, { carry_forward: true })
            return [h.ticker, rows.find((r) => r.model_name === 'signal_5d') ?? null] as const
          } catch {
            return [h.ticker, null] as const
          }
        }),
      )
      return Object.fromEntries(entries) as Record<string, MLSignalRow | null>
    },
    enabled: tickers.length > 0,
  })

  const addMutation = useMutation({
    mutationFn: () =>
      apiPost('/api/v1/holdings/', {
        ticker: addTicker.trim().toUpperCase(),
        purchase_date: addDate || todayStr(),
        qty: Number(addQty),
        purchase_price: addPrice ? Number(addPrice) : null,
        purchase_rationale: addRationale.trim() || null,
      }),
    onSuccess: () => {
      setAddTicker('')
      setAddQty('')
      setAddPrice('')
      setAddRationale('')
      queryClient.invalidateQueries({ queryKey: ['holdings'] })
    },
  })

  const deleteMutation = useMutation({
    mutationFn: (id: number) => apiDelete(`/api/v1/holdings/${id}`),
    onSuccess: () => queryClient.invalidateQueries({ queryKey: ['holdings'] }),
  })

  // POST /api/v1/holdings/upload-csv reads the raw CSV text as the
  // request body (not multipart/form-data — the API deliberately avoids
  // depending on python-multipart), matching how
  // dashboard/static/ml/js/holdings.js read the chosen file with
  // FileReader and posted its text content directly.
  const uploadCsvMutation = useMutation({
    mutationFn: async (text: string) => {
      const url = new URL('/api/v1/holdings/upload-csv', API_BASE_URL).toString()
      const resp = await fetch(url, {
        method: 'POST',
        headers: { 'Content-Type': 'text/csv' },
        body: text,
      })
      if (!resp.ok) {
        const detail = await resp.text().catch(() => '')
        throw new ApiError(resp.status, detail || resp.statusText, url)
      }
      return (await resp.json()) as HoldingRow[]
    },
    onSuccess: () => {
      setCsvError(null)
      if (fileInputRef.current) fileInputRef.current.value = ''
      queryClient.invalidateQueries({ queryKey: ['holdings'] })
    },
    onError: (err: Error) => setCsvError(err.message),
  })

  const handleCsvFile = (file: File) => {
    setCsvError(null)
    const reader = new FileReader()
    reader.onload = () => {
      const text = typeof reader.result === 'string' ? reader.result : ''
      uploadCsvMutation.mutate(text)
    }
    reader.onerror = () => setCsvError('Could not read the selected file')
    reader.readAsText(file)
  }

  const columns: ColumnDef<HoldingRow, unknown>[] = [
    { accessorKey: 'ticker', header: 'Ticker', cell: (i) => <TickerLink ticker={i.getValue<string>()} /> },
    { accessorKey: 'qty', header: 'Qty' },
    { accessorKey: 'purchase_date', header: 'Buy Date' },
    { accessorKey: 'purchase_price', header: 'Buy Price', cell: (i) => fmtMoney(i.getValue<number | null>()) },
    { accessorKey: 'sale_date', header: 'Sale Date', cell: (i) => i.getValue<string | null>() ?? '—' },
    { accessorKey: 'sell_price', header: 'Sell Price', cell: (i) => fmtMoney(i.getValue<number | null>()) },
    {
      id: 'direction',
      header: 'Direction (signal_5d)',
      cell: ({ row }) => {
        const sig = signals.data?.[row.original.ticker]
        return <Badge variant={sig?.signal_direction === 'sell' ? 'destructive' : sig?.signal_direction === 'buy' ? 'success' : 'outline'}>{sig?.signal_direction ?? 'no signal'}</Badge>
      },
    },
    {
      id: 'buy_prob',
      header: () => (
        <span className="inline-flex items-center gap-1">
          Buy Prob
          <InfoTooltip>signal_5d's own probability that its call is "buy" (0-1). The only model AlphaLens actually trades paper positions off of.</InfoTooltip>
        </span>
      ),
      cell: ({ row }) => fmtPct(signals.data?.[row.original.ticker]?.buy_prob),
    },
    {
      id: 'exit_urgency',
      header: () => (
        <span className="inline-flex items-center gap-1">
          Exit Urgency
          <InfoTooltip>rule_based_exit_policy's 0-100 urgency score for exiting an existing position.</InfoTooltip>
        </span>
      ),
      cell: ({ row }) => signals.data?.[row.original.ticker]?.exit_urgency?.toFixed(0) ?? '—',
    },
    {
      id: 'exit_type',
      header: 'Exit Type',
      cell: ({ row }) => <Badge variant="outline">{signals.data?.[row.original.ticker]?.exit_type ?? '—'}</Badge>,
    },
    {
      id: 'pnd_score',
      header: 'P&D Score',
      cell: ({ row }) => signals.data?.[row.original.ticker]?.pnd_score?.toFixed(0) ?? '—',
    },
    {
      id: 'remove',
      header: 'Remove',
      cell: ({ row }) => (
        <Button variant="destructive" size="sm" onClick={() => deleteMutation.mutate(row.original.id)}>
          x
        </Button>
      ),
    },
  ]

  return (
    <AppShell title="ML — My Holdings" description="Your real holdings monitored against daily ML signals — read-only observability, never fed into model training or backtests.">
      <Card>
        <CardHeader>
          <CardTitle>Add a holding</CardTitle>
        </CardHeader>
        <CardContent>
          <div className="flex flex-wrap items-center gap-2">
            <input
              className="h-9 w-32 rounded-[var(--radius-token)] border border-border bg-transparent px-3 text-sm"
              placeholder="Ticker"
              value={addTicker}
              onChange={(e) => setAddTicker(e.target.value.toUpperCase())}
            />
            <input
              className="h-9 w-36 rounded-[var(--radius-token)] border border-border bg-transparent px-3 text-sm"
              type="date"
              value={addDate}
              onChange={(e) => setAddDate(e.target.value)}
            />
            <input
              className="h-9 w-24 rounded-[var(--radius-token)] border border-border bg-transparent px-3 text-sm"
              placeholder="Qty"
              value={addQty}
              onChange={(e) => setAddQty(e.target.value)}
            />
            <input
              className="h-9 w-28 rounded-[var(--radius-token)] border border-border bg-transparent px-3 text-sm"
              placeholder="Price"
              value={addPrice}
              onChange={(e) => setAddPrice(e.target.value)}
            />
            <input
              className="h-9 w-48 rounded-[var(--radius-token)] border border-border bg-transparent px-3 text-sm"
              placeholder="Rationale (optional)"
              value={addRationale}
              onChange={(e) => setAddRationale(e.target.value)}
            />
            <Button disabled={!addTicker || !addQty} onClick={() => addMutation.mutate()}>
              Add
            </Button>
          </div>
        </CardContent>
      </Card>

      <div className="mt-4">
        <Card>
          <CardHeader>
            <CardTitle>Bulk import (CSV)</CardTitle>
          </CardHeader>
          <CardContent>
            <div className="flex flex-wrap items-center gap-2">
              <input
                ref={fileInputRef}
                type="file"
                accept=".csv,text/csv"
                className="text-sm"
                onChange={(e) => {
                  const file = e.target.files?.[0]
                  if (file) handleCsvFile(file)
                }}
              />
              {uploadCsvMutation.isPending && <span className="text-sm text-muted-foreground">Uploading…</span>}
            </div>
            <p className="mt-2 text-xs text-muted-foreground">
              Required columns: ticker, purchase_date, qty. Optional: purchase_price, sale_date, sell_price, purchase_rationale, sell_rationale, journal_entry.
            </p>
            {csvError && <p className="mt-2 text-sm text-red">{csvError}</p>}
            {uploadCsvMutation.isSuccess && !csvError && (
              <p className="mt-2 text-sm text-green">Imported {uploadCsvMutation.data?.length ?? 0} holding(s).</p>
            )}
          </CardContent>
        </Card>
      </div>

      <div className="mt-4">
        <Card>
          <CardHeader>
            <CardTitle>Holdings</CardTitle>
          </CardHeader>
          <CardContent>
            {holdings.error ? (
              <p className="text-sm text-red">
                Could not reach GET /api/v1/holdings/ — {(holdings.error as Error).message}
              </p>
            ) : (
              <DataTable columns={columns} data={tickers} isLoading={holdings.isLoading} emptyMessage="Add a holding above to see your positions' signals." />
            )}
          </CardContent>
        </Card>
      </div>
    </AppShell>
  )
}
