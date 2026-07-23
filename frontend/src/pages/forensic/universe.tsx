import { useState } from 'react'
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query'
import type { ColumnDef } from '@tanstack/react-table'

import { AppShell, Badge, Button, Card, CardContent, CardHeader, CardTitle, DataTable, InfoTooltip, StatCard, tickerColumn } from '@/lib/ui'
import { apiGet, apiPost } from '@/shared/api/client'
import type { ForensicFlaggedResponse, ForensicFlaggedRow, ForensicSummaryResponse } from './types'
import { flagBadgeVariant } from './types'

interface ScanRunResult {
  scanned: number
  succeeded: number
  failed: number
}

const columns: ColumnDef<ForensicFlaggedRow & { rank: number }, unknown>[] = [
  { accessorKey: 'rank', header: '#', meta: { align: 'right' } },
  tickerColumn<ForensicFlaggedRow & { rank: number }>(),
  {
    accessorKey: 'forensic_composite',
    header: () => (
      <span className="inline-flex items-center gap-1">
        Score
        <InfoTooltip>
          Forensic Composite — a 0-100 blended score across the classical forensic scores (Beneish, Altman,
          Piotroski, Ohlson, Dechow, Sloan accrual, Benford). The flag is true above a fixed block threshold.
        </InfoTooltip>
      </span>
    ),
    meta: { align: 'right' },
    cell: (i) => (i.getValue<number | null>() ?? 0).toFixed(0),
  },
  {
    accessorKey: 'forensic_flag_label',
    header: () => (
      <span className="inline-flex items-center gap-1">
        Flag
        <InfoTooltip>
          The 5-level green/yellow/orange/red/black risk taxonomy derived from the forensic composite score.
        </InfoTooltip>
      </span>
    ),
    cell: (i) => {
      const v = i.getValue<string | null>()
      return <Badge variant={flagBadgeVariant(v)}>{v ?? '—'}</Badge>
    },
  },
]

export function UniversePage() {
  const [limitInput, setLimitInput] = useState(300)
  const queryClient = useQueryClient()

  const summary = useQuery({
    queryKey: ['forensic-universe-summary'],
    queryFn: () => apiGet<ForensicSummaryResponse>('/api/v1/signals/ml/forensic/summary'),
  })

  const flagged = useQuery({
    queryKey: ['forensic-universe-flagged'],
    queryFn: () => apiGet<ForensicFlaggedResponse>('/api/v1/signals/ml/forensic/flagged', { flag: 'red,amber,green' }),
  })

  const scan = useMutation({
    mutationFn: (limit: number) => apiPost<ScanRunResult>(`/api/v1/signals/ml/forensic/scan/run?limit=${limit}`, {}),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['forensic-universe-summary'] })
      queryClient.invalidateQueries({ queryKey: ['forensic-universe-flagged'] })
    },
  })

  const sortedRows = [...(flagged.data?.rows ?? [])]
    .sort((a, b) => (b.forensic_composite ?? 0) - (a.forensic_composite ?? 0))
    .map((r, i) => ({ ...r, rank: i + 1 }))

  return (
    <AppShell
      title="Forensic — Universe"
      description="Universe-wide forensic scoring — a point-in-time snapshot, not a time series."
      actions={
        <div className="flex items-center gap-2">
          <input
            type="number"
            min={1}
            max={2500}
            className="h-9 w-28 rounded-[var(--radius-token)] border border-border bg-transparent px-3 text-sm"
            value={limitInput}
            onChange={(e) => setLimitInput(Number(e.target.value))}
          />
          <Button onClick={() => scan.mutate(limitInput || 300)} disabled={scan.isPending}>
            {scan.isPending ? 'Scanning…' : 'Run Scan'}
          </Button>
        </div>
      }
    >
      {scan.isPending && (
        <p className="mb-2 text-sm text-muted-foreground">
          Scanning up to {limitInput} tickers... (this can take a few minutes for the full universe)
        </p>
      )}
      {scan.isSuccess && (
        <p className="mb-2 text-sm text-muted-foreground">
          Scan done: {scan.data.succeeded}/{scan.data.scanned} succeeded{scan.data.failed ? `, ${scan.data.failed} failed` : ''}.
        </p>
      )}
      {scan.isError && <p className="mb-2 text-sm text-red">Scan failed: {(scan.error as Error).message}</p>}

      {summary.error ? (
        <p className="text-sm text-red">
          Could not reach GET /api/v1/signals/ml/forensic/summary — {(summary.error as Error).message}
        </p>
      ) : summary.data && !summary.data.available ? (
        <p className="text-sm text-muted-foreground">No forensic scores yet</p>
      ) : (
        <div className="grid grid-cols-1 gap-4 sm:grid-cols-4">
          <StatCard label="Green" value={summary.data?.green_count ?? '—'} tone="green" />
          <StatCard label="Amber" value={summary.data?.amber_count ?? '—'} tone="amber" />
          <StatCard label="Red" value={summary.data?.red_count ?? '—'} tone="red" />
          <StatCard label="Total Scored" value={summary.data?.total_scored ?? '—'} />
        </div>
      )}

      <div className="mt-4">
        <Card>
          <CardHeader>
            <CardTitle>Universe Scan Results</CardTitle>
          </CardHeader>
          <CardContent>
            {flagged.error ? (
              <p className="text-sm text-red">
                Could not reach GET /api/v1/signals/ml/forensic/flagged — {(flagged.error as Error).message}
              </p>
            ) : (
              <DataTable
                columns={columns}
                data={sortedRows}
                isLoading={flagged.isLoading}
                emptyMessage="No forensic-scored tickers"
              />
            )}
          </CardContent>
        </Card>
      </div>
    </AppShell>
  )
}
