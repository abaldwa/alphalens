import { useQuery } from '@tanstack/react-query'
import type { ColumnDef } from '@tanstack/react-table'

import { AppShell, Badge, Card, CardContent, CardDescription, CardHeader, CardTitle, DataTable, InfoTooltip } from '@/lib/ui'
import { apiGet } from '@/shared/api/client'
import type { SignalUniverseRow } from './types'

function todayStr() {
  return new Date().toISOString().slice(0, 10)
}
function fmtPct(v: number | null | undefined) {
  return v == null ? '—' : `${(v * 100).toFixed(1)}%`
}
function pnlTone(v: number | null | undefined) {
  if (v == null) return undefined
  return v >= 0 ? 'text-green' : 'text-red'
}
function forensicVariant(flag: string | null) {
  if (flag === 'green') return 'success' as const
  if (flag === 'red' || flag === 'black') return 'destructive' as const
  if (flag === 'amber') return 'warning' as const
  return 'outline' as const
}

function shapBasisText(shapJson: string | null | undefined): string {
  if (!shapJson) return '—'
  try {
    const parsed = JSON.parse(shapJson)
    const entries = Array.isArray(parsed) ? parsed : Object.entries(parsed).map(([k, v]) => ({ feature: k, value: v }))
    const top = entries
      .map((e: { feature?: string; value?: number; 0?: string; 1?: number }) => ({ feature: e.feature ?? String(e[0] ?? '—'), value: Number(e.value ?? e[1] ?? 0) }))
      .sort((a, b) => Math.abs(b.value) - Math.abs(a.value))
      .slice(0, 2)
    if (!top.length) return '—'
    return top.map((t) => `${t.feature} (${t.value >= 0 ? '+' : ''}${t.value.toFixed(2)})`).join(', ')
  } catch {
    return '—'
  }
}

const columns: ColumnDef<SignalUniverseRow, unknown>[] = [
  {
    accessorKey: 'ticker',
    header: 'Ticker',
    cell: (i) => (
      <a className="text-teal underline-offset-2 hover:underline" href={`/ml-signal.html?ticker=${i.getValue<string>()}`} target="_blank" rel="noopener noreferrer">
        {i.getValue<string>()}
      </a>
    ),
  },
  {
    accessorKey: 'buy_prob',
    header: () => (
      <span className="inline-flex items-center gap-1">
        Buy Prob
        <InfoTooltip>signal_5d's own probability that its call is "buy" (0-1). The only model AlphaLens actually trades paper positions off of.</InfoTooltip>
      </span>
    ),
    cell: (i) => fmtPct(i.getValue<number | null>()),
  },
  {
    accessorKey: 'q50_return',
    header: () => (
      <span className="inline-flex items-center gap-1">
        Q50 Return
        <InfoTooltip>signal_5d's median (50th percentile) forecast forward return over its holding horizon, from its quantile-regression head.</InfoTooltip>
      </span>
    ),
    cell: (i) => <span className={pnlTone(i.getValue<number | null>())}>{fmtPct(i.getValue<number | null>())}</span>,
  },
  {
    accessorKey: 'meta_label_prob',
    header: () => (
      <span className="inline-flex items-center gap-1">
        Meta Label Prob
        <InfoTooltip>meta_labeler's estimate of whether signal_5d's call is worth acting on at all (a secondary filter, not a return forecast).</InfoTooltip>
      </span>
    ),
    cell: (i) => fmtPct(i.getValue<number | null>()),
  },
  {
    accessorKey: 'pnd_score',
    header: () => (
      <span className="inline-flex items-center gap-1">
        P&amp;D Score
        <InfoTooltip>pnd_detector's 0-100 pump-and-dump risk score from volume/price anomaly features.</InfoTooltip>
      </span>
    ),
    cell: (i) => i.getValue<number | null>()?.toFixed(0) ?? '—',
  },
  {
    accessorKey: 'forensic_flag',
    header: () => (
      <span className="inline-flex items-center gap-1">
        Forensic
        <InfoTooltip>forensic_ml's 5-level taxonomy (green/yellow/orange/red/black), carried forward from ml_forensic's most recent weekly scoring run — can be several days stale.</InfoTooltip>
      </span>
    ),
    cell: (i) => <Badge variant={forensicVariant(i.getValue<string | null>())}>{i.getValue<string | null>() ?? '—'}</Badge>,
  },
  {
    accessorKey: 'mb_probability',
    header: () => (
      <span className="inline-flex items-center gap-1">
        MB Probability
        <InfoTooltip>The MultibaggerModel's probability estimate, carried forward from ml_multibagger's most recent (typically weekly) run. Not a return multiplier prediction.</InfoTooltip>
      </span>
    ),
    cell: (i) => fmtPct(i.getValue<number | null>()),
  },
  {
    id: 'basis',
    header: () => (
      <span className="inline-flex items-center gap-1">
        Basis
        <InfoTooltip>A short 2-feature summary of signal_5d's top-5 SHAP feature attributions — the two largest-magnitude contributors by absolute SHAP value.</InfoTooltip>
      </span>
    ),
    cell: ({ row }) => shapBasisText(row.original.shap_top5_json),
  },
]

export function MlUniversePage() {
  const universe = useQuery({
    queryKey: ['ml-universe', todayStr()],
    queryFn: () => apiGet<SignalUniverseRow[]>(`/api/v1/signals/ml/universe/${todayStr()}`, { carry_forward: true }),
  })

  return (
    <AppShell title="ML — Universe" description="Full cross-model snapshot for every scored ticker as of the latest date — signal_5d, meta-labeler, P&D, forensic, and multibagger, joined into one row.">
      <Card>
        <CardHeader>
          <CardTitle>Universe</CardTitle>
          <CardDescription>Double-click a row to open Signal Deep Dive in a new tab.</CardDescription>
        </CardHeader>
        <CardContent>
          {universe.error ? (
            <p className="text-sm text-red">
              Could not reach GET /api/v1/signals/ml/universe/{'{date}'} — {(universe.error as Error).message}
            </p>
          ) : (
            <DataTable columns={columns} data={universe.data ?? []} isLoading={universe.isLoading} emptyMessage="No scored universe found for the latest date." />
          )}
        </CardContent>
      </Card>
    </AppShell>
  )
}
