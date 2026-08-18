// Momentum rank-band experimentation sweep — scripts/run_momentum_experimentation.py's
// output (bands 1-50 through 501-800 x lookback x rebalance x top_n),
// surfaced as a browsable table instead of a raw JSON report file on disk
// (2026-07-27 user request).
import { useMemo, useState } from 'react'
import { useQuery, useQueryClient } from '@tanstack/react-query'
import type { ColumnDef } from '@tanstack/react-table'

import { AppShell, Card, CardContent, CardDescription, CardHeader, CardTitle, DataTable } from '@/lib/ui'
import { apiGet } from '@/shared/api/client'
import type { MomentumExperimentationReport, MomentumExperimentationVariant } from './types'
import { SweepTriggerButton } from './SweepTriggerButton'

function fmtPct(v: number | null | undefined) {
  return typeof v === 'number' ? `${(v * 100).toFixed(1)}%` : '—'
}
function fmtNum(v: number | null | undefined, digits = 1) {
  return typeof v === 'number' ? v.toFixed(digits) : '—'
}

function bandLabel(rankStart: number, rankEnd: number) {
  return `${rankStart}-${rankEnd}`
}

/** variant key -> true iff this row has the best CAGR among all rows sharing its band. */
function bestCagrByBand(rows: MomentumExperimentationVariant[]): Set<MomentumExperimentationVariant> {
  const bestByBand = new Map<number, { row: MomentumExperimentationVariant; cagr: number }>()
  for (const r of rows) {
    if (typeof r.cagr !== 'number') continue
    const current = bestByBand.get(r.band_id)
    if (!current || r.cagr > current.cagr) {
      bestByBand.set(r.band_id, { row: r, cagr: r.cagr })
    }
  }
  return new Set(Array.from(bestByBand.values()).map((v) => v.row))
}

export function MomentumExperimentationPage() {
  const [bandId, setBandId] = useState<string>('')
  const [rebalancePeriod, setRebalancePeriod] = useState<string>('')
  const [topN, setTopN] = useState<string>('')
  const queryClient = useQueryClient()

  const report = useQuery({
    queryKey: ['momentum-experimentation'],
    queryFn: () => apiGet<MomentumExperimentationReport>('/api/v1/momentum/experimentation'),
  })

  const allRows = report.data?.variants ?? []
  const bestRows = useMemo(() => bestCagrByBand(allRows), [allRows])

  const bandOptions = useMemo(
    () =>
      Array.from(new Set(allRows.map((r) => `${r.band_id}:${bandLabel(r.rank_start, r.rank_end)}`)))
        .sort((a, b) => Number(a.split(':')[0]) - Number(b.split(':')[0])),
    [allRows],
  )
  const rebalanceOptions = useMemo(
    () => Array.from(new Set(allRows.map((r) => r.rebalance_period))).sort(),
    [allRows],
  )
  const topNOptions = useMemo(
    () => Array.from(new Set(allRows.map((r) => r.top_n))).sort((a, b) => a - b),
    [allRows],
  )

  const rows = useMemo(
    () =>
      allRows.filter((r) => {
        if (bandId && String(r.band_id) !== bandId) return false
        if (rebalancePeriod && r.rebalance_period !== rebalancePeriod) return false
        if (topN && String(r.top_n) !== topN) return false
        return true
      }),
    [allRows, bandId, rebalancePeriod, topN],
  )

  const columns = useMemo<ColumnDef<MomentumExperimentationVariant, unknown>[]>(
    () => [
      {
        id: 'band',
        header: 'Universe (rank)',
        cell: (i) => {
          const row = i.row.original
          const isBest = bestRows.has(row)
          return (
            <span className={isBest ? 'font-semibold' : undefined}>
              {bandLabel(row.rank_start, row.rank_end)}
              {isBest ? <span className="ml-1.5 text-xs text-green-600">Best CAGR</span> : null}
            </span>
          )
        },
      },
      { accessorKey: 'top_n', header: 'Top N', meta: { align: 'right' } },
      {
        accessorKey: 'lookback_months',
        header: 'Lookback',
        meta: { align: 'right' },
        cell: (i) => `${i.getValue<number>()}mo`,
      },
      { accessorKey: 'rebalance_period', header: 'Rebalance' },
      {
        accessorKey: 'cagr',
        header: 'CAGR',
        meta: { align: 'right' },
        cell: (i) => fmtPct(i.getValue<number | null>()),
      },
      {
        accessorKey: 'sharpe',
        header: 'Sharpe',
        meta: { align: 'right', priority: 'medium' },
        cell: (i) => fmtNum(i.getValue<number | null>(), 2),
      },
      {
        accessorKey: 'sortino',
        header: 'Sortino',
        meta: { align: 'right', priority: 'medium' },
        cell: (i) => fmtNum(i.getValue<number | null>(), 2),
      },
      {
        accessorKey: 'calmar',
        header: 'Calmar',
        meta: { align: 'right', priority: 'medium' },
        cell: (i) => fmtNum(i.getValue<number | null>(), 2),
      },
      {
        accessorKey: 'post_tax_cagr',
        header: 'Post-Tax CAGR',
        meta: { align: 'right' },
        cell: (i) => fmtPct(i.getValue<number | null>()),
      },
      {
        accessorKey: 'sip_xirr',
        header: 'SIP XIRR',
        meta: { align: 'right', priority: 'medium' },
        cell: (i) => fmtPct(i.getValue<number | null>()),
      },
      {
        accessorKey: 'win_rate',
        header: 'Win Rate',
        meta: { align: 'right' },
        cell: (i) => fmtPct(i.getValue<number | null>()),
      },
      {
        accessorKey: 'churn_avg_transactions_per_year',
        header: 'Churn/yr',
        meta: { align: 'right', priority: 'low' },
        cell: (i) => fmtNum(i.getValue<number | null>()),
      },
      {
        accessorKey: 'n_closed_trades',
        header: 'Closed Trades',
        meta: { align: 'right', priority: 'low' },
        cell: (i) => i.getValue<number | null>() ?? '—',
      },
      {
        accessorKey: 'avg_days_held',
        header: 'Avg Days Held',
        meta: { align: 'right', priority: 'low' },
        cell: (i) => fmtNum(i.getValue<number | null>(), 0),
      },
    ],
    [bestRows],
  )

  return (
    <AppShell
      title="Momentum — Experimentation"
      description="Rank-band universe sweep (1-50 through 501-800) x lookback x rebalance frequency x portfolio size — scripts/run_momentum_experimentation.py."
    >
      <div className="mb-4 rounded-[var(--radius-token)] border border-border bg-accent-soft px-3 py-2 text-xs text-muted-foreground">
        <strong className="text-foreground">Not the same as Technical Analysis's "Momentum" style.</strong>{' '}
        This is the rank/momentum <em>factor strategy</em>: the top 800 by ADTV, split into market-cap
        bands, holding the top N by momentum within a band and swapping the list at each rebalance. Technical Analysis's screener templates separately use "Momentum" as a
        style label for ~16 MACD/breakout/technical-pattern templates (A2, C1&ndash;C4, D4, E5/E6, F2/F8,
        S008, …) — those run through the Technical channel's own orchestrator, unrelated to this page.
      </div>

      <Card>
        <CardHeader>
          <CardTitle>Universe / Lookback Sweep</CardTitle>
          <CardDescription>
            {report.isLoading
              ? 'Loading…'
              : report.error
                ? 'Failed to load'
                : `${rows.length} of ${allRows.length} variant${allRows.length === 1 ? '' : 's'}${
                    report.data?.generated_at ? ` — generated ${new Date(report.data.generated_at).toLocaleString()}` : ''
                  }`}
          </CardDescription>
          <div className="mt-2 flex flex-wrap gap-3">
            <SweepTriggerButton
              label="Universe Sweep"
              triggerUrl="/api/v1/momentum/experimentation/trigger"
              statusUrlPrefix="/api/v1/momentum/experimentation/trigger/status"
              onCompleted={() => queryClient.invalidateQueries({ queryKey: ['momentum-experimentation'] })}
            />
            <SweepTriggerButton
              label="Filter Overlays"
              triggerUrl="/api/v1/momentum/filter_overlays/trigger"
              statusUrlPrefix="/api/v1/momentum/filter_overlays/trigger/status"
              onCompleted={() => queryClient.invalidateQueries({ queryKey: ['momentum-experimentation'] })}
            />
          </div>
          <p className="mt-1 text-xs text-muted-foreground">
            Filter Overlays writes its own report (7 risk/realism filters vs. this baseline) — not shown
            in the table below; see <code>backtest/reports/momentum/momentum_filter_overlays_*.json</code>
            or the published artifact for results.
          </p>
        </CardHeader>
        <CardContent>
          <div className="mb-4 flex flex-wrap items-end gap-3">
            <select
              className="h-9 rounded-[var(--radius-token)] border border-border bg-transparent px-3 text-sm"
              value={bandId}
              onChange={(e) => setBandId(e.target.value)}
            >
              <option value="">All universes</option>
              {bandOptions.map((opt) => {
                const [id, label] = opt.split(':')
                return (
                  <option key={opt} value={id}>
                    {label}
                  </option>
                )
              })}
            </select>
            <select
              className="h-9 rounded-[var(--radius-token)] border border-border bg-transparent px-3 text-sm"
              value={rebalancePeriod}
              onChange={(e) => setRebalancePeriod(e.target.value)}
            >
              <option value="">All rebalance periods</option>
              {rebalanceOptions.map((r) => (
                <option key={r} value={r}>
                  {r}
                </option>
              ))}
            </select>
            <select
              className="h-9 rounded-[var(--radius-token)] border border-border bg-transparent px-3 text-sm"
              value={topN}
              onChange={(e) => setTopN(e.target.value)}
            >
              <option value="">All portfolio sizes</option>
              {topNOptions.map((n) => (
                <option key={n} value={n}>
                  Top {n}
                </option>
              ))}
            </select>
          </div>

          {report.error ? (
            <p className="text-sm text-red">
              Could not reach GET /api/v1/momentum/experimentation — {(report.error as Error).message}
            </p>
          ) : (
            <DataTable
              columns={columns}
              data={rows}
              isLoading={report.isLoading}
              emptyMessage="No experimentation report yet — the sweep hasn't finished running."
            />
          )}
        </CardContent>
      </Card>
    </AppShell>
  )
}
