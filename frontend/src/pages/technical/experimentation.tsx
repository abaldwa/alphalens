// Technical Analysis backtest sweep — scripts/run_technical_experimentation.py's
// output (42 templates x 7 exit-policy variants x 4 max_hold_days x 3 top_n),
// surfaced as a browsable table. Structural copy of
// frontend/src/pages/momentum/experimentation.tsx, adapted for Technical's
// own sweep axes (template/exit_variant/max_hold_days instead of
// band/lookback/rebalance) — 2026-08-01 Momentum-parity backtest reporting.
import { useMemo, useState } from 'react'
import { useQuery, useQueryClient } from '@tanstack/react-query'
import type { ColumnDef } from '@tanstack/react-table'

import { AppShell, Card, CardContent, CardDescription, CardHeader, CardTitle, DataTable } from '@/lib/ui'
import { apiGet } from '@/shared/api/client'
import type { TAExperimentationReport, TABacktestVariant } from './types'
import { SweepTriggerButton } from './SweepTriggerButton'

function fmtPct(v: number | null | undefined) {
  return typeof v === 'number' ? `${(v * 100).toFixed(1)}%` : '—'
}
function fmtNum(v: number | null | undefined, digits = 1) {
  return typeof v === 'number' ? v.toFixed(digits) : '—'
}

/** variant -> true iff this row has the best CAGR among all rows sharing its template. */
function bestCagrByTemplate(rows: TABacktestVariant[]): Set<TABacktestVariant> {
  const bestByTemplate = new Map<string, { row: TABacktestVariant; cagr: number }>()
  for (const r of rows) {
    if (typeof r.cagr !== 'number') continue
    const key = r.template_name ?? ''
    const current = bestByTemplate.get(key)
    if (!current || r.cagr > current.cagr) {
      bestByTemplate.set(key, { row: r, cagr: r.cagr })
    }
  }
  return new Set(Array.from(bestByTemplate.values()).map((v) => v.row))
}

export function TechnicalExperimentationPage() {
  const [templateFilter, setTemplateFilter] = useState<string>('')
  const [exitVariantFilter, setExitVariantFilter] = useState<string>('')
  const [topNFilter, setTopNFilter] = useState<string>('')
  const queryClient = useQueryClient()

  const report = useQuery({
    queryKey: ['technical-experimentation'],
    queryFn: () => apiGet<TAExperimentationReport>('/api/v1/technical_backtest/experimentation'),
  })

  const allRows = report.data?.variants ?? []
  const bestRows = useMemo(() => bestCagrByTemplate(allRows), [allRows])

  const templateOptions = useMemo(
    () => Array.from(new Set(allRows.map((r) => r.template_name).filter((v): v is string => !!v))).sort(),
    [allRows],
  )
  const exitVariantOptions = useMemo(
    () => Array.from(new Set(allRows.map((r) => r.exit_variant).filter((v): v is string => !!v))).sort(),
    [allRows],
  )
  const topNOptions = useMemo(
    () => Array.from(new Set(allRows.map((r) => r.top_n))).sort((a, b) => a - b),
    [allRows],
  )

  const rows = useMemo(
    () =>
      allRows.filter((r) => {
        if (templateFilter && r.template_name !== templateFilter) return false
        if (exitVariantFilter && r.exit_variant !== exitVariantFilter) return false
        if (topNFilter && String(r.top_n) !== topNFilter) return false
        return true
      }),
    [allRows, templateFilter, exitVariantFilter, topNFilter],
  )

  const columns = useMemo<ColumnDef<TABacktestVariant, unknown>[]>(
    () => [
      {
        id: 'template',
        header: 'Template',
        cell: (i) => {
          const row = i.row.original
          const isBest = bestRows.has(row)
          return (
            <span className={isBest ? 'font-semibold' : undefined}>
              {row.template_name}
              {isBest ? <span className="ml-1.5 text-xs text-green-600">Best CAGR</span> : null}
            </span>
          )
        },
      },
      { accessorKey: 'exit_variant', header: 'Exit Variant' },
      {
        accessorKey: 'max_hold_days',
        header: 'Max Hold Days',
        meta: { align: 'right' },
        cell: (i) => i.getValue<number | null>() ?? '—',
      },
      { accessorKey: 'top_n', header: 'Top N', meta: { align: 'right' } },
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
        accessorKey: 'win_rate',
        header: 'Win Rate',
        meta: { align: 'right' },
        cell: (i) => fmtPct(i.getValue<number | null>()),
      },
      {
        accessorKey: 'total_trades',
        header: 'Total Trades',
        meta: { align: 'right' },
        cell: (i) => i.getValue<number | null>() ?? '—',
      },
      {
        accessorKey: 'avg_trade_duration_days',
        header: 'Avg Duration (d)',
        meta: { align: 'right', priority: 'medium' },
        cell: (i) => fmtNum(i.getValue<number | null>(), 0),
      },
      {
        accessorKey: 'n_outlier_trades',
        header: 'Outlier Trades',
        meta: { align: 'right', priority: 'low' },
        cell: (i) => {
          const v = i.getValue<number | null>()
          return v && v > 0 ? <span className="text-amber-600">{v}</span> : (v ?? '—')
        },
      },
      {
        accessorKey: 'max_abs_return_zscore',
        header: 'Max |Z-score|',
        meta: { align: 'right', priority: 'low' },
        cell: (i) => fmtNum(i.getValue<number | null>(), 2),
      },
    ],
    [bestRows],
  )

  return (
    <AppShell
      title="Technical — Backtest Sweep"
      description="Template x exit-policy-variant x max-hold-days x top-N sweep — scripts/run_technical_experimentation.py."
    >
      <div className="mb-4 rounded-[var(--radius-token)] border border-border bg-accent-soft px-3 py-2 text-xs text-muted-foreground">
        <strong className="text-foreground">Momentum-parity reporting.</strong>{' '}
        Same Sharpe/Sortino/Calmar/total-trades/avg-duration/outlier-z-score metrics as the Momentum
        Universe Sweep, applied to the 42 Technical screener templates via the shared BacktestOrchestrator
        (backtest/core/engine.py) instead of a bespoke engine.
      </div>

      <Card>
        <CardHeader>
          <CardTitle>Template / Exit-Variant / Hold-Days Sweep</CardTitle>
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
              label="Backtest Sweep"
              triggerUrl="/api/v1/technical_backtest/experimentation/trigger"
              statusUrlPrefix="/api/v1/technical_backtest/experimentation/trigger/status"
              onCompleted={() => queryClient.invalidateQueries({ queryKey: ['technical-experimentation'] })}
            />
            <SweepTriggerButton
              label="Filter Overlays"
              triggerUrl="/api/v1/technical_backtest/filter_overlays/trigger"
              statusUrlPrefix="/api/v1/technical_backtest/filter_overlays/trigger/status"
              onCompleted={() => queryClient.invalidateQueries({ queryKey: ['technical-experimentation'] })}
            />
          </div>
          <p className="mt-1 text-xs text-muted-foreground">
            Filter Overlays writes its own report (5 entry-side filters vs. this baseline) — not shown in
            the table below; see <code>backtest/reports/technical/technical_filter_overlays_*.json</code>.
          </p>
        </CardHeader>
        <CardContent>
          <div className="mb-4 flex flex-wrap items-end gap-3">
            <select
              className="h-9 rounded-[var(--radius-token)] border border-border bg-transparent px-3 text-sm"
              value={templateFilter}
              onChange={(e) => setTemplateFilter(e.target.value)}
            >
              <option value="">All templates</option>
              {templateOptions.map((t) => (
                <option key={t} value={t}>
                  {t}
                </option>
              ))}
            </select>
            <select
              className="h-9 rounded-[var(--radius-token)] border border-border bg-transparent px-3 text-sm"
              value={exitVariantFilter}
              onChange={(e) => setExitVariantFilter(e.target.value)}
            >
              <option value="">All exit variants</option>
              {exitVariantOptions.map((v) => (
                <option key={v} value={v}>
                  {v}
                </option>
              ))}
            </select>
            <select
              className="h-9 rounded-[var(--radius-token)] border border-border bg-transparent px-3 text-sm"
              value={topNFilter}
              onChange={(e) => setTopNFilter(e.target.value)}
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
              Could not reach GET /api/v1/technical_backtest/experimentation — {(report.error as Error).message}
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
