// Technical Analysis recommended composite/combo strategies —
// scripts/run_technical_recommended_strategies.py's output: Balanced/
// Risk-Managed/Max-Defensive entry-filter tiers across every template,
// plus curated cross-style combo strategies (TechnicalComboAdapter), plus
// a per-variant Signal Failures breakdown (losing trades with their entry
// signal snapshot — 2026-08-01 "test strategies when the actual signal
// which triggered a buy failed" request). Structural copy of
// frontend/src/pages/momentum/recommended-strategies.tsx.
import { useMemo, useState } from 'react'
import { useQuery, useQueryClient } from '@tanstack/react-query'
import type { ColumnDef } from '@tanstack/react-table'

import { AppShell, Card, CardContent, CardDescription, CardHeader, CardTitle, DataTable } from '@/lib/ui'
import { apiGet } from '@/shared/api/client'
import type { TABacktestVariant, TARecommendedStrategiesReport } from './types'
import { SweepTriggerButton } from './SweepTriggerButton'

function fmtPct(v: number | null | undefined) {
  return typeof v === 'number' ? `${(v * 100).toFixed(1)}%` : '—'
}
function fmtNum(v: number | null | undefined, digits = 1) {
  return typeof v === 'number' ? v.toFixed(digits) : '—'
}

export function TechnicalRecommendedStrategiesPage() {
  const [strategyFilter, setStrategyFilter] = useState<string>('')
  const [kindFilter, setKindFilter] = useState<string>('')
  const [selectedVariant, setSelectedVariant] = useState<TABacktestVariant | null>(null)
  const queryClient = useQueryClient()

  const report = useQuery({
    queryKey: ['technical-recommended-strategies'],
    queryFn: () => apiGet<TARecommendedStrategiesReport>('/api/v1/technical_backtest/recommended_strategies'),
  })

  const allRows = report.data?.variants ?? []

  const strategyOptions = useMemo(
    () => Array.from(new Set(allRows.map((r) => r.strategy).filter((v): v is string => !!v))).sort(),
    [allRows],
  )

  const rows = useMemo(
    () =>
      allRows.filter((r) => {
        if (strategyFilter && r.strategy !== strategyFilter) return false
        if (kindFilter && r.variant_kind !== kindFilter) return false
        return true
      }),
    [allRows, strategyFilter, kindFilter],
  )

  const columns = useMemo<ColumnDef<TABacktestVariant, unknown>[]>(
    () => [
      { accessorKey: 'strategy', header: 'Strategy' },
      {
        id: 'kind',
        header: 'Kind',
        cell: (i) => (i.row.original.variant_kind === 'combo' ? <span className="text-blue-600">Combo</span> : 'Single'),
      },
      { accessorKey: 'template', header: 'Template(s)' },
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
        meta: { align: 'right' },
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
        id: 'signal_failures',
        header: 'Signal Failures',
        meta: { align: 'right' },
        cell: (i) => {
          const sf = i.row.original.signal_failures
          if (!sf || sf.n_losing_trades === 0) return '—'
          return (
            <button
              type="button"
              className="text-red underline decoration-dotted"
              onClick={() => setSelectedVariant(i.row.original)}
            >
              {sf.n_losing_trades} losing
            </button>
          )
        },
      },
    ],
    [],
  )

  const selectedFailures = selectedVariant?.signal_failures

  return (
    <AppShell
      title="Technical — Recommended Strategies"
      description="Composite entry-filter strategies (Balanced/Risk-Managed/Max-Defensive) and cross-style combo strategies — scripts/run_technical_recommended_strategies.py."
    >
      <Card>
        <CardHeader>
          <CardTitle>Recommended Strategies</CardTitle>
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
              label="Recommended Strategies"
              triggerUrl="/api/v1/technical_backtest/recommended_strategies/trigger"
              statusUrlPrefix="/api/v1/technical_backtest/recommended_strategies/trigger/status"
              onCompleted={() => queryClient.invalidateQueries({ queryKey: ['technical-recommended-strategies'] })}
            />
          </div>
        </CardHeader>
        <CardContent>
          <div className="mb-4 flex flex-wrap items-end gap-3">
            <select
              className="h-9 rounded-[var(--radius-token)] border border-border bg-transparent px-3 text-sm"
              value={strategyFilter}
              onChange={(e) => setStrategyFilter(e.target.value)}
            >
              <option value="">All strategies</option>
              {strategyOptions.map((s) => (
                <option key={s} value={s}>
                  {s}
                </option>
              ))}
            </select>
            <select
              className="h-9 rounded-[var(--radius-token)] border border-border bg-transparent px-3 text-sm"
              value={kindFilter}
              onChange={(e) => setKindFilter(e.target.value)}
            >
              <option value="">Single + Combo</option>
              <option value="single">Single template only</option>
              <option value="combo">Combo only</option>
            </select>
          </div>

          {report.error ? (
            <p className="text-sm text-red">
              Could not reach GET /api/v1/technical_backtest/recommended_strategies —{' '}
              {(report.error as Error).message}
            </p>
          ) : (
            <DataTable
              columns={columns}
              data={rows}
              isLoading={report.isLoading}
              emptyMessage="No recommended-strategies report yet — the sweep hasn't finished running."
            />
          )}
        </CardContent>
      </Card>

      {selectedVariant && selectedFailures ? (
        <div className="mt-4">
          <Card>
            <CardHeader>
              <CardTitle>
                Signal Failures — {selectedVariant.strategy} / {selectedVariant.template}
              </CardTitle>
              <CardDescription>
                {selectedFailures.n_losing_trades} losing trade{selectedFailures.n_losing_trades === 1 ? '' : 's'} of{' '}
                {selectedFailures.n_losing_trades + selectedFailures.n_winning_trades} total — mean matched-conditions
                ratio: losers {fmtNum(selectedFailures.mean_matched_conditions_ratio_losers, 2)}, winners{' '}
                {fmtNum(selectedFailures.mean_matched_conditions_ratio_winners, 2)}
                {' '}
                <button
                  type="button"
                  className="ml-2 text-xs text-muted-foreground underline"
                  onClick={() => setSelectedVariant(null)}
                >
                  close
                </button>
              </CardDescription>
            </CardHeader>
            <CardContent>
              <div className="overflow-x-auto">
                <table className="w-full text-sm">
                  <thead>
                    <tr className="border-b border-border text-left">
                      <th className="py-1 pr-4">Ticker</th>
                      <th className="py-1 pr-4">Buy Date</th>
                      <th className="py-1 pr-4">Sell Date</th>
                      <th className="py-1 pr-4 text-right">P&amp;L %</th>
                      <th className="py-1 pr-4 text-right">Entry Signal Score</th>
                    </tr>
                  </thead>
                  <tbody>
                    {selectedFailures.losing_trades.map((t, idx) => (
                      <tr key={`${t.ticker}-${t.buy_date}-${idx}`} className="border-b border-border/50">
                        <td className="py-1 pr-4">{t.ticker}</td>
                        <td className="py-1 pr-4">{t.buy_date}</td>
                        <td className="py-1 pr-4">{t.sell_date}</td>
                        <td className="py-1 pr-4 text-right text-red">{fmtPct(t.pnl_pct)}</td>
                        <td className="py-1 pr-4 text-right">{t.entry_signal_score ?? '—'}</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            </CardContent>
          </Card>
        </div>
      ) : null}
    </AppShell>
  )
}
