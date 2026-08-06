// Technical — Batch Backtest: single-place launcher + comprehensive strategy
// report for every Technical Indicator backtest (all 42 screener templates ×
// 3 filter tiers × 3 top-N options + combo strategies), structured
// identically to the momentum-dynamic-report page: legend → hero card →
// top strategies summary → per-template sweep → filters.
//
// Data source: scripts/run_technical_recommended_strategies.py's aggregate
// report, surfaced via GET /api/v1/technical_backtest/recommended_strategies.
// Each variant carries CAGR, Sharpe, Sortino, Calmar, Max DD, Win Rate,
// Trades, Avg Duration, Trade Book link, and Signal Failure breakdown —
// all the metrics needed to judge which strategies are deployment-ready.
import { useMemo, useState } from 'react'
import { useQuery, useQueryClient } from '@tanstack/react-query'
import type { ColumnDef } from '@tanstack/react-table'

import { AppShell, Badge, Card, CardContent, CardDescription, CardHeader, CardTitle, DataTable } from '@/lib/ui'
import { API_BASE_URL, apiGet } from '@/shared/api/client'
import type { TABacktestVariant, TARecommendedStrategiesReport } from './types'
import { SweepTriggerButton } from './SweepTriggerButton'

function fmtPct(v: number | null | undefined) {
  return typeof v === 'number' ? `${(v * 100).toFixed(1)}%` : '—'
}
function fmtNum(v: number | null | undefined, digits = 2) {
  return typeof v === 'number' ? v.toFixed(digits) : '—'
}

const FILTER_LABELS: Record<string, string> = {
  balanced: 'Balanced',
  risk_managed: 'Risk-Managed',
  max_defensive: 'Max-Defensive',
}

export function TechnicalBatchBacktestPage() {
  const [strategyFilter, setStrategyFilter] = useState<string>('')
  const [topNFilter, setTopNFilter] = useState<string>('')
  const [selectedVariant, setSelectedVariant] = useState<TABacktestVariant | null>(null)
  const queryClient = useQueryClient()

  const report = useQuery({
    queryKey: ['technical-recommended-strategies'],
    queryFn: () => apiGet<TARecommendedStrategiesReport>('/api/v1/technical_backtest/recommended_strategies'),
  })

  const allRows = report.data?.variants ?? []

  // Derive the top variant (by Sharpe) per template — these are the
  // "Recommended" picks, equivalent to momentum's "Most Important".
  const bestPerTemplate = useMemo(() => {
    const byTemplate = new Map<string, TABacktestVariant>()
    for (const r of allRows) {
      if (r.variant_kind !== 'single' || r.sharpe == null) continue
      const existing = byTemplate.get(r.template)
      if (!existing || (r.sharpe ?? -Infinity) > (existing.sharpe ?? -Infinity)) {
        byTemplate.set(r.template, r)
      }
    }
    return new Set(Array.from(byTemplate.values()).map((r) => r.template))
  }, [allRows])

  // Overall best variant by Sharpe — the single most deployment-ready pick.
  const bestVariant = useMemo(() => {
    const valid = allRows.filter((r) => r.variant_kind === 'single' && r.sharpe != null)
    return valid.length > 0 ? valid.reduce((best, r) => (r.sharpe! > best.sharpe! ? r : best)) : null
  }, [allRows])

  // Overall best variant by CAGR — the highest raw return pick.
  const bestCagrVariant = useMemo(() => {
    const valid = allRows.filter((r) => r.variant_kind === 'single' && r.cagr != null)
    return valid.length > 0 ? valid.reduce((best, r) => (r.cagr! > best.cagr! ? r : best)) : null
  }, [allRows])

  const strategyOptions = useMemo(
    () => Array.from(new Set(allRows.map((r) => r.strategy).filter(Boolean))).sort(),
    [allRows],
  )
  const topNOptions = useMemo(
    () => Array.from(new Set(allRows.map((r) => r.top_n))).sort((a, b) => a - b),
    [allRows],
  )

  // Group templates: singles first, then combos.
  const templates = useMemo(() => {
    const singles = Array.from(
      new Set(allRows.filter((r) => r.variant_kind !== 'combo').map((r) => r.template)),
    ).sort()
    const combos = Array.from(
      new Set(allRows.filter((r) => r.variant_kind === 'combo').map((r) => r.template)),
    ).sort()
    return [...singles, ...combos]
  }, [allRows])

  const summaryRows = useMemo(
    () =>
      allRows.filter((r) => {
        if (strategyFilter && r.strategy !== strategyFilter) return false
        if (topNFilter && r.top_n !== Number(topNFilter)) return false
        return true
      }),
    [allRows, strategyFilter, topNFilter],
  )

  const columns = useMemo<ColumnDef<TABacktestVariant, unknown>[]>(
    () => [
      {
        id: 'template',
        accessorKey: 'template',
        header: 'Template',
        size: 140,
        cell: (i) => {
          const v = i.row.original
          const isBest = bestPerTemplate.has(v.template) && v.strategy === allRows.find(
            (r) => r.template === v.template && r.variant_kind !== 'combo' && r.sharpe != null &&
              bestPerTemplate.has(r.template),
          )?.strategy
          return (
            <span className="flex flex-wrap items-center gap-1.5">
              {v.template}
              {v.variant_kind === 'combo' ? <Badge variant="outline">Combo</Badge> : null}
              {isBest ? <Badge variant="success">Recommended</Badge> : null}
              {bestVariant?.run_id === v.run_id ? <Badge variant="warning">Best Sharpe</Badge> : null}
              {bestCagrVariant?.run_id === v.run_id ? <Badge variant="default">Best CAGR</Badge> : null}
            </span>
          )
        },
      },
      {
        accessorKey: 'strategy',
        header: 'Filter Tier',
        size: 130,
        cell: (i) => FILTER_LABELS[i.getValue<string>()] ?? i.getValue<string>(),
      },
      { accessorKey: 'top_n', header: 'Top N', size: 60, meta: { align: 'right' } },
      {
        accessorKey: 'cagr',
        header: 'CAGR',
        size: 65,
        meta: { align: 'right' },
        cell: (i) => fmtPct(i.getValue<number | null>()),
      },
      {
        accessorKey: 'sharpe',
        header: 'Sharpe',
        size: 60,
        meta: { align: 'right' },
        cell: (i) => fmtNum(i.getValue<number | null>(), 2),
      },
      {
        accessorKey: 'sortino',
        header: 'Sortino',
        size: 65,
        meta: { align: 'right', priority: 'medium' },
        cell: (i) => fmtNum(i.getValue<number | null>(), 2),
      },
      {
        accessorKey: 'calmar',
        header: 'Calmar',
        size: 65,
        meta: { align: 'right', priority: 'medium' },
        cell: (i) => fmtNum(i.getValue<number | null>(), 2),
      },
      {
        accessorKey: 'win_rate',
        header: 'Win Rate',
        size: 70,
        meta: { align: 'right' },
        cell: (i) => fmtPct(i.getValue<number | null>()),
      },
      {
        accessorKey: 'total_trades',
        header: 'Trades',
        size: 65,
        meta: { align: 'right', priority: 'medium' },
        cell: (i) => i.getValue<number | null>() ?? '—',
      },
      {
        accessorKey: 'avg_trade_duration_days',
        header: 'Avg Days Held',
        size: 85,
        meta: { align: 'right', priority: 'medium' },
        cell: (i) => {
          const v = i.getValue<number | null>()
          return typeof v === 'number' ? `${v.toFixed(0)}d` : '—'
        },
      },
      {
        accessorKey: 'n_outlier_trades',
        header: 'Outliers',
        size: 70,
        meta: { align: 'right', priority: 'low' },
        cell: (i) => i.getValue<number | null>() ?? '—',
      },
      {
        id: 'signal_failures',
        header: 'Signal Failures',
        meta: { align: 'right', priority: 'low' },
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
    [bestPerTemplate, bestVariant, bestCagrVariant, allRows],
  )

  const selectedFailures = selectedVariant?.signal_failures

  return (
    <AppShell
      title="Technical — Batch Backtest"
      description="All Technical Indicator backtest strategies (42 templates × 3 filter tiers × 3 top-N options + combo strategies) — one place to judge which are deployment-ready."
    >
      {/* Legend */}
      <div className="mb-4 rounded-[var(--radius-token)] border border-border bg-accent-soft px-3 py-2 text-xs text-muted-foreground">
        <strong className="text-foreground">Balanced</strong> adds liquidity floor (min ADTV ₹10K), quality
        gating (F-Score ≥ 4), and circuit-breaker proxy (19% daily limit).{' '}
        <strong className="text-foreground">Risk-Managed</strong> adds a downtrend filter (5% trailing 20-day
        return cutoff). <strong className="text-foreground">Max-Defensive</strong> additionally disables buys in
        bear-market regimes. Within each template, the highest-Sharpe variant per filter tier is marked{' '}
        <Badge variant="success">Recommended</Badge>. The single highest-Sharpe variant across all templates is
        marked <Badge variant="warning">Best Sharpe</Badge>, and the highest-CAGR variant is{' '}
        <Badge variant="default">Best CAGR</Badge>.
      </div>

      {/* Hero card */}
      {bestVariant ? (
        <Card className="mb-4 border-accent">
          <CardHeader>
            <CardTitle>Best Strategy (by Sharpe)</CardTitle>
            <CardDescription>
              {bestVariant.template} · {FILTER_LABELS[bestVariant.strategy] ?? bestVariant.strategy} ·
              {' '}Top {bestVariant.top_n} · CAGR {fmtPct(bestVariant.cagr)} · Sharpe {fmtNum(bestVariant.sharpe, 2)} ·
              {' '}Sortino {fmtNum(bestVariant.sortino, 2)} · Win Rate {fmtPct(bestVariant.win_rate)} ·
              {' '}{bestVariant.total_trades ?? '—'} trades
            </CardDescription>
          </CardHeader>
        </Card>
      ) : null}

      {/* Top Strategies summary */}
      <Card className="mb-6">
        <CardHeader>
          <CardTitle>Top Strategies</CardTitle>
          <CardDescription>
            {report.isLoading
              ? 'Loading…'
              : report.error
                ? 'Failed to load'
                : `${summaryRows.length} of ${allRows.length} variants — ${
                    report.data?.generated_at
                      ? `generated ${new Date(report.data.generated_at).toLocaleString()}`
                      : ''
                  }`}
          </CardDescription>
          <div className="mt-2 flex flex-wrap gap-3">
            <SweepTriggerButton
              label="Run Full Technical Sweep"
              triggerUrl="/api/v1/technical_backtest/recommended_strategies/trigger"
              statusUrlPrefix="/api/v1/technical_backtest/recommended_strategies/trigger/status"
              onCompleted={() => {
                queryClient.invalidateQueries({ queryKey: ['technical-recommended-strategies'] })
                queryClient.invalidateQueries({ queryKey: ['backtest-runs', 'technical'] })
              }}
            />
            <select
              className="h-9 rounded-[var(--radius-token)] border border-border bg-transparent px-3 text-sm"
              value={strategyFilter}
              onChange={(e) => setStrategyFilter(e.target.value)}
            >
              <option value="">All filter tiers</option>
              {strategyOptions.map((s) => (
                <option key={s} value={s}>
                  {FILTER_LABELS[s] ?? s}
                </option>
              ))}
            </select>
            <select
              className="h-9 rounded-[var(--radius-token)] border border-border bg-transparent px-3 text-sm"
              value={topNFilter}
              onChange={(e) => setTopNFilter(e.target.value)}
            >
              <option value="">All top-N</option>
              {topNOptions.map((n) => (
                <option key={n} value={n}>
                  Top {n}
                </option>
              ))}
            </select>
          </div>
        </CardHeader>
        <CardContent>
          {report.error ? (
            <p className="text-sm text-red">
              Could not reach GET /api/v1/technical_backtest/recommended_strategies —{' '}
              {(report.error as Error).message}
            </p>
          ) : (
            <DataTable
              columns={columns}
              data={summaryRows}
              isLoading={report.isLoading}
              emptyMessage="No recommended-strategies report yet — trigger the sweep above."
            />
          )}
        </CardContent>
      </Card>

      {/* Per-template sweep */}
      <Card>
        <CardHeader>
          <CardTitle>Strategy Sweep</CardTitle>
          <CardDescription>
            All variants grouped by template — expand each to compare filter tiers and top-N options side by side.
          </CardDescription>
        </CardHeader>
        <CardContent>
          {report.error ? (
            <p className="text-sm text-red">
              Could not reach GET /api/v1/technical_backtest/recommended_strategies —{' '}
              {(report.error as Error).message}
            </p>
          ) : (
            templates.map((template) => {
              const templateRows = allRows.filter(
                (r) =>
                  r.template === template &&
                  (!strategyFilter || r.strategy === strategyFilter) &&
                  (!topNFilter || r.top_n === Number(topNFilter)),
              )
              return (
                <details key={template} className="mb-4 rounded-[var(--radius-token)] border border-border">
                  <summary className="cursor-pointer px-3 py-2 text-sm font-semibold">
                    {template} — {templateRows.length} variants
                  </summary>
                  <div className="border-t border-border p-2">
                    <DataTable
                      columns={columns}
                      data={templateRows}
                      isLoading={report.isLoading}
                      emptyMessage="No variants for this template yet — run the sweep above."
                    />
                  </div>
                </details>
              )
            })
          )}
        </CardContent>
      </Card>

      {/* Signal failures drill-down */}
      {selectedVariant && selectedFailures ? (
        <div className="mt-4">
          <Card>
            <CardHeader>
              <CardTitle>
                Signal Failures — {selectedVariant.template} / {FILTER_LABELS[selectedVariant.strategy] ?? selectedVariant.strategy}
              </CardTitle>
              <CardDescription>
                {selectedFailures.n_losing_trades} losing trade{selectedFailures.n_losing_trades === 1 ? '' : 's'} of{' '}
                {selectedFailures.n_losing_trades + selectedFailures.n_winning_trades} total — mean
                matched-conditions ratio: losers{' '}
                {fmtNum(selectedFailures.mean_matched_conditions_ratio_losers, 2)}, winners{' '}
                {fmtNum(selectedFailures.mean_matched_conditions_ratio_winners, 2)}{' '}
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
