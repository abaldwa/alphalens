/**
 * pages/momentum/CampaignRunAnalysis.tsx
 *
 * [2026-09-05, explicit user instruction] Brings the Long Term CAGR /
 * Regular Returns / Pre-Tax / Post-Tax / Window (rolling returns) /
 * Benchmark / Trade Quality features from /backtest-report/metrics onto
 * ONE campaign run at a time, picked from campaign-results.tsx's grid.
 *
 * SCOPED TO ONE RUN, deliberately -- see datastore/api/routers/
 * framework_backtest_runs.py's analytics-section docstring for why: this
 * campaign has 1,500+ point configs, and FY-netted tax over all of them at
 * once is neither cheap nor a meaningful comparison. A reader picks one
 * config to inspect, exactly as they would pick one strategy on the
 * legacy report.
 *
 * The Regular Returns schedule (withdraw-or-carry, non-topped-up) is NOT
 * re-derived here -- it calls the exact same
 * features/backtest-report/core/regularReturns.ts::regularReturnsSchedule
 * the legacy report uses, fed with this run's own year-on-year series, so
 * the two screens can never compute "regular returns" two different ways.
 */

import { useMemo, useState } from 'react'
import { useQuery } from '@tanstack/react-query'

import { Card, CardContent, CardDescription, CardHeader, CardTitle, DataTable, Input, StatCard } from '@/lib/ui'
import { cn } from '@/lib/utils'
import { regularReturnsSchedule } from '@/features/backtest-report/core/regularReturns'
import type { YoyReturn } from '@/features/backtest-report/core/types'
import {
  getFrameworkRunBenchmark,
  getFrameworkRunRollingReturn,
  getFrameworkRunTradeQuality,
  getFrameworkRunYearlyReturns,
} from '@/shared/api/framework_backtest'
import { EM_DASH, fmtInr, fmtInt, fmtNum, fmtPct, signClass, signTone } from './campaignFormat'

type Mode = 'long_term_cagr' | 'regular_returns'
type TaxBasis = 'pre_tax' | 'post_tax'

/** Same segmented-control look as ReportControls' Segmented, kept local
 * rather than imported: that one writes to the multi-strategy report's URL
 * params (useReportParams), which has no equivalent here -- this panel's
 * state is local to one selected run. */
function Segmented<T extends string>({
  label,
  value,
  options,
  onChange,
}: {
  label: string
  value: T
  options: Array<{ value: T; label: string; title?: string }>
  onChange: (v: T) => void
}) {
  return (
    <div className="flex items-center gap-2">
      <span className="text-xs font-medium text-muted-foreground">{label}</span>
      <div
        role="radiogroup"
        aria-label={label}
        className="inline-flex rounded-[var(--radius-token)] border border-border p-0.5"
      >
        {options.map((o) => (
          <button
            key={o.value}
            type="button"
            role="radio"
            aria-checked={value === o.value}
            title={o.title}
            onClick={() => onChange(o.value)}
            className={cn(
              'rounded-[var(--radius-token)] px-2.5 py-1 text-xs transition-colors',
              value === o.value
                ? 'bg-primary text-primary-foreground'
                : 'text-muted-foreground hover:text-foreground',
            )}
          >
            {o.label}
          </button>
        ))}
      </div>
    </div>
  )
}

export function CampaignRunAnalysis({ runId, runLabel }: { runId: string | null; runLabel: string | null }) {
  const [mode, setMode] = useState<Mode>('long_term_cagr')
  const [taxBasis, setTaxBasis] = useState<TaxBasis>('pre_tax')
  const [fromDate, setFromDate] = useState('')
  const [toDate, setToDate] = useState('')

  const yearlyQuery = useQuery({
    queryKey: ['framework-run-yearly', runId],
    queryFn: () => getFrameworkRunYearlyReturns(runId as string),
    enabled: !!runId,
  })
  const tradeQualityQuery = useQuery({
    queryKey: ['framework-run-trade-quality', runId],
    queryFn: () => getFrameworkRunTradeQuality(runId as string),
    enabled: !!runId,
  })
  const benchmarkQuery = useQuery({
    queryKey: ['framework-run-benchmark', runId],
    queryFn: () => getFrameworkRunBenchmark(runId as string),
    enabled: !!runId,
  })
  const rollingQuery = useQuery({
    queryKey: ['framework-run-rolling', runId, fromDate, toDate],
    queryFn: () => getFrameworkRunRollingReturn(runId as string, fromDate, toDate),
    enabled: !!runId && !!fromDate && !!toDate && toDate > fromDate,
  })

  const yearlyRows = useMemo(() => yearlyQuery.data?.rows ?? [], [yearlyQuery.data])

  // Feed this run's own YoY series (in the selected tax basis) through the
  // SAME schedule function the legacy report's Regular Returns mode uses.
  const regularSchedule = useMemo(() => {
    const yoy: YoyReturn[] = yearlyRows.map((r) => ({
      fyLabel: r.fy_label,
      returnPct: taxBasis === 'pre_tax' ? r.pre_tax_return_pct : r.post_tax_return_pct,
    }))
    return regularReturnsSchedule(yoy, { baseCapital: 1_000_000, topUpAfterLoss: false })
  }, [yearlyRows, taxBasis])

  // Long Term CAGR mode's own year-by-year view: capital compounding
  // through every year's return, nothing withdrawn.
  const compoundingRows = useMemo(() => {
    let capital = 1_000_000
    return yearlyRows.map((r) => {
      const returnPct = taxBasis === 'pre_tax' ? r.pre_tax_return_pct : r.post_tax_return_pct
      capital *= 1 + returnPct
      return { ...r, returnPct, closingCapital: capital }
    })
  }, [yearlyRows, taxBasis])

  if (!runId) {
    return (
      <Card>
        <CardHeader>
          <CardTitle>Run Analysis</CardTitle>
          <CardDescription>
            Click "Analyze" on any row above to see its Long Term CAGR / Regular Returns, tax basis, rolling-window
            return and trade-quality breakdown — the same features as the Backtest Report's Metrics page, computed
            here from this one run's own trades.
          </CardDescription>
        </CardHeader>
      </Card>
    )
  }

  const tradeQuality = tradeQualityQuery.data
  const benchmark = benchmarkQuery.data

  return (
    <Card>
      <CardHeader>
        <div className="flex flex-wrap items-center justify-between gap-3">
          <div>
            <CardTitle>Run Analysis — {runLabel ?? runId}</CardTitle>
            <CardDescription>
              Computed from this run&apos;s own trades (framework_backtest_trades) using the same FY-netted tax
              engine (backtest/core/tax.py) as the Backtest Report.
            </CardDescription>
          </div>
          <div className="flex flex-wrap items-center gap-3">
            <Segmented<Mode>
              label="Mode"
              value={mode}
              onChange={setMode}
              options={[
                { value: 'long_term_cagr', label: 'Long-term CAGR', title: 'Everything compounds; nothing is withdrawn.' },
                { value: 'regular_returns', label: 'Regular returns', title: 'Withdraw the gain above base capital each March; a loss is carried, not topped up.' },
              ]}
            />
            <Segmented<TaxBasis>
              label="Tax"
              value={taxBasis}
              onChange={setTaxBasis}
              options={[
                { value: 'pre_tax', label: 'Pre-Tax' },
                { value: 'post_tax', label: 'Post-Tax' },
              ]}
            />
          </div>
        </div>
      </CardHeader>
      <CardContent className="space-y-6">
        {/* Year-by-year table, one view per mode */}
        {mode === 'long_term_cagr' ? (
          <DataTable
            columns={[
              { accessorKey: 'fy_label', header: 'Financial Year' },
              { accessorKey: 'trade_count', header: 'Trades' },
              {
                accessorKey: 'win_rate',
                header: 'Win Rate',
                meta: { align: 'right' },
                cell: ({ row }) => fmtPct(row.original.win_rate, 1),
              },
              {
                accessorKey: 'returnPct',
                header: `Return (${taxBasis === 'pre_tax' ? 'pre-tax' : 'post-tax'})`,
                meta: { align: 'right' },
                cell: ({ row }) => (
                  <span className={signClass(row.original.returnPct)}>{fmtPct(row.original.returnPct)}</span>
                ),
              },
              {
                accessorKey: 'closingCapital',
                header: 'Cumulative Capital',
                meta: { align: 'right' },
                cell: ({ row }) => fmtInr(row.original.closingCapital),
              },
            ]}
            data={compoundingRows}
            isLoading={yearlyQuery.isLoading}
            emptyMessage="No realized trades for this run."
          />
        ) : (
          <DataTable
            columns={[
              { accessorKey: 'fyLabel', header: 'Financial Year' },
              {
                accessorKey: 'openingCapital',
                header: 'Opening Capital',
                meta: { align: 'right' },
                cell: ({ row }) => fmtInr(row.original.openingCapital),
              },
              {
                accessorKey: 'returnPct',
                header: 'Return',
                meta: { align: 'right' },
                cell: ({ row }) => (
                  <span className={signClass(row.original.returnPct)}>{fmtPct(row.original.returnPct)}</span>
                ),
              },
              {
                accessorKey: 'netCash',
                header: 'Cash Out / (Shortfall)',
                meta: { align: 'right' },
                cell: ({ row }) => fmtInr(row.original.netCash),
              },
              {
                accessorKey: 'closingCapital',
                header: 'Carried Into Next Year',
                meta: { align: 'right' },
                cell: ({ row }) => fmtInr(row.original.closingCapital),
              },
            ]}
            data={regularSchedule}
            isLoading={yearlyQuery.isLoading}
            emptyMessage="No realized trades for this run."
          />
        )}

        {/* Rolling / window returns */}
        <div>
          <h4 className="mb-2 text-sm font-semibold">Rolling Returns — pick any date range</h4>
          <p className="mb-3 text-xs text-muted-foreground">
            Re-slices this run&apos;s own trades to those that closed within the window — no re-simulation, so this
            answers "what did this config return between these two dates" instantly.
          </p>
          <div className="mb-3 flex flex-wrap items-end gap-3">
            <div>
              <label className="mb-1 block text-xs font-semibold uppercase text-muted-foreground">From</label>
              <Input type="date" value={fromDate} onChange={(e) => setFromDate(e.target.value)} className="w-40" />
            </div>
            <div>
              <label className="mb-1 block text-xs font-semibold uppercase text-muted-foreground">To</label>
              <Input type="date" value={toDate} onChange={(e) => setToDate(e.target.value)} className="w-40" />
            </div>
          </div>
          {fromDate && toDate && toDate <= fromDate ? (
            <p className="text-sm text-red-600">"To" must be after "From".</p>
          ) : rollingQuery.data ? (
            <div className="grid grid-cols-2 gap-3 md:grid-cols-4">
              <StatCard
                label={`Return (${taxBasis === 'pre_tax' ? 'pre-tax' : 'post-tax'})`}
                value={fmtPct(taxBasis === 'pre_tax' ? rollingQuery.data.pre_tax_return_pct : rollingQuery.data.post_tax_return_pct)}
                tone={signTone(taxBasis === 'pre_tax' ? rollingQuery.data.pre_tax_return_pct : rollingQuery.data.post_tax_return_pct)}
              />
              <StatCard
                label="Annualised (CAGR)"
                value={fmtPct(taxBasis === 'pre_tax' ? rollingQuery.data.pre_tax_cagr : rollingQuery.data.post_tax_cagr)}
                tone={signTone(taxBasis === 'pre_tax' ? rollingQuery.data.pre_tax_cagr : rollingQuery.data.post_tax_cagr)}
              />
              <StatCard label="Trades Closed" value={fmtInt(rollingQuery.data.trade_count)} />
              <StatCard label="Win Rate" value={fmtPct(rollingQuery.data.win_rate, 1)} />
            </div>
          ) : fromDate && toDate ? (
            <p className="text-sm text-muted-foreground">{rollingQuery.isLoading ? 'Calculating…' : EM_DASH}</p>
          ) : (
            <p className="text-sm text-muted-foreground">Pick both dates to calculate a return for that period.</p>
          )}
        </div>

        {/* Trade quality */}
        <div>
          <h4 className="mb-2 text-sm font-semibold">Trade Quality</h4>
          <p className="mb-3 text-xs text-muted-foreground">
            "Win rate" is how OFTEN this config is right; avg win/loss is how MUCH it makes or loses when it is.
          </p>
          <div className="grid grid-cols-2 gap-3 md:grid-cols-4">
            <StatCard label="Win Rate" value={fmtPct(tradeQuality?.win_rate, 1)} />
            <StatCard label="Avg Win" value={fmtPct(tradeQuality?.avg_win_pct)} tone={signTone(tradeQuality?.avg_win_pct)} />
            <StatCard label="Avg Loss" value={fmtPct(tradeQuality?.avg_loss_pct)} tone={signTone(tradeQuality?.avg_loss_pct)} />
            <StatCard label="Profit Factor" value={fmtNum(tradeQuality?.profit_factor)} />
            <StatCard label="Best Trade" value={fmtPct(tradeQuality?.best_trade_pct)} tone={signTone(tradeQuality?.best_trade_pct)} />
            <StatCard label="Worst Trade" value={fmtPct(tradeQuality?.worst_trade_pct)} tone={signTone(tradeQuality?.worst_trade_pct)} />
            <StatCard label="Avg Holding (days)" value={fmtNum(tradeQuality?.avg_holding_days, 1)} />
            <StatCard label="Total Trades" value={fmtInt(tradeQuality?.trade_count)} />
          </div>
        </div>

        {/* Benchmark */}
        <div>
          <h4 className="mb-2 text-sm font-semibold">Benchmark</h4>
          {benchmark?.index_name ? (
            <>
              <div className="grid grid-cols-2 gap-3 md:grid-cols-4">
                <StatCard label={`${benchmark.index_name} — CAGR`} value={fmtPct(benchmark.benchmark_cagr)} tone={signTone(benchmark.benchmark_cagr)} />
                <StatCard label="Sharpe" value={fmtNum(benchmark.benchmark_sharpe_ratio)} />
                <StatCard label="Max Drawdown" value={fmtPct(benchmark.benchmark_max_drawdown)} tone={signTone(benchmark.benchmark_max_drawdown)} />
                <StatCard
                  label="Period"
                  value={`${benchmark.benchmark_period_start ?? EM_DASH} → ${benchmark.benchmark_period_end ?? EM_DASH}`}
                />
              </div>
              {benchmark.note ? <p className="mt-2 text-xs text-amber">{benchmark.note}</p> : null}
            </>
          ) : (
            <p className="text-sm text-muted-foreground">{benchmark?.note ?? 'Loading…'}</p>
          )}
        </div>
      </CardContent>
    </Card>
  )
}
