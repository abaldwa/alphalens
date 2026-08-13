// Momentum Dynamic Report hub -- explanation banner, the Recommended &
// Most Important Strategies summary table, and links out to each heavier
// section (each now its own page/route). Split (2026-08-12) from one
// 962-line single-page report because it had become too heavy to load.
import { Link } from 'react-router-dom'
import { useQueryClient } from '@tanstack/react-query'

import { AppShell, Badge, Card, CardContent, CardDescription, CardHeader, CardTitle, DataTable } from '@/lib/ui'
import { SweepTriggerButton } from '../SweepTriggerButton'
import {
  useDynamicReportData,
  useSweepColumns,
  useRecommendedColumns,
} from './shared'

const SECTIONS = [
  {
    to: '/momentum-dynamic-report/rolling-returns',
    title: 'Rolling Return Consistency',
    description: '2/3/4-year rolling-window CAGR (min / median / max) per Recommended/Most Important pick.',
  },
  {
    to: '/momentum-dynamic-report/strategy-sweep',
    title: 'Strategy Sweep',
    description: 'Every variant across all 7 rank bands x 4 categories — the heaviest table, its own page.',
  },
  {
    to: '/momentum-dynamic-report/yoy',
    title: 'Year-on-Year (Apr–Mar)',
    description: 'Per-FY return, churn, and Nifty Midcap 150 / Smallcap 250 comparison.',
  },
  {
    to: '/momentum-dynamic-report/income-mode',
    title: 'Income Mode (Annual Capital Reset)',
    description: 'Each strategy starts every FY at ₹10L; surplus withdrawn as cash, shortfalls topped up.',
  },
  {
    to: '/momentum-dynamic-report/yoy-matrix',
    title: 'YoY Consistency Matrix',
    description: 'Per band: strategies x fiscal years, Red/Amber/Green consistency scan — the other heavy table.',
  },
]

export function MomentumDynamicReportPage() {
  const queryClient = useQueryClient()
  const { report, allRows, effectiveTopCagrRank, recommendedRows, avgHoldingQuery, effectiveAvgDaysHeld } =
    useDynamicReportData()

  const sweepColumns = useSweepColumns(effectiveTopCagrRank, () => {})
  const recommendedColumns = useRecommendedColumns(sweepColumns, effectiveAvgDaysHeld, avgHoldingQuery.isLoading)

  return (
    <AppShell
      title="Momentum — Strategy Report"
      description="All Risk / Balanced / Risk-Managed / Max-Defensive strategies across all 7 rank bands (1-50 through 501-800) — scripts/run_momentum_dynamic_report.py."
    >
      <div className="mb-4 rounded-[var(--radius-token)] border border-border bg-accent-soft px-3 py-2 text-xs text-muted-foreground">
        <strong className="text-foreground">All Risk</strong> is the unfiltered baseline.{' '}
        <strong className="text-foreground">Balanced</strong> adds liquidity floor, quality gating, ADTV-capped
        sizing, and a circuit-lock proxy. <strong className="text-foreground">Risk-Managed</strong> adds
        regime-conditional buy-disabling in high-volatility periods.{' '}
        <strong className="text-foreground">Max-Defensive</strong> additionally neutralizes size/beta exposure.
        Within each rank band, the highest-scoring variant per category is marked{' '}
        <Badge variant="success">Recommended</Badge> — score ={' '}
        {report.data?.score_formula ?? '0.30·z(Sharpe) + 0.25·z(Sortino) + 0.25·z(CAGR) − 0.20·z(|Max Drawdown|)'},
        z-scored within each (band, category) group of 60 variants. The single best variant across all 240
        configs in each universe is marked <Badge variant="default">Most Important</Badge> — the strategy
        deployed for that band. The top 2 variants by raw CAGR in each universe (any category/config) are
        marked <Badge variant="outline">Top CAGR #1/#2</Badge> for comparison against the risk-adjusted picks.
      </div>

      <Card className="mb-6">
        <CardHeader>
          <CardTitle>Recommended & Most Important Strategies</CardTitle>
          <CardDescription>
            The highest-scoring variant per (universe, category), the per-universe{' '}
            <Badge variant="default">Most Important</Badge> best strategy, and the per-universe{' '}
            <Badge variant="outline">Top CAGR #1/#2</Badge> variants (any category/config) for comparison
            — {recommendedRows.length} rows across all 7 universes.
          </CardDescription>
          <div className="mt-2 flex flex-wrap gap-3">
            <SweepTriggerButton
              label="Strategy Report"
              triggerUrl="/api/v1/momentum/dynamic_report/trigger"
              statusUrlPrefix="/api/v1/momentum/dynamic_report/trigger/status"
              onCompleted={() => queryClient.invalidateQueries({ queryKey: ['momentum-dynamic-report'] })}
            />
          </div>
        </CardHeader>
        <CardContent>
          <DataTable
            columns={recommendedColumns}
            data={recommendedRows}
            isLoading={report.isLoading}
            emptyMessage="No recommended picks yet — run the sweep above."
          />
        </CardContent>
      </Card>

      <div className="grid grid-cols-1 gap-4 md:grid-cols-2">
        {SECTIONS.map((s) => (
          <Link
            key={s.to}
            to={s.to}
            className="block rounded-[var(--radius-token)] border border-border bg-card p-4 transition-colors hover:border-primary"
          >
            <div className="text-sm font-semibold text-foreground">{s.title} →</div>
            <div className="mt-1 text-xs text-muted-foreground">{s.description}</div>
          </Link>
        ))}
      </div>

      <p className="mt-4 text-xs text-muted-foreground">
        {report.error
          ? `Failed to load — ${(report.error as Error).message}`
          : report.isLoading
            ? 'Loading…'
            : `${allRows.length} variants${
                report.data?.generated_at ? ` — generated ${new Date(report.data.generated_at).toLocaleString()}` : ''
              }`}
      </p>
    </AppShell>
  )
}
