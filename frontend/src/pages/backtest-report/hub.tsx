/**
 * pages/backtest-report/hub.tsx
 *
 * The entry point: what has been backtested, and the single best candidate per
 * investor profile. Everything else in the section is a drill-down from here.
 *
 * The hub deliberately shows very few numbers. Its job is to route — the four
 * metric sections exist precisely so this page does not have to be a wall of
 * columns.
 */

import { useMemo } from 'react'
import { Link } from 'react-router-dom'

import {
  Badge,
  Card,
  CardContent,
  CardDescription,
  CardHeader,
  CardTitle,
} from '@/lib/ui'
import { ReportLayout } from '@/features/backtest-report/ui/ReportLayout'
import { StrategyLink } from '@/features/backtest-report/ui/StrategyLink'
import { ValidationFilter, useValidationFilter } from '@/features/backtest-report/ui/ValidationFilter'

type ValidationStatus = 'valid' | 'alternative_period' | 'flagged' | 'invalid'
import { cagrOn } from '@/features/backtest-report/ui/columns'
import { pct, rate } from '@/features/backtest-report/core/format'
import {
  PERSONAS,
  PERSONA_ORDER,
  recommendAll,
} from '@/features/backtest-report/core/recommendations'
import { CHANNELS } from '@/features/backtest-report/core/strategyKey'
import { METRIC_TABS } from '@/features/backtest-report/ui/MetricTabs'
import { REPORT_SECTIONS, layoutProps } from '@/features/backtest-report/ui/sections'
import { useReportPage } from '@/features/backtest-report/data/useReportPage'

export function BacktestReportHubPage() {
  const page = useReportPage()
  const { filter, setFilter } = useValidationFilter()

  const byChannel = useMemo(() => {
    const counts = new Map<string, number>()
    for (const s of page.strategies) {
      counts.set(s.channel, (counts.get(s.channel) ?? 0) + 1)
    }
    return counts
  }, [page.strategies])

  const validationCounts = useMemo(() => {
    const counts: Record<ValidationStatus, number> = {
      valid: 0,
      alternative_period: 0,
      flagged: 0,
      invalid: 0,
    }
    for (const s of page.strategies) {
      const status = (s.validation_status ?? 'valid') as ValidationStatus
      counts[status]++
    }
    return counts
  }, [page.strategies])

  const recommendations = useMemo(
    () => recommendAll(page.strategies, page.params.taxBasis),
    [page.strategies, page.params.taxBasis],
  )

  return (
    <ReportLayout
      title="Backtest Report"
      description="One report across Momentum, Technical, Fundamental and ML — the same five attributes, the same tables, the same strategy names throughout."
      {...layoutProps(page)}
    >
      {page.errors.length > 0 ? (
        // One channel's report being absent is normal — it may simply never
        // have been generated. It must not blank the other three, so this is
        // a note rather than an error state.
        <p className="mb-4 text-xs text-amber">
          {page.errors.length} channel report{page.errors.length === 1 ? '' : 's'}{' '}
          could not be loaded; the rest are shown.
        </p>
      ) : null}

      <Card className="mb-4">
        <CardHeader>
          <CardTitle>Coverage</CardTitle>
          <CardDescription>
            {page.strategies.length} strategies cover the selected window
            {page.excludedCount > 0
              ? `; ${page.excludedCount} more exist but their runs do not span it`
              : ''}
            .
          </CardDescription>
        </CardHeader>
        <CardContent className="flex flex-wrap gap-2">
          {CHANNELS.map((c) => (
            <button
              key={c}
              type="button"
              onClick={() =>
                page.setParams({ channel: page.params.channel === c ? 'all' : c })
              }
              aria-pressed={page.params.channel === c}
            >
              <Badge variant={page.params.channel === c ? 'default' : 'outline'}>
                {c} — {byChannel.get(c) ?? 0}
              </Badge>
            </button>
          ))}
        </CardContent>
      </Card>

      <Card className="mb-4">
        <CardHeader>
          <CardTitle>Data Quality</CardTitle>
          <CardDescription>
            Filter results by validation status to focus on production-ready or analysis data.
          </CardDescription>
        </CardHeader>
        <CardContent>
          <ValidationFilter value={filter} onChange={setFilter} counts={validationCounts} />
        </CardContent>
      </Card>

      <Card className="mb-4">
        <CardHeader>
          <CardTitle>Top candidate by profile</CardTitle>
          <CardDescription>
            Each is the highest-scoring strategy that cleared every gate for
            that profile. The full ranking, gates and weights are on the
            Recommendations tab.
          </CardDescription>
        </CardHeader>
        <CardContent>
          <ul className="space-y-2">
            {PERSONA_ORDER.map((id) => {
              const top = recommendations[id].find((r) => r.passed)
              return (
                <li key={id} className="flex flex-wrap items-baseline gap-2 text-sm">
                  <span className="w-32 shrink-0 font-medium">
                    {PERSONAS[id].label}
                  </span>
                  {top ? (
                    <>
                      <StrategyLink
                        strategyKey={top.report.key}
                        label={top.report.label}
                      />
                      <span className="tabular-nums text-muted-foreground">
                        {rate(cagrOn(top.report, page.params.taxBasis))} · drawdown{' '}
                        {pct(top.report.risk.maxDrawdown)}
                      </span>
                    </>
                  ) : (
                    <span className="text-muted-foreground">
                      Nothing clears the gates — deploy nothing for this profile.
                    </span>
                  )}
                </li>
              )
            })}
          </ul>
        </CardContent>
      </Card>

      <Card>
        <CardHeader>
          <CardTitle>Sections</CardTitle>
        </CardHeader>
        <CardContent className="flex flex-wrap gap-3 text-sm">
          {/* The metric tabs are listed alongside the top-level sections
              rather than hidden one click behind "Metrics": the hub exists to
              be a directory, and a reader who wants Risk should be able to
              land on Risk from here. */}
          {[
            ...REPORT_SECTIONS.filter((s) => s.path !== '/backtest-report'),
            ...METRIC_TABS.filter((t) => t.id !== 'all').map((t) => ({
              path: t.path,
              label: t.label,
            })),
          ].map((s) => (
            <Link key={s.path} to={s.path} className="text-primary hover:underline">
              {s.label}
            </Link>
          ))}
        </CardContent>
      </Card>
    </ReportLayout>
  )
}
