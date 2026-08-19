/**
 * features/backtest-report/sections.ts
 *
 * The seven sections, and the layout wiring every page repeats. Kept out of the
 * component files so those export components only (fast refresh) and so the
 * section list has one definition feeding both the sub-nav and the hub.
 */

import type { ReportParams } from '../data/useReportParams'
import type { BenchmarkOption } from '../core/types'

/**
 * The site-level strip: which PART of the report you are in.
 *
 * Returns/Consistency/Risk/Trade quality used to be listed here as four peers.
 * They are now tabs inside the Metrics workspace (ui/MetricTabs), because they
 * are four views of one comparison rather than four reports — listing them
 * twice, once here and once as tabs, would give the reader two controls that
 * do the same thing. Their routes are unchanged, so every existing link and
 * bookmark still resolves; they simply land on the workspace with that tab
 * active.
 */
export const REPORT_SECTIONS: Array<{ path: string; label: string }> = [
  { path: '/backtest-report', label: 'Overview' },
  { path: '/backtest-report/recommendations', label: 'Recommendations' },
  { path: '/backtest-report/metrics', label: 'Metrics' },
  { path: '/backtest-report/pivot', label: 'Pivot' },
]

/** What ReportLayout needs from a page's useReportPage() result. */
export interface LayoutProps {
  params: ReportParams
  onChange: (patch: Partial<ReportParams>) => void
  benchmarkOptions: BenchmarkOption[]
  recommendedBenchmark: string | null
  fallbackReason: string | null
  resolvedStart: string | null
}

export function layoutProps(page: {
  params: ReportParams
  setParams: (patch: Partial<ReportParams>) => void
  benchmarkOptions: BenchmarkOption[]
  recommendedBenchmark: string | null
  fallbackReason: string | null
  resolved: { startDate: string | null }
}): LayoutProps {
  return {
    params: page.params,
    onChange: page.setParams,
    benchmarkOptions: page.benchmarkOptions,
    recommendedBenchmark: page.recommendedBenchmark,
    fallbackReason: page.fallbackReason,
    resolvedStart: page.resolved.startDate,
  }
}
