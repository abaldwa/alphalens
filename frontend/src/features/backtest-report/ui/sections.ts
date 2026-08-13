/**
 * features/backtest-report/sections.ts
 *
 * The six sections, and the layout wiring every page repeats. Kept out of the
 * component files so those export components only (fast refresh) and so the
 * section list has one definition feeding both the sub-nav and the hub.
 */

import type { ReportParams } from '../data/useReportParams'
import type { BenchmarkOption } from '../core/types'

export const REPORT_SECTIONS: Array<{ path: string; label: string }> = [
  { path: '/backtest-report', label: 'Overview' },
  { path: '/backtest-report/recommendations', label: 'Recommendations' },
  { path: '/backtest-report/returns', label: 'Returns' },
  { path: '/backtest-report/consistency', label: 'Consistency' },
  { path: '/backtest-report/risk', label: 'Risk' },
  { path: '/backtest-report/trade-quality', label: 'Trade quality' },
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
