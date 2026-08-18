/**
 * features/backtest-report/useReportData.ts
 *
 * One hook, every channel. Each section page calls this and gets a single
 * StrategyReport[] spanning Momentum, Technical, Fundamental and ML, so the
 * five attribute tables are genuinely cross-channel rather than four separate
 * reports wearing the same styling.
 *
 * Three sources, because that is what exists today:
 *   - /api/v1/momentum/dynamic_report   — the richest; most fields populated
 *   - /api/v1/technical_backtest/comparison — rich but differently shaped
 *   - /api/v1/backtest/runs             — thin; serves ML and backfills any
 *                                         channel whose report has not run
 *
 * A83 collapses these into one contract; until then the adapters absorb the
 * difference and this hook only has to merge and de-duplicate.
 *
 * Query keys are shared with the existing pages where the endpoint is the
 * same, so visiting both the old and new screens costs one fetch, not two.
 */

import { useQuery } from '@tanstack/react-query'
import { useMemo } from 'react'

import type { MomentumDynamicReport } from '@/pages/momentum/types'
import type { TAComparisonReport } from '@/pages/technical/types'
import { listBacktestRuns } from '@/shared/api/backtest'
import { apiGet } from '@/shared/api/client'

import { adaptMomentumReport } from '../core/adapters/momentum'
import { adaptTechnicalReport } from '../core/adapters/technical'
import { adaptRuns } from '../core/adapters/runs'
import type { Channel, StrategyReport } from '../core/types'

export interface IndexOption {
  index_name: string
  first_date: string | null
  last_date: string | null
  live_from: string | null
  is_fresh: boolean
  usable_as_benchmark: boolean
  caveat: string | null
}

export interface IndexListResponse {
  indices: IndexOption[]
  default_benchmark: string
  regime_index: string
  live_over_window: string[]
  backcomputed_over_window: string[]
  recommended_benchmark: string | null
  fallback_reason: string | null
}

export interface ReportDataOptions {
  channel?: Channel | 'all'
  /** Window bounds, forwarded to /api/v1/indices so benchmark options and
   * their caveats reflect the period actually being compared. */
  startDate?: string | null
  endDate?: string | null
}

export function useReportData(options: ReportDataOptions = {}) {
  const { channel = 'all' } = options

  const momentum = useQuery({
    // Same key as pages/momentum/dynamic-report/shared.tsx, deliberately: one
    // cache entry serves the old and new screens both.
    queryKey: ['momentum-dynamic-report'],
    queryFn: () =>
      apiGet<MomentumDynamicReport>('/api/v1/momentum/dynamic_report'),
    enabled: channel === 'all' || channel === 'momentum',
  })

  const technical = useQuery({
    queryKey: ['ta-comparison'],
    queryFn: () =>
      apiGet<TAComparisonReport>('/api/v1/technical_backtest/comparison'),
    enabled: channel === 'all' || channel === 'technical',
  })

  const runs = useQuery({
    queryKey: ['backtest-runs-report'],
    queryFn: () => listBacktestRuns({ sort_by: 'cagr', limit: 1000 }),
  })

  const strategies = useMemo<StrategyReport[]>(() => {
    const fromMomentum = momentum.data ? adaptMomentumReport(momentum.data) : []
    const fromTechnical = technical.data ? adaptTechnicalReport(technical.data) : []
    const fromRuns = adaptRuns(runs.data?.runs ?? null)
    // The specialised reports win over /runs for channel-specific richness
    // (rolling windows, YoY, churn). However some metrics (XIRR, post-tax
    // CAGR, or a declared `universe`) may only be present on the generic
    // run summary. Merge so that specialised reports override runs, but
    // missing scalar fields are backfilled from `fromRuns` when available.
    const merged = new Map<string, StrategyReport>()
    for (const r of fromRuns) merged.set(r.key, r)
    for (const r of [...fromTechnical, ...fromMomentum]) merged.set(r.key, r)

    // Backfill missing simple fields on specialised rows from the runs
    // adapter where the run-level metrics exist (xirr, cagrPostTax, universe,
    // finalCapital, totalContributed). This preserves the richer report but
    // supplies missing scalars the table needs to render.
    for (const runRow of fromRuns) {
      const existing = merged.get(runRow.key)
      if (!existing) continue
      // Returns-level fields to backfill when null in the specialised row.
      const ret = existing.returns
      const runRet = runRow.returns
      if ((ret.cagrPostTax == null || ret.cagrPostTax === undefined) && runRet.cagrPostTax != null) {
        ret.cagrPostTax = runRet.cagrPostTax
        delete existing.pending['returns.cagrPostTax']
      }
      if ((ret.xirr == null || ret.xirr === undefined) && runRet.xirr != null) {
        ret.xirr = runRet.xirr
        delete existing.pending['returns.xirr']
      }
      if ((ret.finalCapital == null || ret.finalCapital === undefined) && runRet.finalCapital != null) {
        ret.finalCapital = runRet.finalCapital
        delete existing.pending['returns.finalCapital']
      }
      // Setup-level backfill: universe.
      if ((existing.setup.universe == null || existing.setup.universe === undefined) && runRow.setup.universe) {
        existing.setup.universe = runRow.setup.universe
      }
      merged.set(existing.key, existing)
    }

    const all = [...merged.values()]
    return channel === 'all' ? all : all.filter((r) => r.channel === channel)
  }, [momentum.data, technical.data, runs.data, channel])

  const indices = useQuery({
    queryKey: ['indices', options.startDate, options.endDate],
    queryFn: () => {
      const qs = new URLSearchParams()
      if (options.startDate) qs.set('start_date', options.startDate)
      if (options.endDate) qs.set('end_date', options.endDate)
      const q = qs.toString()
      return apiGet<IndexListResponse>(`/api/v1/indices${q ? `?${q}` : ''}`)
    },
  })

  /** The newest date any channel's report covers. The window selector anchors
   * to this rather than today: anchoring to today silently shortens every
   * window whenever the pipeline is behind, which it periodically is. */
  const latestDate = useMemo(() => {
    const ends = strategies
      .map((s) => s.setup.window.endDate)
      .filter((d): d is string => !!d)
    return ends.length ? ends.sort().at(-1)! : null
  }, [strategies])

  return {
    strategies,
    latestDate,
    indices,
    isLoading: momentum.isLoading || technical.isLoading || runs.isLoading,
    // Reported rather than thrown: one channel's report being absent is
    // normal (it may simply never have been generated), and it must not blank
    // the other three.
    errors: [momentum.error, technical.error, runs.error].filter(Boolean) as Error[],
  }
}
