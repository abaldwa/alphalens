/**
 * features/backtest-report/useReportParams.ts
 *
 * Every control in the report header — window, benchmark, tax basis, mode,
 * channel, selected strategy — lives in the URL rather than in component
 * state. That is the whole reason the section can be linked at all: a
 * recommendation that says "this strategy, post-tax, over 10 years against
 * Nifty 500" is only a claim you can check if the link reproduces it.
 *
 * It also means the browser's back button walks the chain the user actually
 * followed (matrix → rolling → detail), which the current
 * `window.location.href` jump cannot do.
 *
 * Defaults are applied on read, never written into the URL, so a bare
 * /backtest-report link stays clean and a later change of default is not
 * frozen into every link already shared.
 */

import { useCallback, useMemo } from 'react'
import { useSearchParams } from 'react-router-dom'

import { AGGS, findDimension, findMetric, type AggName } from '../core/pivot'
import { CHANNELS } from '../core/strategyKey'
import { WINDOW_PRESETS, type WindowPreset } from '../core/window'
import type { Channel, ReturnMode, StrategyKey, TaxBasis } from '../core/types'

export interface ReportParams {
  window: WindowPreset
  /** Explicit dates, set only by the Custom window option. */
  startDate: string | null
  endDate: string | null
  benchmark: string | null
  taxBasis: TaxBasis
  mode: ReturnMode
  channel: Channel | 'all'
  strategy: StrategyKey | null
  /** Pivot section only: the two axes, the summarised metric and how a bucket
   * collapses. In the URL like everything else, so "median post-tax CAGR by
   * channel x universe" is a link someone else can open. */
  pivotRow: string
  pivotCol: string
  pivotMetric: string
  pivotAgg: AggName
}

function asWindow(v: string | null): WindowPreset {
  return (WINDOW_PRESETS as readonly string[]).includes(v ?? '')
    ? (v as WindowPreset)
    : 'max'
}

export function useReportParams(): [
  ReportParams,
  (patch: Partial<ReportParams>) => void,
] {
  const [searchParams, setSearchParams] = useSearchParams()

  const params = useMemo<ReportParams>(() => {
    const channel = searchParams.get('channel')
    return {
      window: asWindow(searchParams.get('window')),
      startDate: searchParams.get('startDate'),
      endDate: searchParams.get('endDate'),
      benchmark: searchParams.get('benchmark'),
      // Post-tax is the default basis: it is the number the user actually
      // receives, and defaulting to pre-tax flatters every high-churn
      // strategy in the ranking.
      taxBasis: searchParams.get('taxBasis') === 'pre_tax' ? 'pre_tax' : 'post_tax',
      mode:
        searchParams.get('mode') === 'regular_returns'
          ? 'regular_returns'
          : 'long_term_cagr',
      channel: (CHANNELS as string[]).includes(channel ?? '')
        ? (channel as Channel)
        : 'all',
      strategy: searchParams.get('strategy'),
      // An unknown id falls back to the default rather than rendering an empty
      // grid: a stale link from before a dimension was renamed should still
      // open onto something readable.
      pivotRow: findDimension(searchParams.get('pivotRow'))?.id ?? 'channel',
      pivotCol: findDimension(searchParams.get('pivotCol'))?.id ?? 'universe',
      pivotMetric: findMetric(searchParams.get('pivotMetric'))?.id ?? 'cagr',
      pivotAgg: (AGGS as string[]).includes(searchParams.get('pivotAgg') ?? '')
        ? (searchParams.get('pivotAgg') as AggName)
        : 'median',
    }
  }, [searchParams])

  const update = useCallback(
    (patch: Partial<ReportParams>) => {
      setSearchParams(
        (prev) => {
          const next = new URLSearchParams(prev)
          for (const [k, v] of Object.entries(patch)) {
            // null/'' clears the key rather than writing "null" into the URL.
            if (v == null || v === '' || v === 'all') next.delete(k)
            else next.set(k, String(v))
          }
          return next
        },
        // Control changes replace rather than push: otherwise toggling tax
        // basis four times means four presses of Back to leave the page.
        { replace: true },
      )
    },
    [setSearchParams],
  )

  return [params, update]
}
