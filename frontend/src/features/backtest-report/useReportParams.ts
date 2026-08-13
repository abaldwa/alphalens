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

import { CHANNELS } from './strategyKey'
import type { Channel, ReturnMode, StrategyKey, TaxBasis } from './types'

/** Presets for the window selector. `max` means "whatever the run covers". */
export const WINDOW_PRESETS = ['3y', '5y', '10y', '15y', 'max'] as const
export type WindowPreset = (typeof WINDOW_PRESETS)[number]

export const WINDOW_LABELS: Record<WindowPreset, string> = {
  '3y': '3 years',
  '5y': '5 years',
  '10y': '10 years',
  '15y': '15 years',
  max: 'Max available',
}

/**
 * Data before this date crosses the 2007-04-02 legacy/Fyers seam and the
 * unrepaired pre-2017 corporate actions (A99-A102). Windows reaching past it
 * are flagged in the UI rather than silently trusted.
 */
export const RELIABLE_FROM = '2009-04-01'

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

/** Resolve a preset to concrete dates, anchored to the report's own latest
 * date rather than today — anchoring to today silently shortens every window
 * when the pipeline is a few days behind, which it periodically is. */
export function resolveWindow(
  preset: WindowPreset,
  latestDate: string | null,
  explicit?: { startDate: string | null; endDate: string | null },
): { startDate: string | null; endDate: string | null } {
  if (explicit?.startDate && explicit?.endDate) return explicit
  if (!latestDate) return { startDate: null, endDate: null }
  if (preset === 'max') return { startDate: null, endDate: latestDate }
  const years = Number(preset.replace('y', ''))
  const end = new Date(latestDate)
  if (Number.isNaN(end.getTime())) return { startDate: null, endDate: null }
  const start = new Date(end)
  start.setFullYear(start.getFullYear() - years)
  return {
    startDate: start.toISOString().slice(0, 10),
    endDate: latestDate,
  }
}

/** True when the window reaches into the unreliable pre-2009 history. */
export function crossesUnreliableHistory(startDate: string | null): boolean {
  return startDate != null && startDate < RELIABLE_FROM
}
