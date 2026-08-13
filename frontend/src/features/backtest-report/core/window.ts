/**
 * features/backtest-report/core/window.ts
 *
 * Backtest window presets and the pure date arithmetic behind them. Kept in
 * core, separate from the URL-state hook that drives them, so the rules stay
 * testable and survive a UI or router change.
 */

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
