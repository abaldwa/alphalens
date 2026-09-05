/**
 * pages/momentum/campaignFormat.ts
 *
 * Shared formatting helpers for the campaign-results page and its
 * per-run analysis panel — split out rather than duplicated so the two
 * files' numbers can never drift (e.g. one rounding CAGR to 1 decimal and
 * the other to 2).
 */

export const EM_DASH = '—'

export function fmtPct(v: number | null | undefined, digits = 2) {
  return typeof v === 'number' && Number.isFinite(v) ? `${(v * 100).toFixed(digits)}%` : EM_DASH
}

export function fmtNum(v: number | null | undefined, digits = 2) {
  return typeof v === 'number' && Number.isFinite(v) ? v.toFixed(digits) : EM_DASH
}

export function fmtInt(v: number | null | undefined) {
  return typeof v === 'number' && Number.isFinite(v) ? String(Math.round(v)) : EM_DASH
}

export function fmtInr(v: number | null | undefined) {
  if (typeof v !== 'number' || !Number.isFinite(v)) return EM_DASH
  const sign = v < 0 ? '-' : ''
  return `${sign}₹${Math.abs(v).toLocaleString('en-IN', { maximumFractionDigits: 0 })}`
}

/**
 * Sign-based colour for a percentage/return figure: amber for negative,
 * green for positive, unstyled for zero/missing. Amber rather than the
 * more usual red — explicit user choice, applied consistently everywhere
 * a signed return is shown (tables and stat cards alike).
 */
export function signClass(v: number | null | undefined): string {
  if (typeof v !== 'number' || !Number.isFinite(v) || v === 0) return ''
  return v > 0 ? 'text-green' : 'text-amber'
}

export type StatTone = 'default' | 'green' | 'amber'

export function signTone(v: number | null | undefined): StatTone {
  if (typeof v !== 'number' || !Number.isFinite(v) || v === 0) return 'default'
  return v > 0 ? 'green' : 'amber'
}
