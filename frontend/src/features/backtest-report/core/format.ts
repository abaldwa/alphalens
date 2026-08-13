/**
 * features/backtest-report/format.ts
 *
 * One formatter per unit, shared by every table in the section. Two rules are
 * encoded here rather than left to each call site:
 *
 * 1. A return is a RATE — see AGENTS.md. `rate()` appends "%/yr" so a CAGR can
 *    never be read as a total over the window. Anything that is genuinely a
 *    point-in-time percentage (win rate, drawdown, a single trade's P&L) uses
 *    `pct()`, which has no per-year suffix. Picking the wrong one is a
 *    labelling bug, so they are deliberately different functions rather than
 *    one function with a flag.
 * 2. Null is not zero. Every formatter returns EM_DASH for null/undefined/NaN,
 *    and the cell renderers pair that with the pending-field reason.
 */

export const EM_DASH = '—'

function bad(v: number | null | undefined): boolean {
  return v == null || !Number.isFinite(v)
}

/** An annualised rate held as a fraction (0.243 -> "24.3%/yr"). */
export function rate(v: number | null | undefined, digits = 1): string {
  if (bad(v)) return EM_DASH
  return `${(v! * 100).toFixed(digits)}%/yr`
}

/** A rate difference, signed: excess return over a benchmark. */
export function rateDelta(v: number | null | undefined, digits = 1): string {
  if (bad(v)) return EM_DASH
  const s = (v! * 100).toFixed(digits)
  return `${v! > 0 ? '+' : ''}${s} pp/yr`
}

/** A plain percentage that is NOT a rate: win rate, drawdown, share of years. */
export function pct(v: number | null | undefined, digits = 1): string {
  if (bad(v)) return EM_DASH
  return `${(v! * 100).toFixed(digits)}%`
}

export function num(v: number | null | undefined, digits = 2): string {
  if (bad(v)) return EM_DASH
  return v!.toFixed(digits)
}

export function int(v: number | null | undefined): string {
  if (bad(v)) return EM_DASH
  return Math.round(v!).toLocaleString('en-IN')
}

/** Indian-format rupees, abbreviated once past a lakh — a table of 20
 * strategies' final capital is unreadable at full precision. */
export function inr(v: number | null | undefined): string {
  if (bad(v)) return EM_DASH
  const n = v!
  const abs = Math.abs(n)
  if (abs >= 1e7) return `₹${(n / 1e7).toFixed(2)} Cr`
  if (abs >= 1e5) return `₹${(n / 1e5).toFixed(2)} L`
  return `₹${Math.round(n).toLocaleString('en-IN')}`
}

export function days(v: number | null | undefined): string {
  if (bad(v)) return EM_DASH
  return `${v!.toFixed(0)}d`
}

/** Years spanned by a window, for the 3y/5y/10y labels. */
export function years(v: number | null | undefined): string {
  if (bad(v)) return EM_DASH
  return `${v!.toFixed(1)}y`
}
