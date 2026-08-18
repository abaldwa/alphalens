/**
 * features/backtest-report/core/cagrOn.ts
 *
 * "The CAGR on the currently selected basis" — one line, but it moved out of
 * ui/columns.tsx so that core modules (the pivot, and its type-only self-check)
 * can use it without importing a .tsx file and pulling React in behind it.
 * ui/columns.tsx re-exports it, so existing call sites are unchanged.
 */

import type { StrategyReport, TaxBasis } from './types'

/** Post-tax is the headline; the pre-tax figure stays its own column rather
 * than being swapped in silently, so the two are never confused. */
export function cagrOn(r: StrategyReport, basis: TaxBasis): number | null {
  return basis === 'post_tax' ? r.returns.cagrPostTax : r.returns.cagrPreTax
}
