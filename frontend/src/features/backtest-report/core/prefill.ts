/**
 * features/backtest-report/core/prefill.ts
 *
 * Encoding for the deploy hand-off's ?prefill= parameter. Pure, so it lives
 * in core rather than inside the sessionStorage hook that happens to produce
 * the keys.
 */

import type { StrategyKey } from './types'

/** The ?prefill= value for the deploy page: keys in selection order. */
export function prefillParam(keys: StrategyKey[]): string {
  return keys.join(',')
}

export function parsePrefillParam(value: string | null): StrategyKey[] {
  if (!value) return []
  return value
    .split(',')
    .map((s) => s.trim())
    .filter(Boolean)
}
