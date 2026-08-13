/**
 * features/backtest-report/deploy/useDeploySelection.ts
 *
 * Which strategies the user has ticked for deployment, held in sessionStorage
 * keyed by StrategyKey.
 *
 * It has to outlive navigation: the whole point of the six sections is that
 * you compare returns here, check drawdown there, and confirm consistency
 * somewhere else before deciding. A selection that resets on every hop would
 * make the section unusable for the exact workflow it was built for.
 *
 * sessionStorage rather than localStorage: a deploy shortlist is a decision
 * you are making now, not a preference. Finding last month's ticks still
 * present would be a trap.
 *
 * The state is shared across every component that calls this hook, via a
 * module-level subscriber set — otherwise the sticky footer and the table's
 * checkboxes would each hold their own copy and disagree.
 */

import { useCallback, useSyncExternalStore } from 'react'

import type { StrategyKey } from '../core/types'

const STORAGE_KEY = 'backtest_report_deploy_selection'

const listeners = new Set<() => void>()
let cached: StrategyKey[] | null = null

function read(): StrategyKey[] {
  if (cached) return cached
  try {
    const raw = sessionStorage.getItem(STORAGE_KEY)
    const parsed = raw ? JSON.parse(raw) : []
    cached = Array.isArray(parsed) ? parsed.filter((k) => typeof k === 'string') : []
  } catch {
    // A corrupt or unavailable store must not take the page down; an empty
    // selection is a safe reading of "we don't know what was ticked".
    cached = []
  }
  return cached
}

function write(keys: StrategyKey[]) {
  cached = keys
  try {
    sessionStorage.setItem(STORAGE_KEY, JSON.stringify(keys))
  } catch {
    // Private-browsing quota failures leave the in-memory copy authoritative
    // for this session, which is still better than throwing mid-click.
  }
  for (const l of listeners) l()
}

function subscribe(listener: () => void) {
  listeners.add(listener)
  return () => listeners.delete(listener)
}

const EMPTY: StrategyKey[] = []

export function useDeploySelection() {
  const selected = useSyncExternalStore(
    subscribe,
    read,
    // Server/prerender snapshot: a stable reference, or React loops.
    () => EMPTY,
  )

  const toggle = useCallback((key: StrategyKey) => {
    const current = read()
    write(
      current.includes(key)
        ? current.filter((k) => k !== key)
        : [...current, key],
    )
  }, [])

  const clear = useCallback(() => write([]), [])

  const isSelected = useCallback(
    (key: StrategyKey) => selected.includes(key),
    [selected],
  )

  return { selected, toggle, clear, isSelected, count: selected.length }
}
