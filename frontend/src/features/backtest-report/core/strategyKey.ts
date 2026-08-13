/**
 * features/backtest-report/strategyKey.ts
 *
 * One canonical identity and one display label for a strategy, everywhere.
 *
 * Today the same strategy is called four different things — `variant_id` in
 * the momentum report, `strategy_id` + `run_id` in backtest runs,
 * `template_name` + `exit_variant` in technical, and a localStorage
 * `momentum_strategy_id` on the deploy page — and even within the momentum
 * report it is LABELLED differently on the hub (`STRATEGY_LABELS[v.strategy]`)
 * than in the YoY matrix (`rowLabel()`). That is why nothing links to
 * anything: there is no shared identifier to link on.
 *
 * The key format matches the backend registry's `strategy_key` exactly
 * (`{channel}:{name}`, strategies/registry.py::strategy_key), so when A89/A95
 * land and the engines emit it natively, this file becomes a pass-through and
 * nothing else in the UI changes.
 *
 * It is readable rather than hashed deliberately: it appears in URLs and in
 * report JSON, and someone debugging a report has to be able to read it.
 */

import type { Channel, StrategyKey } from './types'

export const CHANNELS: Channel[] = ['momentum', 'technical', 'fundamental', 'ml']

export function formatKey(channel: Channel, name: string): StrategyKey {
  if (!name) throw new Error('strategy name is required')
  if (name.includes(':')) {
    // A ':' would make the key ambiguous to parse back.
    throw new Error(`strategy name must not contain ':' (got "${name}")`)
  }
  return `${channel}:${name}`
}

export function parseKey(key: StrategyKey): { channel: Channel; name: string } {
  const idx = key.indexOf(':')
  if (idx <= 0) throw new Error(`malformed strategy key: "${key}"`)
  const channel = key.slice(0, idx) as Channel
  const name = key.slice(idx + 1)
  if (!CHANNELS.includes(channel) || !name) {
    throw new Error(`malformed strategy key: "${key}"`)
  }
  return { channel, name }
}

export function isStrategyKey(value: string): boolean {
  try {
    parseKey(value)
    return true
  } catch {
    return false
  }
}

// ---------------------------------------------------------------------------
// display labels
// ---------------------------------------------------------------------------

export const MOMENTUM_CATEGORY_LABELS: Record<string, string> = {
  all_risk: 'All Risk',
  balanced: 'Balanced',
  risk_managed: 'Risk-Managed',
  max_defensive: 'Max Defensive',
}

/** Momentum variant ids look like
 * `balanced_b1_1-50_lb6mo_monthly_top15`. The report builds this string
 * inline, and strategies/migrations/momentum.py reproduces it verbatim so
 * registry rows and report rows join without a translation table. */
const MOMENTUM_VARIANT_RE =
  /^(?<category>[a-z_]+)_b(?<band>\d+)_(?<rankStart>\d+)-(?<rankEnd>\d+)_lb(?<lookback>\d+)mo_(?<rebalance>[a-z]+)_top(?<topN>\d+)$/

export interface MomentumVariantParts {
  category: string
  bandId: number
  rankStart: number
  rankEnd: number
  lookbackMonths: number
  rebalance: string
  topN: number
}

export function parseMomentumVariant(
  variantId: string,
): MomentumVariantParts | null {
  const m = MOMENTUM_VARIANT_RE.exec(variantId)
  if (!m?.groups) return null
  const g = m.groups
  return {
    category: g.category,
    bandId: Number(g.band),
    rankStart: Number(g.rankStart),
    rankEnd: Number(g.rankEnd),
    lookbackMonths: Number(g.lookback),
    rebalance: g.rebalance,
    topN: Number(g.topN),
  }
}

/**
 * The ONE display label. Every screen calls this, so a strategy reads
 * identically on the recommendations page, in the YoY matrix, and on its own
 * detail page.
 *
 * Grace cycles are not in the variant id (the sweep holds them constant at 2),
 * so they are passed in when a caller has them rather than being parsed out of
 * a string that does not carry them.
 */
export function displayLabel(
  key: StrategyKey,
  extras?: { graceCycles?: number | null; exitVariant?: string | null },
): string {
  const { channel, name } = parseKey(key)

  if (channel === 'momentum') {
    const p = parseMomentumVariant(name)
    if (p) {
      const category = MOMENTUM_CATEGORY_LABELS[p.category] ?? p.category
      const parts = [
        category,
        `Top${p.topN}`,
        `${p.lookbackMonths}mo`,
        p.rebalance,
        `rank ${p.rankStart}-${p.rankEnd}`,
      ]
      if (extras?.graceCycles != null) parts.push(`g${extras.graceCycles}`)
      return parts.join(' · ')
    }
    // Preset rows registered without a grid point.
    if (name.startsWith('preset_')) {
      const cat = name.slice('preset_'.length)
      return `${MOMENTUM_CATEGORY_LABELS[cat] ?? cat} (preset)`
    }
    return name
  }

  if (channel === 'technical') {
    return extras?.exitVariant ? `${name} · ${extras.exitVariant}` : name
  }

  return name
}

/** Short form for tight cells — drops the rank band and rebalance detail but
 * keeps what distinguishes one row from its neighbours in a sweep. */
export function shortLabel(key: StrategyKey): string {
  const { channel, name } = parseKey(key)
  if (channel === 'momentum') {
    const p = parseMomentumVariant(name)
    if (p) {
      const category = MOMENTUM_CATEGORY_LABELS[p.category] ?? p.category
      return `${category} · Top${p.topN} · ${p.lookbackMonths}mo`
    }
  }
  return name
}

// ---------------------------------------------------------------------------
// deep links
// ---------------------------------------------------------------------------

export type ReportSection =
  | ''
  | 'recommendations'
  | 'returns'
  | 'consistency'
  | 'risk'
  | 'trade-quality'

/**
 * URL for a section with a strategy selected. Every cross-view link goes
 * through this, so the chain (matrix → rolling → sweep → YoY → income → back)
 * is one mechanism rather than a set of bespoke jumps — the current one being
 * a `window.location.href` assignment that throws away SPA state.
 */
export function sectionUrl(
  section: ReportSection,
  params?: {
    strategy?: StrategyKey | null
    channel?: Channel | null
    window?: string | null
    benchmark?: string | null
    taxBasis?: string | null
    mode?: string | null
  },
): string {
  const base = section ? `/backtest-report/${section}` : '/backtest-report'
  const qs = new URLSearchParams()
  if (params?.strategy) qs.set('strategy', params.strategy)
  if (params?.channel) qs.set('channel', params.channel)
  if (params?.window) qs.set('window', params.window)
  if (params?.benchmark) qs.set('benchmark', params.benchmark)
  if (params?.taxBasis) qs.set('taxBasis', params.taxBasis)
  if (params?.mode) qs.set('mode', params.mode)
  const q = qs.toString()
  return q ? `${base}?${q}` : base
}

export function strategyDetailUrl(key: StrategyKey): string {
  return `/backtest-report/strategy/${encodeURIComponent(key)}`
}
