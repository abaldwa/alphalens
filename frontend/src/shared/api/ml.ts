// ML signals API module — mirrors datastore/api/routers/signals.py.
import { apiGet } from './client'

export interface MLSignalRow {
  date: string
  ticker: string
  model_name: string
  signal_direction: string | null
  buy_prob: number | null
  hold_prob: number | null
  sell_prob: number | null
  q50_return: number | null
  meta_label: string | null
  meta_prob: number | null
  pnd_score: number | null
}

export function getMlSignals(ticker: string, date: string) {
  return apiGet<MLSignalRow[]>(`/api/v1/signals/ml/${ticker}/${date}`, { carry_forward: true })
}

/**
 * No single ML composite score exists on the backend today — this derives
 * one client-side from signal_5d's buy_prob (the model AlphaLens actually
 * trades paper positions off of). Flagged as derived, not authoritative,
 * until a backend aggregator exists (see the Confidence Matrix blueprint).
 */
export function deriveMlScore(rows: MLSignalRow[]): { score: number | null; direction: string | null } {
  const primary = rows.find((r) => r.model_name === 'signal_5d') ?? rows[0]
  if (!primary) return { score: null, direction: null }
  return { score: primary.buy_prob != null ? primary.buy_prob * 100 : null, direction: primary.signal_direction }
}
