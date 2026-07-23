// Technical Analysis API module — mirrors datastore/api/routers/technical.py.
import { apiGet } from './client'

export interface TARecommendationRow {
  date: string
  ticker: string
  template_name: string
  category: string
  style: string | null
  score: number
  rationale: string
  matched_conditions: number
  total_conditions: number
  outcome: string | null
  outcome_date: string | null
  entry_price: number | null
  exit_price: number | null
  net_return_pct: number | null
}

export interface TARecommendationResponse {
  date: string | null
  ticker: string
  rows: TARecommendationRow[]
  count: number
}

export function getTaRecommendations(ticker: string, date?: string) {
  return apiGet<TARecommendationResponse>(`/api/v1/ta/${ticker}/recommendations`, date ? { date } : {})
}

/**
 * No single technical composite score exists on the backend today — this
 * derives one client-side from the day's matched screener templates
 * (mean of their `score`, direction inferred from the `category` field's
 * bullish/bearish convention). Flagged as derived, not authoritative,
 * until a backend aggregator exists (see the Confidence Matrix blueprint).
 */
export function deriveTechnicalScore(rows: TARecommendationRow[]): { score: number | null; direction: string | null } {
  if (rows.length === 0) return { score: null, direction: null }
  const avg = rows.reduce((sum, r) => sum + r.score, 0) / rows.length
  const bearish = rows.filter((r) => /bear|sell|short/i.test(r.category)).length
  const bullish = rows.filter((r) => /bull|buy|long/i.test(r.category)).length
  const direction = bullish === bearish ? 'hold' : bullish > bearish ? 'buy' : 'sell'
  return { score: avg, direction }
}
