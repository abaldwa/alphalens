// Fundamentals API module — mirrors datastore/api/routers/fundamentals.py.
import { apiGet } from './client'

export interface FundamentalScores {
  ticker: string
  date: string | null
  quality_score: number | null
  growth_score: number | null
  management_quality_score: number | null
}

export function getFundamentalScores(ticker: string) {
  return apiGet<FundamentalScores>(`/api/v1/fundamentals/${ticker}/scores`)
}
