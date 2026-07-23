// Valuation API module — mirrors datastore/api/routers/valuation.py.
import { apiGet } from './client'

export interface ValuationResult {
  ticker: string
  as_of_date: string | null
  lifecycle_stage: string | null
  intrinsic_value: number | null
  current_price: number | null
  valuation_gap_pct: number | null
  margin_of_safety: number | null
  wacc: number | null
  cost_of_equity: number | null
  dcf_model_type: string | null
  scenario_bull: number | null
  scenario_base: number | null
  scenario_bear: number | null
  mc_probability_undervalued: number | null
  data_quality?: string | null
}

export function getValuation(ticker: string) {
  return apiGet<ValuationResult>(`/api/v1/valuation/${ticker}`)
}
