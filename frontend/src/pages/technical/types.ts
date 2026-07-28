// Mirrors datastore/api/schemas.py TAWatchlistRow/Response and
// TAMarketOverviewResponse/TASectorBreadthRow — kept minimal (only the
// fields this page actually renders).

export interface TAWatchlistStrategyMatch {
  template_name: string
  template_description: string | null
  template_strategy_description: string | null
  category: string
  date: string
  score: number
  rationale: string
  matched_conditions: number
  total_conditions: number
  key_values: Record<string, number | null>
}

export interface TAWatchlistRow {
  ticker: string
  company_name: string | null
  sector: string | null
  market_cap_cr: number | null
  /** Rank by market cap over the full universe (1 = largest), computed
   * live from config/universe.py's market_cap_cr — null for tickers with
   * an unknown/not-yet-sourced market cap, never a fabricated number. */
  market_cap_rank: number | null
  recommendation_date: string | null
  recommended_price: number | null
  current_price: number | null
  /** Every template that fired for this ticker in the lookback window,
   * most recent first — a ticker can carry several. */
  strategies: TAWatchlistStrategyMatch[]
  resistance_levels: number[]
  support_levels: number[]
}

export interface TAWatchlistResponse {
  date: string | null
  rows: TAWatchlistRow[]
  count: number
}

export interface EventRow {
  date: string
  event_type: string
  description: string
}

export interface TASectorBreadthRow {
  sector: string
  advances: number
  declines: number
  unchanged: number
  avg_change_pct: number | null
}

export interface TAMarketOverviewResponse {
  date: string | null
  advances: number
  declines: number
  unchanged: number
  sector_breadth: TASectorBreadthRow[]
  available: boolean
}

// --- Screener (SPEC-TA-005) ---
export interface TATemplateInfo {
  name: string
  category: string
  description: string
  condition_count: number
}
export interface TATemplateListResponse {
  templates: TATemplateInfo[]
  count: number
}
export interface TAScreenerRow {
  ticker: string
  date: string
  template_name: string
  matched_conditions: number
  total_conditions: number
  score: number
  key_values: Record<string, number | null>
}
export interface TAScreenerResponse {
  template_name: string
  date: string | null
  rows: TAScreenerRow[]
  count: number
}

export interface TATickerProfileResponse {
  ticker: string
  company_name: string | null
  sector: string | null
}

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

export interface TAStrategyWinRateRow {
  template_name: string
  category: string
  description: string
  style: string
  times_recommended: number
  wins: number
  losses: number
  pending: number
  win_rate: number | null
  wilson_lo: number | null
  wilson_hi: number | null
  baseline_win_rate: number | null
  delta_vs_baseline: number | null
  deflated_sharpe: number | null
  sortino: number | null
  calmar: number | null
  tier: string
  reasons: string[]
}
export interface TAStrategyWinRateResponse {
  styles: Record<string, TAStrategyWinRateRow[]>
}

// --- Alerts (SPEC-TA-006 / SPEC-TA-009) ---
export interface TAAlertRow {
  date: string
  ticker: string
  template_name: string
  category: string
  score: number
  matched_conditions: number
  total_conditions: number
  key_values: Record<string, number | null>
}
export interface TAAlertResponse {
  as_of_date: string | null
  rows: TAAlertRow[]
  count: number
}
export interface TAUserAlertRow {
  alert_id: number
  ticker: string
  template_name: string
  category: string
  active: boolean
  last_triggered_date: string | null
  triggered_today: boolean
}
export interface TAUserAlertResponse {
  rows: TAUserAlertRow[]
  count: number
}

// --- Compare (SPEC-TA endpoints) ---
export interface TACompareTickerRow {
  ticker: string
  rs_vs_nifty500_21d: number | null
  beta_63d: number | null
  alpha_21d: number | null
}
export interface TACompareResponse {
  date: string | null
  rows: TACompareTickerRow[]
  correlation: Record<string, Record<string, number | null>>
}

// --- Chart (ohlcv + indicators + patterns) ---
export interface OHLCVRow {
  date: string
  ticker: string
  open: number
  high: number
  low: number
  close: number
  volume: number
  adjusted_close: number | null
  delivery_pct: number | null
}
export interface OHLCVResponse {
  ticker: string
  start_date: string
  end_date: string
  data: OHLCVRow[]
  record_count: number
}
export interface TAIndicatorsResponse {
  ticker: string
  date: string | null
  available: boolean
  indicators: Record<string, number | null>
}
export interface TAPatternsResponse {
  ticker: string
  date: string | null
  available: boolean
  patterns: Record<string, number | null>
}

// --- Deep Dive (summary) ---
export interface TASummaryResponse {
  ticker: string
  date: string | null
  available: boolean
  cmp: number | null
  week52_high: number | null
  week52_low: number | null
  sma_20: number | null
  sma_50: number | null
  sma_100: number | null
  sma_200: number | null
  ema_9: number | null
  ema_21: number | null
  rsi_14: number | null
  supertrend_value: number | null
  supertrend_dir: number | null
  macd: number | null
  macd_signal: number | null
  macd_hist: number | null
  vwap_20d: number | null
  dist_from_52w_high: number | null
  dist_from_52w_low: number | null
  sma_50_200_ratio: number | null
  delivery_pct: number | null
  avg_delivery_pct_21d: number | null
  delivery_pct_zscore_21d: number | null
}
