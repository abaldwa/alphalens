// Mirrors the relevant subset of datastore/api/schemas.py for the ML
// section pages — only fields these pages actually render.

export interface MLSignalRow {
  date: string
  ticker: string
  model_name: string
  signal_direction: string | null
  buy_prob: number | null
  hold_prob: number | null
  sell_prob: number | null
  q10_return: number | null
  q50_return: number | null
  q90_return: number | null
  meta_label: string | null
  meta_prob: number | null
  conformal_lower: number | null
  conformal_upper: number | null
  pnd_score: number | null
  pnd_phase: string | null
  pnd_block: boolean | null
  hmm_regime: string | null
  hmm_regime_prob: number | null
  hmm_stability: number | null
  exit_urgency: number | null
  exit_type: string | null
  shap_top5_json: string | null
  is_backfill: boolean | null
}

export interface SignalUniverseRow {
  ticker: string
  date: string
  buy_prob: number | null
  q50_return: number | null
  meta_label_prob: number | null
  pnd_score: number | null
  forensic_flag: string | null
  mb_probability: number | null
  shap_top5_json: string | null
}

export interface ForensicRow {
  date: string
  ticker: string
  forensic_composite: number | null
  forensic_flag_label: string | null
  forensic_ml_prob: number | null
}

export interface MultibaggerRow {
  date: string
  ticker: string
  mb_probability: number | null
  mb_tier: string | null
  mb_archetype: string | null
  survival_6m: number | null
  survival_12m: number | null
  survival_18m: number | null
  survival_24m: number | null
  survival_36m: number | null
}

export interface OHLCVRow {
  date: string
  ticker: string
  open: number
  high: number
  low: number
  close: number
  volume: number
}

export interface RegimeResponse {
  date: string | null
  hmm_regime: string | null
  hmm_regime_prob: number | null
  hmm_stability: number | null
  available: boolean
}

export interface RegimeHistoryRow {
  date: string | null
  hmm_regime: string | null
  hmm_regime_prob: number | null
  hmm_stability: number | null
}

export interface RegimeHistoryResponse {
  days: RegimeHistoryRow[]
}

export interface AlertRow {
  date: string
  ticker: string | null
  alert_type: string
  severity: string
  message: string
}

export interface AlertsResponse {
  date: string
  alerts: AlertRow[]
  count: number
}

export interface BacktestReportsResponse {
  reports: string[]
}

// backtest reports are free-form dicts written by backtest/engine.py — no
// fixed schema server-side, so keep this loose and render generically.
export type BacktestReport = Record<string, unknown>

export interface HoldingRow {
  id: number
  ticker: string
  purchase_date: string
  purchase_price: number | null
  qty: number
  sale_date: string | null
  sell_price: number | null
  purchase_rationale: string | null
}

export interface PaperTradingPosition {
  ticker: string
  company_name: string | null
  sector: string
  entry_date: string
  entry_price: number
  quantity: number
  peak_price: number
  current_price: number | null
  unrealised_pnl_pct: number | null
  buy_prob_entry: number | null
  buy_prob_current: number | null
  target_price: number | null
  target_date: string | null
  exit_criterion: string | null
  stock_gain_pct: number | null
  nifty_gain_pct: number | null
}

export interface PaperTradingStateResponse {
  as_of_date: string | null
  cash: number
  total_equity: number
  initial_capital: number
  positions: PaperTradingPosition[]
  available: boolean
}

export interface PaperTradingTrade {
  date: string
  ticker: string
  signal_type: string
  entry_price: number
  quantity: number
  exit_price: number | null
  exit_date: string | null
  exit_type: string | null
  pnl: number | null
  pnl_pct: number | null
}

export interface PaperTradingTradesResponse {
  trades: PaperTradingTrade[]
  count: number
}

export interface ExitUrgencyRow {
  ticker: string
  company_name: string | null
  entry_date: string
  entry_price: number
  current_price: number | null
  unrealised_pnl_pct: number | null
  exit_urgency: number | null
  exit_type: string | null
}

export interface ExitUrgencyResponse {
  rows: ExitUrgencyRow[]
  as_of_date: string | null
}

export interface EquityCurveResponse {
  points: { date: string; equity: number }[]
}

export interface GateStatusResponse {
  days_count: number
  gate_threshold: number
  gate_cleared: boolean
}

export interface PendingActionRow {
  action_id: string
  date: string
  action_type: string
  ticker: string
  company_name: string | null
  price: number | null
  target_price: number | null
  duration_days: number | null
  reason: string
  status: string
}

export interface PendingActionsResponse {
  date: string | null
  actions: PendingActionRow[]
}

export interface WatchlistResponse {
  tickers: Record<string, unknown>[]
  low_liquidity_tickers: Record<string, unknown>[]
  implemented: boolean
  notes: string
}

export interface SectorRotationRow {
  sector: string
  index_name: string
  rank: number
  relative_strength: number | null
  rs_1d: number | null
  rs_5d: number | null
  rs_21d: number | null
  rs_63d: number | null
  sector_market_cap_cr: number | null
  top_stocks: { ticker: string; buy_prob?: number | null }[]
  sparkline: number[]
  nifty500_sparkline: number[]
}

export interface SectorRotationReport {
  as_of_date: string | null
  sectors: SectorRotationRow[]
}

export interface SectorAccumulationRow {
  date: string
  sector: string
  accumulation_score: number
  delivery_volume: number
  sector_shares_outstanding: number
  n_stocks_included: number
}

export interface SectorAccumulationDrilldownRow {
  ticker: string
  volume: number
  delivery_pct: number
  delivery_volume: number
  shares_outstanding: number
  contribution_pct: number
}

export interface DailyWatchlistRow {
  ticker: string
  company_name: string | null
  sector: string | null
  horizon: string
  horizon_days: number
  current_price: number | null
  buy_prob: number | null
  signal_direction: string | null
  target_price: number | null
  target_low: number | null
  target_high: number | null
  expected_return_pct: number | null
  target_basis: string
}

export interface DailyWatchlistResponse {
  date: string | null
  rows: DailyWatchlistRow[]
  multibagger: Record<string, unknown>[]
  low_liquidity_multibagger: Record<string, unknown>[]
  count: number
}
