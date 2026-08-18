// Mirrors datastore/api/routers/momentum.py's response models — kept
// minimal (only the fields the portfolio/rebalance/universe pages render).

export interface MomentumStrategy {
  strategy_id: string
  band_id: number
  rank_start: number
  rank_end: number
  label: string
}

export interface MomentumTrade {
  id: number
  strategy_id: string
  ticker: string
  purchase_date: string
  qty: number
  purchase_price: number | null
  sale_date: string | null
  sell_price: number | null
  entry_rank: number | null
  exit_rank: number | null
  suggestion_id: number | null
  purchase_rationale: string | null
  sell_rationale: string | null
  journal_entry: string | null
}

export interface MomentumContribution {
  id: number
  strategy_id: string
  contribution_date: string
  amount: number
  note: string | null
}

export interface MomentumSummary {
  strategy_id: string
  as_of_date: string
  capital_invested: number
  current_holdings_value: number
  idle_cash: number
  total_net_worth: number
  cagr: number | null
  xirr: number | null
  total_tax_due: number
  post_tax_value: number
  total_contributed: number
}

export interface MomentumRebalanceNext {
  last_rebalance_date: string | null
  next_rebalance_date: string | null
}

export interface MomentumSuggestion {
  id: number
  rebalance_date: string
  ticker: string
  // [2026-08-18] 'grace_hold' is gone with the grace period. A name is held
  // while it is in the top N on raw momentum and exits the moment it is not.
  action: 'add' | 'exit'
  momentum_rank: number | null
  status: string
}

export interface MomentumRankingRow {
  ticker: string
  company_name: string | null
  momentum_return: number
  momentum_rank: number
  in_top_n: boolean
  return_20d: number | null
  price: number | null
  sparkline: number[]
}

// scripts/run_momentum_experimentation.py's rank-band x lookback x
// rebalance x top_n sweep (bands 1-50 through 501-800), surfaced via
// GET /api/v1/momentum/experimentation.
export interface MomentumExperimentationVariant {
  band_id: number
  rank_start: number
  rank_end: number
  lookback_months: number
  rebalance_period: string
  top_n: number
  cagr: number | null
  sharpe: number | null
  sortino: number | null
  calmar: number | null
  post_tax_cagr: number | null
  sip_xirr: number | null
  win_rate: number | null
  churn_avg_transactions_per_year: number | null
  n_closed_trades: number | null
  n_open_trades: number | null
  avg_days_held: number | null
}

export interface MomentumExperimentationReport {
  generated_at: string | null
  report_file: string
  variants: MomentumExperimentationVariant[]
}

// scripts/run_momentum_dynamic_report.py's consolidated All Risk/Balanced/
// Risk-Managed/Max-Defensive strategy sweep across all 7 rank bands (1-50
// through 501-800), surfaced via GET /api/v1/momentum/dynamic_report.
/** The four cumulative momentum filter presets
 * (features/momentum_strategy.py::build_category_presets). Named once so the
 * report row, the deploy form and the API payload cannot drift apart. */
export type MomentumStrategyCategory = 'all_risk' | 'balanced' | 'risk_managed' | 'max_defensive'

export interface MomentumDynamicReportVariant {
  variant_id: string
  strategy: MomentumStrategyCategory
  band_id: number
  rank_start: number
  rank_end: number
  lookback_months: number
  rebalance_period: string
  top_n: number
  cagr: number | null
  post_tax_cagr: number | null
  total_tax_paid: number | null
  sharpe: number | null
  sortino: number | null
  calmar: number | null
  max_drawdown: number | null
  churn_avg_transactions_per_year: number | null
  win_rate: number | null
  avg_winner_return_pct: number | null
  avg_loser_return_pct: number | null
  total_signals: number | null
  n_closed_trades: number | null
  n_open_trades: number | null
  total_trades: number | null
  avg_days_held: number | null
  rolling_2y_min_cagr: number | null
  rolling_2y_median_cagr: number | null
  rolling_2y_max_cagr: number | null
  rolling_2y_n_windows: number | null
  rolling_3y_min_cagr: number | null
  rolling_3y_median_cagr: number | null
  rolling_3y_max_cagr: number | null
  rolling_3y_n_windows: number | null
  rolling_4y_min_cagr: number | null
  rolling_4y_median_cagr: number | null
  rolling_4y_max_cagr: number | null
  rolling_4y_n_windows: number | null
  income_total_withdrawn: number | null
  income_total_injected: number | null
  income_avg_annual_yield_pct: number | null
  income_years_survived_pct: number | null
  income_n_years: number | null
  value_10L: number | null
  value_10k_sip: number | null
  sip_cagr: number | null
  score: number | null
  is_recommended: boolean | null
  is_most_important: boolean | null
  is_band_most_important: boolean | null
  top_cagr_rank: number | null
  trade_book_file: string | null
}

export interface MomentumDynamicReportYoyRow {
  variant_id: string
  band_id: number
  rank_start: number
  rank_end: number
  lookback_months: number
  rebalance_period: string
  top_n: number
  fy_label: string
  fy_start: string
  fy_end: string
  starting_capital: number | null
  ending_capital: number | null
  return_pct: number | null
  churn: number | null
  avg_holding_days: number | null
  nifty_midcap_150_return_pct: number | null
  nifty_smallcap_250_return_pct: number | null
}

export interface MomentumDynamicReport {
  generated_at: string | null
  report_file: string
  score_formula: string | null
  variants: MomentumDynamicReportVariant[]
  yoy: MomentumDynamicReportYoyRow[]
}

export interface MomentumTriggerResponse {
  job_id: string
  status: string
}

export interface MomentumTriggerStatus {
  job_id: string
  status: 'running' | 'completed' | 'failed' | 'unknown'
  log_tail: string | null
  report_file: string | null
}

// Live Strategy Configuration & Deployment Page (2026-08-08)
// POST/GET/PUT/DELETE /api/v1/momentum/configs
export interface MomentumStrategyConfigCreate {
  band_id: number
  category: MomentumStrategyCategory
  lookback_months: number
  top_n: number
  rebalance_frequency: 'monthly' | 'biweekly'
  // [2026-08-18] grace_period, exit_rank and trailing_stop_pct deprecated.
  // downtrend_filter_pct stays: buy-side filters are retained.
  downtrend_filter_pct?: number | null
  hmm_regime_filter?: 'none' | 'bearish' | 'bearish_sideways'
  initial_capital: number
  sip_amount: number
  start_date: string
  rebalance_day_of_month?: number | null
  portfolio_id?: number | null
}

export interface MomentumStrategyConfigUpdate {
  initial_capital?: number | null
  sip_amount?: number | null
  start_date?: string | null
  rebalance_day_of_month?: number | null
  portfolio_id?: number | null
  is_active?: boolean | null
}

export interface MomentumStrategyConfigResponse extends MomentumStrategyConfigCreate {
  config_id: number
  is_active: boolean
  created_at: string
  updated_at: string
}

export interface MomentumYoyReturnRow {
  fiscal_year: string
  cagr_pct: number
  pnl: number
  max_drawdown_pct: number
  sharpe: number
  sortino: number
  num_trades: number
}

export interface MomentumPortfolio {
  id: number
  name: string
  description?: string
  created_at: string
}
