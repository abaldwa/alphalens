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
  grace_remaining: number | null
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
  action: 'add' | 'exit' | 'grace_hold'
  momentum_rank: number | null
  grace_remaining: number | null
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
