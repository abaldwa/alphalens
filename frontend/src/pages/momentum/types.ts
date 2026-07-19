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
