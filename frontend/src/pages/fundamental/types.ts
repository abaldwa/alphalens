// Mirrors the subset of datastore/api/schemas.py fundamentals/governance
// response shapes actually rendered by these pages.

export interface FundamentalsRow {
  ticker: string
  fiscal_year: number
  quarter: number
  quarter_end_date: string
  announcement_date: string
  revenue: number | null
  ebitda: number | null
  pat: number | null
  eps: number | null
  operating_margin: number | null
  net_margin: number | null
  roe: number | null
  roce: number | null
  debt_to_equity: number | null
}

export interface FundamentalsResponse {
  ticker: string
  as_of: string
  data: FundamentalsRow[]
  record_count: number
}

export interface FARatiosResponse {
  ticker: string
  date: string | null
  available: boolean
  ratios: Record<string, number | null>
}

export interface FAScoresResponse {
  ticker: string
  date: string | null
  quality_score: number | null
  growth_score: number | null
  management_quality_score: number | null
}

export interface FAPeerRow {
  ticker: string
  roe: number | null
  roce: number | null
  debt_to_equity: number | null
  pe_ratio: number | null
}

export interface FAPeersResponse {
  ticker: string
  date: string | null
  sector: string | null
  peers: FAPeerRow[]
}

export interface FASectorResponse {
  sector: string
  date: string | null
  ticker_count: number
  avg_ratios: Record<string, number | null>
  note: string
}

export interface FAScreenerResponse {
  preset: string
  date: string | null
  tickers: string[]
}

export interface GovernanceRow {
  ticker: string
  quarter_end_date: string
  filing_date: string
  promoter_pct: number | null
  promoter_pledge: number | null
  fii_pct: number | null
  dii_pct: number | null
  mf_pct: number | null
  retail_pct: number | null
  superstar_flag: boolean | null
  superstar_change: number | null
}

export interface GovernanceResponse {
  ticker: string
  as_of: string
  data: GovernanceRow[]
  record_count: number
}
