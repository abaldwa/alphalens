export interface ForensicRow {
  date: string
  ticker: string
  beneish_m: number | null
  altman_z: number | null
  piotroski_f: number | null
  ohlson_o: number | null
  dechow_f: number | null
  sloan_accrual: number | null
  benford_mad: number | null
  benford_detail_json: string | null
  forensic_composite: number | null
  forensic_flag: boolean | null
  forensic_flag_label: string | null
  forensic_ml_prob: number | null
  shap_top5_json: string | null
  pattern_match: string | null
}

export interface ForensicFlaggedRow {
  ticker: string
  date: string
  forensic_composite: number | null
  forensic_flag_label: string | null
}

export interface ForensicFlaggedResponse {
  as_of_date: string | null
  rows: ForensicFlaggedRow[]
}

export interface ForensicSummaryResponse {
  as_of_date: string | null
  red_count: number
  amber_count: number
  green_count: number
  total_scored: number
  available: boolean
}

export function flagBadgeVariant(label: string | null | undefined): 'success' | 'destructive' | 'warning' | 'outline' {
  if (label === 'green') return 'success'
  if (label === 'red' || label === 'black') return 'destructive'
  if (label === 'amber' || label === 'orange' || label === 'yellow') return 'warning'
  return 'outline'
}
