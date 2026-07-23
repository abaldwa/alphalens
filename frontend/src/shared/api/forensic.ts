// Forensic API module — mirrors datastore/api/routers/forensic.py.
import { apiGet } from './client'

export interface ForensicRow {
  date: string
  ticker: string
  forensic_composite: number | null
  forensic_flag_label: string | null
  forensic_ml_prob: number | null
}

export function getForensicRow(ticker: string) {
  return apiGet<ForensicRow | null>(`/api/v1/signals/ml/forensic/${ticker}`)
}
