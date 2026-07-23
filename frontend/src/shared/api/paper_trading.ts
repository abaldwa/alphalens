// Unified Paper Trading API module — mirrors
// datastore/api/routers/paper_trading_unified.py (Phase 5 of the Unified
// Backtest & Paper Trading Umbrella, see BacktestUmbrellaPlan.md).
// Distinct from the existing ML-only /api/v1/paper_trading/* client
// (still used elsewhere) — this is the channel-aware
// /api/v1/paper_trading2/{channel}/{strategy_id}/* surface.
import { apiGet, apiPost } from './client'
import type { BacktestChannel } from './backtest'

export interface PendingAction {
  action_id: string
  channel: string
  strategy_id: string
  as_of_date: string
  ticker: string
  action: string
  sector: string
  conviction: number
  adtv_cr: number | null
  status: string
  proposed_at: string
  decided_at: string | null
  executed_price: number | null
  executed_quantity: number | null
}

export interface PendingActionsListResponse {
  channel: string
  strategy_id: string
  as_of_date: string
  actions: PendingAction[]
}

export interface GateStatus {
  channel: string
  strategy_id: string
  days_completed: number
  gate_threshold: number
  gate_passed: boolean
}

export interface StateSummary {
  channel: string
  strategy_id: string
  cash: number
  initial_capital: number
  total_contributed: number
  n_open_positions: number
  n_closed_trades: number
}

export function listPendingActions(channel: BacktestChannel, strategyId: string, asOfDate: string) {
  return apiGet<PendingActionsListResponse>(`/api/v1/paper_trading2/${channel}/${strategyId}/pending`, {
    as_of_date: asOfDate,
  })
}

export function getGateStatus(channel: BacktestChannel, strategyId: string) {
  return apiGet<GateStatus>(`/api/v1/paper_trading2/${channel}/${strategyId}/gate_status`)
}

export function getStateSummary(channel: BacktestChannel, strategyId: string) {
  return apiGet<StateSummary>(`/api/v1/paper_trading2/${channel}/${strategyId}/state`)
}

export function acceptPendingAction(
  channel: BacktestChannel,
  strategyId: string,
  actionId: string,
  body: {
    as_of_date: string
    price: number
    prices?: Record<string, number>
    horizon_bucket?: string
    initial_capital?: number
  },
) {
  return apiPost<PendingAction>(`/api/v1/paper_trading2/${channel}/${strategyId}/pending/${actionId}/accept`, body)
}

export function rejectPendingAction(channel: BacktestChannel, strategyId: string, actionId: string, asOfDate: string) {
  return apiPost<PendingAction>(`/api/v1/paper_trading2/${channel}/${strategyId}/pending/${actionId}/reject`, {
    as_of_date: asOfDate,
  })
}
