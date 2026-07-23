import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query'

import { apiGet, apiPost } from '@/shared/api/client'
import type {
  EquityCurveResponse,
  GateStatusResponse,
  PaperTradingStateResponse,
  PaperTradingTradesResponse,
  PendingActionsResponse,
} from '@/pages/ml/types'

/**
 * Shared paper-trading data layer — extracted from ml/positions.tsx so any
 * page can read/act on the account without duplicating the query/mutation
 * wiring. There is exactly one paper-trading account (/api/v1/paper_trading);
 * positions now carry an optional pillar/template field in position_meta
 * (set at buy time — "ml" for ML-signal/pending-action buys, "technical"
 * for backdated buys made from the Technical > Portfolio page, null for
 * legacy positions opened before this existed). Every page using this hook
 * still sees the same real account/state — pillar-based filtering, where
 * wanted, is left to the caller (e.g. technical/portfolio.tsx filters
 * realPositions down to pillar in (null, 'technical')).
 */
export function usePaperTrading() {
  const queryClient = useQueryClient()

  const state = useQuery({
    queryKey: ['paper-trading-state'],
    queryFn: () => apiGet<PaperTradingStateResponse>('/api/v1/paper_trading/state'),
  })
  const gate = useQuery({
    queryKey: ['paper-trading-gate'],
    queryFn: () => apiGet<GateStatusResponse>('/api/v1/paper_trading/gate_status'),
  })
  const pending = useQuery({
    queryKey: ['paper-trading-pending'],
    queryFn: () => apiGet<PendingActionsResponse>('/api/v1/paper_trading/pending'),
  })
  const equity = useQuery({
    queryKey: ['paper-trading-equity'],
    queryFn: () => apiGet<EquityCurveResponse>('/api/v1/paper_trading/equity_curve'),
  })
  const trades = useQuery({
    queryKey: ['paper-trading-trades'],
    queryFn: () => apiGet<PaperTradingTradesResponse>('/api/v1/paper_trading/trades'),
  })

  const decide = useMutation({
    mutationFn: ({ actionId, decision }: { actionId: string; decision: 'accept' | 'reject' }) =>
      apiPost<{ executed: boolean; status: string; detail?: string }>(`/api/v1/paper_trading/pending/${actionId}/${decision}`),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['paper-trading-pending'] })
      queryClient.invalidateQueries({ queryKey: ['paper-trading-state'] })
    },
  })

  const sell = useMutation({
    mutationFn: (ticker: string) => apiPost(`/api/v1/paper_trading/positions/${ticker}/sell`),
    onSuccess: () => queryClient.invalidateQueries({ queryKey: ['paper-trading-state'] }),
  })

  const realPositions = (state.data?.positions ?? []).filter((p) => p.ticker !== '_HEARTBEAT_')

  return { state, gate, pending, equity, trades, decide, sell, realPositions }
}
