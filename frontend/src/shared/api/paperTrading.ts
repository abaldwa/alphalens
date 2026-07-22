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
 * wiring. There is currently exactly one paper-trading account
 * (/api/v1/paper_trading), driven entirely by ML buy/sell signals
 * (buy_prob_entry/current on every position) — it has no strategy-source
 * or channel field, so it cannot be filtered down to "Technical-only"
 * positions without fabricating data. Every page using this hook shows the
 * same real account.
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
