/**
 * features/backtest-report/data/useCreateDeployment.ts
 *
 * The deploy hand-off: POST /api/v1/deployments (A91).
 *
 * This replaces the momentum-only /api/v1/momentum/configs path, which was
 * momentum-shaped down to its column names and left three channels out of four
 * with no deploy route at all. A deployment REFERENCES a strategy_registry
 * row, so nothing about the rules travels in this body — only the decisions
 * about how it is being run.
 *
 * The caller must merge in the four DeploymentDecisions (capital, start date,
 * SIP, portfolio) before submitting; toDeploymentRequest returns them as
 * `unmapped` precisely so the form requires them rather than defaulting them.
 */

import { useMutation, useQueryClient } from '@tanstack/react-query'

import { apiPost } from '@/shared/api/client'

import type { DeploymentRequest } from '../core/toConfigForm'

export interface DeploymentResponse {
  deployment_id: number
  strategy_key: string
  strategy_version: number
  channel: string
  initial_capital: number
  sip_amount: number
  start_date: string
  portfolio_id: number
  rebalance_frequency: string | null
  rebalance_day_of_month: number | null
  benchmark_index_name: string | null
  capital_mode: string
  source_run_id: string | null
  is_active: boolean
}

export function useCreateDeployment() {
  const queryClient = useQueryClient()
  return useMutation({
    mutationFn: (body: DeploymentRequest) =>
      apiPost<DeploymentResponse>('/api/v1/deployments', body),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['deployments'] })
    },
  })
}
