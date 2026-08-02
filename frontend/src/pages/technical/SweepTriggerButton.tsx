// Launches a named sweep script (POST .../trigger) and polls its status
// endpoint every 5s while running — shared by the Technical Sweep and
// Recommended Strategies pages. Same trigger/status response shape as
// datastore/api/routers/technical_backtest.py's _launch_trigger/
// _trigger_status. Structurally identical to
// frontend/src/pages/momentum/SweepTriggerButton.tsx — kept as a separate
// local copy (not a cross-pillar import) since it's typed against this
// pillar's own TATriggerResponse/TATriggerStatus, matching how each pillar
// already owns its own types.ts.
import { useState } from 'react'
import { useMutation, useQuery } from '@tanstack/react-query'

import { Badge } from '@/lib/ui'
import { apiGet, apiPost } from '@/shared/api/client'
import type { TATriggerResponse, TATriggerStatus } from './types'

export function SweepTriggerButton({
  label,
  triggerUrl,
  statusUrlPrefix,
  onCompleted,
}: {
  label: string
  triggerUrl: string
  statusUrlPrefix: string
  onCompleted: () => void
}) {
  const [jobId, setJobId] = useState<string | null>(null)
  const [notifiedDone, setNotifiedDone] = useState(false)

  const trigger = useMutation({
    mutationFn: () => apiPost<TATriggerResponse>(triggerUrl),
    onSuccess: (res) => {
      setJobId(res.job_id)
      setNotifiedDone(false)
    },
  })

  const status = useQuery({
    queryKey: ['technical-sweep-status', statusUrlPrefix, jobId],
    queryFn: () => apiGet<TATriggerStatus>(`${statusUrlPrefix}/${jobId}`),
    enabled: jobId != null,
    refetchInterval: (query) => (query.state.data?.status === 'running' ? 5000 : false),
  })

  const currentStatus = status.data?.status
  if (currentStatus === 'completed' && !notifiedDone) {
    setNotifiedDone(true)
    onCompleted()
  }

  const isRunning = jobId != null && (currentStatus === 'running' || currentStatus === undefined)

  return (
    <div className="flex items-center gap-2">
      <button
        type="button"
        disabled={trigger.isPending || isRunning}
        onClick={() => trigger.mutate()}
        className="h-9 rounded-[var(--radius-token)] border border-accent bg-transparent px-3 text-sm font-medium text-primary disabled:cursor-not-allowed disabled:opacity-50"
      >
        {isRunning ? `Running ${label}…` : `Run ${label}`}
      </button>
      {currentStatus === 'completed' ? <Badge variant="success">Done — table refreshed</Badge> : null}
      {currentStatus === 'failed' ? <Badge variant="destructive">Failed — check server logs</Badge> : null}
    </div>
  )
}
