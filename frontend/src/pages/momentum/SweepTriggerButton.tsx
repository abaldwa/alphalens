// Launches a named sweep script (POST .../trigger) and polls its status
// endpoint every 5s while running — shared by the Universe Sweep,
// Filter Overlays, and Recommended Strategies pages, which all use the same
// trigger/status response shape (datastore/api/routers/momentum.py's
// _launch_trigger / _trigger_status). Calls onCompleted() once, when status
// flips to "completed", so the caller can refetch the report it displays.
import { useState } from 'react'
import { useMutation, useQuery } from '@tanstack/react-query'

import { Badge } from '@/lib/ui'
import { apiGet, apiPost } from '@/shared/api/client'
import type { MomentumTriggerResponse, MomentumTriggerStatus } from './types'

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
    mutationFn: () => apiPost<MomentumTriggerResponse>(triggerUrl),
    onSuccess: (res) => {
      setJobId(res.job_id)
      setNotifiedDone(false)
    },
  })

  const status = useQuery({
    queryKey: ['momentum-sweep-status', statusUrlPrefix, jobId],
    queryFn: () => apiGet<MomentumTriggerStatus>(`${statusUrlPrefix}/${jobId}`),
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
