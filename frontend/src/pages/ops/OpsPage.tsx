import { useMemo, useState } from 'react'
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query'
import type { ColumnDef } from '@tanstack/react-table'

import { AppShell, Badge, Button, Card, CardContent, CardHeader, CardTitle, DataTable, StatCard } from '@/lib/ui'
import { apiGet, apiPost } from '@/shared/api/client'

// ===== Types (mirror datastore/api/schemas.py's Ops* models) =====

interface OpsStepRow {
  step_name: string
  step_index: number
  is_backfillable: boolean
  status: string
  started_at?: string | null
  completed_at?: string | null
  error_message?: string | null
  is_backfill?: boolean
  last_success_date?: string | null
  next_scheduled_run?: string | null
}
interface OpsStepsResponse { date: string; steps: OpsStepRow[] }

interface SchedulerJobHeartbeat {
  job_id: string
  last_attempt_at?: string | null
  last_status?: string | null
  last_error?: string | null
  last_success_at?: string | null
  next_run_time?: string | null
  is_stale: boolean
}

interface OpsFreshnessRow {
  source: string
  row_count?: number | null
  latest_data_date?: string | null
  last_write_at?: string | null
  error?: string | null
}
interface OpsFreshnessResponse { sources: OpsFreshnessRow[] }

interface OpsFailedStepInfo { step_name: string; error_message?: string | null }
interface OpsRunRow {
  run_id?: number | null
  date?: string | null
  status?: string | null
  stocks_processed?: number | null
  started_at?: string | null
  completed_at?: string | null
  error_message?: string | null
  is_backfill: boolean
  failed_steps: OpsFailedStepInfo[]
  sanity_check_passed?: boolean | null
  is_stale: boolean
}
interface OpsRunsResponse { runs: OpsRunRow[] }

interface OpsSchedulerResourceStatus {
  service_active?: boolean | null
  service_state?: string | null
  mem_available_pct?: number | null
  load1?: number | null
  hmm_feature_workers?: number | null
  feature_cache_preload_workers?: number | null
  throttled: boolean
  last_monitor_run_at?: string | null
  last_deferred_step?: string | null
  error?: string | null
}

interface OpsLiveResourceStatus {
  pid?: number | null
  rss_mb?: number | null
  cpu_percent?: number | null
  memory_ceiling_mb?: number | null
  high_pressure: boolean
  polled_at?: string | null
  error?: string | null
}

interface OpsLockStatusEntry {
  name: string
  path: string
  exists: boolean
  locked: boolean
  last_modified_at?: string | null
}
interface OpsLockStatusResponse { locks: OpsLockStatusEntry[] }

interface OpsUnusedModelEntry { model_name: string; last_trained_date?: string | null }
interface OpsUnusedModelsResponse { unused: OpsUnusedModelEntry[] }

interface OpsExceptionCatalogEntry {
  step_name: string
  location: string
  caught: string
  impact: string
  remediation: string
  severity: string
}
interface OpsExceptionCatalogResponse { entries: OpsExceptionCatalogEntry[] }

interface OpsIntegrityFinding {
  id: number
  check_name: string
  ticker?: string | null
  finding_date: string
  severity: string
  description: string
  proposed_fix_sql?: string | null
  status: string
  reviewed_by?: string | null
  reviewed_at?: string | null
  created_at?: string | null
}
interface OpsIntegrityFindingsResponse { findings: OpsIntegrityFinding[] }

interface OpsMissedJobFinding {
  id: number
  job_id: string
  missed_date: string
  severity: string
  description: string
  proposed_catchup_action?: string | null
  status: string
  reviewed_by?: string | null
  reviewed_at?: string | null
  created_at?: string | null
}
interface OpsMissedJobFindingsResponse { findings: OpsMissedJobFinding[] }

// ===== Helpers =====

function severityVariant(sev: string): 'destructive' | 'warning' | 'secondary' {
  if (sev === 'critical') return 'destructive'
  if (sev === 'warning') return 'warning'
  return 'secondary'
}

function statusVariant(status?: string | null): 'success' | 'destructive' | 'warning' | 'secondary' {
  if (status === 'success' || status === 'active' || status === 'applied' || status === 'approved') return 'success'
  if (status === 'failed' || status === 'rejected') return 'destructive'
  if (status === 'running' || status === 'pending') return 'warning'
  return 'secondary'
}

function fmt(v: unknown): string {
  if (v === null || v === undefined || v === '') return '—'
  return String(v)
}

// ===== Page =====

export function OpsPage() {
  const queryClient = useQueryClient()
  const [findingsStatus, setFindingsStatus] = useState('pending')
  const [missedJobsStatus, setMissedJobsStatus] = useState('pending')

  const steps = useQuery({
    queryKey: ['ops-steps'],
    queryFn: () => apiGet<OpsStepsResponse>('/api/v1/ops/steps'),
  })
  const heartbeats = useQuery({
    queryKey: ['ops-heartbeats'],
    queryFn: () => apiGet<SchedulerJobHeartbeat[]>('/api/v1/ops/heartbeats'),
  })
  const runs = useQuery({
    queryKey: ['ops-runs'],
    queryFn: () => apiGet<OpsRunsResponse>('/api/v1/ops/runs', { limit: 10 }),
  })
  const freshness = useQuery({
    queryKey: ['ops-freshness'],
    queryFn: () => apiGet<OpsFreshnessResponse>('/api/v1/ops/freshness'),
  })
  const hasRunningRun = (runs.data?.runs ?? []).some((r) => r.status === 'running')
  const schedulerResources = useQuery({
    queryKey: ['ops-scheduler-resources'],
    queryFn: () => apiGet<OpsSchedulerResourceStatus>('/api/v1/ops/scheduler-resources'),
  })
  const liveResources = useQuery({
    queryKey: ['ops-live-resources'],
    queryFn: () => apiGet<OpsLiveResourceStatus>('/api/v1/ops/live-resources'),
    enabled: hasRunningRun,
    refetchInterval: hasRunningRun ? 15000 : false,
  })
  const lockStatus = useQuery({
    queryKey: ['ops-lock-status'],
    queryFn: () => apiGet<OpsLockStatusResponse>('/api/v1/ops/lock-status'),
  })
  const unusedModels = useQuery({
    queryKey: ['ops-unused-models'],
    queryFn: () => apiGet<OpsUnusedModelsResponse>('/api/v1/ops/unused-models'),
  })
  const exceptionCatalog = useQuery({
    queryKey: ['ops-exception-catalog'],
    queryFn: () => apiGet<OpsExceptionCatalogResponse>('/api/v1/ops/exception-catalog'),
  })
  const integrityFindings = useQuery({
    queryKey: ['ops-integrity-findings', findingsStatus],
    queryFn: () =>
      apiGet<OpsIntegrityFindingsResponse>('/api/v1/ops/integrity-findings', {
        status: findingsStatus || undefined,
      }),
  })
  const missedJobs = useQuery({
    queryKey: ['ops-missed-jobs', missedJobsStatus],
    queryFn: () =>
      apiGet<OpsMissedJobFindingsResponse>('/api/v1/ops/missed-jobs', {
        status: missedJobsStatus || undefined,
      }),
  })

  const forceStep = useMutation({
    mutationFn: (stepName: string) => apiPost(`/api/v1/ops/steps/${stepName}/force`),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['ops-steps'] })
      queryClient.invalidateQueries({ queryKey: ['ops-runs'] })
    },
  })

  const decideFinding = useMutation({
    mutationFn: ({ id, decision }: { id: number; decision: 'approve' | 'reject' }) =>
      apiPost(`/api/v1/ops/integrity-findings/${id}/${decision}`),
    onSuccess: () => queryClient.invalidateQueries({ queryKey: ['ops-integrity-findings'] }),
  })

  const decideMissedJob = useMutation({
    mutationFn: ({ id, decision }: { id: number; decision: 'approve' | 'reject' }) =>
      apiPost(`/api/v1/ops/missed-jobs/${id}/${decision}`),
    onSuccess: () => queryClient.invalidateQueries({ queryKey: ['ops-missed-jobs'] }),
  })

  const stepColumns = useMemo<ColumnDef<OpsStepRow, unknown>[]>(
    () => [
      { header: 'Step', accessorKey: 'step_name' },
      {
        header: 'Status',
        accessorKey: 'status',
        cell: ({ row }) => <Badge variant={statusVariant(row.original.status)}>{row.original.status}</Badge>,
      },
      { header: 'Started', accessorFn: (r) => fmt(r.started_at) },
      { header: 'Completed', accessorFn: (r) => fmt(r.completed_at) },
      { header: 'Last Success', accessorFn: (r) => fmt(r.last_success_date) },
      { header: 'Error', accessorFn: (r) => fmt(r.error_message) },
      {
        header: 'Force',
        id: 'force',
        cell: ({ row }) => (
          <Button
            size="sm"
            variant="outline"
            disabled={forceStep.isPending}
            onClick={() => forceStep.mutate(row.original.step_name)}
          >
            Force run
          </Button>
        ),
      },
    ],
    [forceStep],
  )

  const heartbeatColumns = useMemo<ColumnDef<SchedulerJobHeartbeat, unknown>[]>(
    () => [
      { header: 'Job', accessorKey: 'job_id' },
      {
        header: 'Last Status',
        accessorKey: 'last_status',
        cell: ({ row }) => <Badge variant={statusVariant(row.original.last_status)}>{fmt(row.original.last_status)}</Badge>,
      },
      { header: 'Last Attempt', accessorFn: (r) => fmt(r.last_attempt_at) },
      { header: 'Last Success', accessorFn: (r) => fmt(r.last_success_at) },
      { header: 'Next Run', accessorFn: (r) => fmt(r.next_run_time) },
      {
        header: 'Stale',
        accessorKey: 'is_stale',
        cell: ({ row }) => (row.original.is_stale ? <Badge variant="destructive">Stale</Badge> : null),
      },
      { header: 'Error', accessorFn: (r) => fmt(r.last_error) },
    ],
    [],
  )

  const runColumns = useMemo<ColumnDef<OpsRunRow, unknown>[]>(
    () => [
      { header: 'Run', accessorKey: 'run_id' },
      { header: 'Date', accessorKey: 'date' },
      {
        header: 'Status',
        accessorKey: 'status',
        cell: ({ row }) => (
          <span className="inline-flex items-center gap-1">
            <Badge variant={statusVariant(row.original.status)}>{fmt(row.original.status)}</Badge>
            {row.original.is_stale ? <Badge variant="destructive">Stale</Badge> : null}
            {row.original.is_backfill ? <Badge variant="secondary">Backfill</Badge> : null}
          </span>
        ),
      },
      { header: 'Stocks', accessorKey: 'stocks_processed' },
      { header: 'Started', accessorFn: (r) => fmt(r.started_at) },
      { header: 'Completed', accessorFn: (r) => fmt(r.completed_at) },
      {
        header: 'Sanity Check',
        accessorKey: 'sanity_check_passed',
        cell: ({ row }) => {
          const v = row.original.sanity_check_passed
          if (v === null || v === undefined) return '—'
          return <Badge variant={v ? 'success' : 'destructive'}>{v ? 'passed' : 'failed'}</Badge>
        },
      },
      {
        header: 'Failed Steps',
        id: 'failed_steps',
        cell: ({ row }) =>
          row.original.failed_steps.length
            ? row.original.failed_steps.map((s) => s.step_name).join(', ')
            : '—',
      },
    ],
    [],
  )

  const freshnessColumns = useMemo<ColumnDef<OpsFreshnessRow, unknown>[]>(
    () => [
      { header: 'Source', accessorKey: 'source' },
      { header: 'Rows', accessorFn: (r) => fmt(r.row_count) },
      { header: 'Latest Data Date', accessorFn: (r) => fmt(r.latest_data_date) },
      { header: 'Last Write', accessorFn: (r) => fmt(r.last_write_at) },
      {
        header: 'Error',
        id: 'error',
        cell: ({ row }) => (row.original.error ? <span className="text-red">{row.original.error}</span> : '—'),
      },
    ],
    [],
  )

  const lockColumns = useMemo<ColumnDef<OpsLockStatusEntry, unknown>[]>(
    () => [
      { header: 'Lock', accessorKey: 'name' },
      { header: 'Path', accessorKey: 'path' },
      {
        header: 'Locked',
        accessorKey: 'locked',
        cell: ({ row }) => <Badge variant={row.original.locked ? 'warning' : 'success'}>{row.original.locked ? 'locked' : 'free'}</Badge>,
      },
      { header: 'Last Activity', accessorFn: (r) => fmt(r.last_modified_at) },
    ],
    [],
  )

  const unusedModelColumns = useMemo<ColumnDef<OpsUnusedModelEntry, unknown>[]>(
    () => [
      { header: 'Model', accessorKey: 'model_name' },
      { header: 'Last Trained', accessorFn: (r) => fmt(r.last_trained_date) },
    ],
    [],
  )

  const exceptionColumns = useMemo<ColumnDef<OpsExceptionCatalogEntry, unknown>[]>(
    () => [
      { header: 'Step', accessorKey: 'step_name' },
      { header: 'Location', accessorKey: 'location' },
      { header: 'Caught', accessorKey: 'caught' },
      { header: 'Impact', accessorKey: 'impact' },
      { header: 'Remediation', accessorKey: 'remediation' },
      {
        header: 'Severity',
        accessorKey: 'severity',
        cell: ({ row }) => <Badge variant={severityVariant(row.original.severity)}>{row.original.severity}</Badge>,
      },
    ],
    [],
  )

  const integrityColumns = useMemo<ColumnDef<OpsIntegrityFinding, unknown>[]>(
    () => [
      { header: 'ID', accessorKey: 'id' },
      { header: 'Check', accessorKey: 'check_name' },
      { header: 'Ticker', accessorFn: (r) => fmt(r.ticker) },
      { header: 'Date', accessorKey: 'finding_date' },
      {
        header: 'Severity',
        accessorKey: 'severity',
        cell: ({ row }) => <Badge variant={severityVariant(row.original.severity)}>{row.original.severity}</Badge>,
      },
      { header: 'Description', accessorKey: 'description' },
      {
        header: 'Status',
        accessorKey: 'status',
        cell: ({ row }) => <Badge variant={statusVariant(row.original.status)}>{row.original.status}</Badge>,
      },
      {
        header: 'Actions',
        id: 'actions',
        cell: ({ row }) =>
          row.original.status === 'pending' ? (
            <div className="flex gap-2">
              <Button
                size="sm"
                disabled={decideFinding.isPending}
                onClick={() => decideFinding.mutate({ id: row.original.id, decision: 'approve' })}
              >
                Approve
              </Button>
              <Button
                size="sm"
                variant="outline"
                disabled={decideFinding.isPending}
                onClick={() => decideFinding.mutate({ id: row.original.id, decision: 'reject' })}
              >
                Reject
              </Button>
            </div>
          ) : (
            <span className="text-xs text-muted-foreground">by {fmt(row.original.reviewed_by)}</span>
          ),
      },
    ],
    [decideFinding],
  )

  const missedJobColumns = useMemo<ColumnDef<OpsMissedJobFinding, unknown>[]>(
    () => [
      { header: 'ID', accessorKey: 'id' },
      { header: 'Job', accessorKey: 'job_id' },
      { header: 'Missed Date', accessorKey: 'missed_date' },
      {
        header: 'Severity',
        accessorKey: 'severity',
        cell: ({ row }) => <Badge variant={severityVariant(row.original.severity)}>{row.original.severity}</Badge>,
      },
      { header: 'Description', accessorKey: 'description' },
      { header: 'Proposed Catch-up', accessorFn: (r) => fmt(r.proposed_catchup_action) },
      {
        header: 'Status',
        accessorKey: 'status',
        cell: ({ row }) => <Badge variant={statusVariant(row.original.status)}>{row.original.status}</Badge>,
      },
      {
        header: 'Actions',
        id: 'actions',
        cell: ({ row }) =>
          row.original.status === 'pending' ? (
            <div className="flex gap-2">
              <Button
                size="sm"
                disabled={decideMissedJob.isPending}
                onClick={() => decideMissedJob.mutate({ id: row.original.id, decision: 'approve' })}
              >
                Approve
              </Button>
              <Button
                size="sm"
                variant="outline"
                disabled={decideMissedJob.isPending}
                onClick={() => decideMissedJob.mutate({ id: row.original.id, decision: 'reject' })}
              >
                Reject
              </Button>
            </div>
          ) : (
            <span className="text-xs text-muted-foreground">by {fmt(row.original.reviewed_by)}</span>
          ),
      },
    ],
    [decideMissedJob],
  )

  const unusedModelRows = unusedModels.data?.unused ?? []

  return (
    <AppShell title="Ops" description="Scheduled-job heartbeats, today's pipeline step checklist, recent runs, and force-start for a step that hasn't run yet.">
      <div className="grid grid-cols-2 gap-3 md:grid-cols-4">
        <StatCard
          label="Scheduler Service"
          value={schedulerResources.data?.service_active ? 'Active' : 'Inactive'}
          tone={schedulerResources.data?.service_active ? 'green' : 'red'}
          hint={fmt(schedulerResources.data?.service_state)}
        />
        <StatCard
          label="Mem Available"
          value={schedulerResources.data?.mem_available_pct != null ? `${schedulerResources.data.mem_available_pct.toFixed(1)}%` : '—'}
          hint={`load1 ${fmt(schedulerResources.data?.load1)}`}
        />
        <StatCard
          label="Workers"
          value={`${fmt(schedulerResources.data?.hmm_feature_workers)} / ${fmt(schedulerResources.data?.feature_cache_preload_workers)}`}
          tone={schedulerResources.data?.throttled ? 'amber' : 'default'}
          hint={schedulerResources.data?.throttled ? 'throttled' : 'HMM / preload'}
        />
        <StatCard
          label="Locks"
          value={(lockStatus.data?.locks ?? []).filter((l) => l.locked).length}
          hint="currently held"
        />
      </div>

      {hasRunningRun && liveResources.data ? (
        <div className="mt-3">
          <Card>
            <CardHeader>
              <CardTitle>Live Resources (active run)</CardTitle>
            </CardHeader>
            <CardContent className="flex flex-wrap gap-6 text-sm">
              <span>PID: <span className="font-mono-data">{fmt(liveResources.data.pid)}</span></span>
              <span>RSS: <span className="font-mono-data">{fmt(liveResources.data.rss_mb)} MB</span></span>
              <span>CPU: <span className="font-mono-data">{fmt(liveResources.data.cpu_percent)}%</span></span>
              {liveResources.data.high_pressure ? <Badge variant="destructive">High memory pressure</Badge> : null}
            </CardContent>
          </Card>
        </div>
      ) : null}

      <div className="mt-4">
        <Card>
          <CardHeader>
            <CardTitle>Pipeline Steps — {steps.data?.date ?? '…'}</CardTitle>
          </CardHeader>
          <CardContent>
            <DataTable columns={stepColumns} data={steps.data?.steps ?? []} isLoading={steps.isLoading} resizableColumns={false} />
          </CardContent>
        </Card>
      </div>

      <div className="mt-4">
        <Card>
          <CardHeader>
            <CardTitle>Scheduler Heartbeats</CardTitle>
          </CardHeader>
          <CardContent>
            <DataTable columns={heartbeatColumns} data={heartbeats.data ?? []} isLoading={heartbeats.isLoading} resizableColumns={false} />
          </CardContent>
        </Card>
      </div>

      <div className="mt-4">
        <Card>
          <CardHeader>
            <CardTitle>Recent Runs</CardTitle>
          </CardHeader>
          <CardContent>
            <DataTable columns={runColumns} data={runs.data?.runs ?? []} isLoading={runs.isLoading} resizableColumns={false} />
          </CardContent>
        </Card>
      </div>

      <div className="mt-4">
        <Card>
          <CardHeader>
            <CardTitle>Data Freshness</CardTitle>
          </CardHeader>
          <CardContent>
            <DataTable columns={freshnessColumns} data={freshness.data?.sources ?? []} isLoading={freshness.isLoading} resizableColumns={false} />
          </CardContent>
        </Card>
      </div>

      <div className="mt-4 grid grid-cols-1 gap-4 lg:grid-cols-2">
        <Card>
          <CardHeader>
            <CardTitle>Locks</CardTitle>
          </CardHeader>
          <CardContent>
            <DataTable columns={lockColumns} data={lockStatus.data?.locks ?? []} isLoading={lockStatus.isLoading} resizableColumns={false} />
          </CardContent>
        </Card>
        <Card>
          <CardHeader>
            <CardTitle>Unused Models</CardTitle>
          </CardHeader>
          <CardContent>
            <DataTable columns={unusedModelColumns} data={unusedModelRows} isLoading={unusedModels.isLoading} resizableColumns={false} />
          </CardContent>
        </Card>
      </div>

      <div className="mt-4">
        <Card>
          <CardHeader>
            <CardTitle>Exception Catalog</CardTitle>
          </CardHeader>
          <CardContent>
            <DataTable columns={exceptionColumns} data={exceptionCatalog.data?.entries ?? []} isLoading={exceptionCatalog.isLoading} resizableColumns={false} />
          </CardContent>
        </Card>
      </div>

      <div className="mt-4">
        <Card>
          <CardHeader className="flex flex-row items-center justify-between gap-2">
            <CardTitle>Integrity Findings</CardTitle>
            <select
              className="h-8 rounded-[var(--radius-token)] border border-border bg-transparent px-2 text-xs"
              value={findingsStatus}
              onChange={(e) => setFindingsStatus(e.target.value)}
            >
              <option value="pending">Pending</option>
              <option value="approved">Approved</option>
              <option value="rejected">Rejected</option>
              <option value="applied">Applied</option>
              <option value="">All</option>
            </select>
          </CardHeader>
          <CardContent>
            <DataTable columns={integrityColumns} data={integrityFindings.data?.findings ?? []} isLoading={integrityFindings.isLoading} resizableColumns={false} />
          </CardContent>
        </Card>
      </div>

      <div className="mt-4">
        <Card>
          <CardHeader className="flex flex-row items-center justify-between gap-2">
            <CardTitle>Missed Job Findings</CardTitle>
            <select
              className="h-8 rounded-[var(--radius-token)] border border-border bg-transparent px-2 text-xs"
              value={missedJobsStatus}
              onChange={(e) => setMissedJobsStatus(e.target.value)}
            >
              <option value="pending">Pending</option>
              <option value="approved">Approved</option>
              <option value="rejected">Rejected</option>
              <option value="applied">Applied</option>
              <option value="">All</option>
            </select>
          </CardHeader>
          <CardContent>
            <DataTable columns={missedJobColumns} data={missedJobs.data?.findings ?? []} isLoading={missedJobs.isLoading} resizableColumns={false} />
          </CardContent>
        </Card>
      </div>
    </AppShell>
  )
}
