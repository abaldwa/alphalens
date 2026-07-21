import { useEffect, useState } from 'react'
import { useQueries, useQuery, useQueryClient } from '@tanstack/react-query'

import {
  AppShell,
  Badge,
  Button,
  Card,
  CardContent,
  CardDescription,
  CardHeader,
  CardTitle,
  Input,
  StatCard,
  Table,
  TableHeader,
  TableBody,
  TableRow,
  TableHead,
  TableCell,
} from '@/lib/ui'
import {
  listBacktestRuns,
  getBacktestRunLineage,
  getBacktestRunFeatureLog,
  triggerIterativeRetrain,
  getIterativeRetrainStatus,
  triggerOrchestratorBacktest,
  getOrchestratorStatus,
  listScreenerTemplates,
  triggerStrategyQueue,
  getStrategyQueueStatus,
  type BacktestChannel,
  type BacktestMode,
  type BacktestRunSummary,
  type StrategyQueueJob,
} from '@/shared/api/backtest'
import {
  listPendingActions,
  getGateStatus,
  getStateSummary,
  acceptPendingAction,
  rejectPendingAction,
} from '@/shared/api/paper_trading'

const CHANNELS: BacktestChannel[] = ['technical', 'fundamental', 'ml', 'momentum']
const MODES: BacktestMode[] = ['backtest', 'walk_forward', 'paper']

function todayIso() {
  return new Date().toISOString().slice(0, 10)
}

const HORIZON_BUCKETS = ['5_day', '21_day', '63_day', '1_year', 'multibagger', 'custom'] as const

function PaperTradingPanel() {
  const [channel, setChannel] = useState<BacktestChannel>('technical')
  const [strategyId, setStrategyId] = useState('')
  const [asOfDate, setAsOfDate] = useState(todayIso())
  const [activeStrategy, setActiveStrategy] = useState<{ channel: BacktestChannel; strategyId: string } | null>(null)
  const [bootstrapHorizonBucket, setBootstrapHorizonBucket] = useState<string>(HORIZON_BUCKETS[0])
  const [bootstrapInitialCapital, setBootstrapInitialCapital] = useState('1000000')
  const queryClient = useQueryClient()

  const pending = useQuery({
    queryKey: ['paper-trading-pending', activeStrategy, asOfDate],
    queryFn: () => listPendingActions(activeStrategy!.channel, activeStrategy!.strategyId, asOfDate),
    enabled: !!activeStrategy,
  })
  const gate = useQuery({
    queryKey: ['paper-trading-gate', activeStrategy],
    queryFn: () => getGateStatus(activeStrategy!.channel, activeStrategy!.strategyId),
    enabled: !!activeStrategy,
  })
  const state = useQuery({
    queryKey: ['paper-trading-state', activeStrategy],
    queryFn: () => getStateSummary(activeStrategy!.channel, activeStrategy!.strategyId),
    enabled: !!activeStrategy,
    retry: false,
  })

  function refetchAll() {
    queryClient.invalidateQueries({ queryKey: ['paper-trading-pending'] })
    queryClient.invalidateQueries({ queryKey: ['paper-trading-gate'] })
    queryClient.invalidateQueries({ queryKey: ['paper-trading-state'] })
  }

  async function handleReject(actionId: string) {
    if (!activeStrategy) return
    await rejectPendingAction(activeStrategy.channel, activeStrategy.strategyId, actionId, asOfDate)
    refetchAll()
  }

  const needsBootstrap = !!activeStrategy && !state.data && !!state.error

  async function handleAccept(actionId: string, price: string) {
    if (!activeStrategy) return
    const numericPrice = Number(price)
    if (!Number.isFinite(numericPrice) || numericPrice <= 0) return
    const numericCapital = Number(bootstrapInitialCapital)
    await acceptPendingAction(activeStrategy.channel, activeStrategy.strategyId, actionId, {
      as_of_date: asOfDate,
      price: numericPrice,
      prices: { [pending.data?.actions.find((a) => a.action_id === actionId)?.ticker ?? '']: numericPrice },
      // Only actually required (and only sent) the very first time this
      // strategy accepts anything — persisted state wins on every call
      // after that (see PaperTradingRunner._portfolio()'s docstring).
      // Sending them unconditionally once state exists is harmless (the
      // backend ignores them), so we don't need to track "was this really
      // the first accept" client-side.
      ...(needsBootstrap
        ? { horizon_bucket: bootstrapHorizonBucket, initial_capital: numericCapital }
        : {}),
    })
    refetchAll()
  }

  return (
    <Card className="mt-4">
      <CardHeader>
        <CardTitle>Paper Trading</CardTitle>
        <CardDescription>
          Per-strategy pending-action review queue — same nav section as Backtest, not a separate menu item
          (BacktestUmbrellaPlan.md Phase 5). Nothing here executes without an explicit accept.
        </CardDescription>
      </CardHeader>
      <CardContent>
        <div className="flex flex-wrap items-end gap-3">
          <select
            className="h-9 rounded-[var(--radius-token)] border border-border bg-transparent px-3 text-sm"
            value={channel}
            onChange={(e) => setChannel(e.target.value as BacktestChannel)}
          >
            {CHANNELS.map((c) => (
              <option key={c} value={c}>
                {c}
              </option>
            ))}
          </select>
          <Input
            placeholder="strategy_id (e.g. ta_5d)"
            value={strategyId}
            onChange={(e) => setStrategyId(e.target.value)}
            className="w-48"
          />
          <Input type="date" value={asOfDate} onChange={(e) => setAsOfDate(e.target.value)} className="w-40" />
          <Button
            disabled={!strategyId}
            onClick={() => setActiveStrategy({ channel, strategyId })}
          >
            Load
          </Button>
        </div>

        {activeStrategy ? (
          <div className="mt-4 space-y-4">
            <div className="flex flex-wrap gap-3">
              {gate.data ? (
                <StatCard
                  label="Gate 7 (≥90 days)"
                  value={`${gate.data.days_completed}/${gate.data.gate_threshold}`}
                  tone={gate.data.gate_passed ? 'green' : 'default'}
                />
              ) : null}
              {state.data ? (
                <>
                  <StatCard label="Cash" value={`₹${state.data.cash.toLocaleString('en-IN', { maximumFractionDigits: 0 })}`} />
                  <StatCard label="Open Positions" value={String(state.data.n_open_positions)} />
                  <StatCard label="Closed Trades" value={String(state.data.n_closed_trades)} />
                </>
              ) : state.error ? (
                <div className="flex flex-wrap items-end gap-3">
                  <p className="text-sm text-muted-foreground">
                    No portfolio state yet — set this strategy's starting horizon bucket and capital, then Accept
                    its first pending action below to create it.
                  </p>
                </div>
              ) : null}
            </div>

            {needsBootstrap ? (
              <div className="flex flex-wrap items-end gap-3 rounded-[var(--radius-token)] border border-border p-3">
                <span className="text-xs font-semibold uppercase text-muted-foreground">First-accept setup</span>
                <select
                  className="h-9 rounded-[var(--radius-token)] border border-border bg-transparent px-3 text-sm"
                  value={bootstrapHorizonBucket}
                  onChange={(e) => setBootstrapHorizonBucket(e.target.value)}
                >
                  {HORIZON_BUCKETS.map((h) => (
                    <option key={h} value={h}>
                      {h}
                    </option>
                  ))}
                </select>
                <Input
                  type="number"
                  placeholder="initial capital (₹)"
                  value={bootstrapInitialCapital}
                  onChange={(e) => setBootstrapInitialCapital(e.target.value)}
                  className="w-48"
                />
              </div>
            ) : null}

            <div>
              <span className="text-xs font-semibold uppercase text-muted-foreground">
                Pending actions for {asOfDate}
              </span>
              {pending.error ? (
                <p className="mt-1 text-sm text-red">
                  Could not reach pending-actions endpoint — {(pending.error as Error).message}
                </p>
              ) : pending.data?.actions.length ? (
                <Table className="mt-1">
                  <TableHeader>
                    <TableRow>
                      <TableHead>Ticker</TableHead>
                      <TableHead>Action</TableHead>
                      <TableHead>Sector</TableHead>
                      <TableHead>Conviction</TableHead>
                      <TableHead>Status</TableHead>
                      <TableHead>Price</TableHead>
                      <TableHead />
                    </TableRow>
                  </TableHeader>
                  <TableBody>
                    {pending.data.actions.map((a) => (
                      <PendingActionRow
                        key={a.action_id}
                        action={a}
                        onAccept={handleAccept}
                        onReject={handleReject}
                      />
                    ))}
                  </TableBody>
                </Table>
              ) : (
                <p className="mt-1 text-sm text-muted-foreground">No pending actions for this date.</p>
              )}
            </div>
          </div>
        ) : null}
      </CardContent>
    </Card>
  )
}

function PendingActionRow({
  action,
  onAccept,
  onReject,
}: {
  action: import('@/shared/api/paper_trading').PendingAction
  onAccept: (actionId: string, price: string) => void
  onReject: (actionId: string) => void
}) {
  const [price, setPrice] = useState('')
  const isDecided = action.status !== 'pending'

  return (
    <TableRow>
      <TableCell className="font-mono-data">{action.ticker}</TableCell>
      <TableCell>
        <Badge variant="outline">{action.action}</Badge>
      </TableCell>
      <TableCell>{action.sector}</TableCell>
      <TableCell className="font-mono-data">{action.conviction.toFixed(2)}</TableCell>
      <TableCell>
        <Badge variant={action.status === 'accepted' ? 'success' : action.status === 'rejected' ? 'destructive' : 'outline'}>
          {action.status}
        </Badge>
      </TableCell>
      <TableCell>
        {isDecided ? (
          action.executed_price != null ? `₹${action.executed_price}` : '—'
        ) : (
          <Input
            placeholder="fill price"
            value={price}
            onChange={(e) => setPrice(e.target.value)}
            className="h-8 w-24"
          />
        )}
      </TableCell>
      <TableCell>
        {isDecided ? null : (
          <div className="flex gap-2">
            <Button size="sm" onClick={() => onAccept(action.action_id, price)} disabled={!price}>
              Accept
            </Button>
            <Button size="sm" variant="outline" onClick={() => onReject(action.action_id)}>
              Reject
            </Button>
          </div>
        )}
      </TableCell>
    </TableRow>
  )
}

function fmtPct(v: number | null | undefined) {
  return typeof v === 'number' ? `${(v * 100).toFixed(1)}%` : '—'
}
function fmtInr(v: number | null | undefined) {
  return typeof v === 'number' ? `₹${v.toLocaleString('en-IN', { maximumFractionDigits: 0 })}` : '—'
}
function fmtNum(v: number | null | undefined, digits = 2) {
  return typeof v === 'number' ? v.toFixed(digits) : '—'
}

function RunDetail({ run }: { run: BacktestRunSummary }) {
  const lineage = useQuery({
    queryKey: ['backtest-run-lineage', run.run_id],
    queryFn: () => getBacktestRunLineage(run.run_id),
  })
  const featureLog = useQuery({
    queryKey: ['backtest-run-feature-log', run.run_id],
    queryFn: () => getBacktestRunFeatureLog(run.run_id),
  })
  const m = run.metrics

  return (
    <Card className="mt-4">
      <CardHeader>
        <div className="flex items-center justify-between">
          <CardTitle>
            {run.strategy_id} <span className="text-muted-foreground">({run.channel} · {run.horizon_bucket} · {run.mode})</span>
          </CardTitle>
          <div className="flex gap-2">
            {run.integrity_passed === false ? <Badge variant="destructive">integrity FAILED</Badge> : null}
            {run.integrity_passed === true ? <Badge variant="success">integrity passed</Badge> : null}
            <Badge variant={run.live_eligible ? 'success' : 'outline'}>{run.live_eligible ? 'live-eligible' : 'not live-eligible'}</Badge>
          </div>
        </div>
        <CardDescription>
          {run.start_date} → {run.end_date} · {run.capital_mode === 'sip' ? 'SIP' : 'lump-sum'} capital ₹
          {run.initial_capital.toLocaleString('en-IN')}
        </CardDescription>
      </CardHeader>
      <CardContent>
        {m ? (
          <div className="grid grid-cols-2 gap-3 sm:grid-cols-4">
            <StatCard label="CAGR" value={fmtPct(m.cagr)} hint={m.cagr == null ? 'legacy: ' + fmtPct(m.cagr_trading_day_legacy) : undefined} />
            <StatCard label="XIRR" value={fmtPct(m.xirr)} />
            <StatCard label="Final Capital" value={fmtInr(m.final_capital)} />
            <StatCard label="Total Contributed" value={fmtInr(m.total_contributed)} />
            <StatCard label="Max Drawdown" value={fmtPct(m.max_drawdown)} tone={m.max_drawdown < -0.2 ? 'red' : 'default'} />
            <StatCard label="Win Rate" value={fmtPct(m.win_rate)} />
            <StatCard label="Sortino" value={fmtNum(m.sortino)} />
            <StatCard label="Calmar" value={fmtNum(m.calmar)} />
            <StatCard label="Trades" value={String(m.n_trades)} />
            <StatCard label="Distinct Tickers Traded" value={String(m.n_distinct_tickers_traded)} />
            <StatCard label="Turnover Ratio" value={fmtNum(m.turnover_ratio)} />
            <StatCard
              label="Excess Return vs Benchmark"
              value={m.benchmark_status === 'ok' ? fmtPct(m.excess_return) : 'insufficient benchmark history'}
              hint={m.benchmark_status !== 'ok' ? 'index_ohlcv only covers 2023-07+' : undefined}
            />
          </div>
        ) : (
          <p className="text-sm text-muted-foreground">No metrics recorded for this run.</p>
        )}

        {run.data_gaps.length ? (
          <div className="mt-4">
            <span className="text-xs font-semibold uppercase text-muted-foreground">
              Data gaps ({run.data_gaps.length}) — excluded, never fabricated
            </span>
            <Table className="mt-1">
              <TableHeader>
                <TableRow>
                  <TableHead>Ticker</TableHead>
                  <TableHead>Date</TableHead>
                  <TableHead>Reason</TableHead>
                </TableRow>
              </TableHeader>
              <TableBody>
                {run.data_gaps.slice(0, 50).map((g, i) => (
                  <TableRow key={i}>
                    <TableCell className="font-mono-data">{g.ticker}</TableCell>
                    <TableCell className="font-mono-data">{g.as_of_date}</TableCell>
                    <TableCell>{g.reason}</TableCell>
                  </TableRow>
                ))}
              </TableBody>
            </Table>
          </div>
        ) : null}

        {lineage.data && lineage.data.lineage.length > 1 ? (
          <div className="mt-4">
            <span className="text-xs font-semibold uppercase text-muted-foreground">Run lineage (oldest → this run)</span>
            <div className="mt-1 flex flex-wrap items-center gap-2">
              {lineage.data.lineage.map((r, i) => (
                <span key={r.run_id} className="flex items-center gap-2">
                  <Badge variant={r.run_id === run.run_id ? 'default' : 'outline'}>{r.strategy_id} · {fmtPct(r.metrics?.cagr)}</Badge>
                  {i < lineage.data!.lineage.length - 1 ? <span className="text-muted-foreground">→</span> : null}
                </span>
              ))}
            </div>
          </div>
        ) : null}

        {featureLog.data ? (
          <div className="mt-4">
            <span className="text-xs font-semibold uppercase text-muted-foreground">
              Feature log ({featureLog.data.rows.length} decisions logged) — feedback-loop inspector
            </span>
            {featureLog.data.rows.length ? (
              <Table className="mt-1">
                <TableHeader>
                  <TableRow>
                    <TableHead>Date</TableHead>
                    <TableHead>Ticker</TableHead>
                    <TableHead>Decision</TableHead>
                    <TableHead>Signal</TableHead>
                    <TableHead>Feature vector</TableHead>
                  </TableRow>
                </TableHeader>
                <TableBody>
                  {featureLog.data.rows.slice(0, 100).map((row, i) => (
                    <TableRow key={i}>
                      <TableCell className="font-mono-data">{row.as_of_date}</TableCell>
                      <TableCell className="font-mono-data">{row.ticker}</TableCell>
                      <TableCell>
                        <Badge variant={row.decision_taken.startsWith('sk') ? 'outline' : 'default'}>{row.decision_taken}</Badge>
                      </TableCell>
                      <TableCell>{row.signal_output ?? '—'}</TableCell>
                      <TableCell className="max-w-md truncate font-mono-data text-xs" title={JSON.stringify(row.feature_vector)}>
                        {JSON.stringify(row.feature_vector)}
                      </TableCell>
                    </TableRow>
                  ))}
                </TableBody>
              </Table>
            ) : (
              <p className="mt-1 text-sm text-muted-foreground">No decisions logged for this run.</p>
            )}
          </div>
        ) : null}
      </CardContent>
    </Card>
  )
}

// Tracks everything triggered from this page in the current session so the
// operator has live feedback the moment they hit "Trigger" — the persisted
// Runs table below only gains a row once a run actually finishes, which
// otherwise looks like nothing happened for however long the backtest takes.
type ActiveJobKind = 'orchestrator' | 'queue' | 'iterative_retrain'
interface ActiveJob {
  id: string
  kind: ActiveJobKind
  label: string
}

function fetchActiveJobStatus(job: ActiveJob) {
  if (job.kind === 'orchestrator') return getOrchestratorStatus(job.id)
  if (job.kind === 'queue') return getStrategyQueueStatus(job.id)
  return getIterativeRetrainStatus(job.id)
}

function RunsStatusBoard({ jobs, onDismiss }: { jobs: ActiveJob[]; onDismiss: (id: string) => void }) {
  const results = useQueries({
    queries: jobs.map((job) => ({
      queryKey: ['active-job-status', job.kind, job.id],
      queryFn: () => fetchActiveJobStatus(job),
      refetchInterval: (query: { state: { data?: { status?: string } } }) =>
        query.state.data?.status === 'running' || query.state.data?.status === 'unknown' ? 4000 : false,
    })),
  })

  if (!jobs.length) return null

  type Bucket = 'queued' | 'in_progress' | 'completed'
  const buckets: Record<Bucket, { job: ActiveJob; status: string }[]> = { queued: [], in_progress: [], completed: [] }

  jobs.forEach((job, i) => {
    const data = results[i]?.data
    let bucket: Bucket = 'queued'
    let status = 'queued'
    if (data) {
      status = data.status
      if (status === 'running') bucket = 'in_progress'
      else if (status === 'completed' || status === 'failed') bucket = 'completed'
    }
    buckets[bucket].push({ job, status })
  })

  const BUCKET_LABELS: Record<Bucket, string> = { queued: 'Queued', in_progress: 'In Progress', completed: 'Completed' }

  return (
    <Card className="mt-4">
      <CardHeader>
        <CardTitle>Active Strategies</CardTitle>
        <CardDescription>
          Everything triggered from this page this session, moved automatically as each strategy progresses.
        </CardDescription>
      </CardHeader>
      <CardContent className="grid gap-4 sm:grid-cols-3">
        {(['queued', 'in_progress', 'completed'] as const).map((bucket) => (
          <div key={bucket}>
            <span className="mb-2 block text-xs font-semibold uppercase text-muted-foreground">
              {BUCKET_LABELS[bucket]} ({buckets[bucket].length})
            </span>
            <div className="space-y-2">
              {buckets[bucket].length === 0 ? (
                <p className="text-xs text-muted-foreground">—</p>
              ) : (
                buckets[bucket].map(({ job, status }) => (
                  <div
                    key={job.id}
                    className="flex items-center justify-between gap-2 rounded-[var(--radius-token)] border border-border p-2 text-xs"
                  >
                    <div className="min-w-0">
                      <div className="truncate font-medium">{job.label}</div>
                      <div className="truncate font-mono-data text-muted-foreground">{job.id}</div>
                    </div>
                    <div className="flex shrink-0 items-center gap-1">
                      <Badge variant={status === 'failed' ? 'destructive' : status === 'completed' ? 'default' : 'outline'}>
                        {status}
                      </Badge>
                      {bucket === 'completed' ? (
                        <button
                          type="button"
                          onClick={() => onDismiss(job.id)}
                          aria-label="Dismiss"
                          className="text-muted-foreground hover:text-foreground"
                        >
                          ×
                        </button>
                      ) : null}
                    </div>
                  </div>
                ))
              )}
            </div>
          </div>
        ))}
      </CardContent>
    </Card>
  )
}

function IterativeRetrainPanel({ onTriggered }: { onTriggered: (job: ActiveJob) => void }) {
  const [jobId, setJobId] = useState<string | null>(null)
  const [horizonDays, setHorizonDays] = useState('5')

  const status = useQuery({
    queryKey: ['iterative-retrain-status', jobId],
    queryFn: () => getIterativeRetrainStatus(jobId!),
    enabled: !!jobId,
    // Poll while the loop is running (each iteration is a full walk-forward
    // backtest — this can run for a while); stop once it lands on a
    // terminal status so we're not polling forever.
    refetchInterval: (query) => (query.state.data?.status === 'running' ? 5000 : false),
  })

  async function handleTrigger() {
    const numericHorizon = Number(horizonDays)
    const res = await triggerIterativeRetrain({
      horizon_days: Number.isFinite(numericHorizon) && numericHorizon > 0 ? numericHorizon : undefined,
    })
    setJobId(res.job_id)
    onTriggered({ id: res.job_id, kind: 'iterative_retrain', label: 'Iterative Retrain (MetaLabeler)' })
  }

  const report = status.data?.status === 'completed' ? status.data.report : null

  return (
    <Card className="mt-4">
      <CardHeader>
        <CardTitle>Iterative Retrain (MetaLabeler)</CardTitle>
        <CardDescription>
          Repeatedly retrains the entry-filter model over a small fixed hyperparameter grid, promoting only
          deflated-Sharpe-cleared improvements, then evaluates the winner exactly once on an untouched holdout
          fiscal year — never tuned toward a target win-rate/CAGR.
        </CardDescription>
      </CardHeader>
      <CardContent>
        <div className="flex flex-wrap items-end gap-3">
          <div>
            <span className="block text-xs font-semibold uppercase text-muted-foreground">Horizon (days)</span>
            <Input
              type="number"
              value={horizonDays}
              onChange={(e) => setHorizonDays(e.target.value)}
              className="w-28"
            />
          </div>
          <Button onClick={handleTrigger} disabled={status.data?.status === 'running'}>
            {status.data?.status === 'running' ? 'Running…' : 'Trigger Iterative Retrain'}
          </Button>
          {jobId ? <span className="text-xs text-muted-foreground">job_id: {jobId}</span> : null}
        </div>

        {status.data ? (
          <div className="mt-4">
            <Badge
              variant={
                status.data.status === 'completed' ? 'default' : status.data.status === 'failed' ? 'outline' : 'outline'
              }
            >
              {status.data.status}
            </Badge>

            {status.data.status === 'failed' && status.data.log_tail ? (
              <pre className="mt-2 max-h-48 overflow-auto rounded-[var(--radius-token)] border border-border p-2 text-xs">
                {status.data.log_tail}
              </pre>
            ) : null}

            {report ? (
              <div className="mt-4 space-y-3">
                <div className="rounded-[var(--radius-token)] border border-border p-3 text-sm">
                  <span className="text-xs font-semibold uppercase text-muted-foreground">
                    Holdout selection (explainability)
                  </span>
                  <p className="mt-1">{report.holdout_selection.explanation}</p>
                  <p className="mt-1 text-muted-foreground">
                    Rows excluded entirely (too-recent-to-resolve buffer): {report.excluded_buffer_rows}
                  </p>
                </div>

                <div className="flex flex-wrap gap-3">
                  <StatCard label="Iterations run" value={String(report.iterations.length)} />
                  <StatCard label="Stopped" value={report.stopped_reason} />
                  <StatCard
                    label="Best Sharpe"
                    value={report.best_iteration_index !== null ? report.iterations[report.best_iteration_index].sharpe_mean.toFixed(3) : '—'}
                  />
                  <StatCard label="Total runtime" value={`${report.runtime_seconds.toFixed(0)}s`} />
                </div>

                <div>
                  <span className="text-xs font-semibold uppercase text-muted-foreground">Iterations</span>
                  <Table className="mt-1">
                    <TableHeader>
                      <TableRow>
                        <TableHead>#</TableHead>
                        <TableHead>Sharpe</TableHead>
                        <TableHead>Win Rate</TableHead>
                        <TableHead>DSR</TableHead>
                        <TableHead>Random-Feature Acc.</TableHead>
                        <TableHead>Runtime</TableHead>
                        <TableHead>Outcome</TableHead>
                        <TableHead>Dropped candidates (explainability)</TableHead>
                      </TableRow>
                    </TableHeader>
                    <TableBody>
                      {report.iterations.map((it) => (
                        <TableRow key={it.iteration}>
                          <TableCell>{it.iteration}</TableCell>
                          <TableCell className="font-mono-data">{it.sharpe_mean.toFixed(3)}</TableCell>
                          <TableCell className="font-mono-data">{(it.win_rate_mean * 100).toFixed(1)}%</TableCell>
                          <TableCell className="font-mono-data">{it.dsr.toFixed(3)}</TableCell>
                          <TableCell className="font-mono-data">
                            {it.random_feature_accuracy !== null ? it.random_feature_accuracy.toFixed(3) : '—'}
                          </TableCell>
                          <TableCell className="font-mono-data">{it.runtime_seconds.toFixed(1)}s</TableCell>
                          <TableCell>
                            <Badge variant={it.promoted ? 'default' : 'outline'}>
                              {it.promoted ? 'promoted' : 'dropped'}
                            </Badge>
                            {!it.promoted && it.rejection_reason ? (
                              <div className="mt-1 text-xs text-muted-foreground">{it.rejection_reason}</div>
                            ) : null}
                          </TableCell>
                          <TableCell className="text-xs">
                            {Object.entries(it.dropped_candidates)
                              .map(([k, v]) => `${k}=${v}`)
                              .join(', ') || '—'}
                          </TableCell>
                        </TableRow>
                      ))}
                    </TableBody>
                  </Table>
                </div>

                {report.holdout_aggregate ? (
                  <div className="rounded-[var(--radius-token)] border border-border p-3 text-sm">
                    <span className="text-xs font-semibold uppercase text-muted-foreground">
                      Holdout evaluation (one-shot, never seen during tuning)
                    </span>
                    <pre className="mt-1 overflow-auto text-xs">{JSON.stringify(report.holdout_aggregate, null, 2)}</pre>
                  </div>
                ) : (
                  <p className="text-sm text-muted-foreground">No iteration was promoted — no holdout evaluation was run.</p>
                )}
              </div>
            ) : null}
          </div>
        ) : null}
      </CardContent>
    </Card>
  )
}

const ORCHESTRATOR_CHANNELS = ['technical', 'fundamental', 'momentum'] as const
type OrchestratorChannel = (typeof ORCHESTRATOR_CHANNELS)[number]
const FUNDAMENTAL_PRESETS = ['quality_compounder', 'garp', 'turnaround'] as const

function OrchestratorTriggerPanel({
  onCompleted,
  onTriggered,
}: {
  onCompleted: () => void
  onTriggered: (job: ActiveJob) => void
}) {
  const [channel, setChannel] = useState<OrchestratorChannel>('technical')
  // Both left blank by default = "auto" — the backend codifies strategy_id
  // ({channel}_{descriptor}_{horizon}_{YYYYMMDD}) and defaults horizon_bucket
  // per the Explainer's published style table (backtest/strategy_id.py) so
  // an operator doesn't have to pick either by hand. Still overridable.
  const [strategyId, setStrategyId] = useState('')
  const [horizonBucket, setHorizonBucket] = useState<string>('')
  const [startDate, setStartDate] = useState('2023-01-01')
  const [endDate, setEndDate] = useState(todayIso())
  const [initialCapital, setInitialCapital] = useState('1000000')
  const [templateName, setTemplateName] = useState('')
  const [preset, setPreset] = useState<string>(FUNDAMENTAL_PRESETS[0])
  const [topN, setTopN] = useState('10')
  const [lookbackMonths, setLookbackMonths] = useState('6')
  const [runId, setRunId] = useState<string | null>(null)

  const templates = useQuery({
    queryKey: ['screener-templates'],
    queryFn: listScreenerTemplates,
    enabled: channel === 'technical',
  })

  const status = useQuery({
    queryKey: ['orchestrator-status', runId],
    queryFn: () => getOrchestratorStatus(runId!),
    enabled: !!runId,
    refetchInterval: (query) => (query.state.data?.status === 'running' || query.state.data?.status === 'unknown' ? 4000 : false),
  })

  useEffect(() => {
    if (status.data?.status === 'completed') onCompleted()
  }, [status.data?.status, onCompleted])

  async function handleTrigger() {
    const numericCapital = Number(initialCapital)
    const res = await triggerOrchestratorBacktest({
      channel,
      strategy_id: strategyId || undefined,
      horizon_bucket: horizonBucket || undefined,
      start_date: startDate,
      end_date: endDate,
      initial_capital: Number.isFinite(numericCapital) && numericCapital > 0 ? numericCapital : undefined,
      top_n: Number(topN) || undefined,
      lookback_months: channel === 'momentum' ? Number(lookbackMonths) || undefined : undefined,
      template_name: channel === 'technical' ? templateName || undefined : undefined,
      preset: channel === 'fundamental' ? preset : undefined,
    })
    setRunId(res.run_id)
    const descriptor = channel === 'technical' ? templateName : channel === 'fundamental' ? preset : `top${topN}_${lookbackMonths}m`
    onTriggered({ id: res.run_id, kind: 'orchestrator', label: `${channel} · ${descriptor || 'orchestrator'}` })
  }

  const canTrigger =
    !!startDate && !!endDate && (channel !== 'technical' || !!templateName) && status.data?.status !== 'running'

  return (
    <Card className="mt-4">
      <CardHeader>
        <CardTitle>Run a Backtest (Orchestrator)</CardTitle>
        <CardDescription>
          Drives backtest/core/engine.py's BacktestOrchestrator — the shared, channel-agnostic engine every
          Technical/Fundamental/Momentum adapter plugs into — against real data. Results land in the Runs table
          above once complete.{' '}
          <a
            href="/explain/backtest-guide.html"
            target="_blank"
            rel="noreferrer"
            className="text-primary underline"
          >
            📖 Backtest module &amp; strategy reference guide
          </a>
        </CardDescription>
      </CardHeader>
      <CardContent>
        <div className="flex flex-wrap items-end gap-3">
          <div>
            <span className="block text-xs font-semibold uppercase text-muted-foreground">Channel</span>
            <select
              className="h-9 rounded-[var(--radius-token)] border border-border bg-transparent px-3 text-sm"
              value={channel}
              onChange={(e) => setChannel(e.target.value as OrchestratorChannel)}
            >
              {ORCHESTRATOR_CHANNELS.map((c) => (
                <option key={c} value={c}>
                  {c}
                </option>
              ))}
            </select>
          </div>
          <div>
            <span className="block text-xs font-semibold uppercase text-muted-foreground">Strategy ID</span>
            <Input
              value={strategyId}
              onChange={(e) => setStrategyId(e.target.value)}
              className="w-40"
              placeholder="auto (e.g. ta_e2_63d_20260722)"
            />
          </div>
          <div>
            <span className="block text-xs font-semibold uppercase text-muted-foreground">Horizon</span>
            <select
              className="h-9 rounded-[var(--radius-token)] border border-border bg-transparent px-3 text-sm"
              value={horizonBucket}
              onChange={(e) => setHorizonBucket(e.target.value)}
            >
              <option value="">Auto (per Explainer)</option>
              {HORIZON_BUCKETS.map((h) => (
                <option key={h} value={h}>
                  {h}
                </option>
              ))}
            </select>
          </div>
          <div>
            <span className="block text-xs font-semibold uppercase text-muted-foreground">Start</span>
            <Input type="date" value={startDate} onChange={(e) => setStartDate(e.target.value)} className="w-40" />
          </div>
          <div>
            <span className="block text-xs font-semibold uppercase text-muted-foreground">End</span>
            <Input type="date" value={endDate} onChange={(e) => setEndDate(e.target.value)} className="w-40" />
          </div>
          <div>
            <span className="block text-xs font-semibold uppercase text-muted-foreground">Initial Capital (₹)</span>
            <Input type="number" value={initialCapital} onChange={(e) => setInitialCapital(e.target.value)} className="w-36" />
          </div>
          <div>
            <span className="block text-xs font-semibold uppercase text-muted-foreground">Top N</span>
            <Input type="number" value={topN} onChange={(e) => setTopN(e.target.value)} className="w-20" />
          </div>

          {channel === 'technical' ? (
            <div>
              <span className="block text-xs font-semibold uppercase text-muted-foreground">
                Screener Template{' '}
                <a
                  href="/explain/backtest-guide.html#technical-strategies"
                  target="_blank"
                  rel="noreferrer"
                  className="font-normal normal-case text-primary underline"
                >
                  (strategy definitions)
                </a>
              </span>
              <select
                className="h-9 rounded-[var(--radius-token)] border border-border bg-transparent px-3 text-sm"
                value={templateName}
                onChange={(e) => setTemplateName(e.target.value)}
              >
                <option value="">Select a template…</option>
                {templates.data?.templates.map((t) => (
                  <option key={t.name} value={t.name}>
                    {t.name} — {t.category}
                  </option>
                ))}
              </select>
            </div>
          ) : null}

          {channel === 'fundamental' ? (
            <div>
              <span className="block text-xs font-semibold uppercase text-muted-foreground">Screener Preset</span>
              <select
                className="h-9 rounded-[var(--radius-token)] border border-border bg-transparent px-3 text-sm"
                value={preset}
                onChange={(e) => setPreset(e.target.value)}
              >
                {FUNDAMENTAL_PRESETS.map((p) => (
                  <option key={p} value={p}>
                    {p}
                  </option>
                ))}
              </select>
            </div>
          ) : null}

          {channel === 'momentum' ? (
            <div>
              <span className="block text-xs font-semibold uppercase text-muted-foreground">Lookback (months)</span>
              <Input type="number" value={lookbackMonths} onChange={(e) => setLookbackMonths(e.target.value)} className="w-24" />
            </div>
          ) : null}

          <Button onClick={handleTrigger} disabled={!canTrigger}>
            {status.data?.status === 'running' ? 'Running…' : 'Trigger Backtest'}
          </Button>
        </div>

        {runId ? (
          <div className="mt-4">
            <span className="text-xs text-muted-foreground">run_id: {runId}</span>
            {status.data ? (
              <div className="mt-1">
                <Badge variant={status.data.status === 'completed' ? 'default' : 'outline'}>{status.data.status}</Badge>
                {status.data.status === 'completed' && status.data.run ? (
                  <>
                    <p className="mt-2 text-xs text-muted-foreground">
                      strategy_id: <span className="font-mono-data">{status.data.run.strategy_id}</span> · horizon:{' '}
                      <span className="font-mono-data">{status.data.run.horizon_bucket}</span>
                    </p>
                    <div className="mt-2 flex flex-wrap gap-3">
                      <StatCard label="CAGR" value={fmtPct(status.data.run.metrics?.cagr)} />
                      <StatCard label="Final Capital" value={fmtInr(status.data.run.metrics?.final_capital)} />
                      <StatCard label="Max DD" value={fmtPct(status.data.run.metrics?.max_drawdown)} />
                      <StatCard label="Trades" value={String(status.data.run.metrics?.n_trades ?? '—')} />
                    </div>
                  </>
                ) : null}
                {status.data.status === 'failed' && status.data.log_tail ? (
                  <pre className="mt-2 max-h-48 overflow-auto rounded-[var(--radius-token)] border border-border p-2 text-xs">
                    {status.data.log_tail}
                  </pre>
                ) : null}
                {status.data.status === 'completed' ? (
                  <div className="mt-2">
                    <Button variant="outline" onClick={onCompleted}>
                      Refresh Runs table
                    </Button>
                  </div>
                ) : null}
              </div>
            ) : null}
          </div>
        ) : null}
      </CardContent>
    </Card>
  )
}

function emptyQueueJob(): StrategyQueueJob {
  return { kind: 'orchestrator', channel: 'technical', start_date: '2023-01-01', end_date: todayIso(), top_n: 10 }
}

function StrategyQueuePanel({
  onCompleted,
  onTriggered,
}: {
  onCompleted: () => void
  onTriggered: (job: ActiveJob) => void
}) {
  const [jobs, setJobs] = useState<StrategyQueueJob[]>([emptyQueueJob()])
  const [queueId, setQueueId] = useState<string | null>(null)

  const status = useQuery({
    queryKey: ['strategy-queue-status', queueId],
    queryFn: () => getStrategyQueueStatus(queueId!),
    enabled: !!queueId,
    refetchInterval: (query) => (query.state.data?.status === 'running' || query.state.data?.status === 'unknown' ? 5000 : false),
  })

  useEffect(() => {
    if (status.data?.status === 'completed') onCompleted()
  }, [status.data?.status, onCompleted])

  function updateJob(index: number, patch: Partial<StrategyQueueJob>) {
    setJobs((prev) => prev.map((j, i) => (i === index ? { ...j, ...patch } : j)))
  }

  function addJob() {
    setJobs((prev) => [...prev, emptyQueueJob()])
  }

  function removeJob(index: number) {
    setJobs((prev) => prev.filter((_, i) => i !== index))
  }

  function addRetrainJob() {
    setJobs((prev) => [...prev, { kind: 'iterative_retrain', horizon_days: 5, folds: 4 }])
  }

  async function handleTrigger() {
    const res = await triggerStrategyQueue(jobs)
    setQueueId(res.queue_id)
    onTriggered({ id: res.queue_id, kind: 'queue', label: `Strategy queue (${jobs.length} job${jobs.length === 1 ? '' : 's'})` })
  }

  const isRunning = status.data?.status === 'running'

  return (
    <Card className="mt-4">
      <CardHeader>
        <CardTitle>Schedule a Strategy Queue</CardTitle>
        <CardDescription>
          Queue up several strategies — and optionally an iterative MetaLabeler retrain — to run sequentially in one
          go (backtest/run_strategy_queue.py). No manual one-at-a-time triggering: submit the queue, walk away, come
          back to every result. Runs are isolated subprocesses, memory-gated between jobs, exactly like the
          single-strategy trigger above.
        </CardDescription>
      </CardHeader>
      <CardContent>
        <div className="space-y-3">
          {jobs.map((job, i) => (
            <div key={i} className="flex flex-wrap items-end gap-2 rounded-[var(--radius-token)] border border-border p-2">
              <span className="text-xs text-muted-foreground">#{i + 1}</span>
              <select
                className="h-9 rounded-[var(--radius-token)] border border-border bg-transparent px-2 text-sm"
                value={job.kind}
                onChange={(e) => updateJob(i, { kind: e.target.value as StrategyQueueJob['kind'] })}
              >
                <option value="orchestrator">Backtest</option>
                <option value="iterative_retrain">Iterative Retrain</option>
              </select>

              {job.kind === 'orchestrator' ? (
                <>
                  <select
                    className="h-9 rounded-[var(--radius-token)] border border-border bg-transparent px-2 text-sm"
                    value={job.channel}
                    onChange={(e) => updateJob(i, { channel: e.target.value as StrategyQueueJob['channel'] })}
                  >
                    {ORCHESTRATOR_CHANNELS.map((c) => (
                      <option key={c} value={c}>
                        {c}
                      </option>
                    ))}
                  </select>
                  {job.channel === 'technical' ? (
                    <Input
                      placeholder="template (e.g. E2)"
                      value={job.template_name ?? ''}
                      onChange={(e) => updateJob(i, { template_name: e.target.value })}
                      className="w-32"
                    />
                  ) : null}
                  {job.channel === 'fundamental' ? (
                    <select
                      className="h-9 rounded-[var(--radius-token)] border border-border bg-transparent px-2 text-sm"
                      value={job.preset ?? FUNDAMENTAL_PRESETS[0]}
                      onChange={(e) => updateJob(i, { preset: e.target.value })}
                    >
                      {FUNDAMENTAL_PRESETS.map((p) => (
                        <option key={p} value={p}>
                          {p}
                        </option>
                      ))}
                    </select>
                  ) : null}
                  {job.channel === 'momentum' ? (
                    <Input
                      type="number"
                      placeholder="lookback months"
                      value={job.lookback_months ?? ''}
                      onChange={(e) => updateJob(i, { lookback_months: Number(e.target.value) || undefined })}
                      className="w-28"
                    />
                  ) : null}
                  <Input
                    type="number"
                    placeholder="top N"
                    value={job.top_n ?? ''}
                    onChange={(e) => updateJob(i, { top_n: Number(e.target.value) || undefined })}
                    className="w-20"
                  />
                  <Input
                    type="date"
                    value={job.start_date ?? ''}
                    onChange={(e) => updateJob(i, { start_date: e.target.value })}
                    className="w-40"
                  />
                  <Input
                    type="date"
                    value={job.end_date ?? ''}
                    onChange={(e) => updateJob(i, { end_date: e.target.value })}
                    className="w-40"
                  />
                  <select
                    className="h-9 rounded-[var(--radius-token)] border border-border bg-transparent px-2 text-sm"
                    value={job.horizon_bucket ?? ''}
                    onChange={(e) => updateJob(i, { horizon_bucket: e.target.value || undefined })}
                  >
                    <option value="">Auto horizon</option>
                    {HORIZON_BUCKETS.map((h) => (
                      <option key={h} value={h}>
                        {h}
                      </option>
                    ))}
                  </select>
                </>
              ) : (
                <>
                  <Input
                    type="number"
                    placeholder="horizon days"
                    value={job.horizon_days ?? ''}
                    onChange={(e) => updateJob(i, { horizon_days: Number(e.target.value) || undefined })}
                    className="w-28"
                  />
                  <Input
                    type="number"
                    placeholder="folds"
                    value={job.folds ?? ''}
                    onChange={(e) => updateJob(i, { folds: Number(e.target.value) || undefined })}
                    className="w-20"
                  />
                </>
              )}

              <Button variant="outline" onClick={() => removeJob(i)} disabled={jobs.length <= 1}>
                Remove
              </Button>
            </div>
          ))}
        </div>

        <div className="mt-3 flex flex-wrap gap-2">
          <Button variant="outline" onClick={addJob}>
            + Add backtest
          </Button>
          <Button variant="outline" onClick={addRetrainJob}>
            + Add iterative retrain
          </Button>
          <Button onClick={handleTrigger} disabled={isRunning}>
            {isRunning ? 'Running…' : `Trigger Queue (${jobs.length} job${jobs.length === 1 ? '' : 's'})`}
          </Button>
        </div>

        {queueId ? (
          <div className="mt-4">
            <span className="text-xs text-muted-foreground">queue_id: {queueId}</span>
            {status.data ? (
              <div className="mt-1">
                <Badge variant={status.data.status === 'completed' ? 'default' : 'outline'}>{status.data.status}</Badge>
                {status.data.summary ? (
                  <>
                    <p className="mt-2 text-sm text-muted-foreground">
                      {status.data.summary.jobs_run}/{status.data.summary.total_jobs} job(s) ran in{' '}
                      {status.data.summary.runtime_seconds.toFixed(0)}s
                    </p>
                    <Table className="mt-1">
                      <TableHeader>
                        <TableRow>
                          <TableHead>#</TableHead>
                          <TableHead>Kind</TableHead>
                          <TableHead>Result</TableHead>
                          <TableHead>Runtime</TableHead>
                        </TableRow>
                      </TableHeader>
                      <TableBody>
                        {status.data.summary.results.map((r) => (
                          <TableRow key={r.job_index}>
                            <TableCell>{r.job_index + 1}</TableCell>
                            <TableCell>{r.kind}</TableCell>
                            <TableCell>
                              <Badge variant={r.returncode === 0 ? 'default' : 'outline'}>
                                {r.returncode === 0 ? 'ok' : `exit ${r.returncode}`}
                              </Badge>
                            </TableCell>
                            <TableCell className="font-mono-data">{r.elapsed_s.toFixed(1)}s</TableCell>
                          </TableRow>
                        ))}
                      </TableBody>
                    </Table>
                  </>
                ) : null}
                {status.data.status === 'failed' && status.data.log_tail && !status.data.summary ? (
                  <pre className="mt-2 max-h-48 overflow-auto rounded-[var(--radius-token)] border border-border p-2 text-xs">
                    {status.data.log_tail}
                  </pre>
                ) : null}
              </div>
            ) : null}
          </div>
        ) : null}
      </CardContent>
    </Card>
  )
}

export function BacktestPage() {
  const [channel, setChannel] = useState<BacktestChannel | ''>('')
  const [mode, setMode] = useState<BacktestMode | ''>('')
  const [selectedRunId, setSelectedRunId] = useState<string | null>(null)
  const [activeJobs, setActiveJobs] = useState<ActiveJob[]>([])
  const queryClient = useQueryClient()

  function registerJob(job: ActiveJob) {
    setActiveJobs((prev) => [job, ...prev.filter((j) => j.id !== job.id)])
  }

  function dismissJob(id: string) {
    setActiveJobs((prev) => prev.filter((j) => j.id !== id))
  }

  const runs = useQuery({
    queryKey: ['backtest-runs', channel, mode],
    queryFn: () => listBacktestRuns({ channel: channel || undefined, mode: mode || undefined }),
  })

  const selectedRun = runs.data?.runs.find((r) => r.run_id === selectedRunId) ?? null

  return (
    <AppShell
      title="Backtest"
      description="Unified backtest, walk-forward, and paper-trading run history across Technical, Fundamental, ML, and Momentum — each strategy run against its own dedicated capital base (BacktestUmbrellaPlan.md)."
    >
      <Card>
        <CardHeader>
          <CardTitle>Runs</CardTitle>
          <CardDescription>
            {runs.data?.runs.length ? `${runs.data.runs.length} run(s)` : 'No runs recorded yet'}
          </CardDescription>
        </CardHeader>
        <CardContent>
          <div className="flex flex-wrap gap-3">
            <select
              className="h-9 rounded-[var(--radius-token)] border border-border bg-transparent px-3 text-sm"
              value={channel}
              onChange={(e) => setChannel(e.target.value as BacktestChannel | '')}
            >
              <option value="">All channels</option>
              {CHANNELS.map((c) => (
                <option key={c} value={c}>
                  {c}
                </option>
              ))}
            </select>
            <select
              className="h-9 rounded-[var(--radius-token)] border border-border bg-transparent px-3 text-sm"
              value={mode}
              onChange={(e) => setMode(e.target.value as BacktestMode | '')}
            >
              <option value="">All modes</option>
              {MODES.map((m) => (
                <option key={m} value={m}>
                  {m}
                </option>
              ))}
            </select>
          </div>

          {runs.error ? (
            <p className="mt-4 text-sm text-red">Could not reach GET /api/v1/backtest/runs — {(runs.error as Error).message}</p>
          ) : runs.data?.runs.length ? (
            <Table className="mt-4">
              <TableHeader>
                <TableRow>
                  <TableHead>Strategy</TableHead>
                  <TableHead>Channel</TableHead>
                  <TableHead>Horizon</TableHead>
                  <TableHead>Mode</TableHead>
                  <TableHead>Period</TableHead>
                  <TableHead>CAGR</TableHead>
                  <TableHead>XIRR</TableHead>
                  <TableHead>Final Capital</TableHead>
                  <TableHead>Max DD</TableHead>
                </TableRow>
              </TableHeader>
              <TableBody>
                {runs.data.runs.map((r) => (
                  <TableRow
                    key={r.run_id}
                    className="cursor-pointer hover:bg-muted/40"
                    onClick={() => setSelectedRunId(r.run_id === selectedRunId ? null : r.run_id)}
                    aria-selected={r.run_id === selectedRunId}
                  >
                    <TableCell>{r.strategy_id}</TableCell>
                    <TableCell>
                      <Badge variant="outline">{r.channel}</Badge>
                    </TableCell>
                    <TableCell className="font-mono-data">{r.horizon_bucket}</TableCell>
                    <TableCell>{r.mode}</TableCell>
                    <TableCell className="font-mono-data">
                      {r.start_date} → {r.end_date}
                    </TableCell>
                    <TableCell className="font-mono-data">{fmtPct(r.metrics?.cagr)}</TableCell>
                    <TableCell className="font-mono-data">{fmtPct(r.metrics?.xirr)}</TableCell>
                    <TableCell className="font-mono-data">{fmtInr(r.metrics?.final_capital)}</TableCell>
                    <TableCell className="font-mono-data">{fmtPct(r.metrics?.max_drawdown)}</TableCell>
                  </TableRow>
                ))}
              </TableBody>
            </Table>
          ) : (
            <p className="mt-4 text-sm text-muted-foreground">
              No runs yet — runs are written by backtest/core/run_store.py from a BacktestOrchestrator/WalkForwardRunner
              invocation, not triggered from this page (see BacktestUmbrellaPlan.md Phase 3).
            </p>
          )}
        </CardContent>
      </Card>

      <RunsStatusBoard jobs={activeJobs} onDismiss={dismissJob} />

      {selectedRun ? <RunDetail run={selectedRun} /> : null}

      <OrchestratorTriggerPanel
        onCompleted={() => queryClient.invalidateQueries({ queryKey: ['backtest-runs'] })}
        onTriggered={registerJob}
      />
      <StrategyQueuePanel
        onCompleted={() => queryClient.invalidateQueries({ queryKey: ['backtest-runs'] })}
        onTriggered={registerJob}
      />
      <IterativeRetrainPanel onTriggered={registerJob} />
      <PaperTradingPanel />
    </AppShell>
  )
}
