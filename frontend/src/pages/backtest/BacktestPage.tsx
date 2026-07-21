import { useState } from 'react'
import { useQuery, useQueryClient } from '@tanstack/react-query'

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
  type BacktestChannel,
  type BacktestMode,
  type BacktestRunSummary,
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

function IterativeRetrainPanel() {
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

export function BacktestPage() {
  const [channel, setChannel] = useState<BacktestChannel | ''>('')
  const [mode, setMode] = useState<BacktestMode | ''>('')
  const [selectedRunId, setSelectedRunId] = useState<string | null>(null)

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

      {selectedRun ? <RunDetail run={selectedRun} /> : null}

      <IterativeRetrainPanel />
      <PaperTradingPanel />
    </AppShell>
  )
}
