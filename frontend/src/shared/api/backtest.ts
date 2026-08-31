// Unified Backtest API module — mirrors datastore/api/routers/backtest_runs.py
// (Phase 3 of the Unified Backtest & Paper Trading Umbrella, see
// BacktestUmbrellaPlan.md at the repo root). Distinct from the existing
// legacy backtest_reports.py passthrough (still used by src/pages/ml/backtest.tsx)
// — this module is the new cross-channel run listing/detail/feature-log API.
import { API_BASE_URL, apiGet, apiPost } from './client'

export type BacktestChannel = 'technical' | 'fundamental' | 'ml' | 'momentum'
export type BacktestMode = 'backtest' | 'walk_forward' | 'paper'

export interface BacktestRunMetrics {
  cagr: number | null
  cagr_trading_day_legacy: number | null
  xirr: number | null
  final_capital: number
  total_contributed: number
  max_drawdown: number
  win_rate: number | null
  profit_factor: number | null
  sharpe: number | null
  sortino: number | null
  calmar: number | null
  n_distinct_tickers_traded: number
  turnover_ratio: number | null
  n_trades: number
  benchmark_cagr: number | null
  excess_return: number | null
  benchmark_status: string
  cash_position_series: { date: string; cash: number }[]
  avg_days_held: number | null
  /** Which index `benchmark_cagr` was computed against. Without it two rows
   * compared against different indices read as directly comparable. */
  benchmark_index_name: string | null
  /** Which basis `cagr` is stated on. The orchestrator reports post-tax by
   * default, so `cagr` must NOT be assumed pre-tax. */
  tax_basis: 'pre_tax' | 'post_tax' | null
  /** The same CAGR on the opposite basis to `tax_basis`. */
  cagr_other_basis: number | null
  total_tax_paid: number | null
  churn_per_year: number | null
  avg_winner_pct: number | null
  avg_loser_pct: number | null
  /** Per financial year, e.g. `{ fy_label: "FY2023", return_pct: 0.0078 }`. */
  fy_returns: { fy_label: string; return_pct: number | null; partial?: boolean }[] | null
  /** Keyed "2y".."5y". `min/median/max_cagr` are annualised, never totals. */
  rolling_returns: Record<
    string,
    {
      min_cagr: number | null
      median_cagr: number | null
      max_cagr: number | null
      positive_share: number | null
      n_windows: number | null
    }
  > | null
  /** Why the engine could not compute the ratio — surfaced as the em-dash
   * tooltip instead of a backlog ID, since the reason is a real fact here. */
  sortino_none_reason: string | null
  calmar_none_reason: string | null
  /** Trade-book integrity. Rendered on the strategy detail page only, so it
   * does not compete with the numbers a deploy decision turns on. */
  n_outlier_trades: number | null
  max_abs_return_zscore: number | null
  /** Annualised standard deviation of periodic returns, as a fraction. Added
   * 2026-08-19: the Risk table has always had a Volatility column and the
   * engine never emitted the field behind it, so every row rendered an
   * explained em dash. Null on runs backtested before that date. */
  volatility?: number | null
  /** A90: mark-to-market portfolio value per trading day. Served only by the
   * single-run detail endpoint — `list_runs` strips it (and
   * `cash_position_series`) because at ~one entry per trading day they
   * dominate the payload and no list column renders them. */
  equity_curve_series?: { date: string; equity: number }[] | null
}

export interface BacktestDataGap {
  ticker: string
  as_of_date: string
  reason: string
}

// Channel-specific run config, as stored in backtest_runs.config_json —
// only the fields the Runs table needs to name the actual strategy that
// ran (e.g. "E2"), not a full mirror of every adapter's config shape.
export interface BacktestRunConfig {
  template_name?: string | null
  preset?: string | null
  top_n?: number | null
  lookback_months?: number | null
  exit_variant?: string | null
}

// One row per Bull/Bear/Sideways market_regimes segment this run's window
// overlapped (backtest/core/regime_breakdown.py) — omitted, not zeroed,
// for a regime with no equity/trade data inside the run's window.
export interface RegimeBreakdownRow {
  regime: 'bull' | 'bear' | 'sideways'
  start_date: string
  end_date: string
  cagr: number | null
  max_drawdown: number
  win_rate: number | null
  profit_factor: number | null
  n_trades: number
  n_days: number
}

export interface BacktestRunSummary {
  run_id: string
  parent_run_id: string | null
  channel: BacktestChannel
  strategy_id: string
  horizon_bucket: string
  mode: BacktestMode
  start_date: string
  end_date: string
  capital_mode: 'lump' | 'sip'
  initial_capital: number
  created_at: string
  config: BacktestRunConfig | null
  metrics: BacktestRunMetrics | null
  data_gaps: BacktestDataGap[]
  integrity_passed: boolean | null
  live_eligible: boolean
  buy_signal_count: number
  sell_signal_count: number
  regime_breakdown: RegimeBreakdownRow[]
  is_valid?: boolean
  validation_status?: 'valid' | 'alternative_period' | 'flagged' | 'invalid'
  marked_invalid_reason?: string | null
  run_executed_at?: string | null
}

export interface BacktestRunListResponse {
  runs: BacktestRunSummary[]
  total_count: number
}

export interface BacktestRunLineageResponse {
  run_id: string
  lineage: BacktestRunSummary[]
}

export interface FeatureLogRow {
  ticker: string
  as_of_date: string
  horizon_bucket: string
  feature_vector: Record<string, unknown>
  signal_output: string | null
  decision_taken: string
}

export interface FeatureLogResponse {
  run_id: string
  rows: FeatureLogRow[]
}

export function listBacktestRuns(filters?: {
  channel?: BacktestChannel
  mode?: BacktestMode
  strategy_id?: string
  limit?: number
  sort_by?: 'created_at' | 'cagr'
}) {
  return apiGet<BacktestRunListResponse>('/api/v1/backtest/runs', filters as Record<string, string | number | boolean | undefined>)
}

export function getBacktestRun(runId: string) {
  return apiGet<BacktestRunSummary>(`/api/v1/backtest/runs/${runId}`)
}

export function getBacktestRunLineage(runId: string) {
  return apiGet<BacktestRunLineageResponse>(`/api/v1/backtest/runs/${runId}/lineage`)
}

export function getBacktestRunFeatureLog(runId: string) {
  return apiGet<FeatureLogResponse>(`/api/v1/backtest/runs/${runId}/feature_log`)
}

// Iterative MetaLabeler retrain loop (datastore/api/routers/backtest_runs.py's
// /iterative/trigger + /iterative/status/{job_id}) — the one deliberate
// exception to this API being read-only, see that router's module docstring.
export interface IterativeRetrainTriggerResponse {
  job_id: string
  status: string
}

export interface IterativeRetrainIteration {
  iteration: number
  run_id: string
  hyperparams: Record<string, unknown>
  sharpe_mean: number
  sortino_mean: number | null
  calmar_mean: number | null
  win_rate_mean: number
  dsr: number
  random_feature_accuracy: number | null
  promoted: boolean
  rejection_reason: string | null
  runtime_seconds: number
  dropped_candidates: Record<string, number>
}

export interface IterativeRetrainReport {
  generated_at: string
  loop_run_id: string
  runtime_seconds: number
  stopped_reason: string
  holdout_selection: {
    holdout_start: string
    holdout_end: string
    skipped_fiscal_years: number[]
    explanation: string
  }
  excluded_buffer_rows: number
  iterations: IterativeRetrainIteration[]
  best_iteration_index: number | null
  best_hyperparams: Record<string, unknown> | null
  holdout_run_id: string | null
  holdout_runtime_seconds: number | null
  holdout_aggregate: Record<string, unknown> | null
}

export interface IterativeRetrainStatusResponse {
  job_id: string
  status: 'running' | 'completed' | 'failed' | 'unknown'
  report: IterativeRetrainReport | null
  log_tail: string | null
}

export function triggerIterativeRetrain(params?: { horizon_days?: number; folds?: number; max_iterations?: number }) {
  const query = new URLSearchParams()
  if (params?.horizon_days !== undefined) query.set('horizon_days', String(params.horizon_days))
  if (params?.folds !== undefined) query.set('folds', String(params.folds))
  if (params?.max_iterations !== undefined) query.set('max_iterations', String(params.max_iterations))
  const qs = query.toString()
  return apiPost<IterativeRetrainTriggerResponse>(`/api/v1/backtest/iterative/trigger${qs ? `?${qs}` : ''}`)
}

export function getIterativeRetrainStatus(jobId: string) {
  return apiGet<IterativeRetrainStatusResponse>(`/api/v1/backtest/iterative/status/${jobId}`)
}

// BacktestOrchestrator trigger (datastore/api/routers/backtest_runs.py's
// /orchestrator/trigger + /orchestrator/status/{run_id}) — the second
// deliberate exception to this API being read-only. Drives
// backtest/core/engine.py's BacktestOrchestrator for the Technical/
// Fundamental/Momentum channels against real data.
export interface OrchestratorTriggerParams {
  channel: 'technical' | 'fundamental' | 'momentum'
  // Both optional — the backend defaults strategy_id to the codified
  // {channel}_{descriptor}_{horizon}_{YYYYMMDD} form and horizon_bucket
  // per the Explainer's published style table (backtest/strategy_id.py).
  strategy_id?: string
  horizon_bucket?: string
  start_date: string
  end_date: string
  capital_mode?: 'lump' | 'sip'
  initial_capital?: number
  sip_amount?: number
  universe_spec?: string
  max_tickers?: number
  min_history_days?: number
  template_name?: string
  preset?: string
  top_n?: number
  lookback_months?: number
}

export interface OrchestratorTriggerResponse {
  run_id: string
  status: string
}

export interface OrchestratorStatusResponse {
  run_id: string
  // 'integrity_check_failed' added (4th fundamental-strategies review, item
  // 4) — a persisted run whose integrity_passed is False is no longer
  // indistinguishable from a genuinely clean 'completed' run here.
  status: 'running' | 'completed' | 'integrity_check_failed' | 'failed' | 'unknown'
  run: BacktestRunSummary | null
  log_tail: string | null
}

export function triggerOrchestratorBacktest(params: OrchestratorTriggerParams) {
  const query = new URLSearchParams()
  for (const [k, v] of Object.entries(params)) {
    if (v !== undefined) query.set(k, String(v))
  }
  return apiPost<OrchestratorTriggerResponse>(`/api/v1/backtest/orchestrator/trigger?${query.toString()}`)
}

export function getOrchestratorStatus(runId: string) {
  return apiGet<OrchestratorStatusResponse>(`/api/v1/backtest/orchestrator/status/${runId}`)
}

export interface TATemplateInfo {
  name: string
  category: string
  description: string
  condition_count: number
}

export function listScreenerTemplates() {
  return apiGet<{ templates: TATemplateInfo[]; count: number }>('/api/v1/ta/screener/templates')
}

// Strategy queue (datastore/api/routers/backtest_runs.py's /queue/trigger
// + /queue/status/{queue_id}) — schedule several strategies (backtests
// and/or an iterative retrain) to run sequentially from one submission,
// instead of triggering each one by hand (backtest/run_strategy_queue.py).
export interface StrategyQueueJob {
  kind: 'orchestrator' | 'iterative_retrain'
  channel?: 'technical' | 'fundamental' | 'momentum'
  strategy_id?: string
  horizon_bucket?: string
  start_date?: string
  end_date?: string
  template_name?: string
  preset?: string
  top_n?: number
  lookback_months?: number
  horizon_days?: number
  folds?: number
}

export interface StrategyQueueTriggerResponse {
  queue_id: string
  status: string
}

export interface StrategyQueueJobResult {
  job_index: number
  kind: string
  job: StrategyQueueJob
  returncode: number
  elapsed_s: number
}

export interface StrategyQueueSummary {
  generated_at: string
  total_jobs: number
  jobs_run: number
  results: StrategyQueueJobResult[]
  all_passed: boolean
  runtime_seconds: number
}

// Per-job Queued/Running/Completed breakdown while a queue is still in
// progress (backtest/run_strategy_queue.py's progress file) — [] once
// `summary` is populated (final per-job state is in summary.results then).
export interface StrategyQueueJobStatus {
  job_index: number
  kind: string
  label: string
  // 'dsr_gate_failed' / 'integrity_check_failed' added (4th fundamental-
  // strategies review, item 4): backend/run_strategy_queue.py can mark a
  // job with either status when its computed DSR falls below an opt-in
  // min_dsr_threshold, or when its persisted run's integrity_passed is
  // False (SPEC-BT-001 critical checks failed) — both are distinct from a
  // subprocess crash ('failed') and from a genuinely clean run
  // ('completed').
  status: 'queued' | 'running' | 'completed' | 'failed' | 'skipped' | 'dsr_gate_failed' | 'integrity_check_failed'
}

export interface StrategyQueueStatusResponse {
  queue_id: string
  status: 'running' | 'completed' | 'failed' | 'unknown'
  summary: StrategyQueueSummary | null
  log_tail: string | null
  jobs: StrategyQueueJobStatus[]
}

export function triggerStrategyQueue(jobs: StrategyQueueJob[], continueOnFailure = false) {
  return apiPost<StrategyQueueTriggerResponse>('/api/v1/backtest/queue/trigger', {
    jobs,
    continue_on_failure: continueOnFailure,
  })
}

export function getStrategyQueueStatus(queueId: string) {
  return apiGet<StrategyQueueStatusResponse>(`/api/v1/backtest/queue/status/${queueId}`)
}

// Discovers queues still running regardless of how/where they were
// triggered (CLI, curl, a different browser session) — so the Backtest
// page's status board isn't limited to only what THIS session triggered.
export interface ActiveQueuesResponse {
  queue_ids: string[]
}

export function listActiveQueues() {
  return apiGet<ActiveQueuesResponse>('/api/v1/backtest/queue/active')
}

// Market regime timeline (datastore/api/routers/regime.py's
// GET /api/v1/macro/market_regimes) — rule-based Bull/Bear/Sideways
// date-range segments, a separate taxonomy from the HMM daily regime
// used elsewhere in the ML pages. See systems/regime/market_regime.py.
export interface MarketRegimeSegment {
  index_name: string
  regime: 'bull' | 'bear' | 'sideways'
  start_date: string
  end_date: string
  confirmed_date: string
  method: string
  move_pct: number | null
}

export interface MarketRegimeSegmentListResponse {
  index_name: string
  segments: MarketRegimeSegment[]
}

// method: classification method, e.g. "20pct_threshold_v1" (default —
// matches the backend's own default, preserving prior single-threshold
// behavior), "15pct_threshold_v1", "10pct_threshold_v1", "5pct_threshold_v1".
// The Backtest page's regime-threshold comparison calls this 4x, once per
// threshold, rather than fetching all methods in one response.
export function getMarketRegimes(indexName = 'Nifty 500', method?: string) {
  return apiGet<MarketRegimeSegmentListResponse>('/api/v1/macro/market_regimes', {
    index_name: indexName,
    ...(method ? { method } : {}),
  })
}

// Experiments comparison page (datastore/api/routers/backtest_runs.py's
// GET /experiments) — one row per backtest_runs entry, metrics unpacked
// from metrics_json, for comparing Entry-template x Exit-variant
// combinations across the 270-job experiment_matrix_45x6.json queue.
export type ExitPolicyVariant =
  | 'baseline'
  | 'condition'
  | 'combined'
  | 'trailing'
  | 'atr_adaptive'
  | 'regime_conditional'

export interface ExperimentRow {
  run_id: string
  strategy_id: string
  channel: BacktestChannel
  exit_policy_variant: ExitPolicyVariant | null
  regime_label: 'bull' | 'bear' | 'sideways' | null
  horizon_bucket: string
  created_at: string
  cagr: number | null
  xirr: number | null
  sharpe: number | null
  sortino: number | null
  calmar: number | null
  max_drawdown: number | null
  win_rate: number | null
  profit_factor: number | null
  turnover_ratio: number | null
  avg_days_held: number | null
  n_trades: number | null
  excess_return: number | null
  has_trade_log: boolean
}

export interface ExperimentListResponse {
  experiments: ExperimentRow[]
}

export function listBacktestExperiments(filters?: {
  strategy_id?: string
  channel?: BacktestChannel
  exit_policy_variant?: ExitPolicyVariant
  regime_label?: string
  limit?: number
}) {
  return apiGet<ExperimentListResponse>('/api/v1/backtest/experiments', filters as Record<string, string | number | boolean | undefined>)
}

// Trade-log CSV download — a plain same-origin-to-API link, not fetched
// via apiGet (it's a file stream, not JSON); the API never hands back the
// raw filesystem path, only this run_id-scoped route.
export function experimentTradeLogUrl(runId: string): string {
  return `${API_BASE_URL}/api/v1/backtest/experiments/${runId}/trade_log`
}

// ---------------------------------------------------------------------------
// TA strategy comparison reports (backtest/ta_comparison_report.py, served by
// datastore/api/routers/backtest_reports.py). One report collates a whole
// queue — up to 65 screener templates over 2007-2026 — on a REALIZED basis:
// realised P&L from the trade books, not mark-to-market, except `rolling`
// which is explicitly mark-to-market off the equity curve.
// ---------------------------------------------------------------------------

export interface TaComparisonListItem {
  name: string
  queue_suffix: string | null
  tax_regime: string | null
  generated_at: string | null
  n_strategies: number | null
  n_failed: number
}

export interface TaEngineMetrics {
  cagr: number | null
  sharpe: number | null
  sortino: number | null
  calmar: number | null
  max_drawdown: number | null
  win_rate: number | null
  profit_factor: number | null
  turnover_ratio: number | null
  xirr: number | null
  final_capital: number | null
  n_trades: number | null
  n_distinct_tickers_traded: number | null
  avg_days_held: number | null
  benchmark_cagr: number | null
  excess_return: number | null
  [key: string]: number | null | undefined
}

/** One Indian financial year (Apr 1 - Mar 31), e.g. "FY2007-08". */
export interface TaYearlyRow {
  trading_year: string
  realized_pnl_inr: number | null
  invested_inr: number | null
  n_trades: number | null
  /** Realised return for the year. NOTE: the field is `return_pct`, not
   *  `return_on_capital` — the latter exists only at strategy level. */
  return_pct: number | null
  win_rate: number | null
  avg_holding_days: number | null
}

/** Concurrent-position stats — "average stocks held". */
export interface TaHoldings {
  avg_concurrent_positions_calendar: number | null
  peak_concurrent_positions: number | null
  n_days_spanned: number | null
}

/** Entry-signal frequency — "signals per month". */
export interface TaEntries {
  avg_entries_per_month: number | null
  by_month: Record<string, number>
}

/** Mark-to-market rolling-window stats, keyed "2y" | "3y" | "4y" | "5y". */
export interface TaRollingStats {
  n_windows: number | null
  min: number | null
  median: number | null
  max: number | null
  mean: number | null
  positive_share: number | null
  median_annualized: number | null
}

/** Per-financial-year tax detail, keyed "FY2007-08". */
export interface TaTaxYear {
  short_term_gain_inr: number | null
  long_term_gain_inr: number | null
  stcg_tax_inr: number | null
  ltcg_tax_inr: number | null
  total_tax_inr: number | null
}

export interface TaTaxBreakdown {
  regime: string | null
  short_term_gain_inr: number | null
  long_term_gain_inr: number | null
  stcg_tax_inr: number | null
  ltcg_tax_inr: number | null
  total_tax_inr: number | null
  pre_tax_pnl_inr: number | null
  post_tax_pnl_inr: number | null
  /** Tax is assessed PER FINANCIAL YEAR — the LTCG exemption is a per-year
   *  allowance, so pooling the whole period would grant it once instead of
   *  once per year. Empty on reports written before 2026-08-10, when
   *  tax_liability() was dropping this breakdown on the floor. */
  per_year: Record<string, TaTaxYear>
}

export interface TaComparisonStrategy {
  template_name: string
  style: string
  start_date: string | null
  end_date: string | null
  initial_capital: number | null
  strategy_id: string | null
  run_id: string | null
  basis: string | null
  engine_metrics: TaEngineMetrics
  yearly: TaYearlyRow[]
  /** Null on runs predating the equity_curve addition — rolling stats are
   *  derived from it, so older reports legitimately have no rolling block. */
  rolling: Record<string, TaRollingStats> | null
  taxes: Record<string, TaTaxBreakdown>
  holdings: TaHoldings | null
  entries: TaEntries | null
  exit_reasons: Record<string, number> | null
  /** Count of CLOSED trades. Note the top-level field is `closed_trades`;
   *  `n_trades` lives under engine_metrics. */
  closed_trades: number | null
  avg_holding_days: number | null
  median_holding_days: number | null
  realized_pnl_inr: number | null
  realized_return_on_capital: number | null
  post_tax_pnl_inr: number | null
  total_tax_inr: number | null
  post_tax_return_on_capital: number | null
  [key: string]: unknown
}

export interface TaComparisonReport {
  generated_at: string
  queue_suffix: string
  tax_regime: string
  basis: string
  n_strategies: number
  failed_reports: Array<{ report: string; error: string }>
  strategies: TaComparisonStrategy[]
}

export function listTaComparisons() {
  return apiGet<{ comparisons: TaComparisonListItem[] }>('/api/v1/backtest/ta-comparisons')
}

export function getTaComparison(name: string) {
  return apiGet<TaComparisonReport>(`/api/v1/backtest/ta-comparisons/${name}`)
}
