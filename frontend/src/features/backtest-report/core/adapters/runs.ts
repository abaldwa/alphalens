/**
 * features/backtest-report/adapters/runs.ts
 *
 * `GET /api/v1/backtest/runs` -> StrategyReport[].
 *
 * This is the generic orchestrator-run adapter. It serves ML — which has no
 * richer source, because ml_adapter.py wraps the frozen engine as a black box
 * — and doubles as the fallback for any channel whose specialised report has
 * not been generated yet.
 *
 * It is deliberately the thinnest of the three. BacktestRunMetrics has no
 * rolling windows, no year-on-year series, no churn and no post-tax figure, so
 * rather than leave those blank and unexplained, every one is recorded in
 * `pending` with the backlog item that will supply it. A83's shared report
 * contract is what eventually makes this adapter unnecessary.
 *
 * One deliberate omission: `cagr_trading_day_legacy` is not mapped. It is a
 * second CAGR on a different day-count basis, and carrying both into a table
 * whose header just says "CAGR" is how two numbers that disagree end up
 * looking like a data bug.
 */

import type {
  BacktestRunSummary,
  BacktestRunMetrics,
} from '@/shared/api/backtest'

import { displayLabel, formatKey } from '../strategyKey.ts'
import {
  PENDING_REASONS,
  type Channel,
  type MlSetup,
  type PendingField,
  type RollingWindow,
  type StrategyReport,
  type StrategySetup,
  type TaxBasis,
  type YoyReturn,
} from '../types.ts'

/**
 * `avg_winner_pct` / `avg_loser_pct` arrive in PERCENT (24.13 meaning 24.13%)
 * — backtest/core/engine.py feeds compute_metrics `t.pnl_pct * 100`. Every
 * other number in StrategyReport is a FRACTION, and the table's `pct()`
 * formatter multiplies by 100, so passing these through unscaled rendered an
 * average winner of 24% as "2413.2%". Converted once, here at the boundary,
 * rather than left for each call site to remember.
 */
function fractionFromPercent(v: number | null | undefined): number | null {
  return v == null || !Number.isFinite(v) ? null : v / 100
}

/**
 * The API origin, read the same way shared/api/client.ts reads it.
 *
 * Deliberately NOT imported from there. This module is executed by
 * `npm run selfcheck` under jiti, which resolves plain relative paths but not
 * the `@/` alias — every other import here is `import type` and therefore
 * erased, so pulling in a runtime value from an aliased module is what would
 * break the check. The duplication is two lines and is asserted against by
 * the self-check itself.
 */
function apiOrigin(): string {
  const env = (import.meta as { env?: Record<string, string | undefined> }).env
  return (env?.VITE_API_BASE_URL ?? 'http://localhost:8000').replace(/\/$/, '')
}

/**
 * Metrics this endpoint structurally cannot supply.
 *
 * Only `equityCurve` is left: the orchestrator computes a cash position series
 * but /runs strips it from `metrics_json` before serving, so there is nothing
 * to plot. Everything that used to sit here — rolling windows, year-on-year,
 * churn, avg winner/loser, post-tax CAGR — IS emitted by the orchestrator and
 * is now mapped below. Those entries were stale: they described the engine as
 * it stood when this adapter was written, and kept six populated metrics
 * rendering as explained em dashes long after the engine started emitting them.
 *
 * Pending is now computed per row rather than being a fixed constant, because
 * whether a metric is missing is a property of the run, not of the endpoint.
 */
const STRUCTURAL_PENDING: Record<string, PendingField> = {
  equityCurve: PENDING_REASONS.equityCurve,
}

function buildSetup(run: BacktestRunSummary): StrategySetup {
  const common = {
    universe: null,
    window: {
      startDate: run.start_date,
      endDate: run.end_date,
      years: yearsBetween(run.start_date, run.end_date),
    },
    capitalDeployed: run.initial_capital,
    sipAmount: null,
    capitalMode: (run.capital_mode === 'sip' ? 'sip' : 'lump_sum') as
      | 'sip'
      | 'lump_sum',
    filters: [],
    exitCriterion: {
      variant: null,
      stopPct: null,
      targetPct: null,
      maxHoldDays: null,
      trailingPct: null,
    },
    benchmarkIndexName: null,
  }

  // Only the ML shape is fully expressible from a run summary; the others are
  // represented well enough to render, with their specialised adapters taking
  // over when their report exists.
  if (run.channel === 'ml') {
    const ml: MlSetup = {
      ...common,
      channel: 'ml',
      modelName: run.strategy_id,
      modelVersion: null,
      horizonDays: horizonDays(run.horizon_bucket),
      signalThreshold: null,
      metaLabeler: null,
    }
    return ml
  }

  if (run.channel === 'technical') {
    return {
      ...common,
      channel: 'technical',
      templateName: run.strategy_id,
      templateCategory: null,
      entryConditions: [],
      exitPolicyVariant: null,
      holdingHorizon: run.horizon_bucket,
    }
  }

  if (run.channel === 'fundamental') {
    return {
      ...common,
      channel: 'fundamental',
      preset: run.strategy_id,
      scoreFunction: null,
      kind: null,
      rebalanceFreq: null,
      topN: null,
      excludedSectors: [],
    }
  }

  return {
    ...common,
    channel: 'momentum',
    lookbackMonths: null,
    rebalanceFreq: null,
    topN: null,
    rankBand: null,
    rankStart: null,
    rankEnd: null,
    category: null,
  }
}

/** "21d" / "63d" -> 21 / 63. */
export function horizonDays(bucket: string | null | undefined): number | null {
  if (!bucket) return null
  const n = Number(String(bucket).replace(/[^0-9]/g, ''))
  return Number.isFinite(n) && n > 0 ? n : null
}

export function yearsBetween(
  start: string | null,
  end: string | null,
): number | null {
  if (!start || !end) return null
  const s = Date.parse(start)
  const e = Date.parse(end)
  if (!Number.isFinite(s) || !Number.isFinite(e) || e <= s) return null
  return (e - s) / (365.25 * 24 * 3600 * 1000)
}

/** "2y".."5y" -> RollingWindow[], dropping windows the run was too short to
 * fill. A window with n_windows = 0 is not a zero return, it is no data. */
export function adaptRolling(
  rolling: BacktestRunMetrics['rolling_returns'] | undefined,
): RollingWindow[] {
  if (!rolling) return []
  return Object.entries(rolling)
    .map(([label, w]) => ({
      window: Number(String(label).replace(/[^0-9]/g, '')),
      minCagr: w?.min_cagr ?? null,
      medianCagr: w?.median_cagr ?? null,
      maxCagr: w?.max_cagr ?? null,
      positiveShare: w?.positive_share ?? null,
      nWindows: w?.n_windows ?? null,
    }))
    .filter((w) => Number.isFinite(w.window) && (w.nWindows ?? 0) > 0)
    .sort((a, b) => a.window - b.window)
}

/** The engine marks the first and last financial year `partial` when the
 * window opens or closes mid-year. Those are kept — a partial year is a real
 * return over a real period — but flagged, so "positive years" is not read as
 * a count of full years. */
export function adaptYoy(
  fy: BacktestRunMetrics['fy_returns'] | undefined,
): YoyReturn[] {
  if (!fy?.length) return []
  return fy.map((y) => ({
    fyLabel: y.partial ? `${y.fy_label}*` : y.fy_label,
    returnPct: y.return_pct ?? null,
  }))
}

export function adaptRun(run: BacktestRunSummary): StrategyReport {
  const m: BacktestRunMetrics | null = run.metrics
  const key = formatKey(run.channel as Channel, run.strategy_id)

  // `cagr` is stated on whichever basis `tax_basis` names — the orchestrator
  // defaults to post-tax. Mapping it unconditionally to cagrPreTax (as this
  // adapter used to) labelled a post-tax figure as pre-tax and left the
  // post-tax column empty while the number sat in the payload.
  const basis = m?.tax_basis ?? null
  const cagrPostTax =
    basis === 'post_tax' ? m?.cagr ?? null
    : basis === 'pre_tax' ? m?.cagr_other_basis ?? null
    : null
  const cagrPreTax =
    basis === 'pre_tax' ? m?.cagr ?? null
    : basis === 'post_tax' ? m?.cagr_other_basis ?? null
    // Basis unstated: report the figure without claiming which basis it is.
    : m?.cagr ?? null

  const rolling = adaptRolling(m?.rolling_returns)
  const yoy = adaptYoy(m?.fy_returns)

  // Only claim a metric is pending when it is actually absent, and prefer the
  // engine's own stated reason over a backlog ID when it gave one.
  const pending: Record<string, PendingField> = { ...STRUCTURAL_PENDING }
  if (!rolling.length) pending['consistency.rolling'] = PENDING_REASONS['consistency.rolling']
  if (!yoy.length) pending['consistency.yoy'] = PENDING_REASONS['consistency.yoy']
  if (cagrPostTax == null) pending['returns.cagrPostTax'] = PENDING_REASONS['returns.cagrPostTax']
  if (m?.churn_per_year == null) {
    pending['tradeQuality.churnPerYear'] = PENDING_REASONS['tradeQuality.churnPerYear']
  }
  if (m?.avg_winner_pct == null && m?.avg_loser_pct == null) {
    pending['tradeQuality.avgWinnerPct'] = PENDING_REASONS['tradeQuality.avgWinnerPct']
  }
  if (m?.volatility == null) pending['risk.volatility'] = PENDING_REASONS['risk.volatility']
  if (m?.sortino == null && m?.sortino_none_reason) {
    pending['risk.sortino'] = { reason: m.sortino_none_reason }
  }
  if (m?.calmar == null && m?.calmar_none_reason) {
    pending['risk.calmar'] = { reason: m.calmar_none_reason }
  }

  return {
    key,
    label: displayLabel(key),
    channel: run.channel as Channel,
    setup: buildSetup(run),
    returns: {
      cagrPreTax,
      cagrPostTax,
      xirr: m?.xirr ?? null,
      sipXirr: null,
      finalCapital: m?.final_capital ?? null,
      totalContributed: m?.total_contributed ?? null,
      benchmarkCagr: m?.benchmark_cagr ?? null,
      excessReturn: m?.excess_return ?? null,
      benchmarkIndexName: m?.benchmark_index_name ?? null,
      // benchmark_status records whether the comparison is usable at all
      // (e.g. the index series was missing). Surfaced as the caveat rather
      // than dropped, so a null excess return is explained.
      benchmarkCaveat:
        m?.benchmark_status && m.benchmark_status !== 'ok'
          ? `Benchmark status: ${m.benchmark_status}`
          : null,
    },
    consistency: { rolling, yoy, ragCounts: null },
    risk: {
      maxDrawdown: m?.max_drawdown ?? null,
      sharpe: m?.sharpe ?? null,
      sortino: m?.sortino ?? null,
      calmar: m?.calmar ?? null,
      volatility: m?.volatility ?? null,
    },
    tradeQuality: {
      nTrades: m?.n_trades ?? null,
      nClosedTrades: null,
      nOpenTrades: null,
      winRate: m?.win_rate ?? null,
      profitFactor: m?.profit_factor ?? null,
      avgHoldDays: m?.avg_days_held ?? null,
      churnPerYear: m?.churn_per_year ?? null,
      avgWinnerPct: fractionFromPercent(m?.avg_winner_pct),
      avgLoserPct: fractionFromPercent(m?.avg_loser_pct),
      turnoverRatio: m?.turnover_ratio ?? null,
      nDistinctTickers: m?.n_distinct_tickers_traded ?? null,
      totalTaxPaid: m?.total_tax_paid ?? null,
      nOutlierTrades: m?.n_outlier_trades ?? null,
      maxAbsReturnZscore: m?.max_abs_return_zscore ?? null,
    },
    income: null,
    // Left null here on purpose: the list endpoint strips the series, so the
    // only honest value from THIS payload is "not present". The detail page
    // fetches it per run via useEquityCurve(sourceRunId) rather than the list
    // carrying ~2,500 points per row for a chart no list column draws.
    equityCurve: null,
    // ABSOLUTE, against the API origin. The frontend is served by Vite on
    // :5173 with no /api proxy, so a root-relative href opened the dev
    // server's own 404 page instead of the CSV — the "Trades link 404s" bug.
    // Every other call goes through apiGet, which does this via `new
    // URL(path, API_BASE_URL)`; a plain <a href> has to do it itself.
    tradeBookUrl: `${apiOrigin()}/api/v1/backtest/experiments/${encodeURIComponent(run.run_id)}/trade_log`,
    sourceRunId: run.run_id,
    reportedTaxBasis: (basis as TaxBasis | null) ?? null,
    pending,
  }
}

export function adaptRuns(
  runs: BacktestRunSummary[] | null | undefined,
): StrategyReport[] {
  if (!runs?.length) return []
  return runs.map(adaptRun)
}

/** ML-only convenience, since /runs mixes every channel. */
export function adaptMlRuns(
  runs: BacktestRunSummary[] | null | undefined,
): StrategyReport[] {
  return adaptRuns((runs ?? []).filter((r) => r.channel === 'ml'))
}
