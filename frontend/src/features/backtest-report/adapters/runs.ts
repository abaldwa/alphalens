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

import { displayLabel, formatKey } from '../strategyKey'
import {
  PENDING_REASONS,
  type Channel,
  type MlSetup,
  type PendingField,
  type StrategyReport,
  type StrategySetup,
} from '../types'

/** Metrics this endpoint structurally cannot supply. */
const RUN_PENDING: Record<string, PendingField> = {
  'consistency.rolling': PENDING_REASONS['consistency.rolling'],
  'consistency.yoy': PENDING_REASONS['consistency.yoy'],
  'returns.cagrPostTax': PENDING_REASONS['returns.cagrPostTax'],
  'tradeQuality.churnPerYear': PENDING_REASONS['tradeQuality.churnPerYear'],
  'tradeQuality.avgWinnerPct': PENDING_REASONS['tradeQuality.avgWinnerPct'],
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
    graceCycles: null,
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

export function adaptRun(run: BacktestRunSummary): StrategyReport {
  const m: BacktestRunMetrics | null = run.metrics
  const key = formatKey(run.channel as Channel, run.strategy_id)

  return {
    key,
    label: displayLabel(key),
    channel: run.channel as Channel,
    setup: buildSetup(run),
    returns: {
      cagrPreTax: m?.cagr ?? null,
      cagrPostTax: null,
      xirr: m?.xirr ?? null,
      sipXirr: null,
      finalCapital: m?.final_capital ?? null,
      totalContributed: m?.total_contributed ?? null,
      benchmarkCagr: m?.benchmark_cagr ?? null,
      excessReturn: m?.excess_return ?? null,
      benchmarkIndexName: null,
      // benchmark_status records whether the comparison is usable at all
      // (e.g. the index series was missing). Surfaced as the caveat rather
      // than dropped, so a null excess return is explained.
      benchmarkCaveat:
        m?.benchmark_status && m.benchmark_status !== 'ok'
          ? `Benchmark status: ${m.benchmark_status}`
          : null,
    },
    consistency: { rolling: [], yoy: [], ragCounts: null },
    risk: {
      maxDrawdown: m?.max_drawdown ?? null,
      sharpe: m?.sharpe ?? null,
      sortino: m?.sortino ?? null,
      calmar: m?.calmar ?? null,
      volatility: null,
    },
    tradeQuality: {
      nTrades: m?.n_trades ?? null,
      nClosedTrades: null,
      nOpenTrades: null,
      winRate: m?.win_rate ?? null,
      profitFactor: m?.profit_factor ?? null,
      avgHoldDays: m?.avg_days_held ?? null,
      churnPerYear: null,
      avgWinnerPct: null,
      avgLoserPct: null,
      turnoverRatio: m?.turnover_ratio ?? null,
    },
    income: null,
    equityCurve: null,
    tradeBookUrl: `/api/v1/backtest/experiments/${encodeURIComponent(run.run_id)}/trade_log`,
    pending: { ...RUN_PENDING },
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
