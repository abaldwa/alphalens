/**
 * features/backtest-report/adapters/momentum.ts
 *
 * `GET /api/v1/momentum/dynamic_report` -> StrategyReport[].
 *
 * Momentum is the richest source: it already emits post-tax CAGR, churn,
 * average winner/loser, rolling windows and income-mode fields, so almost
 * everything maps directly. What it does not emit is an equity curve (A90).
 *
 * The YoY rows arrive as a separate flat array keyed by variant_id, so they
 * are grouped once here rather than re-scanned per row by each screen.
 */

import type {
  MomentumDynamicReport,
  MomentumDynamicReportVariant,
  MomentumDynamicReportYoyRow,
} from '@/pages/momentum/types'

import { formatKey, displayLabel } from '../strategyKey'
import {
  PENDING_REASONS,
  type Consistency,
  type IncomeMode,
  type MomentumSetup,
  type PendingField,
  type RollingWindow,
  type StrategyReport,
  type YoyReturn,
} from '../types'

/** The sweep holds grace cycles constant; it is not encoded in variant_id. */
const GRACE_CYCLES = 2

const TRADE_BOOK_BASE = '/api/v1/momentum/dynamic_report/trades'

function rollingWindows(v: MomentumDynamicReportVariant): RollingWindow[] {
  const spec: Array<[number, keyof MomentumDynamicReportVariant, keyof MomentumDynamicReportVariant, keyof MomentumDynamicReportVariant, keyof MomentumDynamicReportVariant]> = [
    [2, 'rolling_2y_min_cagr', 'rolling_2y_median_cagr', 'rolling_2y_max_cagr', 'rolling_2y_n_windows'],
    [3, 'rolling_3y_min_cagr', 'rolling_3y_median_cagr', 'rolling_3y_max_cagr', 'rolling_3y_n_windows'],
    [4, 'rolling_4y_min_cagr', 'rolling_4y_median_cagr', 'rolling_4y_max_cagr', 'rolling_4y_n_windows'],
  ]
  return spec
    .map(([years, minK, medK, maxK, nK]) => ({
      window: years,
      minCagr: (v[minK] as number | null) ?? null,
      medianCagr: (v[medK] as number | null) ?? null,
      maxCagr: (v[maxK] as number | null) ?? null,
      nWindows: (v[nK] as number | null) ?? null,
      // The report gives min/median/max but not the positive share. min > 0
      // proves every window was positive; otherwise it is genuinely unknown
      // from these three numbers, and guessing would put a fabricated figure
      // straight into a Conservative gate.
      positiveShare:
        (v[minK] as number | null) != null && (v[minK] as number) > 0 ? 1 : null,
    }))
    .filter((w) => w.nWindows != null || w.medianCagr != null)
}

function incomeMode(v: MomentumDynamicReportVariant): IncomeMode | null {
  if (
    v.income_n_years == null &&
    v.income_total_withdrawn == null &&
    v.income_avg_annual_yield_pct == null
  ) {
    return null
  }
  return {
    targetWithdrawal: null,
    totalWithdrawn: v.income_total_withdrawn,
    totalInjected: v.income_total_injected,
    avgAnnualYieldPct: v.income_avg_annual_yield_pct,
    yearsSurvivedPct: v.income_years_survived_pct,
    nYears: v.income_n_years,
    // A88's top_up_after_loss reached the orchestrator, not
    // MomentumBacktester, so this is genuinely unknown for momentum rows.
    topUpAfterLoss: null,
  }
}

function consistency(
  v: MomentumDynamicReportVariant,
  yoyRows: MomentumDynamicReportYoyRow[],
): Consistency {
  const yoy: YoyReturn[] = yoyRows
    .map((r) => ({ fyLabel: r.fy_label, returnPct: r.return_pct }))
    .sort((a, b) => a.fyLabel.localeCompare(b.fyLabel))
  return { rolling: rollingWindows(v), yoy, ragCounts: null }
}

export function adaptMomentumVariant(
  v: MomentumDynamicReportVariant,
  yoyRows: MomentumDynamicReportYoyRow[] = [],
): StrategyReport {
  const key = formatKey('momentum', v.variant_id)

  const setup: MomentumSetup = {
    channel: 'momentum',
    universe: 'momentum_rank_band',
    window: { startDate: null, endDate: null, years: null },
    capitalDeployed: v.value_10L != null ? 1_000_000 : null,
    sipAmount: v.value_10k_sip != null ? 10_000 : null,
    capitalMode: 'lump_sum',
    filters: [],
    exitCriterion: {
      variant: 'rank_grace',
      stopPct: null,
      targetPct: null,
      maxHoldDays: null,
      trailingPct: null,
      exitRank: v.top_n,
      graceCycles: GRACE_CYCLES,
    },
    benchmarkIndexName: null,
    lookbackMonths: v.lookback_months,
    rebalanceFreq: v.rebalance_period,
    topN: v.top_n,
    rankBand: v.band_id,
    rankStart: v.rank_start,
    rankEnd: v.rank_end,
    graceCycles: GRACE_CYCLES,
    category: v.strategy,
  }

  const pending: Record<string, PendingField> = {
    equityCurve: PENDING_REASONS.equityCurve,
  }

  return {
    key,
    label: displayLabel(key, { graceCycles: GRACE_CYCLES }),
    channel: 'momentum',
    setup,
    returns: {
      cagrPreTax: v.cagr,
      cagrPostTax: v.post_tax_cagr,
      xirr: null,
      sipXirr: v.sip_cagr,
      finalCapital: v.value_10L,
      totalContributed: null,
      benchmarkCagr: null,
      excessReturn: null,
      benchmarkIndexName: null,
      benchmarkCaveat: null,
    },
    consistency: consistency(v, yoyRows),
    risk: {
      maxDrawdown: v.max_drawdown,
      sharpe: v.sharpe,
      sortino: v.sortino,
      calmar: v.calmar,
      volatility: null,
    },
    tradeQuality: {
      nTrades: v.total_trades,
      nClosedTrades: v.n_closed_trades,
      nOpenTrades: v.n_open_trades,
      winRate: v.win_rate,
      profitFactor: null,
      avgHoldDays: v.avg_days_held,
      churnPerYear: v.churn_avg_transactions_per_year,
      avgWinnerPct: v.avg_winner_return_pct,
      avgLoserPct: v.avg_loser_return_pct,
      turnoverRatio: null,
    },
    income: incomeMode(v),
    equityCurve: null,
    tradeBookUrl: `${TRADE_BOOK_BASE}/${encodeURIComponent(v.variant_id)}`,
    pending,
  }
}

export function adaptMomentumReport(
  report: MomentumDynamicReport | null | undefined,
): StrategyReport[] {
  if (!report?.variants) return []

  const yoyByVariant = new Map<string, MomentumDynamicReportYoyRow[]>()
  for (const row of report.yoy ?? []) {
    const list = yoyByVariant.get(row.variant_id)
    if (list) list.push(row)
    else yoyByVariant.set(row.variant_id, [row])
  }

  return report.variants.map((v) =>
    adaptMomentumVariant(v, yoyByVariant.get(v.variant_id) ?? []),
  )
}
