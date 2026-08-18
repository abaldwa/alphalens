/**
 * features/backtest-report/adapters/technical.ts
 *
 * `GET /api/v1/technical_backtest/comparison` -> StrategyReport[].
 *
 * A correction to the assumption this module was designed under: the Technical
 * COMPARISON report is far richer than `backtest_runs`' metrics block. It
 * already carries fy_returns, rolling_returns, trade_stats and a monthly
 * equity curve. What T13 is really about is therefore not "Technical has no
 * rolling/YoY" but two narrower problems:
 *
 * PERCENT vs FRACTION. Technical reports percentages (24.3), Momentum
 * fractions (0.243). Everything here is normalised to fractions, which is what
 * the persona gates and formatters expect.
 *
 * [2026-08-13] Both channels' rolling windows are ALREADY annualised rates:
 * ta_comparison_report.py computes ((e1 / e0) ** (1 / years) - 1) * 100 per
 * window, and momentum_metrics.rolling_window_returns returns cagr_pct. An
 * earlier version of this adapter annualised them a second time, which
 * understated every Technical rolling return by roughly the window length.
 * The project rule is that a return is ALWAYS a rate (XIRR% or CAGR%), never
 * a total over a period — so an adapter should only ever be converting units,
 * never re-deriving a rate.
 *
 * The annual-reset block carries `unverified: true` with a reason and the type
 * says "render the caveat, never bare numbers" — so income figures are passed
 * through with that flag intact rather than silently promoted to fact.
 */

import type {
  TAComparisonLump,
  TAComparisonReport,
  TAComparisonStrategy,
  TARollingWindow,
} from '@/pages/technical/types'

import { displayLabel, formatKey } from '../strategyKey.ts'
import {
  PENDING_REASONS,
  type Consistency,
  type EquityPoint,
  type IncomeMode,
  type PendingField,
  type RollingWindow,
  type StrategyReport,
  type TechnicalSetup,
} from '../types.ts'

/** Technical reports percentages; the rest of the app uses fractions. */
function frac(pct: number | null | undefined): number | null {
  return pct == null ? null : pct / 100
}

function rollingWindows(
  rolling: Record<string, TARollingWindow> | undefined,
): RollingWindow[] {
  if (!rolling) return []
  return Object.entries(rolling)
    .map(([label, w]): RollingWindow | null => {
      // Keys look like "3y" / "3" depending on the writer.
      const years = Number(String(label).replace(/[^0-9.]/g, ''))
      if (!Number.isFinite(years) || years <= 0) return null
      // best/median/worst_pct are ALREADY annualised rates, not totals:
      // ta_comparison_report.py's _rolling_windows computes
      // ((e1 / e0) ** (1 / years) - 1) * 100 per window. Only the percent ->
      // fraction conversion is needed. Annualising again here understated
      // every Technical rolling return by roughly the window length.
      return {
        window: years,
        minCagr: frac(w.worst_pct),
        medianCagr: frac(w.median_pct),
        maxCagr: frac(w.best_pct),
        positiveShare:
          w.n_windows > 0 ? w.positive_windows / w.n_windows : null,
        nWindows: w.n_windows,
      }
    })
    .filter((w): w is RollingWindow => w !== null)
    .sort((a, b) => a.window - b.window)
}

function consistency(lump: TAComparisonLump | null): Consistency {
  if (!lump) return { rolling: [], yoy: [], ragCounts: null }
  return {
    rolling: rollingWindows(lump.rolling_returns),
    yoy: (lump.fy_returns ?? [])
      // A partial financial year is not a year's return; including it would
      // drag the "share of positive years" gate around with a stub period.
      .filter((fy) => !fy.partial)
      .map((fy) => ({ fyLabel: fy.fy_label, returnPct: frac(fy.return_pct) }))
      .sort((a, b) => a.fyLabel.localeCompare(b.fyLabel)),
    ragCounts: null,
  }
}

function equityCurve(lump: TAComparisonLump | null): EquityPoint[] | null {
  if (!lump?.equity_monthly?.length) return null
  return lump.equity_monthly.map((p) => ({ date: p.date, value: p.index }))
}

function incomeMode(s: TAComparisonStrategy): IncomeMode | null {
  const resets = Object.values(s.annual_reset ?? {})
  if (resets.length === 0) return null
  const r = resets[0]
  return {
    targetWithdrawal: null,
    totalWithdrawn: r.withdrawn_post_tax_total,
    totalInjected: r.topped_up_total,
    avgAnnualYieldPct: null,
    yearsSurvivedPct:
      r.n_financial_years > 0
        ? (r.n_financial_years - r.losing_years) / r.n_financial_years
        : null,
    nYears: r.n_financial_years,
    // topped_up_total > 0 means losing years WERE refunded to base capital,
    // which is A88's top_up_after_loss = true.
    topUpAfterLoss: r.topped_up_total > 0 ? true : null,
  }
}

export function adaptTechnicalStrategy(s: TAComparisonStrategy): StrategyReport {
  const lump = s.lump
  const key = formatKey('technical', s.template)
  const stats = lump?.trade_stats

  const setup: TechnicalSetup = {
    channel: 'technical',
    universe: null,
    window: {
      startDate: lump?.start_date ?? null,
      endDate: lump?.end_date ?? null,
      years: null,
    },
    capitalDeployed: null,
    sipAmount: null,
    capitalMode: 'lump_sum',
    filters: [],
    exitCriterion: {
      variant: s.exit_variant,
      stopPct: null,
      targetPct: null,
      maxHoldDays: null,
      trailingPct: null,
    },
    benchmarkIndexName: null,
    templateName: s.template,
    templateCategory: s.template?.[0] ?? null,
    entryConditions: [],
    exitPolicyVariant: s.exit_variant,
    holdingHorizon: null,
  }

  const pending: Record<string, PendingField> = {}
  // The comparison report has no post-tax CAGR: tax is computed on the trade
  // book afterwards, not as a rate.
  pending['returns.cagrPostTax'] = PENDING_REASONS['returns.cagrPostTax']
  if (!lump) {
    pending['returns.cagrPreTax'] = {
      backlogId: 'T13',
      reason: 'This strategy has no lump-sum run in the comparison report.',
    }
  }

  const cagr = frac(lump?.cagr_pct ?? null)
  const benchmarkCagr = frac(lump?.benchmark_cagr_pct ?? null)

  return {
    key,
    label: displayLabel(key, { exitVariant: s.exit_variant }),
    channel: 'technical',
    setup,
    returns: {
      cagrPreTax: cagr,
      cagrPostTax: null,
      xirr: null,
      sipXirr: null,
      finalCapital: lump?.final_capital ?? null,
      totalContributed: null,
      benchmarkCagr,
      excessReturn:
        cagr != null && benchmarkCagr != null ? cagr - benchmarkCagr : null,
      // The report does not record WHICH index it compared against; A98 adds
      // that. Naming a guess here would be worse than admitting the gap.
      benchmarkIndexName: null,
      benchmarkCaveat: null,
    },
    consistency: consistency(lump),
    risk: {
      maxDrawdown: frac(lump?.max_drawdown_pct ?? null),
      sharpe: lump?.sharpe ?? null,
      sortino: lump?.sortino ?? null,
      calmar: lump?.calmar ?? null,
      volatility: null,
    },
    tradeQuality: {
      nTrades: lump?.total_trades ?? stats?.n_closed ?? null,
      nClosedTrades: stats?.n_closed ?? null,
      nOpenTrades: null,
      winRate: frac(lump?.win_rate_pct ?? stats?.win_rate_pct ?? null),
      profitFactor: lump?.profit_factor ?? null,
      avgHoldDays: stats?.avg_hold_days ?? lump?.avg_days_held ?? null,
      churnPerYear: null,
      avgWinnerPct: frac(stats?.avg_win_pct ?? null),
      avgLoserPct: frac(stats?.avg_loss_pct ?? null),
      turnoverRatio: null,
    },
    income: incomeMode(s),
    equityCurve: equityCurve(lump),
    tradeBookUrl: lump?.run_id
      ? `/api/v1/technical_backtest/trade_book?run_id=${encodeURIComponent(lump.run_id)}`
      : null,
    pending,
  }
}

export function adaptTechnicalReport(
  report: TAComparisonReport | null | undefined,
): StrategyReport[] {
  if (!report?.strategies) return []
  return report.strategies.map(adaptTechnicalStrategy)
}

/** True when the report's annual-reset figures are flagged unverified, which
 * they currently always are. Callers must render the caveat rather than the
 * bare numbers — see TAComparisonAnnualReset's own type comment. */
export function incomeIsUnverified(report: TAComparisonReport | null): boolean {
  if (!report?.strategies) return false
  return report.strategies.some((s) =>
    Object.values(s.annual_reset ?? {}).some((r) => r.unverified),
  )
}
