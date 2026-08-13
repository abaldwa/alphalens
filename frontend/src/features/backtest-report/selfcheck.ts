/**
 * features/backtest-report/selfcheck.ts
 *
 * Runnable check for the pure logic in this module — strategy identity,
 * display labels, deep links, the momentum adapter, and the persona
 * gates/scoring.
 *
 * Run:
 *     cd frontend && node --experimental-strip-types \
 *         src/features/backtest-report/selfcheck.ts
 *
 * No framework, per AGENTS.md: this repo's frontend has no test runner
 * installed, and adding vitest for a handful of pure functions would be a new
 * dependency where an assert-based self-check does the job. Node 24 strips the
 * types natively and every import here is either type-only or local, so this
 * runs against the real source with nothing to build.
 *
 * The checks worth reading are the ones about MISSING data. A metric the
 * engines do not emit yet must fail a persona gate rather than pass it —
 * Technical currently has no rolling or YoY figures (T13), and the permissive
 * reading would rank every Technical strategy above every Momentum one purely
 * by having less known about it.
 */

import {
  displayLabel,
  formatKey,
  isStrategyKey,
  parseKey,
  parseMomentumVariant,
  sectionUrl,
  shortLabel,
  strategyDetailUrl,
} from './strategyKey.ts'
import { EM_DASH, inr, pct, rate, rateDelta } from './format.ts'
import { classifyRag, periodCagr, ragCounts } from './matrix.ts'
import { crossesUnreliableHistory, resolveWindow } from './useReportParams.ts'
import { toConfigForm } from './deploy/toConfigForm.ts'
import { parsePrefillParam, prefillParam } from './deploy/useDeploySelection.ts'
import { adaptMomentumReport, adaptMomentumVariant } from './adapters/momentum.ts'
import {
  adaptTechnicalReport,
  adaptTechnicalStrategy,
  incomeIsUnverified,
} from './adapters/technical.ts'
import { adaptMlRuns, adaptRun, adaptRuns, horizonDays, yearsBetween } from './adapters/runs.ts'
import {
  PERSONAS,
  evaluatePersona,
  recommendAll,
  rollingMedian,
  rollingPositiveShare,
  worstYear,
  yoyPositiveShare,
} from './recommendations.ts'
import type { StrategyReport } from './types.ts'

let failures = 0
let checks = 0

function ok(condition: boolean, what: string): void {
  checks += 1
  if (!condition) {
    failures += 1
    console.error(`  FAIL  ${what}`)
  }
}

function eq(actual: unknown, expected: unknown, what: string): void {
  const a = JSON.stringify(actual)
  const e = JSON.stringify(expected)
  checks += 1
  if (a !== e) {
    failures += 1
    console.error(`  FAIL  ${what}\n          expected ${e}\n          actual   ${a}`)
  }
}

function throws(fn: () => unknown, what: string): void {
  checks += 1
  try {
    fn()
    failures += 1
    console.error(`  FAIL  ${what} (did not throw)`)
  } catch {
    /* expected */
  }
}

function section(name: string): void {
  console.log(`\n${name}`)
}

// ---------------------------------------------------------------------------
section('strategy identity')
// ---------------------------------------------------------------------------

eq(formatKey('momentum', 'b1_top15'), 'momentum:b1_top15', 'formatKey composes')
eq(parseKey('momentum:b1_top15'), { channel: 'momentum', name: 'b1_top15' }, 'parseKey round-trips')
throws(() => formatKey('momentum', 'a:b'), "':' in a name is rejected (would make the key ambiguous)")
throws(() => parseKey('astrology:x'), 'unknown channel rejected')
throws(() => parseKey('nocolon'), 'missing separator rejected')
ok(!isStrategyKey('nocolon'), 'isStrategyKey is false for a malformed key')
ok(isStrategyKey('technical:A1'), 'isStrategyKey is true for a real key')

// A name containing a colon after the first is legitimate for other channels'
// free-form names, so parseKey must split on the FIRST colon only.
eq(parseKey('technical:A1:v2').name, 'A1:v2', 'splits on the first colon only')

// ---------------------------------------------------------------------------
section('momentum variant parsing and labels')
// ---------------------------------------------------------------------------

const variantId = 'balanced_b1_1-50_lb6mo_monthly_top15'
eq(
  parseMomentumVariant(variantId),
  {
    category: 'balanced',
    bandId: 1,
    rankStart: 1,
    rankEnd: 50,
    lookbackMonths: 6,
    rebalance: 'monthly',
    topN: 15,
  },
  'variant id parses into its parts',
)
eq(parseMomentumVariant('not-a-variant'), null, 'unparseable variant returns null')

const momKey = formatKey('momentum', variantId)
eq(
  displayLabel(momKey),
  'Balanced · Top15 · 6mo · monthly · rank 1-50',
  'one canonical label',
)
eq(
  displayLabel(momKey, { graceCycles: 2 }),
  'Balanced · Top15 · 6mo · monthly · rank 1-50 · g2',
  'grace cycles appended when known',
)
eq(
  displayLabel(formatKey('momentum', 'preset_max_defensive')),
  'Max Defensive (preset)',
  'preset rows labelled',
)
eq(displayLabel(formatKey('technical', 'A1')), 'A1', 'technical label is the template name')
eq(
  displayLabel(formatKey('technical', 'A1'), { exitVariant: 'risk_managed' }),
  'A1 · risk_managed',
  'exit variant appended when known',
)
eq(shortLabel(momKey), 'Balanced · Top15 · 6mo', 'short label drops band and rebalance')

// The label must not vary by screen — that mismatch (hub vs YoY matrix) is
// the specific defect this function exists to remove.
ok(displayLabel(momKey) === displayLabel(momKey), 'label is deterministic')

// ---------------------------------------------------------------------------
section('deep links')
// ---------------------------------------------------------------------------

eq(sectionUrl(''), '/backtest-report', 'hub url')
ok(
  sectionUrl('consistency', { strategy: momKey }).startsWith(
    '/backtest-report/consistency?strategy=',
  ),
  'section url carries the strategy',
)
eq(
  new URL(sectionUrl('consistency', { strategy: momKey }), 'http://x').searchParams.get(
    'strategy',
  ),
  momKey,
  'the strategy param round-trips through url encoding',
)
ok(
  sectionUrl('risk', { strategy: momKey, window: '10y', benchmark: 'Nifty 500' }).includes('window=10y'),
  'window travels in the url',
)
ok(
  sectionUrl('risk', { strategy: momKey, benchmark: 'Nifty 500' }).includes('benchmark=Nifty+500'),
  'benchmark travels in the url',
)
ok(
  strategyDetailUrl(momKey) === `/backtest-report/strategy/${encodeURIComponent(momKey)}`,
  'detail url encodes the key',
)

// ---------------------------------------------------------------------------
section('momentum adapter')
// ---------------------------------------------------------------------------

// Shape mirrors MomentumDynamicReportVariant in pages/momentum/types.ts.
const rawVariant = {
  variant_id: variantId,
  strategy: 'balanced',
  band_id: 1,
  rank_start: 1,
  rank_end: 50,
  lookback_months: 6,
  rebalance_period: 'monthly',
  top_n: 15,
  cagr: 0.24,
  post_tax_cagr: 0.208,
  total_tax_paid: 120000,
  sharpe: 1.1,
  sortino: 1.4,
  calmar: 0.8,
  max_drawdown: -0.31,
  churn_avg_transactions_per_year: 42,
  win_rate: 0.55,
  avg_winner_return_pct: 0.18,
  avg_loser_return_pct: -0.07,
  total_signals: 500,
  n_closed_trades: 300,
  n_open_trades: 15,
  total_trades: 315,
  avg_days_held: 47,
  rolling_2y_min_cagr: 0.02,
  rolling_2y_median_cagr: 0.19,
  rolling_2y_max_cagr: 0.41,
  rolling_2y_n_windows: 15,
  rolling_3y_min_cagr: -0.03,
  rolling_3y_median_cagr: 0.21,
  rolling_3y_max_cagr: 0.36,
  rolling_3y_n_windows: 14,
  rolling_4y_min_cagr: 0.05,
  rolling_4y_median_cagr: 0.22,
  rolling_4y_max_cagr: 0.33,
  rolling_4y_n_windows: 13,
  income_total_withdrawn: 500000,
  income_total_injected: 0,
  income_avg_annual_yield_pct: 0.12,
  income_years_survived_pct: 1,
  income_n_years: 10,
  value_10L: 8_500_000,
  value_10k_sip: 4_200_000,
  sip_cagr: 0.22,
  score: 0.8,
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
} as any

const yoyRows = [
  { variant_id: variantId, fy_label: 'FY2024', return_pct: 0.31 },
  { variant_id: variantId, fy_label: 'FY2023', return_pct: -0.08 },
  { variant_id: variantId, fy_label: 'FY2025', return_pct: 0.14 },
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
] as any

const adapted = adaptMomentumVariant(rawVariant, yoyRows)

eq(adapted.key, momKey, 'adapter produces the canonical key')
eq(adapted.channel, 'momentum', 'channel set')
eq(adapted.returns.cagrPreTax, 0.24, 'pre-tax cagr mapped')
eq(adapted.returns.cagrPostTax, 0.208, 'post-tax cagr mapped')
eq(adapted.risk.maxDrawdown, -0.31, 'drawdown mapped')
eq(adapted.tradeQuality.churnPerYear, 42, 'churn mapped')
eq(adapted.tradeQuality.avgWinnerPct, 0.18, 'avg winner mapped')
eq(adapted.setup.channel, 'momentum', 'setup is the momentum variant')
ok(adapted.setup.channel === 'momentum' && adapted.setup.topN === 15, 'setup carries topN')
ok(adapted.tradeBookUrl?.includes(variantId) === true, 'trade book url points at the variant')

eq(adapted.consistency.rolling.length, 3, 'three rolling windows')
eq(rollingMedian(adapted, 3), 0.21, 'rolling 3y median accessible')

// min > 0 proves every window was positive; a negative min leaves the share
// genuinely unknown from min/median/max alone, and inventing it would feed a
// fabricated number straight into a Conservative gate.
eq(rollingPositiveShare(adapted, 2), 1, '2y positive share inferred from a positive min')
eq(rollingPositiveShare(adapted, 3), null, '3y positive share unknown when min is negative')

eq(adapted.consistency.yoy.map((y) => y.fyLabel), ['FY2023', 'FY2024', 'FY2025'], 'yoy sorted by label')
eq(worstYear(adapted), -0.08, 'worst year found')
eq(Math.round((yoyPositiveShare(adapted) ?? 0) * 100) / 100, 0.67, 'yoy positive share computed')

eq(adapted.equityCurve, null, 'no equity curve yet')
eq(adapted.pending.equityCurve?.backlogId, 'A90', 'missing equity curve names its backlog item')

// The report groups a flat yoy array by variant_id.
const report = adaptMomentumReport({
  generated_at: null,
  report_file: 'x.json',
  score_formula: null,
  variants: [rawVariant],
  yoy: yoyRows,
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
} as any)
eq(report.length, 1, 'report adapts one row')
eq(report[0].consistency.yoy.length, 3, 'yoy rows attached to their variant')
eq(adaptMomentumReport(null).length, 0, 'null report is empty, not a crash')
eq(adaptMomentumReport({ variants: [] } as any).length, 0, 'empty report is empty')

// ---------------------------------------------------------------------------
section('persona gates and scoring')
// ---------------------------------------------------------------------------

function makeReport(over: {
  key: string
  cagr: number
  drawdown: number
  rolling3yShare: number | null
  rolling3yMedian?: number
  yoy: number[]
  trades?: number
  churn?: number
}): StrategyReport {
  return {
    key: over.key,
    label: over.key,
    channel: 'momentum',
    setup: {
      channel: 'momentum',
      universe: null,
      window: { startDate: null, endDate: null, years: null },
      capitalDeployed: 1_000_000,
      sipAmount: null,
      capitalMode: 'lump_sum',
      filters: [],
      exitCriterion: {
        variant: null, stopPct: null, targetPct: null, maxHoldDays: null, trailingPct: null,
      },
      benchmarkIndexName: null,
      lookbackMonths: 6, rebalanceFreq: 'monthly', topN: 15, rankBand: 1,
      rankStart: 1, rankEnd: 50, graceCycles: 2, category: 'balanced',
    },
    returns: {
      cagrPreTax: over.cagr + 0.03,
      cagrPostTax: over.cagr,
      xirr: null, sipXirr: null, finalCapital: null, totalContributed: null,
      benchmarkCagr: null, excessReturn: null, benchmarkIndexName: null, benchmarkCaveat: null,
    },
    consistency: {
      rolling: [
        {
          window: 3,
          minCagr: null,
          medianCagr: over.rolling3yMedian ?? over.cagr,
          maxCagr: null,
          positiveShare: over.rolling3yShare,
          nWindows: 12,
        },
      ],
      yoy: over.yoy.map((v, i) => ({ fyLabel: `FY${2015 + i}`, returnPct: v })),
      ragCounts: null,
    },
    risk: {
      maxDrawdown: over.drawdown, sharpe: null, sortino: null, calmar: null, volatility: null,
    },
    tradeQuality: {
      nTrades: over.trades ?? 200, nClosedTrades: null, nOpenTrades: null,
      winRate: null, profitFactor: null, avgHoldDays: null,
      churnPerYear: over.churn ?? 40, avgWinnerPct: null, avgLoserPct: null, turnoverRatio: null,
    },
    income: null,
    equityCurve: null,
    tradeBookUrl: null,
    pending: {},
  }
}

const steady = makeReport({
  key: 'momentum:steady', cagr: 0.16, drawdown: -0.18,
  rolling3yShare: 1, yoy: [0.12, 0.09, 0.2, 0.05, 0.14],
})
const wild = makeReport({
  key: 'momentum:wild', cagr: 0.34, drawdown: -0.52,
  rolling3yShare: 0.7, yoy: [0.8, -0.35, 0.5, -0.2, 0.6], churn: 120,
})
const middling = makeReport({
  key: 'momentum:middling', cagr: 0.22, drawdown: -0.33,
  rolling3yShare: 0.9, yoy: [0.25, -0.05, 0.3, 0.02, 0.18],
})

const conservative = evaluatePersona([steady, wild, middling], PERSONAS.conservative, 'post_tax')
const conservativePassed = conservative.filter((r) => r.passed).map((r) => r.report.key)
eq(conservativePassed, ['momentum:steady'], 'only the steady strategy clears Conservative')

const wildUnderConservative = conservative.find((r) => r.report.key === 'momentum:wild')
ok(wildUnderConservative?.failedGates.includes('drawdown') === true, 'wild fails the drawdown gate')
ok(wildUnderConservative?.failedGates.includes('rolling3y') === true, 'wild fails the rolling gate')
ok(wildUnderConservative?.failedGates.includes('worstYear') === true, 'wild fails the worst-year gate')
eq(wildUnderConservative?.score, null, 'a rejected strategy has no score')
eq(wildUnderConservative?.rank, null, 'a rejected strategy has no rank')

const highRisk = evaluatePersona([steady, wild, middling], PERSONAS.high_risk, 'post_tax')
eq(highRisk.filter((r) => r.passed)[0]?.report.key, 'momentum:wild', 'High Risk ranks the aggressive one first')
ok(highRisk.filter((r) => r.passed).length === 3, 'High Risk admits all three')

const moderate = evaluatePersona([steady, wild, middling], PERSONAS.moderate, 'post_tax')
eq(
  moderate.filter((r) => r.passed).map((r) => r.report.key).sort(),
  ['momentum:middling', 'momentum:steady'],
  'Moderate admits steady and middling but not wild',
)

// Missing data must FAIL a gate, never pass it.
const unknown = makeReport({
  key: 'momentum:unknown', cagr: 0.5, drawdown: -0.05, rolling3yShare: null, yoy: [],
})
const unknownResult = evaluatePersona([unknown], PERSONAS.conservative, 'post_tax')[0]
ok(!unknownResult.passed, 'a strategy with unknown consistency does not clear Conservative')
ok(unknownResult.failedGates.includes('rolling3y'), 'unknown rolling share fails the gate')
ok(unknownResult.failedGates.includes('worstYear'), 'no yoy history fails the worst-year gate')

// Too few trades is not a track record.
const thin = makeReport({
  key: 'momentum:thin', cagr: 0.4, drawdown: -0.1, rolling3yShare: 1,
  yoy: [0.3, 0.2, 0.1, 0.15, 0.25], trades: 4,
})
ok(
  !evaluatePersona([thin], PERSONAS.conservative, 'post_tax')[0].passed,
  'a 4-trade strategy fails the sample gate',
)

// Tax basis changes the ranking input, not just a label.
const basisPre = evaluatePersona([steady, middling], PERSONAS.moderate, 'pre_tax')
const basisPost = evaluatePersona([steady, middling], PERSONAS.moderate, 'post_tax')
ok(
  basisPre.find((r) => r.report.key === 'momentum:steady')?.components.find((c) => c.metric === 'cagr')?.raw === 0.19,
  'pre-tax basis scores on pre-tax cagr',
)
ok(
  basisPost.find((r) => r.report.key === 'momentum:steady')?.components.find((c) => c.metric === 'cagr')?.raw === 0.16,
  'post-tax basis scores on post-tax cagr',
)

// Score components must explain the score.
const top = moderate.find((r) => r.passed)
if (top) {
  const summed = top.components.reduce((a, c) => a + c.contribution, 0)
  ok(Math.abs(summed - (top.score ?? 0)) < 1e-9, 'components sum to the score')
  ok(top.components.length === 5, 'every weighted metric is reported')
}

// A single candidate cannot be z-scored against anything; it must not NaN.
const solo = evaluatePersona([steady], PERSONAS.moderate, 'post_tax')[0]
ok(solo.passed && solo.score === 0, 'a lone survivor scores 0 rather than NaN')

// Identical candidates have no spread; zero variance must not divide by zero.
const twin = makeReport({
  key: 'momentum:twin', cagr: 0.16, drawdown: -0.18,
  rolling3yShare: 1, yoy: [0.12, 0.09, 0.2, 0.05, 0.14],
})
const twins = evaluatePersona([steady, twin], PERSONAS.moderate, 'post_tax')
ok(twins.every((r) => Number.isFinite(r.score ?? 0)), 'identical candidates score finitely')

const all = recommendAll([steady, wild, middling], 'post_tax')
ok(
  all.conservative.length === 3 && all.moderate.length === 3 && all.high_risk.length === 3,
  'recommendAll reports every strategy for every persona',
)
eq(Object.keys(all).sort(), ['conservative', 'high_risk', 'moderate'], 'three personas')


// ---------------------------------------------------------------------------
section('technical adapter')
// ---------------------------------------------------------------------------

// Rolling-window figures arrive ALREADY annualised from both engines
// (ta_comparison_report.py computes ((e1/e0) ** (1/years) - 1) * 100;
// momentum_metrics returns cagr_pct). The adapter must convert units only.
// An earlier version annualised a second time, understating every Technical
// rolling return by roughly the window length.

const taStrategy = {
  template: 'A1',
  exit_variant: 'risk_managed',
  lump: {
    run_id: 'run_ta_1',
    start_date: '2009-04-01',
    end_date: '2026-08-12',
    cagr_pct: 18.4,
    benchmark_cagr_pct: 12.1,
    sharpe: 0.9,
    sortino: 1.2,
    calmar: 0.6,
    max_drawdown_pct: -38.2,
    total_trades: 412,
    win_rate_pct: 52.5,
    profit_factor: 1.6,
    final_capital: 5_200_000,
    avg_days_held: 19,
    fy_returns: [
      { fy_end: '2024-03-31', fy_label: 'FY2024', opening_equity: 1, closing_equity: 1, return_pct: 22.5, partial: false },
      { fy_end: '2025-03-31', fy_label: 'FY2025', opening_equity: 1, closing_equity: 1, return_pct: -6.2, partial: false },
      { fy_end: '2026-03-31', fy_label: 'FY2026', opening_equity: 1, closing_equity: 1, return_pct: 9.9, partial: true },
    ],
    rolling_returns: {
      '3y': { n_windows: 10, best_pct: 90, median_pct: 33.1, worst_pct: -10, positive_windows: 8 },
      '5y': { n_windows: 8, best_pct: 200, median_pct: 61, worst_pct: 5, positive_windows: 8 },
    },
    trade_log_path: null,
    trade_stats: {
      n_closed: 400, n_wins: 210, n_losses: 190, win_rate_pct: 52.5,
      avg_win_pct: 8.4, avg_loss_pct: -4.1, payoff_ratio: 2.0,
      avg_hold_days: 19, avg_win_hold_days: 24, avg_loss_hold_days: 13,
      best_trade_pct: 140, worst_trade_pct: -32, expectancy_pct: 2.1,
    },
    equity_monthly: [
      { date: '2009-04-30', index: 100 },
      { date: '2009-05-31', index: 108 },
    ],
  },
  annual_reset: {
    ltcg_12_5pct_1_25L: {
      run_id: 'run_ta_1_reset', ltcg_rate: 0.125, ltcg_exemption: 125000,
      n_financial_years: 17, withdrawn_pretax_total: 900000,
      withdrawn_post_tax_total: 820000, tax_paid_total: 80000,
      topped_up_total: 150000, net_extracted: 670000, losing_years: 4,
      unverified: true, unverified_reason: 'measure_3 not yet verified',
    },
  },
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
} as any

const ta = adaptTechnicalStrategy(taStrategy)

eq(ta.key, 'technical:A1', 'technical key')
eq(ta.label, 'A1 · risk_managed', 'technical label carries the exit variant')

// Percent -> fraction, so both channels mean the same thing.
eq(ta.returns.cagrPreTax, 0.184, 'cagr converted from percent to fraction')
eq(ta.returns.benchmarkCagr, 0.121, 'benchmark cagr converted')
eq(Math.round((ta.returns.excessReturn ?? 0) * 1000) / 1000, 0.063, 'excess return computed')
eq(ta.risk.maxDrawdown, -0.382, 'drawdown converted')
eq(ta.tradeQuality.winRate, 0.525, 'win rate converted')
eq(ta.tradeQuality.avgWinnerPct, 0.084, 'avg winner converted')

// Rolling windows annualised, not passed through as totals.
eq(rollingMedian(ta, 3) != null, true, 'technical exposes a 3y rolling median')
eq(
  rollingMedian(ta, 3),
  0.331,
  'rolling median is converted percent -> fraction and NOT re-annualised',
)
eq(ta.consistency.rolling[0].minCagr, -0.1, 'worst window converted, not re-annualised')
eq(ta.consistency.rolling[0].maxCagr, 0.9, 'best window converted, not re-annualised')

// The rule: a return is always a RATE. If the adapter re-derived one, a 3y
// median of 33.1%/yr would collapse to ~10%/yr.
ok(
  (rollingMedian(ta, 3) ?? 0) > 0.3,
  'a 33.1%/yr rolling median stays 33.1%/yr rather than collapsing to ~10%',
)
eq(rollingPositiveShare(ta, 3), 0.8, 'positive share taken from positive_windows/n_windows')
eq(rollingPositiveShare(ta, 5), 1, 'a fully positive window set reports 1')
eq(ta.consistency.rolling.map((w) => w.window), [3, 5], 'rolling windows sorted ascending')

// A partial financial year is not a year's return.
eq(ta.consistency.yoy.map((y) => y.fyLabel), ['FY2024', 'FY2025'], 'partial FY excluded')
eq(ta.consistency.yoy[0].returnPct, 0.225, 'yoy converted to fraction')

eq(ta.equityCurve?.length, 2, 'technical carries an equity curve')
eq(ta.equityCurve?.[0].value, 100, 'equity curve maps index -> value')
ok(ta.tradeBookUrl?.includes('run_ta_1') === true, 'trade book url carries the run id')
eq(ta.pending['returns.cagrPostTax']?.backlogId, 'A86', 'post-tax cagr is pending A86')
eq(ta.income?.nYears, 17, 'income mode mapped')
eq(ta.income?.topUpAfterLoss, true, 'topped-up total implies losing years were refunded')

ok(
  incomeIsUnverified({ strategies: [taStrategy] } as any),
  'unverified annual-reset figures are reported as such',
)

const noLump = adaptTechnicalStrategy({ template: 'B2', exit_variant: 'trailing', lump: null, annual_reset: {} } as any)
eq(noLump.returns.cagrPreTax, null, 'a strategy with no lump run has no cagr')
eq(noLump.consistency.rolling.length, 0, 'and no rolling windows')
ok(noLump.pending['returns.cagrPreTax'] != null, 'and says why')
eq(adaptTechnicalReport(null).length, 0, 'null technical report is empty')

// ---------------------------------------------------------------------------
section('run adapter (ML and fallback)')
// ---------------------------------------------------------------------------

eq(horizonDays('21d'), 21, 'horizon bucket parsed')
eq(horizonDays(null), null, 'missing horizon is null')
eq(horizonDays('abc'), null, 'unparseable horizon is null')
eq(Math.round((yearsBetween('2009-04-01', '2026-04-01') ?? 0)), 17, 'window years computed')
eq(yearsBetween('2026-01-01', '2020-01-01'), null, 'a backwards window is null')
eq(yearsBetween(null, '2020-01-01'), null, 'missing bound is null')

const mlRun = {
  run_id: 'run_ml_1', parent_run_id: null, channel: 'ml', strategy_id: 'signal_21d',
  horizon_bucket: '21d', mode: 'backtest', start_date: '2009-04-01', end_date: '2026-08-12',
  capital_mode: 'lump', initial_capital: 1_000_000, created_at: '2026-08-12',
  config: null,
  metrics: {
    cagr: 0.14, cagr_trading_day_legacy: 0.15, xirr: 0.135, final_capital: 4_000_000,
    total_contributed: 1_000_000, max_drawdown: -0.29, win_rate: 0.51, profit_factor: 1.3,
    sharpe: 0.8, sortino: 1.0, calmar: 0.5, n_distinct_tickers_traded: 200,
    turnover_ratio: 3.2, n_trades: 900, benchmark_cagr: 0.12, excess_return: 0.02,
    benchmark_status: 'ok', cash_position_series: [], avg_days_held: 21,
  },
  data_gaps: [], integrity_passed: true, live_eligible: false,
  buy_signal_count: 0, sell_signal_count: 0, regime_breakdown: [],
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
} as any

const ml = adaptRun(mlRun)
eq(ml.key, 'ml:signal_21d', 'ml key')
eq(ml.channel, 'ml', 'ml channel')
eq(ml.setup.channel === 'ml' ? ml.setup.horizonDays : null, 21, 'ml horizon days')
eq(ml.returns.cagrPreTax, 0.14, 'ml cagr mapped')
eq(ml.returns.benchmarkCaveat, null, 'a healthy benchmark has no caveat')
eq(ml.risk.maxDrawdown, -0.29, 'ml drawdown mapped')
eq(ml.consistency.rolling.length, 0, 'run summaries carry no rolling windows')
eq(ml.pending['consistency.rolling']?.backlogId, 'T13', 'and say which item supplies them')
eq(ml.setup.window.startDate, '2009-04-01', 'run window captured')

const degraded = adaptRun({ ...mlRun, metrics: { ...mlRun.metrics, benchmark_status: 'missing_index' } } as any)
ok(
  degraded.returns.benchmarkCaveat?.includes('missing_index') === true,
  'a degraded benchmark status is surfaced, not dropped',
)

const noMetrics = adaptRun({ ...mlRun, metrics: null } as any)
eq(noMetrics.returns.cagrPreTax, null, 'a run with no metrics does not crash')
eq(noMetrics.risk.sharpe, null, 'and reports nulls')

eq(adaptRuns(null).length, 0, 'null runs list is empty')
eq(
  adaptMlRuns([mlRun, { ...mlRun, channel: 'technical', strategy_id: 'A1' }] as any).length,
  1,
  'adaptMlRuns filters to the ml channel',
)

// Every adapter must agree on units, or the shared tables compare apples to
// oranges. All three express CAGR as a fraction.
ok(
  (adapted.returns.cagrPreTax ?? 0) < 1 &&
    (ta.returns.cagrPreTax ?? 0) < 1 &&
    (ml.returns.cagrPreTax ?? 0) < 1,
  'all three adapters express CAGR as a fraction',
)

// ---------------------------------------------------------------------------
console.log('\nformatters')

// The rate rule made visible: a CAGR renders with a per-year suffix, a
// point-in-time percentage does not. Reading a "24.3%" cell as a total over
// ten years rather than a rate is the exact confusion the suffix prevents.
eq(rate(0.2431), '24.3%/yr', 'rates carry a per-year suffix')
eq(pct(0.2431), '24.3%', 'plain percentages do not')
eq(rateDelta(0.031), '+3.1 pp/yr', 'an excess return is signed and in points per year')
eq(rateDelta(-0.031), '-3.1 pp/yr', 'a negative excess keeps its sign')
eq(rate(null), EM_DASH, 'null is an em dash')
eq(rate(Number.NaN), EM_DASH, 'NaN is an em dash, never "NaN%"')
eq(pct(0), '0.0%', 'a real zero still renders as zero, not as an em dash')
eq(inr(15_000_000), '₹1.50 Cr', 'rupees abbreviate to crore')
eq(inr(250_000), '₹2.50 L', 'and to lakh')

// ---------------------------------------------------------------------------
console.log('\nmatrix')

const matrixCols = [
  { key: 'FY2021', label: 'FY2021' },
  { key: 'FY2022', label: 'FY2022' },
  { key: 'FY2023', label: 'FY2023' },
]
const boundaries = { red: 0, green: 0.18 }

// Geometric mean, not arithmetic: +50% then -50% is a loss, and the
// arithmetic mean says it is flat.
const flat = periodCagr({ FY2021: 0.5, FY2022: -0.5 }, matrixCols.slice(0, 2))
ok(flat != null && flat < -0.13 && flat > -0.14, 'period CAGR compounds rather than averaging')

// A year with no value is skipped, not counted as 0%. A strategy that did not
// exist in FY2021 did not return zero in FY2021 — treating it as zero drags
// its CAGR down and makes a young strategy look worse than it was.
const partial = periodCagr({ FY2022: 0.2, FY2023: 0.2 }, matrixCols)
eq(partial != null ? Number(partial.toFixed(4)) : null, 0.2, 'absent periods are skipped, not zeroed')
eq(periodCagr({}, matrixCols), null, 'no periods at all yields null, not zero')

const counts = ragCounts({ FY2021: -0.1, FY2022: 0.05, FY2023: 0.25 }, matrixCols, boundaries)
eq(counts.red, 1, 'a negative year is red')
eq(counts.amber, 1, 'a positive-but-below-target year is amber')
eq(counts.green, 1, 'a year at or above the green boundary is green')
eq(
  ragCounts({ FY2021: null, FY2022: 0.25 }, matrixCols, boundaries).green,
  1,
  'a null year is counted in no band at all',
)
eq(classifyRag(0.18, boundaries), 'green', 'the green boundary is inclusive')
eq(classifyRag(0, boundaries), 'amber', 'the red boundary is exclusive: flat is not a loss')

// ---------------------------------------------------------------------------
console.log('\nwindow selection')

// Anchored to the report's latest date, not today: anchoring to today
// silently shortens every window whenever the pipeline is a few days behind,
// which it periodically is.
eq(
  resolveWindow('3y', '2026-03-31').startDate,
  '2023-03-31',
  'a 3y window is measured back from the latest available date',
)
eq(resolveWindow('max', '2026-03-31').startDate, null, 'max leaves the start open')
eq(resolveWindow('5y', null).endDate, null, 'no data means no window')
eq(
  resolveWindow('3y', '2026-03-31', { startDate: '2011-01-01', endDate: '2020-01-01' }).startDate,
  '2011-01-01',
  'explicit custom dates win over the preset',
)
ok(
  crossesUnreliableHistory(resolveWindow('20y' as never, '2026-03-31').startDate) ||
    crossesUnreliableHistory('2007-01-01'),
  'a pre-2009 window is flagged as unreliable history',
)
eq(crossesUnreliableHistory('2009-04-01'), false, 'the reliable-from date itself is not flagged')
eq(crossesUnreliableHistory(null), false, 'an open start is not flagged')

// ---------------------------------------------------------------------------
console.log('\ndeploy hand-off')

const deployable = toConfigForm(adapted)
eq(deployable.blockedReason, null, 'a momentum strategy can be carried to the deploy form')
eq(deployable.values.top_n, adapted.setup.channel === 'momentum' ? adapted.setup.topN ?? undefined : undefined, 'top N carries across')
eq(
  deployable.values.lookback_months,
  adapted.setup.channel === 'momentum' ? adapted.setup.lookbackMonths ?? undefined : undefined,
  'lookback carries across',
)

// Deployment choices are NOT strategy attributes. A backtest cannot know how
// much capital you are putting in or when you start, and prefilling a
// plausible-looking zero that the user does not notice is worse than an empty
// field that blocks submit.
ok(deployable.unmapped.includes('initial_capital'), 'initial capital is required input, not inherited')
ok(deployable.unmapped.includes('start_date'), 'start date is required input, not inherited')
ok(deployable.unmapped.includes('portfolio_id'), 'portfolio is required input, not inherited')

// A91: the deploy config schema is momentum-only, so every other channel is
// blocked with a reason rather than silently doing nothing.
const taDeploy = toConfigForm(ta)
ok(taDeploy.blockedReason?.includes('A91') === true, 'a technical strategy is blocked, naming A91')
eq(Object.keys(taDeploy.values).length, 0, 'and carries no values across')
ok(toConfigForm(ml).blockedReason != null, 'an ML strategy is blocked too')

eq(prefillParam(['momentum:a', 'momentum:b']), 'momentum:a,momentum:b', 'prefill param round-trips')
eq(parsePrefillParam('momentum:a,momentum:b').length, 2, 'and parses back')
eq(parsePrefillParam(null).length, 0, 'an absent prefill param is an empty queue')
eq(parsePrefillParam(' , ,').length, 0, 'blank entries are dropped rather than becoming empty keys')

// ---------------------------------------------------------------------------
console.log(`\n${checks - failures}/${checks} checks passed`)
if (failures > 0) {
  // Throwing rather than process.exit: `process` is not in the app tsconfig's
  // type environment, and an uncaught throw exits non-zero just the same, so
  // this file stays type-checked by `npm run build` alongside the code it
  // checks.
  throw new Error(`${failures} of ${checks} self-checks FAILED`)
}
