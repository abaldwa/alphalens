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
import { adaptMomentumReport, adaptMomentumVariant } from './adapters/momentum.ts'
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
console.log(`\n${checks - failures}/${checks} checks passed`)
if (failures > 0) {
  // Throwing rather than process.exit: `process` is not in the app tsconfig's
  // type environment, and an uncaught throw exits non-zero just the same, so
  // this file stays type-checked by `npm run build` alongside the code it
  // checks.
  throw new Error(`${failures} of ${checks} self-checks FAILED`)
}
