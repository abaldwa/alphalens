/**
 * features/backtest-report/recommendations.ts
 *
 * Persona recommendations: hard gates first, then a weighted score over the
 * survivors.
 *
 * The gates and weights are exported as plain data and rendered next to each
 * persona in the UI, so a ranking can be argued with. A recommendation that
 * cannot be interrogated is not a recommendation, it is an oracle — and the
 * whole point of the backtest module is to support a deploy/don't-deploy
 * decision, which means showing the reasoning.
 *
 * Two deliberate choices worth knowing about:
 *
 * 1. A strategy that is MISSING a gated metric fails the gate rather than
 *    passing it. Absent data is not evidence of safety, and with Technical
 *    currently lacking rolling/YoY (T13) the permissive reading would rank
 *    every Technical strategy above every Momentum one purely by having less
 *    known about it.
 *
 * 2. Scoring is z-scored across the candidate set, so it ranks strategies
 *    against each other rather than against absolute thresholds. The gates
 *    carry the absolute judgements; the score only orders what survives.
 */

import type { StrategyReport, TaxBasis } from './types'

export type PersonaId = 'conservative' | 'moderate' | 'high_risk'

export interface GateSpec {
  id: string
  label: string
  /** Human-readable rule, shown in the UI beside the persona. */
  describe: string
  test: (r: StrategyReport) => boolean
}

export interface PersonaSpec {
  id: PersonaId
  label: string
  summary: string
  gates: GateSpec[]
  /** Weights over normalized (z-scored) metrics. Negative = lower is better. */
  weights: {
    cagr: number
    rolling3yMedian: number
    yoyPositiveShare: number
    maxDrawdown: number
    churn: number
  }
}

// ---------------------------------------------------------------------------
// metric accessors
// ---------------------------------------------------------------------------

export function cagrFor(r: StrategyReport, basis: TaxBasis): number | null {
  return basis === 'post_tax' ? r.returns.cagrPostTax : r.returns.cagrPreTax
}

export function rollingMedian(r: StrategyReport, years: number): number | null {
  return r.consistency.rolling.find((w) => w.window === years)?.medianCagr ?? null
}

export function rollingPositiveShare(
  r: StrategyReport,
  years: number,
): number | null {
  return (
    r.consistency.rolling.find((w) => w.window === years)?.positiveShare ?? null
  )
}

/** Share of financial years with a positive return, 0..1. */
export function yoyPositiveShare(r: StrategyReport): number | null {
  const years = r.consistency.yoy.filter((y) => y.returnPct != null)
  if (years.length === 0) return null
  return years.filter((y) => (y.returnPct as number) > 0).length / years.length
}

export function worstYear(r: StrategyReport): number | null {
  const values = r.consistency.yoy
    .map((y) => y.returnPct)
    .filter((v): v is number => v != null)
  return values.length ? Math.min(...values) : null
}

// ---------------------------------------------------------------------------
// gates
// ---------------------------------------------------------------------------

/** Missing data fails. See the note at the top of this file. */
function atLeast(value: number | null, min: number): boolean {
  return value != null && value >= min
}
function atMost(value: number | null, max: number): boolean {
  return value != null && value <= max
}

const MIN_TRADES = 30

function minimumSample(): GateSpec {
  return {
    id: 'sample',
    label: 'Enough trades to mean anything',
    describe: `at least ${MIN_TRADES} trades`,
    test: (r) => atLeast(r.tradeQuality.nTrades, MIN_TRADES),
  }
}

export const PERSONAS: Record<PersonaId, PersonaSpec> = {
  conservative: {
    id: 'conservative',
    label: 'Conservative',
    summary:
      'Prefers never having a bad year to having a great one. Drawdown and consistency dominate; raw CAGR is close to a tiebreaker.',
    gates: [
      minimumSample(),
      {
        id: 'drawdown',
        label: 'Shallow worst-case loss',
        describe: 'max drawdown no worse than 25%',
        test: (r) => atMost(Math.abs(r.risk.maxDrawdown ?? NaN), 0.25),
      },
      {
        id: 'rolling3y',
        label: 'Every 3-year holding period positive',
        describe: 'all rolling 3-year windows positive',
        test: (r) => atLeast(rollingPositiveShare(r, 3), 1),
      },
      {
        id: 'worstYear',
        label: 'No severe single year',
        describe: 'worst financial year no worse than -20%',
        test: (r) => atLeast(worstYear(r), -0.2),
      },
    ],
    weights: {
      cagr: 0.2,
      rolling3yMedian: 0.25,
      yoyPositiveShare: 0.25,
      maxDrawdown: -0.25,
      churn: -0.05,
    },
  },

  moderate: {
    id: 'moderate',
    label: 'Moderate',
    summary:
      'Wants growth but not white knuckles. Balanced weighting, with consistency still counting for more than a single headline number.',
    gates: [
      minimumSample(),
      {
        id: 'drawdown',
        label: 'Tolerable worst-case loss',
        describe: 'max drawdown no worse than 40%',
        test: (r) => atMost(Math.abs(r.risk.maxDrawdown ?? NaN), 0.4),
      },
      {
        id: 'rolling3y',
        label: 'Most 3-year holding periods positive',
        describe: 'at least 80% of rolling 3-year windows positive',
        test: (r) => atLeast(rollingPositiveShare(r, 3), 0.8),
      },
      {
        id: 'yoy',
        label: 'More good years than bad',
        describe: 'at least 60% of financial years positive',
        test: (r) => atLeast(yoyPositiveShare(r), 0.6),
      },
    ],
    weights: {
      cagr: 0.35,
      rolling3yMedian: 0.2,
      yoyPositiveShare: 0.15,
      maxDrawdown: -0.2,
      churn: -0.1,
    },
  },

  high_risk: {
    id: 'high_risk',
    label: 'High Risk',
    summary:
      'Optimises for compounding and accepts deep drawdowns to get it. Gates are a floor against strategies that are merely volatile without being productive.',
    gates: [
      minimumSample(),
      {
        id: 'drawdown',
        label: 'Not ruinous',
        describe: 'max drawdown no worse than 60%',
        test: (r) => atMost(Math.abs(r.risk.maxDrawdown ?? NaN), 0.6),
      },
      {
        id: 'rolling3y',
        label: 'Long holds usually pay',
        describe: 'at least 60% of rolling 3-year windows positive',
        test: (r) => atLeast(rollingPositiveShare(r, 3), 0.6),
      },
    ],
    weights: {
      cagr: 0.6,
      rolling3yMedian: 0.15,
      yoyPositiveShare: 0.05,
      maxDrawdown: -0.1,
      churn: -0.1,
    },
  },
}

export const PERSONA_ORDER: PersonaId[] = [
  'conservative',
  'moderate',
  'high_risk',
]

// ---------------------------------------------------------------------------
// evaluation
// ---------------------------------------------------------------------------

export interface GateResult {
  id: string
  label: string
  describe: string
  passed: boolean
}

export interface ScoreComponent {
  metric: string
  raw: number | null
  z: number
  weight: number
  contribution: number
}

export interface Recommendation {
  report: StrategyReport
  passed: boolean
  gates: GateResult[]
  failedGates: string[]
  score: number | null
  components: ScoreComponent[]
  rank: number | null
}

function mean(values: number[]): number {
  return values.reduce((a, b) => a + b, 0) / values.length
}

function stdev(values: number[], mu: number): number {
  if (values.length < 2) return 0
  const variance =
    values.reduce((acc, v) => acc + (v - mu) ** 2, 0) / (values.length - 1)
  return Math.sqrt(variance)
}

/** z-score against the candidate set. A metric with no spread contributes
 * nothing rather than dividing by zero — if every strategy has the same
 * drawdown, drawdown cannot discriminate between them. */
function zScores(values: Array<number | null>): number[] {
  const present = values.filter((v): v is number => v != null && Number.isFinite(v))
  if (present.length === 0) return values.map(() => 0)
  const mu = mean(present)
  const sd = stdev(present, mu)
  if (sd === 0) return values.map(() => 0)
  return values.map((v) => (v == null || !Number.isFinite(v) ? 0 : (v - mu) / sd))
}

export function evaluatePersona(
  reports: StrategyReport[],
  persona: PersonaSpec,
  basis: TaxBasis,
): Recommendation[] {
  const gated = reports.map((report) => {
    const gates: GateResult[] = persona.gates.map((g) => ({
      id: g.id,
      label: g.label,
      describe: g.describe,
      passed: g.test(report),
    }))
    return {
      report,
      gates,
      passed: gates.every((g) => g.passed),
      failedGates: gates.filter((g) => !g.passed).map((g) => g.id),
    }
  })

  const survivors = gated.filter((g) => g.passed)

  // z-scores are computed over the SURVIVORS, not the whole set: the score
  // ranks the strategies actually on offer, and including rejected ones would
  // let a strategy nobody can pick drag the distribution around.
  const rawCagr = survivors.map((g) => cagrFor(g.report, basis))
  const rawRolling = survivors.map((g) => rollingMedian(g.report, 3))
  const rawYoy = survivors.map((g) => yoyPositiveShare(g.report))
  const rawDd = survivors.map((g) =>
    g.report.risk.maxDrawdown == null ? null : Math.abs(g.report.risk.maxDrawdown),
  )
  const rawChurn = survivors.map((g) => g.report.tradeQuality.churnPerYear)

  const z = {
    cagr: zScores(rawCagr),
    rolling3yMedian: zScores(rawRolling),
    yoyPositiveShare: zScores(rawYoy),
    maxDrawdown: zScores(rawDd),
    churn: zScores(rawChurn),
  }
  const raws = {
    cagr: rawCagr,
    rolling3yMedian: rawRolling,
    yoyPositiveShare: rawYoy,
    maxDrawdown: rawDd,
    churn: rawChurn,
  }

  const scored = survivors.map((g, i) => {
    const components: ScoreComponent[] = (
      Object.keys(persona.weights) as Array<keyof typeof persona.weights>
    ).map((metric) => {
      const weight = persona.weights[metric]
      const zi = z[metric][i]
      return {
        metric,
        raw: raws[metric][i],
        z: zi,
        weight,
        contribution: zi * weight,
      }
    })
    const score = components.reduce((a, c) => a + c.contribution, 0)
    return { ...g, score, components, rank: null as number | null }
  })

  scored.sort((a, b) => (b.score ?? 0) - (a.score ?? 0))
  scored.forEach((s, i) => {
    s.rank = i + 1
  })

  const rejected: Recommendation[] = gated
    .filter((g) => !g.passed)
    .map((g) => ({ ...g, score: null, components: [], rank: null }))

  return [...scored, ...rejected]
}

export function recommendAll(
  reports: StrategyReport[],
  basis: TaxBasis,
): Record<PersonaId, Recommendation[]> {
  return {
    conservative: evaluatePersona(reports, PERSONAS.conservative, basis),
    moderate: evaluatePersona(reports, PERSONAS.moderate, basis),
    high_risk: evaluatePersona(reports, PERSONAS.high_risk, basis),
  }
}

/** Top N that cleared the gates, for the hub summary. */
export function topPicks(
  recommendations: Recommendation[],
  n = 5,
): Recommendation[] {
  return recommendations.filter((r) => r.passed).slice(0, n)
}
