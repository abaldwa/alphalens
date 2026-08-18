/**
 * features/backtest-report/types.ts
 *
 * The normalized shape every backtest report screen renders, whatever channel
 * produced it (A83).
 *
 * Today each channel emits something different: Momentum's report rows carry
 * post_tax_cagr, churn, avg winner/loser and rolling windows; Technical's
 * metrics block carries none of those and gets YoY/rolling/tax only afterwards
 * from trade-book CSVs, with different semantics. The adapters in ./adapters
 * absorb that, so screens code against one shape.
 *
 * Every metric is nullable on purpose. A field the engine does not yet emit
 * renders as an em dash with the backlog item that will supply it, rather than
 * as a zero — a missing number and a real zero are different facts, and
 * conflating them is how a strategy with no data starts looking like a
 * strategy with no drawdown.
 */

export type Channel = 'momentum' | 'technical' | 'fundamental' | 'ml'

/** Canonical cross-application identity: `{channel}:{name}` (A89). */
export type StrategyKey = string

/** Which basis a returns figure is stated on. Post-tax is the default. */
export type TaxBasis = 'pre_tax' | 'post_tax'

/** Top-level classification: compound everything, or withdraw the excess
 * each year (A88's annual reset). */
export type ReturnMode = 'long_term_cagr' | 'regular_returns'

/** A field the engines do not populate yet. Carried so the UI can say WHY a
 * cell is empty instead of leaving the reader guessing. */
export interface PendingField {
  /** Omitted when the engine itself reported WHY the metric is absent (e.g.
   * `sortino_none_reason`). That is a fact about this run, not an unbuilt
   * backlog item, so attaching a backlog ID to it would misattribute it. */
  backlogId?: string
  reason: string
}

// ---------------------------------------------------------------------------
// setup — channel-discriminated
// ---------------------------------------------------------------------------

export interface BacktestWindow {
  startDate: string | null
  endDate: string | null
  /** Whole years spanned, for the 3y/5y/10y selector. */
  years: number | null
}

export interface FilterRef {
  filterId: string
  params: Record<string, unknown>
}

export interface ExitCriterion {
  variant: string | null
  stopPct: number | null
  targetPct: number | null
  maxHoldDays: number | null
  trailingPct: number | null
  // [2026-08-18] exitRank and graceCycles removed. Momentum exits by rank, but
  // the exit rank IS top_n -- it was never an independent knob once the
  // asymmetric exit band and the grace period were deprecated.
}

export interface StrategySetupCommon {
  universe: string | null
  window: BacktestWindow
  capitalDeployed: number | null
  sipAmount: number | null
  capitalMode: 'lump_sum' | 'sip' | 'annual_reset' | null
  filters: FilterRef[]
  exitCriterion: ExitCriterion
  /** What this strategy's returns are compared against (A98). Distinct from
   * the regime index — conflating them means changing a comparison also
   * changes which regimes the strategy was allowed to trade in. */
  benchmarkIndexName: string | null
}

export interface MomentumSetup extends StrategySetupCommon {
  channel: 'momentum'
  lookbackMonths: number | null
  rebalanceFreq: string | null
  topN: number | null
  rankBand: number | null
  rankStart: number | null
  rankEnd: number | null
  category: string | null
}

export interface TechnicalSetup extends StrategySetupCommon {
  channel: 'technical'
  templateName: string | null
  templateCategory: string | null
  entryConditions: Array<Record<string, unknown>>
  exitPolicyVariant: string | null
  holdingHorizon: string | null
}

export interface FundamentalSetup extends StrategySetupCommon {
  channel: 'fundamental'
  preset: string | null
  scoreFunction: string | null
  /** preset | composite_score | bespoke — a composite score is a ranking with
   * no thresholds, and a bespoke strategy has no declarative form at all. */
  kind: string | null
  rebalanceFreq: string | null
  topN: number | null
  excludedSectors: string[]
}

export interface MlSetup extends StrategySetupCommon {
  channel: 'ml'
  modelName: string | null
  modelVersion: string | null
  horizonDays: number | null
  signalThreshold: number | null
  metaLabeler: boolean | null
}

export type StrategySetup =
  | MomentumSetup
  | TechnicalSetup
  | FundamentalSetup
  | MlSetup

/** A selectable benchmark index for the current window. `live` distinguishes
 * an index that was actually trading throughout from one that reaches the
 * window only through NSE's retrospective back-computation (A104). */
export interface BenchmarkOption {
  indexName: string
  live: boolean
  caveat?: string | null
}

// ---------------------------------------------------------------------------
// metric groups
// ---------------------------------------------------------------------------

export interface Returns {
  cagrPreTax: number | null
  cagrPostTax: number | null
  xirr: number | null
  sipXirr: number | null
  finalCapital: number | null
  totalContributed: number | null
  benchmarkCagr: number | null
  excessReturn: number | null
  /** Named so a cell can state what it was compared against, and so a row
   * stored against a different benchmark is labelled rather than silently
   * re-compared. */
  benchmarkIndexName: string | null
  /** Set when the benchmark did not trade across the whole window and the
   * comparison rests on NSE's back-computed history (A104). */
  benchmarkCaveat: string | null
}

export interface RollingWindow {
  /** Years in the window: 2, 3, 4, 5. */
  window: number
  minCagr: number | null
  medianCagr: number | null
  maxCagr: number | null
  /** Share of windows with a positive return, 0..1. */
  positiveShare: number | null
  nWindows: number | null
}

export interface YoyReturn {
  fyLabel: string
  returnPct: number | null
}

export interface Consistency {
  rolling: RollingWindow[]
  yoy: YoyReturn[]
  /** Counts by RAG band, computed from the YoY series against the user's
   * chosen boundaries. */
  ragCounts: { red: number; amber: number; green: number } | null
}

export interface Risk {
  maxDrawdown: number | null
  sharpe: number | null
  sortino: number | null
  calmar: number | null
  volatility: number | null
}

export interface TradeQuality {
  nTrades: number | null
  nClosedTrades: number | null
  nOpenTrades: number | null
  winRate: number | null
  profitFactor: number | null
  avgHoldDays: number | null
  churnPerYear: number | null
  avgWinnerPct: number | null
  avgLoserPct: number | null
  turnoverRatio: number | null
  /** Breadth: how many different names the strategy actually touched. A high
   * trade count over three tickers is a different strategy from the same
   * count over eighty. */
  nDistinctTickers?: number | null
  /** Realised tax across the window, on the basis `tax_basis` names. */
  totalTaxPaid?: number | null
  /** Trade-book integrity, surfaced on the strategy detail page rather than
   * the section tables — it qualifies the numbers rather than being one of
   * the numbers a deploy decision turns on. */
  nOutlierTrades?: number | null
  maxAbsReturnZscore?: number | null
}

export interface IncomeMode {
  targetWithdrawal: number | null
  totalWithdrawn: number | null
  totalInjected: number | null
  avgAnnualYieldPct: number | null
  yearsSurvivedPct: number | null
  nYears: number | null
  /** A88: whether a losing year is topped back up to base capital, or the
   * strategy continues on what it has left and must earn its way back. */
  topUpAfterLoss: boolean | null
}

export interface EquityPoint {
  date: string
  value: number
}

// ---------------------------------------------------------------------------
// the row every screen renders
// ---------------------------------------------------------------------------

export interface StrategyReport {
  key: StrategyKey
  label: string
  channel: Channel
  setup: StrategySetup
  returns: Returns
  consistency: Consistency
  risk: Risk
  tradeQuality: TradeQuality
  income: IncomeMode | null
  equityCurve: EquityPoint[] | null
  tradeBookUrl: string | null
  /** Registry lifecycle state, when the source knows it. A retired strategy
   * is refused by POST /api/v1/deployments (409), so the UI blocks it rather
   * than letting the user discover that after submit. Absent means "not
   * stated", which is treated as deployable. */
  status?: string | null
  /** Registry version this row was backtested at. Passed to the deployment so
   * the deployed rules are PINNED to the tested ones; omitted, the backend
   * pins the strategy's current version instead. */
  strategyVersion?: number | null
  /** The backtest run this row came from, so a live deployment traces back to
   * its evidence. */
  sourceRunId?: string | null
  /** Which metrics this row cannot supply yet, keyed by dotted path
   * ("returns.cagrPostTax"). Drives the em-dash tooltip. */
  pending: Record<string, PendingField>
}

/** Metrics awaiting engine work, so a blank cell can explain itself. Keys are
 * dotted paths into StrategyReport. */
export const PENDING_REASONS: Record<string, PendingField> = {
  'returns.cagrPostTax': {
    backlogId: 'A86',
    reason:
      'Technical computes tax after the fact on the trade book; post-tax CAGR is not emitted by the engine yet.',
  },
  'consistency.rolling': {
    backlogId: 'T13',
    reason:
      'Rolling-window returns are not in this report. Where both channels do emit them they agree: annualised CAGR per window, never a total.',
  },
  'consistency.yoy': {
    backlogId: 'T13',
    reason: 'Year-on-year returns are not in the Technical report.',
  },
  'tradeQuality.churnPerYear': {
    backlogId: 'T13',
    reason: 'Churn is computed only for Momentum today.',
  },
  'tradeQuality.avgWinnerPct': {
    backlogId: 'T13',
    reason: 'Average winner/loser return is computed only for Momentum today.',
  },
  equityCurve: {
    backlogId: 'A90',
    reason:
      'Both engines compute an equity curve, but neither writes it to the report JSON the frontend reads.',
  },
  income: {
    backlogId: 'A88',
    reason:
      'Income mode exists for the orchestrator; MomentumBacktester has only the withdrawal half, with no shortfall backfill or run-until-profitable.',
  },
}
