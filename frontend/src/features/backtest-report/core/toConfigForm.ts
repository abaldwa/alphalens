/**
 * features/backtest-report/core/toConfigForm.ts
 *
 * StrategyReport -> the deploy form's fields, and -> the POST body for
 * /api/v1/deployments.
 *
 * The point of the whole report section is a deploy/don't-deploy decision, so
 * the decision has to be actionable from the report itself.
 *
 * `unmapped` is the load-bearing part. Some fields are deployment choices, not
 * strategy attributes: how much capital you are putting in, when you start,
 * whether you are dripping a SIP in, which portfolio it belongs to. A backtest
 * cannot supply them and should not pretend to, so they are returned as a list
 * for the form to mark as required rather than being silently defaulted to a
 * plausible-looking number. A prefilled ₹0 that the user does not notice is
 * worse than an empty field that blocks submit.
 *
 * A91: deployment is no longer momentum-only. /api/v1/deployments is
 * channel-agnostic — it stores a REFERENCE to a strategy_registry row plus the
 * deployment decisions — so Technical, Fundamental and ML strategies deploy
 * through the same path. `blockedReason` survives as the mechanism for the one
 * thing that genuinely cannot be deployed: a retired registry row, which the
 * backend refuses with a 409.
 */

import type { MomentumSetup, StrategyReport } from './types'

/** Mirrors ConfigFormData in pages/momentum/StrategyDeployPage.tsx. Kept
 * structural rather than imported to avoid the report feature depending on a
 * page module; the self-check asserts the field names still line up. */
export interface ConfigFormPrefill {
  band_id?: number
  categories?: string[]
  lookback_months?: number
  top_n?: number
  rebalance_frequency?: 'monthly' | 'biweekly'
  // [2026-08-18] grace_period, exit_rank and trailing_stop_pct deprecated.
  downtrend_filter_pct?: number | null
  hmm_regime_filter?: 'none' | 'bearish' | 'bearish_sideways'
  initial_capital?: number
  sip_amount?: number
  start_date?: string
  rebalance_day_of_month?: number | null
}

export interface PrefillResult {
  values: ConfigFormPrefill
  /** Form fields the report cannot supply. The form must require these
   * before allowing submit. */
  unmapped: string[]
  /** Set when the strategy cannot be deployed at all. */
  blockedReason: string | null
}

/**
 * The POST /api/v1/deployments body, minus the four fields a backtest cannot
 * supply. Those stay out of the type entirely rather than being optional: an
 * optional `initial_capital` is one forgotten spread away from posting
 * `undefined` and letting the backend's own default decide how much money to
 * commit.
 */
export interface DeploymentRequestDraft {
  strategy_key: string
  /** Omitted when unknown, in which case the backend pins the strategy's
   * current registry version. */
  strategy_version?: number
  rebalance_frequency?: string | null
  rebalance_day_of_month?: number | null
  filter_overrides: Record<string, Record<string, unknown>>
  benchmark_index_name?: string | null
  capital_mode: string
  source_run_id?: string | null
}

/** The deployment decisions the caller must collect before POSTing. */
export interface DeploymentDecisions {
  initial_capital: number
  start_date: string
  sip_amount: number
  portfolio_id: number
}

export type DeploymentRequest = DeploymentRequestDraft & DeploymentDecisions

export interface DeploymentDraftResult {
  payload: DeploymentRequestDraft | null
  /** Fields of DeploymentDecisions (plus anything else the report lacks) that
   * the form must collect before this can be submitted. */
  unmapped: string[]
  blockedReason: string | null
}

/** Deployment choices, not strategy attributes — always unmapped. These are
 * exactly the required fields of POST /api/v1/deployments that no backtest can
 * answer. */
export const DEPLOYMENT_CHOICES = [
  'initial_capital',
  'start_date',
  'sip_amount',
  'portfolio_id',
]

/**
 * Why this strategy cannot be deployed, or null.
 *
 * Channel is deliberately NOT a reason any more (A91). Retirement is: the
 * registry says the rules are withdrawn, and deploying them is almost always
 * an accident.
 */
function blockedReasonFor(report: StrategyReport): string | null {
  if (report.status === 'retired') {
    return (
      `${report.label} is retired in the strategy registry. Deploying a ` +
      'retired strategy is almost always an accident; revive it in the ' +
      'registry first if it is deliberate.'
    )
  }
  return null
}

/** StrategySetup.capitalMode -> the API's capital_mode vocabulary. The report
 * says `lump_sum`; the deployments API says `lump`. */
function capitalMode(mode: string | null | undefined): string {
  if (mode === 'sip') return 'sip'
  if (mode === 'annual_reset') return 'annual_reset'
  return 'lump'
}

/** Rebalance cadence, where the channel has one. Technical and ML strategies
 * are event-driven and genuinely have none — that is a null, not a gap. */
function rebalanceFreq(report: StrategyReport): string | null {
  const setup = report.setup
  if (setup.channel === 'momentum' || setup.channel === 'fundamental') {
    return setup.rebalanceFreq ?? null
  }
  return null
}

/**
 * The channel-agnostic deployment body, ready for POST once the caller merges
 * in the four DeploymentDecisions.
 */
export function toDeploymentRequest(report: StrategyReport): DeploymentDraftResult {
  const blockedReason = blockedReasonFor(report)
  if (blockedReason) return { payload: null, unmapped: [], blockedReason }

  const payload: DeploymentRequestDraft = {
    strategy_key: report.key,
    rebalance_frequency: rebalanceFreq(report),
    rebalance_day_of_month: null,
    // A deployment REFERENCES the registry row; the filters are already part
    // of that row, so overriding them here would deploy something other than
    // what was tested.
    filter_overrides: {},
    benchmark_index_name: report.setup.benchmarkIndexName,
    capital_mode: capitalMode(report.setup.capitalMode),
    source_run_id: report.sourceRunId ?? null,
  }
  if (report.strategyVersion != null) payload.strategy_version = report.strategyVersion

  return { payload, unmapped: [...DEPLOYMENT_CHOICES], blockedReason: null }
}

/** Merge the collected decisions into a draft to get a submittable body. */
export function withDeploymentDecisions(
  draft: DeploymentRequestDraft,
  decisions: DeploymentDecisions,
): DeploymentRequest {
  return { ...draft, ...decisions }
}

/**
 * Prefill for the momentum deploy form. Non-momentum channels have no
 * momentum-shaped fields to carry, so `values` is empty for them — that is a
 * mapping fact, not a block: their deploy path is toDeploymentRequest().
 */
export function toConfigForm(report: StrategyReport): PrefillResult {
  const blockedReason = blockedReasonFor(report)
  if (blockedReason) return { values: {}, unmapped: [], blockedReason }

  if (report.setup.channel !== 'momentum') {
    return { values: {}, unmapped: [...DEPLOYMENT_CHOICES], blockedReason: null }
  }

  const setup = report.setup as MomentumSetup
  const values: ConfigFormPrefill = {}
  const unmapped: string[] = DEPLOYMENT_CHOICES.filter((f) => f !== 'sip_amount')

  if (setup.rankBand != null) values.band_id = setup.rankBand
  else unmapped.push('band_id')

  if (setup.category) values.categories = [setup.category]
  else unmapped.push('categories')

  if (setup.lookbackMonths != null) values.lookback_months = setup.lookbackMonths
  else unmapped.push('lookback_months')

  if (setup.topN != null) values.top_n = setup.topN
  else unmapped.push('top_n')

  if (setup.rebalanceFreq === 'monthly' || setup.rebalanceFreq === 'biweekly') {
    values.rebalance_frequency = setup.rebalanceFreq
  } else unmapped.push('rebalance_frequency')

  // SIP is only meaningful if the backtest actually ran one; otherwise it is
  // a deployment choice like the initial capital.
  if (setup.capitalMode === 'sip' && setup.sipAmount != null) {
    values.sip_amount = setup.sipAmount
  } else {
    unmapped.push('sip_amount')
  }

  return { values, unmapped, blockedReason: null }
}

/** True when this strategy can be carried into the deploy flow at all. */
export function isDeployable(report: StrategyReport): boolean {
  return blockedReasonFor(report) === null
}

/** Why the Deploy checkbox is disabled, for the tooltip. */
export function deployBlockedReason(report: StrategyReport): string | null {
  return blockedReasonFor(report)
}
