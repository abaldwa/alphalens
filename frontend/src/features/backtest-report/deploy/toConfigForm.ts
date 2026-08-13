/**
 * features/backtest-report/deploy/toConfigForm.ts
 *
 * StrategyReport -> the momentum deploy form's fields.
 *
 * The point of the whole report section is a deploy/don't-deploy decision, so
 * the decision has to be actionable from the report itself — "with minor
 * changes, we should be able to deploy the strategy".
 *
 * `unmapped` is the load-bearing part. Some fields are deployment choices, not
 * strategy attributes: how much capital you are putting in, when you start,
 * which portfolio it belongs to. A backtest cannot supply them and should not
 * pretend to, so they are returned as a list for the form to mark as required
 * rather than being silently defaulted to a plausible-looking number. A
 * prefilled ₹0 that the user does not notice is worse than an empty field
 * that blocks submit.
 */

import type { MomentumSetup, StrategyReport } from '../types'

/** Mirrors ConfigFormData in pages/momentum/StrategyDeployPage.tsx. Kept
 * structural rather than imported to avoid the report feature depending on a
 * page module; the self-check asserts the field names still line up. */
export interface ConfigFormPrefill {
  band_id?: number
  categories?: string[]
  lookback_months?: number
  top_n?: number
  grace_period?: number
  rebalance_frequency?: 'monthly' | 'biweekly'
  exit_rank?: number | null
  trailing_stop_pct?: number | null
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
  /** Set when the strategy cannot be deployed through this form at all. */
  blockedReason: string | null
}

/** Deployment choices, not strategy attributes — always unmapped. */
const DEPLOYMENT_CHOICES = ['initial_capital', 'start_date', 'portfolio_id']

export function toConfigForm(report: StrategyReport): PrefillResult {
  if (report.channel !== 'momentum') {
    // A91: /api/v1/momentum/configs and the deploy page are momentum-only, so
    // a Technical or ML strategy has nowhere to go. Saying so is better than
    // a checkbox that appears to work and quietly does nothing.
    return {
      values: {},
      unmapped: [],
      blockedReason:
        `${report.channel} strategies cannot be deployed yet: the deploy config schema and ` +
        '/api/v1/momentum/configs are momentum-only (A91).',
    }
  }

  const setup = report.setup as MomentumSetup
  const values: ConfigFormPrefill = {}
  const unmapped: string[] = [...DEPLOYMENT_CHOICES]

  if (setup.rankBand != null) values.band_id = setup.rankBand
  else unmapped.push('band_id')

  if (setup.category) values.categories = [setup.category]
  else unmapped.push('categories')

  if (setup.lookbackMonths != null) values.lookback_months = setup.lookbackMonths
  else unmapped.push('lookback_months')

  if (setup.topN != null) values.top_n = setup.topN
  else unmapped.push('top_n')

  if (setup.graceCycles != null) values.grace_period = setup.graceCycles
  else unmapped.push('grace_period')

  if (setup.rebalanceFreq === 'monthly' || setup.rebalanceFreq === 'biweekly') {
    values.rebalance_frequency = setup.rebalanceFreq
  } else unmapped.push('rebalance_frequency')

  // Exit parameters: null is a real value here (no trailing stop configured),
  // so these are mapped rather than flagged.
  values.exit_rank = setup.exitCriterion.exitRank ?? null
  values.trailing_stop_pct = setup.exitCriterion.trailingPct ?? null

  // SIP is only meaningful if the backtest actually ran one; otherwise it is
  // a deployment choice like the initial capital.
  if (setup.capitalMode === 'sip' && setup.sipAmount != null) {
    values.sip_amount = setup.sipAmount
  } else {
    unmapped.push('sip_amount')
  }

  return { values, unmapped, blockedReason: null }
}

/** True when this strategy can be carried into the deploy form at all. */
export function isDeployable(report: StrategyReport): boolean {
  return report.channel === 'momentum'
}

/** Why the Deploy checkbox is disabled, for the tooltip. */
export function deployBlockedReason(report: StrategyReport): string | null {
  return toConfigForm(report).blockedReason
}
