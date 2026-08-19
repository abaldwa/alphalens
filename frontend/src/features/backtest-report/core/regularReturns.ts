/**
 * features/backtest-report/core/regularReturns.ts
 *
 * The "Regular returns" mode (A88's annual reset), computed from the
 * year-on-year series.
 *
 * WHAT THE MODE MEANS. Long-term CAGR asks "if I never touch it, what does it
 * compound to?". Regular returns asks the opposite and far more practical
 * question: "if I put a fixed sum in and take the year's gain out every
 * March, what income does this actually pay me, and how often does it pay me
 * nothing?" A strategy with a magnificent CAGR carried by two explosive years
 * is a poor answer to the second question, and the two screens rank
 * differently for exactly that reason — which is the point of having both.
 *
 * WHY IT IS COMPUTED HERE. The mode was in the URL and in the type union for
 * months with nothing behind it: `income` is null on every row, so selecting
 * it changed a query parameter, added four permanently-empty columns and
 * nothing else. The engine CAN simulate an annual reset
 * (StrategyPortfolio.annual_reset), but no run in the report was executed
 * that way, and rendering four em dashes is not a feature.
 *
 * The FY return series is enough to answer it exactly, because an annual
 * reset is by construction a sequence of independent one-year bets on the
 * same base capital: withdraw the gain, and next year starts from base again.
 * No re-simulation is needed and none is implied — position sizing is
 * unchanged year to year, which is what a reset means.
 *
 * WHAT IT IS NOT. This is not the engine's income mode and does not pretend
 * to be: it inherits the trades of the compounding run it is derived from. A
 * genuinely reset book would have sized positions off base capital every
 * year and diverged. The difference is small in the first years and grows
 * with the run, so the screen states its basis rather than presenting these
 * as engine output.
 */

import type { IncomeMode, YoyReturn } from './types'

export interface RegularReturnsOptions {
  /** Capital put to work at the start of every financial year. */
  baseCapital: number
  /**
   * A88's two variants.
   *
   * `true` — a losing year is topped back up to base capital out of pocket,
   * so every year starts from the same stake and the withdrawals are a clean
   * income series.
   * `false` — the book carries its loss and must earn its way back before it
   * pays anything again. Harsher, and the honest default: nobody refunds a
   * bad year.
   */
  topUpAfterLoss: boolean
}

export interface RegularReturnsYear {
  fyLabel: string
  /** Capital at work entering the year. */
  openingCapital: number
  returnPct: number | null
  /** Paid out at year end. Zero in a year that did not clear base capital. */
  withdrawn: number
  /** Put back in to restore base capital. Always zero when topUpAfterLoss is
   * false — the shortfall is carried rather than funded. */
  injected: number
  /**
   * How far the year ended BELOW base capital, before any reset.
   *
   * Under `topUpAfterLoss` this is the same money as `injected`, and it was
   * actually funded. Under the carry variant it is NOTIONAL — nobody put it
   * in; it is what the book would need to be whole again, and it is the only
   * honest way to say "this year was a deficit" when the answer to "how much
   * cash came out?" is a flat zero for every losing year alike. A year that
   * ended 4% under base and one that ended 40% under are not the same year,
   * and a column of zeros cannot tell them apart.
   */
  shortfall: number
  /**
   * The single figure the Regular-returns year columns render: cash OUT to
   * the investor as a positive, deficit as a negative. `withdrawn - shortfall`
   * — one of the two is always zero, so this never nets a real payout against
   * a notional hole.
   */
  netCash: number
  /** Carried into next year. */
  closingCapital: number
}

/**
 * Year-by-year, so the summary can be audited rather than trusted.
 *
 * Years with no return figure are skipped entirely rather than treated as
 * flat: a strategy that did not exist in FY2011 neither paid nor lost that
 * year, and counting it as a 0% year would dilute the yield and inflate the
 * "years that paid nothing" count at the same time.
 */
export function regularReturnsSchedule(
  yoy: YoyReturn[],
  { baseCapital, topUpAfterLoss }: RegularReturnsOptions,
): RegularReturnsYear[] {
  const out: RegularReturnsYear[] = []
  let capital = baseCapital
  for (const year of yoy) {
    if (year.returnPct == null || !Number.isFinite(year.returnPct)) continue
    const opening = capital
    const closing = opening * (1 + year.returnPct)
    let withdrawn = 0
    let injected = 0
    const shortfall = Math.max(0, baseCapital - closing)
    if (closing > baseCapital) {
      // Only the excess over base comes out. A year that recovers a previous
      // loss without clearing base pays nothing, which is the whole reason
      // the two variants rank strategies differently.
      withdrawn = closing - baseCapital
      capital = baseCapital
    } else if (topUpAfterLoss) {
      injected = shortfall
      capital = baseCapital
    } else {
      capital = closing
    }
    out.push({
      fyLabel: year.fyLabel,
      openingCapital: opening,
      returnPct: year.returnPct,
      withdrawn,
      injected,
      shortfall,
      netCash: withdrawn - shortfall,
      closingCapital: capital,
    })
  }
  return out
}

/** The schedule collapsed into the IncomeMode block the table renders. */
export function regularReturns(
  yoy: YoyReturn[],
  options: RegularReturnsOptions,
): IncomeMode | null {
  const schedule = regularReturnsSchedule(yoy, options)
  if (!schedule.length) return null
  const totalWithdrawn = schedule.reduce((a, y) => a + y.withdrawn, 0)
  const totalInjected = schedule.reduce((a, y) => a + y.injected, 0)
  const paidYears = schedule.filter((y) => y.withdrawn > 0).length
  return {
    // What the investor set out to live on each year is theirs to choose, so
    // the mode reports what the strategy PAID rather than scoring it against
    // a target this screen never asked for.
    targetWithdrawal: null,
    totalWithdrawn,
    totalInjected,
    // Mean payout as a share of the capital at risk — the closest thing this
    // mode has to a headline, and deliberately not called a CAGR: nothing
    // compounds here, so it is a yield, not a growth rate.
    avgAnnualYieldPct:
      options.baseCapital > 0
        ? totalWithdrawn / options.baseCapital / schedule.length
        : null,
    yearsSurvivedPct: paidYears / schedule.length,
    nYears: schedule.length,
    topUpAfterLoss: options.topUpAfterLoss,
  }
}

/** Base capital for a row: what the run actually deployed, falling back to a
 * round ₹10 lakh only when the run never recorded it — stated in the UI, so a
 * fallback is visible rather than silently scaling everyone's income. */
export function baseCapitalFor(capitalDeployed: number | null): number {
  return capitalDeployed != null && capitalDeployed > 0 ? capitalDeployed : 1_000_000
}

/**
 * The schedule keyed by financial year, for the year columns and the chart.
 *
 * Returns null when the row has no measurable year, so callers render an
 * explained blank rather than a row of zero rupees — a strategy with no data
 * did not pay nothing, it is not known what it paid.
 */
export function regularReturnsByYear(
  yoy: YoyReturn[],
  options: RegularReturnsOptions,
): Map<string, RegularReturnsYear> | null {
  const schedule = regularReturnsSchedule(yoy, options)
  if (!schedule.length) return null
  return new Map(schedule.map((y) => [y.fyLabel, y]))
}
