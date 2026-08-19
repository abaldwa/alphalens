/**
 * features/backtest-report/core/selectByBasis.ts
 *
 * One strategy, several runs, one row.
 *
 * THE BUG THIS EXISTS TO KILL. A strategy is normally backtested twice — once
 * deducting capital-gains tax at each financial-year boundary, once not. Those
 * are two independent simulations: paying tax leaves the book with less cash,
 * so it takes different positions and ends with a different equity curve,
 * different trades and a different excess return. `/api/v1/backtest/runs`
 * returns both, keyed by the same `strategy_id`.
 *
 * The report used to reduce them with a Map keyed on strategy — last run in
 * the response won, arbitrarily — and then patch whichever scalars came out
 * null from the OTHER run. The result was a row assembled from two different
 * simulations: `mom_top10_6m_combined_63d` rendered the post-tax run's CAGR
 * (19.0%/yr) beside the pre-tax run's excess return (-9.3 pp/yr) against a
 * benchmark of 14.2%/yr, three numbers that cannot all be true at once. It
 * also made the tax-basis toggle inert, since whichever run had won already
 * supplied both bases.
 *
 * The rule here instead: A ROW IS ONE RUN. Choosing a tax basis chooses which
 * run to render, and every figure in that row — CAGR, benchmark, excess,
 * drawdown, trades, rolling windows — comes from that single simulation. When
 * no run was measured on the requested basis, the closest run is shown and
 * `reportedTaxBasis` says so, so the header can label it rather than the table
 * quietly answering a different question.
 */

import type { StrategyReport, TaxBasis } from './types'

/**
 * How well a run answers a request for `basis`. Higher wins.
 *
 * A run measured ON the basis is best. A run that carries the basis as its
 * derived `cagr_other_basis` is second: it is arithmetic over the same
 * simulation, honest but a bound rather than an independent result. A run
 * that states neither is last — still rendered, because a row with real
 * drawdown, trade and consistency figures is worth more than a blank line,
 * but never preferred.
 */
export function basisScore(r: StrategyReport, basis: TaxBasis): number {
  if (r.reportedTaxBasis === basis) return 3
  const derived =
    basis === 'post_tax' ? r.returns.cagrPostTax : r.returns.cagrPreTax
  if (derived != null && Number.isFinite(derived)) return 2
  return r.reportedTaxBasis == null ? 1 : 0
}

/**
 * Collapse several candidate rows for the same strategy down to the one that
 * answers `basis` best. Ties break on the row that carries more populated
 * metrics, then on the later run id — so the choice is deterministic across
 * reloads rather than dependent on the order the API happened to return.
 */
export function pickForBasis(
  candidates: StrategyReport[],
  basis: TaxBasis,
): StrategyReport | null {
  if (!candidates.length) return null
  let best = candidates[0]
  let bestScore = basisScore(best, basis)
  for (const c of candidates.slice(1)) {
    const score = basisScore(c, basis)
    if (score > bestScore) {
      best = c
      bestScore = score
      continue
    }
    if (score < bestScore) continue
    const richer = countPopulated(c) - countPopulated(best)
    if (richer > 0 || (richer === 0 && (c.sourceRunId ?? '') > (best.sourceRunId ?? ''))) {
      best = c
    }
  }
  return best
}

/** How many of the headline metrics this row actually has. Only used to break
 * a tie between runs that answer the basis equally well. */
export function countPopulated(r: StrategyReport): number {
  const values = [
    r.returns.cagrPreTax,
    r.returns.cagrPostTax,
    r.returns.xirr,
    r.returns.benchmarkCagr,
    r.returns.excessReturn,
    r.risk.maxDrawdown,
    r.risk.sharpe,
    r.risk.volatility,
    r.tradeQuality.nTrades,
    r.tradeQuality.winRate,
  ]
  let n = values.filter((v) => v != null && Number.isFinite(v)).length
  if (r.consistency.yoy.length) n += 1
  if (r.consistency.rolling.length) n += 1
  return n
}

/** Group by strategy key, then keep one run per key for the requested basis. */
export function selectByBasis(
  reports: StrategyReport[],
  basis: TaxBasis,
): StrategyReport[] {
  const byKey = new Map<string, StrategyReport[]>()
  for (const r of reports) {
    const bucket = byKey.get(r.key)
    if (bucket) bucket.push(r)
    else byKey.set(r.key, [r])
  }
  const out: StrategyReport[] = []
  for (const candidates of byKey.values()) {
    const chosen = pickForBasis(candidates, basis)
    if (chosen) out.push(chosen)
  }
  return out
}
