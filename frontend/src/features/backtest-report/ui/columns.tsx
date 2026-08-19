/**
 * features/backtest-report/columns.tsx
 *
 * One set of column builders over StrategyReport, replacing the five
 * `use*Columns` hooks in pages/momentum/dynamic-report/shared.tsx. They feed
 * the existing lib/ui/DataTable — which already has sorting, facet filters,
 * search, resize and priority collapse — rather than a second table wrapper.
 *
 * Three decisions worth stating, because they are the difference between a
 * decision table and a data dump:
 *
 * - COLUMNS ARE GROUPED BY DECISION, not by source. Returns / consistency /
 *   risk / trade quality are the five attributes a deploy decision needs, so
 *   each section builds from `identity + one group`.
 * - A NULL METRIC RENDERS ITS REASON. `metricCell` looks the dotted path up in
 *   the row's `pending` map and renders an em dash carrying the backlog ID,
 *   so an empty cell says why it is empty instead of reading as a zero.
 * - PRUNED COLUMNS ARE NOT LOST. cagr_trading_day_legacy, raw run ids and the
 *   outlier-integrity fields are deliberately absent here and live on the
 *   strategy detail page. Collected data stays used; it just stops competing
 *   with the numbers a decision turns on.
 */

import type { ColumnDef } from '@tanstack/react-table'

import { Badge } from '@/lib/ui'

import { StrategyLink } from './StrategyLink'
import { TradesLink } from './TradesLink'
import { cagrOn } from '../core/cagrOn'
import { EM_DASH, days, inr, int, num, pct, rate, rateDelta } from '../core/format'
import { countOf, rollingFromYoy } from '../core/rollingFromYoy'
import type { StrategyReport, TaxBasis } from '../core/types'

/**
 * Excess return DERIVED from the two numbers on screen, never read from the
 * stored field.
 *
 * The engine records `excess_return` against the basis THAT run was measured
 * on. Once the table can show a CAGR on the other basis, or re-score the row
 * against a benchmark the run never used, the stored figure stops being the
 * difference between the two cells beside it — which is how a row came to
 * read "CAGR 34%, Benchmark 14.2%, Excess +16.2%". Subtraction is cheap;
 * three numbers that do not add up are not.
 */
function excessOn(r: StrategyReport, basis: TaxBasis): number | null {
  const own = cagrOn(r, basis)
  const bench = r.returns.benchmarkCagr
  if (own == null || bench == null || !Number.isFinite(own) || !Number.isFinite(bench)) {
    return null
  }
  return own - bench
}

type Col = ColumnDef<StrategyReport, unknown>

/**
 * Renders a metric, or an explained em dash when it is null.
 *
 * The `path` is the dotted key into StrategyReport.pending. A null value with
 * no pending entry still renders an em dash — the row genuinely has no value —
 * but without a tooltip, since inventing a reason would be worse than none.
 */
function metricCell(
  row: StrategyReport,
  path: string,
  value: number | null | undefined,
  format: (v: number | null | undefined) => string,
) {
  if (value != null && Number.isFinite(value)) return format(value)
  const pending = row.pending[path]
  if (!pending) return <span className="text-muted-foreground">{EM_DASH}</span>
  return (
    <span
      className="cursor-help text-muted-foreground underline decoration-dotted underline-offset-2"
      title={
        pending.backlogId
          ? `${pending.backlogId}: ${pending.reason}`
          : pending.reason
      }
    >
      {EM_DASH}
    </span>
  )
}

// Defined in core/cagrOn.ts so core modules can use it without importing a
// .tsx file; re-exported here because this is where every call site expects it.
export { cagrOn } from '../core/cagrOn'

const CHANNEL_VARIANT: Record<
  StrategyReport['channel'],
  'default' | 'secondary' | 'outline'
> = {
  momentum: 'default',
  technical: 'secondary',
  fundamental: 'outline',
  ml: 'outline',
}

/** Strategy name + channel. Present on every table so the same row reads the
 * same way in every section. */
export function identityColumns(section?: string): Col[] {
  return [
    {
      id: 'strategy',
      accessorFn: (r) => r.label,
      header: 'Strategy',
      size: 260,
      cell: (i) => {
        const r = i.row.original
        return (
          <span className="flex flex-wrap items-center gap-1.5">
            <StrategyLink strategyKey={r.key} label={r.label} section={section} />
          </span>
        )
      },
    },
    {
      id: 'channel',
      accessorFn: (r) => r.channel,
      header: 'Channel',
      size: 100,
      cell: (i) => {
        const c = i.row.original.channel
        return <Badge variant={CHANNEL_VARIANT[c]}>{c}</Badge>
      },
    },
  ]
}

/** The common part of `setup` — the part that means the same thing in every
 * channel. Channel-specific fields are deliberately not here; they belong in
 * the detail page, where a screener template is not forced into columns built
 * for a lookback window. */
export function setupColumns(): Col[] {
  return [
    {
      id: 'universe',
      accessorFn: (r) => r.setup.universe,
      header: 'Universe',
      size: 150,
      meta: { priority: 'low', group: 'setup' },
      cell: (i) => {
        const s = i.row.original.setup
        // Prefer an explicit universe string when available.
        if (s.universe) return s.universe
        // Fall back to channel-specific hints so the column is not empty
        // after the refactor: template name for Technical, preset for
        // Fundamental, and a TopN/rank-band hint for Momentum.
        if (s && (s as any).templateName) return (s as any).templateName
        if (s && (s as any).preset) return (s as any).preset
        if (s && (s as any).topN) return `Top${(s as any).topN}`
        if (s && (s as any).rankBand) return `Band ${(s as any).rankBand}`
        return EM_DASH
      },
    },
    {
      id: 'window',
      accessorFn: (r) => r.setup.window.years,
      header: 'Window',
      size: 90,
      meta: { align: 'right', priority: 'low', group: 'setup' },
      cell: (i) => {
        const w = i.row.original.setup.window
        return w.years != null ? `${w.years.toFixed(1)}y` : EM_DASH
      },
    },
    {
      id: 'capital',
      accessorFn: (r) => r.setup.capitalDeployed,
      header: 'Capital',
      size: 100,
      meta: { align: 'right', priority: 'low', group: 'setup' },
      cell: (i) => inr(i.row.original.setup.capitalDeployed),
    },
  ]
}

export function returnsColumns(basis: TaxBasis): Col[] {
  const headline = basis === 'post_tax' ? 'CAGR (post-tax)' : 'CAGR (pre-tax)'
  const secondaryPath =
    basis === 'post_tax' ? 'returns.cagrPreTax' : 'returns.cagrPostTax'
  return [
    {
      id: 'cagr',
      accessorFn: (r) => cagrOn(r, basis),
      header: headline,
      size: 120,
      meta: { align: 'right' },
      cell: (i) =>
        metricCell(
          i.row.original,
          basis === 'post_tax' ? 'returns.cagrPostTax' : 'returns.cagrPreTax',
          cagrOn(i.row.original, basis),
          rate,
        ),
    },
    {
      id: 'cagrOther',
      accessorFn: (r) => cagrOn(r, basis === 'post_tax' ? 'pre_tax' : 'post_tax'),
      header: basis === 'post_tax' ? 'CAGR (pre-tax)' : 'CAGR (post-tax)',
      size: 120,
      meta: { align: 'right', priority: 'medium' },
      cell: (i) =>
        metricCell(
          i.row.original,
          secondaryPath,
          cagrOn(i.row.original, basis === 'post_tax' ? 'pre_tax' : 'post_tax'),
          rate,
        ),
    },
    {
      id: 'xirr',
      accessorFn: (r) => r.returns.xirr,
      header: 'XIRR',
      size: 90,
      meta: { align: 'right', priority: 'medium' },
      cell: (i) => metricCell(i.row.original, 'returns.xirr', i.row.original.returns.xirr, rate),
    },
    {
      id: 'benchmarkCagr',
      accessorFn: (r) => r.returns.benchmarkCagr,
      header: 'Benchmark',
      size: 130,
      meta: { align: 'right', priority: 'medium' },
      cell: (i) => {
        const { benchmarkCagr, benchmarkIndexName, benchmarkCaveat } =
          i.row.original.returns
        if (benchmarkCagr == null) return <span className="text-muted-foreground">{EM_DASH}</span>
        return (
          // The index name rides with the number: two rows compared against
          // different benchmarks look identical otherwise, and the excess
          // return silently means two different things.
          <span title={benchmarkCaveat ?? undefined}>
            {rate(benchmarkCagr)}
            {benchmarkIndexName ? (
              <span className="ml-1 text-muted-foreground">{benchmarkIndexName}</span>
            ) : null}
            {benchmarkCaveat ? <span className="ml-1 text-amber">⚠</span> : null}
          </span>
        )
      },
    },
    {
      id: 'excess',
      accessorFn: (r) => excessOn(r, basis),
      header: 'Excess',
      size: 100,
      meta: { align: 'right' },
      cell: (i) => (
        <span
          title={`${headline} minus the benchmark over the same window. Both figures are annualised, so the difference is in percentage points per year.`}
        >
          {metricCell(
            i.row.original,
            'returns.excessReturn',
            excessOn(i.row.original, basis),
            rateDelta,
          )}
        </span>
      ),
    },
    {
      id: 'finalCapital',
      accessorFn: (r) => r.returns.finalCapital,
      header: 'Final capital',
      size: 110,
      meta: { align: 'right', priority: 'low', group: 'returns' },
      cell: (i) => inr(i.row.original.returns.finalCapital),
    },
    {
      // Low priority and grouped with returns: the post-tax CAGR already
      // carries the tax, so this is the audit trail behind that number, not a
      // second headline competing with it.
      id: 'taxPaid',
      accessorFn: (r) => r.tradeQuality.totalTaxPaid ?? null,
      header: 'Tax paid',
      size: 110,
      meta: { align: 'right', priority: 'low', group: 'returns' },
      cell: (i) => inr(i.row.original.tradeQuality.totalTaxPaid ?? null),
    },
  ]
}

/**
 * Consistency, measured in FINANCIAL YEARS.
 *
 * [FIX 2026-08-19] These columns used to read the engine's `rolling_returns`,
 * which slides a window along the daily equity curve: a 17-year run produced
 * 57 near-identical "3-year windows", one per rebalance date. That made
 * "positive 3-year windows: 87.7%" a statement about days rather than about
 * decisions, and it disagreed with the year-on-year matrix printed directly
 * below it — on `mom_top10_3m_condition_21d` the daily basis reported a 29.7%
 * median and a -8.7% worst window where the sixteen actual three-financial-
 * year holdings gave 31.0% and -6.2%.
 *
 * core/rollingFromYoy recomputes them over consecutive financial years, so
 * every figure here can be checked by eye against the matrix, and counts are
 * shown as counts ("13 of 16") rather than as a share that hides how few
 * windows there were. The engine's daily-basis figures are not discarded —
 * they remain on the strategy detail page, where the difference in method can
 * be stated rather than silently swapped in.
 */
export function consistencyColumns(): Col[] {
  const windowCol = (years: number): Col => ({
    id: `rolling${years}y`,
    accessorFn: (r) => rollingFromYoy(r.consistency.yoy, years)?.medianCagr ?? null,
    header: `${years}y median`,
    size: 110,
    meta: { align: 'right' },
    cell: (i) => {
      const w = rollingFromYoy(i.row.original.consistency.yoy, years)
      return (
        <span
          title={
            w
              ? `Median of the ${w.nWindows} rolling ${years}-financial-year windows in this run, annualised.`
              : undefined
          }
        >
          {metricCell(i.row.original, 'consistency.yoy', w?.medianCagr ?? null, rate)}
        </span>
      )
    },
  })

  return [
    windowCol(3),
    windowCol(5),
    {
      id: 'worstWindow',
      accessorFn: (r) => rollingFromYoy(r.consistency.yoy, 3)?.minCagr ?? null,
      header: 'Worst 3y',
      size: 100,
      meta: { align: 'right', priority: 'medium' },
      cell: (i) => {
        const w = rollingFromYoy(i.row.original.consistency.yoy, 3)
        return (
          <span title="The worst any three consecutive financial years did, annualised. This is the stretch you would actually have had to sit through.">
            {metricCell(i.row.original, 'consistency.yoy', w?.minCagr ?? null, rate)}
          </span>
        )
      },
    },
    {
      // Sorts on the share so the ranking is comparable across runs of
      // different lengths; DISPLAYS the count, because "13 of 16" and "13 of
      // 16000" are the same percentage and not remotely the same evidence.
      id: 'positiveWindows',
      accessorFn: (r) => {
        const w = rollingFromYoy(r.consistency.yoy, 3)
        return w && w.nWindows ? w.nPositive / w.nWindows : null
      },
      header: 'Positive 3y windows',
      size: 150,
      meta: { align: 'right', priority: 'medium' },
      cell: (i) => {
        const w = rollingFromYoy(i.row.original.consistency.yoy, 3)
        if (!w) return metricCell(i.row.original, 'consistency.yoy', null, pct)
        return (
          <span
            className="tabular-nums"
            title={`${w.nPositive} of the ${w.nWindows} rolling 3-financial-year windows ended positive (${pct(w.nPositive / w.nWindows)}).`}
          >
            {countOf(w.nPositive, w.nWindows)}
          </span>
        )
      },
    },
    {
      id: 'positiveYears',
      accessorFn: (r) => {
        const yoy = r.consistency.yoy.filter((y) => y.returnPct != null)
        return yoy.length ? yoy.filter((y) => (y.returnPct ?? 0) > 0).length / yoy.length : null
      },
      header: 'Positive years',
      size: 120,
      meta: { align: 'right' },
      cell: (i) => {
        const yoy = i.row.original.consistency.yoy.filter((y) => y.returnPct != null)
        if (!yoy.length) return metricCell(i.row.original, 'consistency.yoy', null, pct)
        const up = yoy.filter((y) => (y.returnPct ?? 0) > 0).length
        return (
          <span
            className="tabular-nums"
            title={`${up} of ${yoy.length} financial years ended positive (${pct(up / yoy.length)}). A year marked * in the matrix is partial.`}
          >
            {countOf(up, yoy.length)}
          </span>
        )
      },
    },
  ]
}

export function riskColumns(): Col[] {
  return [
    {
      id: 'maxDrawdown',
      accessorFn: (r) => r.risk.maxDrawdown,
      header: 'Max drawdown',
      size: 120,
      meta: { align: 'right' },
      cell: (i) => metricCell(i.row.original, 'risk.maxDrawdown', i.row.original.risk.maxDrawdown, pct),
    },
    {
      id: 'sharpe',
      accessorFn: (r) => r.risk.sharpe,
      header: 'Sharpe',
      size: 80,
      meta: { align: 'right' },
      cell: (i) => metricCell(i.row.original, 'risk.sharpe', i.row.original.risk.sharpe, num),
    },
    {
      id: 'sortino',
      accessorFn: (r) => r.risk.sortino,
      header: 'Sortino',
      size: 80,
      meta: { align: 'right', priority: 'medium' },
      cell: (i) => metricCell(i.row.original, 'risk.sortino', i.row.original.risk.sortino, num),
    },
    {
      id: 'calmar',
      accessorFn: (r) => r.risk.calmar,
      header: 'Calmar',
      size: 80,
      meta: { align: 'right', priority: 'medium' },
      cell: (i) => metricCell(i.row.original, 'risk.calmar', i.row.original.risk.calmar, num),
    },
    {
      id: 'volatility',
      accessorFn: (r) => r.risk.volatility,
      header: 'Volatility',
      size: 95,
      meta: { align: 'right', priority: 'low', group: 'risk' },
      cell: (i) => metricCell(i.row.original, 'risk.volatility', i.row.original.risk.volatility, pct),
    },
  ]
}

export function tradeQualityColumns(): Col[] {
  return [
    {
      id: 'nTrades',
      accessorFn: (r) => r.tradeQuality.nTrades,
      header: 'Trades',
      size: 80,
      meta: { align: 'right' },
      cell: (i) => int(i.row.original.tradeQuality.nTrades),
    },
    {
      id: 'churn',
      accessorFn: (r) => r.tradeQuality.churnPerYear,
      header: 'Churn/yr',
      size: 90,
      meta: { align: 'right' },
      cell: (i) =>
        metricCell(
          i.row.original,
          'tradeQuality.churnPerYear',
          i.row.original.tradeQuality.churnPerYear,
          (v) => num(v, 1),
        ),
    },
    {
      id: 'avgHoldDays',
      accessorFn: (r) => r.tradeQuality.avgHoldDays,
      header: 'Avg hold',
      size: 85,
      meta: { align: 'right' },
      cell: (i) =>
        metricCell(
          i.row.original,
          'tradeQuality.avgHoldDays',
          i.row.original.tradeQuality.avgHoldDays,
          days,
        ),
    },
    {
      // "Win rate" and "Avg win" were being read as the same idea in two
      // units. They are different questions: HOW OFTEN the strategy is right,
      // and HOW MUCH it makes when it is. The headers now say which.
      id: 'winRate',
      accessorFn: (r) => r.tradeQuality.winRate,
      header: '% trades won',
      size: 110,
      meta: { align: 'right' },
      cell: (i) => (
        <span title="Share of closed trades that ended in profit. Says nothing about size — a strategy can win 70% of the time and still lose money.">
          {metricCell(
            i.row.original,
            'tradeQuality.winRate',
            i.row.original.tradeQuality.winRate,
            pct,
          )}
        </span>
      ),
    },
    {
      id: 'avgWinLoss',
      // Sorts on the ratio, displays both sides: a 3:1 winner/loser with a 30%
      // win rate and a 1:1 with a 60% win rate are different strategies, and
      // either number alone hides which one you are looking at.
      accessorFn: (r) => {
        const { avgWinnerPct, avgLoserPct } = r.tradeQuality
        if (avgWinnerPct == null || !avgLoserPct) return null
        return Math.abs(avgWinnerPct / avgLoserPct)
      },
      header: 'Avg gain / avg loss per trade',
      size: 190,
      meta: { align: 'right' },
      cell: (i) => {
        const { avgWinnerPct, avgLoserPct } = i.row.original.tradeQuality
        if (avgWinnerPct == null && avgLoserPct == null) {
          return metricCell(i.row.original, 'tradeQuality.avgWinnerPct', null, pct)
        }
        // These are per-trade outcomes, not period performance, so they are
        // plain percentages — annualising a 3-day trade is meaningless.
        //
        // [FIX 2026-08-19] They arrive from the engine already in percent
        // (24.13 meaning 24.13%) and were being run through pct(), which
        // multiplies by 100 — a 24% average winner rendered as "2413.2%",
        // which is the "numbers out of place" on the Trade quality screen.
        // The conversion now happens once in core/adapters/runs.ts, at the
        // boundary where the unit changes.
        return (
          <span
            className="tabular-nums"
            title="Mean return of the winning trades against the mean return of the losing ones. Read it beside '% trades won': a 3:1 ratio at a 30% win rate and a 1:1 ratio at a 60% win rate are different strategies."
          >
            <span className="text-green">{pct(avgWinnerPct)}</span>
            {' / '}
            <span className="text-red">{pct(avgLoserPct)}</span>
          </span>
        )
      },
    },
    {
      id: 'profitFactor',
      accessorFn: (r) => r.tradeQuality.profitFactor,
      header: 'Profit factor',
      size: 105,
      meta: { align: 'right', priority: 'medium' },
      cell: (i) => metricCell(i.row.original, 'tradeQuality.profitFactor', i.row.original.tradeQuality.profitFactor, num),
    },
    {
      id: 'turnover',
      accessorFn: (r) => r.tradeQuality.turnoverRatio,
      header: 'Turnover',
      size: 95,
      meta: { align: 'right', priority: 'low', group: 'trade' },
      cell: (i) => metricCell(i.row.original, 'tradeQuality.turnoverRatio', i.row.original.tradeQuality.turnoverRatio, num),
    },
    {
      // Breadth. Low priority because it qualifies the trade count rather than
      // standing beside it: 40 trades over 8 names and 40 over 40 are
      // different strategies, and `Trades` alone cannot tell them apart.
      id: 'distinctTickers',
      accessorFn: (r) => r.tradeQuality.nDistinctTickers ?? null,
      // "Names" was market shorthand nobody outside a trading desk reads as
      // "how many different stocks". Spelled out.
      header: 'Distinct stocks traded',
      size: 165,
      meta: { align: 'right', priority: 'low', group: 'trade' },
      cell: (i) => (
        <span title="How many different tickers the strategy ever held. 40 trades across 8 stocks and 40 across 40 are different strategies, and the trade count alone cannot tell them apart.">
          {int(i.row.original.tradeQuality.nDistinctTickers ?? null)}
        </span>
      ),
    },
  ]
}

/**
 * Regular-returns mode: what the strategy PAYS, not what it compounds to.
 *
 * Every figure comes from core/regularReturns, which replays the year-on-year
 * series as a sequence of one-year bets on the same base capital. Counts are
 * counts here for the same reason they are on the consistency table — "paid
 * in 11 of 18 years" is a fact you can act on; "61.1%" is a statistic.
 */
export function incomeColumns(): Col[] {
  return [
    {
      id: 'avgAnnualYield',
      accessorFn: (r) => r.income?.avgAnnualYieldPct ?? null,
      header: 'Avg annual payout',
      size: 145,
      meta: { align: 'right' },
      cell: (i) => (
        <span title="Mean yearly withdrawal as a share of the capital at work. A yield, not a growth rate — nothing compounds in this mode, so it is deliberately not called a CAGR.">
          {metricCell(
            i.row.original,
            'income',
            i.row.original.income?.avgAnnualYieldPct ?? null,
            pct,
          )}
        </span>
      ),
    },
    {
      id: 'totalWithdrawn',
      accessorFn: (r) => r.income?.totalWithdrawn ?? null,
      header: 'Total drawn',
      size: 115,
      meta: { align: 'right' },
      cell: (i) => (
        <span title="Every year's gain above base capital, added up across the run.">
          {inr(i.row.original.income?.totalWithdrawn)}
        </span>
      ),
    },
    {
      id: 'yearsPaid',
      // Sorts on the share so runs of different lengths rank comparably;
      // displays the count.
      accessorFn: (r) => r.income?.yearsSurvivedPct ?? null,
      header: 'Years it paid',
      size: 120,
      meta: { align: 'right' },
      cell: (i) => {
        const income = i.row.original.income
        if (!income || income.nYears == null || income.yearsSurvivedPct == null) {
          return metricCell(i.row.original, 'income', null, pct)
        }
        const paid = Math.round(income.yearsSurvivedPct * income.nYears)
        return (
          <span
            className="tabular-nums"
            title={`Paid something in ${paid} of ${income.nYears} financial years (${pct(income.yearsSurvivedPct)}). The other years cleared nothing above base capital.`}
          >
            {countOf(paid, income.nYears)}
          </span>
        )
      },
    },
    {
      id: 'totalInjected',
      accessorFn: (r) => r.income?.totalInjected ?? null,
      header: 'Topped back up',
      size: 130,
      meta: { align: 'right', priority: 'medium' },
      cell: (i) => {
        const income = i.row.original.income
        if (!income) return metricCell(i.row.original, 'income', null, pct)
        if (!income.topUpAfterLoss) {
          return (
            <span
              className="text-muted-foreground"
              title="This run carries its losses instead: nothing is put back after a bad year, so the book must earn its way back to base capital before it pays again."
            >
              {EM_DASH}
            </span>
          )
        }
        return (
          <span title="Cash put back in after losing years to restore base capital. Money in, not money earned — subtract it from the total drawn before calling the strategy an income source.">
            {inr(income.totalInjected)}
          </span>
        )
      },
    },
    {
      id: 'netIncome',
      // The number the mode exists to produce: what the investor actually
      // ends up with, after funding the bad years.
      accessorFn: (r) =>
        r.income ? (r.income.totalWithdrawn ?? 0) - (r.income.totalInjected ?? 0) : null,
      header: 'Net of top-ups',
      size: 135,
      meta: { align: 'right' },
      cell: (i) => {
        const income = i.row.original.income
        if (!income) return metricCell(i.row.original, 'income', null, pct)
        const net = (income.totalWithdrawn ?? 0) - (income.totalInjected ?? 0)
        return (
          <span
            className={net < 0 ? 'text-red tabular-nums' : 'tabular-nums'}
            title="Total drawn less everything put back. Negative means the strategy consumed more capital than it ever paid out."
          >
            {inr(net)}
          </span>
        )
      },
    },
  ]
}

export function tradesColumn(): Col {
  return {
    id: 'trades',
    enableSorting: false,
    header: 'Trades',
    size: 80,
    cell: (i) => <TradesLink url={i.row.original.tradeBookUrl} />,
  }
}
