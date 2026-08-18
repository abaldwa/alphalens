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
import type { StrategyReport, TaxBasis } from '../core/types'

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
      accessorFn: (r) => r.returns.excessReturn,
      header: 'Excess',
      size: 100,
      meta: { align: 'right' },
      cell: (i) =>
        metricCell(
          i.row.original,
          'returns.excessReturn',
          i.row.original.returns.excessReturn,
          rateDelta,
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

export function consistencyColumns(): Col[] {
  const windowCol = (years: number): Col => ({
    id: `rolling${years}y`,
    accessorFn: (r) =>
      r.consistency.rolling.find((w) => w.window === years)?.medianCagr ?? null,
    header: `${years}y median`,
    size: 105,
    meta: { align: 'right' },
    cell: (i) => {
      const w = i.row.original.consistency.rolling.find((x) => x.window === years)
      return metricCell(
        i.row.original,
        'consistency.rolling',
        w?.medianCagr ?? null,
        rate,
      )
    },
  })

  return [
    windowCol(3),
    windowCol(5),
    {
      id: 'worstWindow',
      accessorFn: (r) =>
        r.consistency.rolling.find((w) => w.window === 3)?.minCagr ?? null,
      header: 'Worst 3y',
      size: 100,
      meta: { align: 'right', priority: 'medium' },
      cell: (i) =>
        metricCell(
          i.row.original,
          'consistency.rolling',
          i.row.original.consistency.rolling.find((w) => w.window === 3)?.minCagr ??
            null,
          rate,
        ),
    },
    {
      id: 'positiveWindows',
      accessorFn: (r) =>
        r.consistency.rolling.find((w) => w.window === 3)?.positiveShare ?? null,
      header: 'Positive 3y windows',
      size: 140,
      meta: { align: 'right', priority: 'medium' },
      cell: (i) =>
        metricCell(
          i.row.original,
          'consistency.rolling',
          i.row.original.consistency.rolling.find((w) => w.window === 3)
            ?.positiveShare ?? null,
          pct,
        ),
    },
    {
      id: 'positiveYears',
      accessorFn: (r) => {
        const yoy = r.consistency.yoy.filter((y) => y.returnPct != null)
        return yoy.length ? yoy.filter((y) => (y.returnPct ?? 0) > 0).length / yoy.length : null
      },
      header: 'Positive years',
      size: 115,
      meta: { align: 'right' },
      cell: (i) => {
        const yoy = i.row.original.consistency.yoy.filter((y) => y.returnPct != null)
        const share = yoy.length
          ? yoy.filter((y) => (y.returnPct ?? 0) > 0).length / yoy.length
          : null
        return (
          <span title={yoy.length ? `${yoy.length} financial years` : undefined}>
            {metricCell(i.row.original, 'consistency.yoy', share, pct)}
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
      id: 'winRate',
      accessorFn: (r) => r.tradeQuality.winRate,
      header: 'Win rate',
      size: 90,
      meta: { align: 'right' },
      cell: (i) => metricCell(i.row.original, 'tradeQuality.winRate', i.row.original.tradeQuality.winRate, pct),
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
      header: 'Avg win / loss',
      size: 130,
      meta: { align: 'right' },
      cell: (i) => {
        const { avgWinnerPct, avgLoserPct } = i.row.original.tradeQuality
        if (avgWinnerPct == null && avgLoserPct == null) {
          return metricCell(i.row.original, 'tradeQuality.avgWinnerPct', null, pct)
        }
        // These are per-trade outcomes, not period performance, so they are
        // plain percentages — annualising a 3-day trade is meaningless.
        return (
          <span className="tabular-nums">
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
      header: 'Names',
      size: 85,
      meta: { align: 'right', priority: 'low', group: 'trade' },
      cell: (i) => int(i.row.original.tradeQuality.nDistinctTickers ?? null),
    },
  ]
}

export function incomeColumns(): Col[] {
  return [
    {
      id: 'totalWithdrawn',
      accessorFn: (r) => r.income?.totalWithdrawn ?? null,
      header: 'Withdrawn',
      size: 110,
      meta: { align: 'right' },
      cell: (i) => inr(i.row.original.income?.totalWithdrawn),
    },
    {
      id: 'totalInjected',
      accessorFn: (r) => r.income?.totalInjected ?? null,
      header: 'Backfilled',
      size: 110,
      meta: { align: 'right' },
      cell: (i) => inr(i.row.original.income?.totalInjected),
    },
    {
      id: 'yearsSurvived',
      accessorFn: (r) => r.income?.yearsSurvivedPct ?? null,
      header: 'Profitable years',
      size: 125,
      meta: { align: 'right' },
      cell: (i) =>
        metricCell(i.row.original, 'income', i.row.original.income?.yearsSurvivedPct ?? null, pct),
    },
    {
      id: 'topUpAfterLoss',
      accessorFn: (r) => r.income?.topUpAfterLoss ?? null,
      header: 'After a losing year',
      size: 150,
      cell: (i) => {
        const v = i.row.original.income?.topUpAfterLoss
        if (v == null) return metricCell(i.row.original, 'income', null, pct)
        // The two variants the user asked for, named so the difference is
        // legible: refund the shortfall, or carry on with what is left.
        return v ? 'Topped back up' : 'Runs on current capital'
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
