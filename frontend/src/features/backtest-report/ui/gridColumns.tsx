/**
 * features/backtest-report/ui/gridColumns.tsx
 *
 * AG Grid column definitions over StrategyReport, for the AnalyticsGrid
 * workspace the four section pages render.
 *
 * These are the same measurements as ./columns.tsx, expressed for the other
 * grid. The formatting rules do NOT fork: both files import the same
 * core/format helpers, so a rate still carries "%/yr" and a per-trade outcome
 * still does not, whichever grid is on screen. What differs is only what the
 * two grids can express — grouped headers spanning the fiscal years, pinned
 * identity, and a value/display split AG Grid needs for sorting and CSV
 * export to agree with each other.
 *
 * THE VALUE/DISPLAY SPLIT MATTERS. `valueGetter` returns the raw fraction so
 * sorting is numeric and the CSV carries a number a spreadsheet can total;
 * `valueFormatter` renders it. Formatting inside the value getter is the
 * classic way a grid ends up sorting "9.1%" above "84.0%".
 */

import type { ColDef, ColGroupDef, ValueFormatterParams } from 'ag-grid-community'

import { HEATMAP_COLUMN } from '@/lib/ui'

import { StrategyLink } from './StrategyLink'
import { TradesLink } from './TradesLink'
import { cagrOn } from '../core/cagrOn'
import { EM_DASH, inr, int, num, pct, rate, rateDelta } from '../core/format'
import { collectFiscalYears, shortFyLabel, yoyValueFor, isPartialFor } from '../core/fiscalYears'
import { baseCapitalFor, regularReturnsByYear } from '../core/regularReturns'
import { rollingFromYoy } from '../core/rollingFromYoy'
import type { StrategyReport, TaxBasis } from '../core/types'

type Col = ColDef<StrategyReport>
type Group = ColGroupDef<StrategyReport>

/** A formatter that survives a null, matching the em-dash rule everywhere
 * else in the report: a blank cell and a zero are different facts. */
function fmt(
  format: (v: number | null | undefined) => string,
): (params: ValueFormatterParams<StrategyReport>) => string {
  return (params) =>
    params.value == null || !Number.isFinite(Number(params.value))
      ? EM_DASH
      : format(Number(params.value))
}

const NUMERIC: Partial<Col> = {
  type: 'numericColumn',
  cellClass: 'tabular-nums',
  width: 115,
}

/** Identity, pinned left so the row never loses its label while the reader
 * scrolls eighteen fiscal years sideways. */
export function identityGroup(section: string): Group {
  return {
    headerName: 'Strategy',
    marryChildren: true,
    // A group is pinned by pinning its children; AG Grid has no `pinned` on
    // the group itself, and marryChildren keeps them travelling together.
    children: [
      {
        colId: 'strategy',
        headerName: 'Name',
        headerTooltip:
          'The strategy as registered. Click it for the full setup, equity curve and trade-book integrity checks.',
        valueGetter: (p) => p.data?.label ?? '',
        width: 280,
        pinned: 'left',
        // Dragging the label column away leaves a grid of anonymous numbers.
        lockPinned: true,
        suppressMovable: true,
        cellRenderer: (p: { data?: StrategyReport }) =>
          p.data ? (
            <StrategyLink
              strategyKey={p.data.key}
              label={p.data.label}
              section={section}
            />
          ) : null,
      },
      {
        colId: 'channel',
        headerName: 'Channel',
        headerTooltip:
          'Which engine produced this row: momentum, technical, fundamental or ML. Rows from different channels are comparable here because every figure is computed by the same shared metrics layer.',
        valueGetter: (p) => p.data?.channel ?? '',
        width: 105,
        columnGroupShow: 'open',
      },
    ],
  }
}

/**
 * Returns.
 *
 * THE TWO CAGR COLUMNS ARE IN A FIXED ORDER: post-tax first, pre-tax second,
 * ALWAYS — regardless of which basis the toggle selects.
 *
 * They used to swap places with the toggle, because the "headline" column
 * rendered the selected basis and the "other" column rendered its opposite.
 * The values and headers were correct in both positions, but a reader who
 * knows the first CAGR column as post-tax and flips to the pre-tax basis sees
 * 48.8% land in the slot where 46.8% used to be, and reads the two as
 * swapped. A column whose MEANING moves is a bad column even when every cell
 * in it is right.
 *
 * Now the toggle changes emphasis, not position: the selected basis is the
 * one that carries the heatmap and the one Excess is computed from, and its
 * header says so. Nothing moves.
 *
 * (For the record, pre-tax ABOVE post-tax is correct and expected. Tax is a
 * cash outflow, so the post-tax curve is the smaller one — the gap between
 * the two columns is what the taxman took, and it should reconcile against
 * "Tax paid" in this same group.)
 */
export function returnsGroup(basis: TaxBasis): Group {
  const cagrCol = (
    which: TaxBasis,
    label: string,
    explanation: string,
  ): Col => {
    const active = which === basis
    return {
      ...NUMERIC,
      colId: which === 'post_tax' ? 'cagrPostTax' : 'cagrPreTax',
      headerName: active ? `${label} ●` : label,
      headerTooltip: active
        ? `${explanation} This is the basis currently selected, so it is what Excess is measured against and what the shading tracks.`
        : `${explanation} Not the selected basis — switch the Basis control above to rank on it.`,
      valueGetter: (p) => (p.data ? cagrOn(p.data, which) : null),
      valueFormatter: fmt(rate),
      width: 150,
      // Only the selected basis is shaded. Shading both turns the pair into a
      // colour comparison between two numbers that are not alternatives —
      // they are the same run measured two ways.
      ...(active ? { context: HEATMAP_COLUMN } : {}),
      cellClass: active ? 'tabular-nums font-semibold' : 'tabular-nums',
    }
  }

  return {
    headerName: 'Returns',
    marryChildren: true,
    children: [
      cagrCol(
        'post_tax',
        'CAGR (post-tax)',
        'Annualised growth after STCG/LTCG is paid as a cash outflow each financial year — the money you actually keep.',
      ),
      cagrCol(
        'pre_tax',
        'CAGR (pre-tax)',
        'Annualised growth before any capital-gains tax. Always the higher of the two, and it flatters high-churn strategies most.',
      ),
      {
        ...NUMERIC,
        colId: 'xirr',
        headerName: 'XIRR',
        headerTooltip:
          'Money-weighted return over the actual cash flows: capital in, withdrawals out, and the book liquidated on the last day. Equals the CAGR for a plain lump sum; diverges once money moves in or out mid-run.',
        valueGetter: (p) => p.data?.returns.xirr ?? null,
        valueFormatter: fmt(rate),
        columnGroupShow: 'open',
      },
      {
        ...NUMERIC,
        colId: 'benchmarkCagr',
        headerName: 'Benchmark',
        headerTooltip:
          'The selected index over this run\u2019s own window, buy and hold. The index name travels with the number — two rows scored against different indices are not comparable.',
        valueGetter: (p) => p.data?.returns.benchmarkCagr ?? null,
        valueFormatter: fmt(rate),
        tooltipValueGetter: (p) =>
          [
            (p.data as StrategyReport | undefined)?.returns.benchmarkIndexName,
            (p.data as StrategyReport | undefined)?.returns.benchmarkCaveat,
          ]
            .filter(Boolean)
            .join(' — ') || null,
        width: 125,
      },
      {
        ...NUMERIC,
        colId: 'excess',
        headerName: 'Excess',
        headerTooltip:
          'The selected basis\u2019 CAGR minus the benchmark, in percentage points per year. Derived from the two cells in this row rather than read from storage, so the three always agree.',
        valueGetter: (p) => {
          const own = p.data ? cagrOn(p.data, basis) : null
          const bench = p.data?.returns.benchmarkCagr ?? null
          return own != null && bench != null ? own - bench : null
        },
        valueFormatter: fmt(rateDelta),
        width: 125,
        context: HEATMAP_COLUMN,
      },
      {
        ...NUMERIC,
        colId: 'finalCapital',
        headerName: 'Final capital',
        headerTooltip:
          'What the book was worth on the last day, on the basis the run was measured on. Read it against Capital in the Setup group — the two give the total multiple.',
        valueGetter: (p) => p.data?.returns.finalCapital ?? null,
        valueFormatter: fmt(inr),
        columnGroupShow: 'open',
        width: 125,
      },
      {
        ...NUMERIC,
        colId: 'taxPaid',
        headerName: 'Tax paid',
        headerTooltip:
          'Capital-gains tax across the whole window. This is the gap between the two CAGR columns, in rupees — the audit trail behind the post-tax figure rather than a second headline.',
        valueGetter: (p) => p.data?.tradeQuality.totalTaxPaid ?? null,
        valueFormatter: fmt(inr),
        columnGroupShow: 'open',
        width: 120,
      },
    ],
  }
}

export function consistencyGroup(): Group {
  const windowCol = (years: number): Col => ({
    ...NUMERIC,
    colId: `rolling${years}y`,
    headerName: `${years}y median`,
    headerTooltip: `Median of every rolling ${years}-consecutive-financial-year window in the run, annualised.`,
    valueGetter: (p) =>
      p.data ? rollingFromYoy(p.data.consistency.yoy, years)?.medianCagr ?? null : null,
    valueFormatter: fmt(rate),
    context: HEATMAP_COLUMN,
  })
  return {
    headerName: 'Consistency',
    marryChildren: true,
    children: [
      windowCol(3),
      windowCol(5),
      {
        ...NUMERIC,
        colId: 'worst3y',
        headerName: 'Worst 3y',
        headerTooltip:
          'The worst any three consecutive financial years did, annualised — the stretch you would have had to sit through.',
        valueGetter: (p) =>
          p.data ? rollingFromYoy(p.data.consistency.yoy, 3)?.minCagr ?? null : null,
        valueFormatter: fmt(rate),
        context: HEATMAP_COLUMN,
      },
      {
        // Sorted on the share, DISPLAYED as a count: "13 of 16" and "13,000 of
        // 16,000" are the same percentage and not the same evidence.
        colId: 'positive3y',
        headerName: 'Positive 3y windows',
        headerTooltip:
          'How many rolling three-financial-year holdings ended positive, out of how many the run contains. Shown as a count, not a share: 13 of 16 and 13,000 of 16,000 are the same percentage and not the same evidence. Sorts on the share so runs of different lengths rank comparably.',
        type: 'numericColumn',
        cellClass: 'tabular-nums',
        width: 165,
        valueGetter: (p) => {
          const w = p.data ? rollingFromYoy(p.data.consistency.yoy, 3) : null
          return w && w.nWindows ? w.nPositive / w.nWindows : null
        },
        valueFormatter: (p) => {
          const w = p.data ? rollingFromYoy(p.data.consistency.yoy, 3) : null
          return w ? `${w.nPositive} of ${w.nWindows}` : EM_DASH
        },
      },
      {
        colId: 'positiveYears',
        headerName: 'Positive years',
        headerTooltip:
          'Financial years that ended up, out of the years the run covers. A year marked * in the year columns is partial — a real return over a real period, but not a full twelve months.',
        type: 'numericColumn',
        cellClass: 'tabular-nums',
        width: 130,
        valueGetter: (p) => {
          const yoy = (p.data?.consistency.yoy ?? []).filter((y) => y.returnPct != null)
          return yoy.length
            ? yoy.filter((y) => (y.returnPct ?? 0) > 0).length / yoy.length
            : null
        },
        valueFormatter: (p) => {
          const yoy = (p.data?.consistency.yoy ?? []).filter((y) => y.returnPct != null)
          if (!yoy.length) return EM_DASH
          return `${yoy.filter((y) => (y.returnPct ?? 0) > 0).length} of ${yoy.length}`
        },
      },
    ],
  }
}

export function riskGroup(): Group {
  return {
    headerName: 'Risk',
    marryChildren: true,
    children: [
      {
        ...NUMERIC,
        colId: 'maxDrawdown',
        headerName: 'Max drawdown',
        headerTooltip:
          'Deepest peak-to-trough fall in portfolio value. The number that decides whether you would actually have stayed invested.',
        valueGetter: (p) => p.data?.risk.maxDrawdown ?? null,
        valueFormatter: fmt(pct),
        width: 140,
        context: HEATMAP_COLUMN,
      },
      {
        ...NUMERIC,
        colId: 'volatility',
        headerName: 'Volatility',
        headerTooltip:
          'Annualised standard deviation of returns. Says whether a given Sharpe came from a calm book or a wild one — Sharpe alone cannot.',
        valueGetter: (p) => p.data?.risk.volatility ?? null,
        valueFormatter: fmt(rate),
        width: 115,
      },
      {
        ...NUMERIC,
        colId: 'sharpe',
        headerName: 'Sharpe',
        headerTooltip:
          'Return per unit of total volatility, annualised, with no risk-free rate deducted. Treats upside and downside swings alike — compare it with Sortino, which does not.',
        valueGetter: (p) => p.data?.risk.sharpe ?? null,
        valueFormatter: fmt(num),
        width: 100,
      },
      {
        ...NUMERIC,
        colId: 'sortino',
        headerName: 'Sortino',
        headerTooltip: 'Like Sharpe, but only downside moves count as risk.',
        valueGetter: (p) => p.data?.risk.sortino ?? null,
        valueFormatter: fmt(num),
        width: 100,
      },
      {
        ...NUMERIC,
        colId: 'calmar',
        headerName: 'Calmar',
        headerTooltip: 'CAGR divided by the worst drawdown.',
        valueGetter: (p) => p.data?.risk.calmar ?? null,
        valueFormatter: fmt(num),
        width: 100,
        columnGroupShow: 'open',
      },
    ],
  }
}

export function tradeQualityGroup(): Group {
  return {
    headerName: 'Trade quality',
    marryChildren: true,
    children: [
      {
        ...NUMERIC,
        colId: 'nTrades',
        headerName: 'Trades',
        headerTooltip: 'Closed round trips over the whole run.',
        valueGetter: (p) => p.data?.tradeQuality.nTrades ?? null,
        valueFormatter: fmt(int),
        width: 95,
      },
      {
        ...NUMERIC,
        colId: 'distinctTickers',
        headerName: 'Distinct stocks traded',
        headerTooltip:
          'How many different tickers the strategy ever held. 40 trades over 8 stocks and 40 over 40 are different strategies.',
        valueGetter: (p) => p.data?.tradeQuality.nDistinctTickers ?? null,
        valueFormatter: fmt(int),
        width: 175,
      },
      {
        ...NUMERIC,
        colId: 'churn',
        headerName: 'Churn/yr',
        headerTooltip:
          'Round trips per year. What the strategy costs to run, and what decides whether a pre-tax edge survives STCG.',
        valueGetter: (p) => p.data?.tradeQuality.churnPerYear ?? null,
        valueFormatter: fmt((v) => num(v, 1)),
        width: 110,
      },
      {
        ...NUMERIC,
        colId: 'avgHoldDays',
        // Unit in the HEADER, bare number in the cell. "368d" reads as text
        // and invites the suspicion that the column is not really numeric;
        // the value getter was always a number, but the cell has to look like
        // one for the sort to be believed.
        headerName: 'Avg hold (days)',
        headerTooltip:
          'Mean calendar days from entry to exit across closed trades. Short holds mean short-term capital gains, which is what separates the pre-tax and post-tax CAGR columns.',
        valueGetter: (p) => p.data?.tradeQuality.avgHoldDays ?? null,
        valueFormatter: fmt((v) => num(v, 0)),
        width: 130,
      },
      {
        ...NUMERIC,
        colId: 'winRate',
        headerName: '% trades won',
        headerTooltip:
          'How OFTEN the strategy is right. Says nothing about size — a book can win 70% of its trades and still lose money.',
        valueGetter: (p) => p.data?.tradeQuality.winRate ?? null,
        valueFormatter: fmt(pct),
        width: 130,
      },
      {
        ...NUMERIC,
        colId: 'avgWin',
        headerName: 'Avg gain per winning trade',
        headerTooltip:
          'How MUCH it makes when it is right — the other half of the question "% trades won" answers.',
        valueGetter: (p) => p.data?.tradeQuality.avgWinnerPct ?? null,
        valueFormatter: fmt(pct),
        width: 200,
      },
      {
        ...NUMERIC,
        colId: 'avgLoss',
        headerName: 'Avg loss per losing trade',
        headerTooltip:
          'Mean return of the trades that lost money. A per-trade outcome, so a plain percentage — annualising a three-day trade is meaningless.',
        valueGetter: (p) => p.data?.tradeQuality.avgLoserPct ?? null,
        valueFormatter: fmt(pct),
        width: 190,
      },
      {
        ...NUMERIC,
        colId: 'winLossRatio',
        headerName: 'Gain : loss',
        headerTooltip:
          'Average winner divided by average loser. Read beside "% trades won": 3:1 at a 30% win rate and 1:1 at 60% are different strategies.',
        valueGetter: (p) => {
          const { avgWinnerPct, avgLoserPct } = p.data?.tradeQuality ?? {}
          if (avgWinnerPct == null || !avgLoserPct) return null
          return Math.abs(avgWinnerPct / avgLoserPct)
        },
        valueFormatter: fmt((v) => `${num(v, 2)}×`),
        width: 120,
      },
      {
        ...NUMERIC,
        colId: 'profitFactor',
        headerName: 'Profit factor',
        headerTooltip: 'Gross profit divided by gross loss, in rupees.',
        valueGetter: (p) => p.data?.tradeQuality.profitFactor ?? null,
        valueFormatter: fmt(num),
        width: 125,
        columnGroupShow: 'open',
      },
      {
        ...NUMERIC,
        colId: 'turnover',
        headerName: 'Turnover',
        headerTooltip:
          'Total value traded against the average portfolio value. Higher means more of the book was bought and sold, which is where costs and short-term tax come from.',
        valueGetter: (p) => p.data?.tradeQuality.turnoverRatio ?? null,
        valueFormatter: fmt(num),
        width: 110,
        columnGroupShow: 'open',
      },
    ],
  }
}

/** Regular-returns mode only. See core/regularReturns for what it derives. */
export function incomeGroup(): Group {
  return {
    headerName: 'Regular returns',
    marryChildren: true,
    children: [
      {
        ...NUMERIC,
        colId: 'avgAnnualYield',
        headerName: 'Avg annual payout',
        headerTooltip:
          'Mean yearly withdrawal as a share of the capital at work. A yield, not a growth rate — nothing compounds in this mode.',
        valueGetter: (p) => p.data?.income?.avgAnnualYieldPct ?? null,
        valueFormatter: fmt(pct),
        width: 165,
        context: HEATMAP_COLUMN,
      },
      {
        ...NUMERIC,
        colId: 'totalDrawn',
        headerName: 'Total cash out',
        headerTooltip:
          'Every year’s gain above base capital, added up across the whole run — the total the strategy actually paid you. Gross of anything put back in; read it beside “Net of top-ups”.',
        valueGetter: (p) => p.data?.income?.totalWithdrawn ?? null,
        valueFormatter: fmt(inr),
        width: 155,
        cellClass: 'tabular-nums font-semibold',
        context: HEATMAP_COLUMN,
      },
      {
        colId: 'yearsPaid',
        headerName: 'Years it paid',
        headerTooltip:
          'Years that cleared base capital and therefore paid something, out of the years the run covers. The rest paid nothing at all.',
        type: 'numericColumn',
        cellClass: 'tabular-nums',
        width: 135,
        valueGetter: (p) => p.data?.income?.yearsSurvivedPct ?? null,
        valueFormatter: (p) => {
          const income = p.data?.income
          if (!income?.nYears || income.yearsSurvivedPct == null) return EM_DASH
          return `${Math.round(income.yearsSurvivedPct * income.nYears)} of ${income.nYears}`
        },
      },
      {
        ...NUMERIC,
        colId: 'toppedUp',
        headerName: 'Topped back up',
        headerTooltip:
          'Cash put back after losing years. Money in, not money earned — subtract it before calling this an income source.',
        valueGetter: (p) => p.data?.income?.totalInjected ?? null,
        valueFormatter: fmt(inr),
        width: 155,
      },
      {
        ...NUMERIC,
        colId: 'netIncome',
        headerName: 'Net of top-ups',
        headerTooltip:
          'Total drawn less everything put back. Negative means the strategy consumed more capital than it ever paid out.',
        valueGetter: (p) =>
          p.data?.income
            ? (p.data.income.totalWithdrawn ?? 0) - (p.data.income.totalInjected ?? 0)
            : null,
        valueFormatter: fmt(inr),
        width: 150,
        context: HEATMAP_COLUMN,
      },
    ],
  }
}

/** Setup + the trade-book link. Last, and collapsed by default: it qualifies
 * the numbers rather than being one of them. */
export function setupGroup(): Group {
  return {
    headerName: 'Setup',
    marryChildren: true,
    children: [
      {
        colId: 'universe',
        headerName: 'Universe',
        headerTooltip:
          'The tradable set the strategy ranked within. Two strategies scored against the same benchmark but drawn from different universes are not measuring the same skill.',
        valueGetter: (p) => p.data?.setup.universe ?? null,
        width: 150,
        columnGroupShow: 'open',
      },
      {
        ...NUMERIC,
        colId: 'windowYears',
        headerName: 'Window',
        headerTooltip:
          'Years the run itself covers. Rows whose run does not span the selected window are excluded from the table and counted above it, rather than ranked against longer ones.',
        valueGetter: (p) => p.data?.setup.window.years ?? null,
        valueFormatter: fmt((v) => `${num(v, 1)}y`),
        width: 105,
        columnGroupShow: 'open',
      },
      {
        ...NUMERIC,
        colId: 'capital',
        headerName: 'Capital',
        headerTooltip:
          'Capital deployed at the start of the run. Position sizing scales with it, so two runs at different capital are not strictly comparable on trade counts.',
        valueGetter: (p) => p.data?.setup.capitalDeployed ?? null,
        valueFormatter: fmt(inr),
        width: 120,
        columnGroupShow: 'open',
      },
      {
        colId: 'trades',
        headerName: 'Trade book',
        headerTooltip:
          'Downloads this run’s full trade log as CSV — every entry, exit, holding period and P&L behind the numbers in this row.',
        width: 120,
        sortable: false,
        filter: false,
        cellRenderer: (p: { data?: StrategyReport }) => (
          <TradesLink url={p.data?.tradeBookUrl ?? null} label="Download" />
        ),
      },
    ],
  }
}

/**
 * One column per financial year, under a spanning header, NEWEST FIRST.
 *
 * The recent years are the ones a deploy decision turns on; oldest-first
 * buries them off the right edge of an 18-column span. See core/fiscalYears
 * for why the chart beneath the grid deliberately runs the other way.
 */
export function fiscalYearGroup(rows: StrategyReport[]): Group | null {
  const sorted = collectFiscalYears(rows, 'newest-first')
  if (!sorted.length) return null
  return {
    headerName: 'Year-on-year returns',
    marryChildren: true,
    children: sorted.map<Col>((label) => ({
      ...NUMERIC,
      colId: `fy-${label}`,
      // Abbreviated in the header, spelled out in the tooltip: eighteen
      // four-digit years is what makes the span scroll sideways, and nobody
      // reading FY27 in a run that starts in 2009 wonders which century.
      headerName: shortFyLabel(label),
      // The header names a CALENDAR year and nothing more. Whether the year
      // was partial depends on the row, so it is stated per cell below rather
      // than in a header shared by every strategy.
      headerTooltip: label,
      width: 100,
      valueGetter: (p) =>
        yoyValueFor(p.data?.consistency.yoy ?? [], label)?.returnPct ?? null,
      tooltipValueGetter: (p) =>
        isPartialFor(p.data?.consistency.yoy ?? [], label)
          ? `${label} is a PARTIAL financial year for this strategy — its run opened or closed mid-year.`
          : undefined,
      valueFormatter: fmt(pct),
      context: HEATMAP_COLUMN,
    })),
  }
}

/**
 * Year columns for REGULAR-RETURNS mode: rupees out, not percent earned.
 *
 * In this mode the year-on-year return is the wrong thing to show. The mode
 * exists to answer "what did this pay me, and when did it pay me nothing?",
 * and a row of percentages answers a question the reader has already left
 * behind — worse, a +40% year on a book still under water pays exactly ₹0, so
 * the percentage and the cash disagree in precisely the years that matter.
 *
 * Each cell is `netCash`: money OUT to the investor as a positive, the year's
 * deficit as a negative. Under the top-up variant that deficit is real cash
 * you had to find; under the carry variant it is notional — the hole the book
 * must climb out of before it pays again — and the tooltip says which.
 */
export function cashFlowYearGroup(
  rows: StrategyReport[],
  topUpAfterLoss: boolean,
): Group | null {
  const sorted = collectFiscalYears(rows, 'newest-first')
  if (!sorted.length) return null

  const scheduleFor = (row: StrategyReport) =>
    regularReturnsByYear(row.consistency.yoy, {
      baseCapital: baseCapitalFor(row.setup.capitalDeployed),
      topUpAfterLoss,
    })

  return {
    headerName: topUpAfterLoss
      ? 'Cash out / top-up needed, by year'
      : 'Cash out / deficit carried, by year',
    marryChildren: true,
    children: sorted.map<Col>((label) => ({
      ...NUMERIC,
      colId: `cash-${label}`,
      headerName: shortFyLabel(label),
      headerTooltip: label.endsWith('*')
        ? `${label.slice(0, -1)} — a PARTIAL financial year, so its payout covers less than twelve months.`
        : `Cash withdrawn in ${label}, or the deficit if the year ended below base capital.`,
      width: 120,
      valueGetter: (p) =>
        p.data ? scheduleFor(p.data)?.get(label)?.netCash ?? null : null,
      valueFormatter: fmt(inr),
      tooltipValueGetter: (p) => {
        const row = p.data as StrategyReport | undefined
        const year = row ? scheduleFor(row)?.get(label) : null
        if (!year) return null
        if (year.withdrawn > 0) {
          return `${label}: ${inr(year.withdrawn)} withdrawn. Opened at ${inr(year.openingCapital)}, returned ${pct(year.returnPct)}, then reset to base capital.`
        }
        const kind = topUpAfterLoss
          ? `${inr(year.shortfall)} had to be put back to restore base capital`
          : `${inr(year.shortfall)} below base capital, carried into next year rather than funded`
        return `${label}: nothing paid out. Opened at ${inr(year.openingCapital)}, returned ${pct(year.returnPct)} — ${kind}.`
      },
      context: HEATMAP_COLUMN,
    })),
  }
}

/**
 * Rupee shading ceilings, scaled off the capital actually deployed, so a cell
 * is shaded by how big the payout was RELATIVE TO THE STAKE. The fraction
 * ceilings the other tabs use would paint every rupee figure solid.
 */
export function cashHeatmapFor(rows: StrategyReport[]) {
  const bases = rows
    .map((r) => baseCapitalFor(r.setup.capitalDeployed))
    .sort((a, b) => a - b)
  const base = bases.length ? bases[bases.length >> 1] : 1_000_000
  return { positiveCeiling: base * 0.5, negativeCeiling: base * 0.35 }
}
