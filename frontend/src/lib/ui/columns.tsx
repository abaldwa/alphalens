import type { ColumnDef } from '@tanstack/react-table'

import { TickerLink } from '@/lib/ui/TickerLink'
import { DeepDiveLink, type DeepDivePillar } from '@/lib/ui/DeepDiveLink'
import { formatCurrencyINR } from '@/lib/ui/table-utils'

/** Whole calendar days between an ISO entry date and today — used by
 * `tradeDurationColumn`/`exitTodayCagrColumn` below for "if I exited this
 * open position today" math. Not a trading-day count (holidays/weekends
 * aren't excluded) — a lightweight duration for display, not a backtest
 * metric. */
function daysSince(entryDateIso: string): number {
  const entry = new Date(`${entryDateIso}T00:00:00`)
  const today = new Date()
  today.setHours(0, 0, 0, 0)
  return Math.max(0, Math.round((today.getTime() - entry.getTime()) / 86_400_000))
}

/**
 * Shared column-definition catalog: one canonical factory per field that
 * recurs across multiple pages' tables (ticker, sector, CMP, etc.), each
 * fixing the header label, width, and alignment in one place. Pages should
 * spread these into their `columns` array for any field they carry instead
 * of hand-writing an equivalent `ColumnDef` — that's what previously let
 * the same field drift into different names/widths across tables (e.g.
 * "Ticker" vs "Stock" for the identical `ticker` field). Column ORDER is
 * still each page's own call (these don't assume position), but name/width
 * /alignment for a given field are fixed here, not per page.
 */

/** The ticker/symbol column — every table with a `ticker: string` field
 * should use this instead of a local `accessorKey: 'ticker'` def, so the
 * header always reads "Ticker" (not "Stock"/"Symbol") at a fixed width.
 * `pillar` determines where the row's microscope icon links — pass the
 * pillar the page belongs to so "Deep Dive" always lands on the right
 * pillar's deep-dive page. Omit it (as forensic/big-investors pages do)
 * for pages with no corresponding deep-dive page yet — no microscope
 * icon renders in that case. */
export function tickerColumn<TData extends { ticker: string }>(pillar?: DeepDivePillar): ColumnDef<TData, unknown> {
  return {
    accessorKey: 'ticker',
    header: 'Ticker',
    size: 110,
    cell: (i) => {
      const ticker = i.getValue<string>()
      return (
        <span className="inline-flex items-center gap-1.5">
          <TickerLink ticker={ticker} />
          <DeepDiveLink pillar={pillar} ticker={ticker} />
        </span>
      )
    },
  }
}

/** The sector column — collapses into the low-priority disclosure by
 * default since it's supplementary identifying info, not a primary sort
 * key, matching how it's used on every existing table that has it. */
export function sectorColumn<TData extends { sector: string | null }>(): ColumnDef<TData, unknown> {
  return {
    accessorKey: 'sector',
    header: 'Sector',
    meta: { priority: 'low' },
    cell: (i) => i.getValue<string | null>() ?? '—',
  }
}

/** The current-market-price column — every table with a CMP-equivalent
 * numeric field should use this so "CMP" always renders right-aligned via
 * `formatCurrencyINR`, never a page-local `fmtPrice`/`fmtMoney`/`toFixed`.
 * `accessorKey` is a param because the underlying field name varies
 * (`cmp`, `current_price`) even though the displayed column is identical. */
export function cmpColumn<TData>(accessorKey: keyof TData & string): ColumnDef<TData, unknown> {
  return {
    accessorKey,
    header: 'CMP',
    size: 100,
    meta: { align: 'right' },
    cell: (i) => formatCurrencyINR(i.getValue<number | null>()),
  }
}

interface OpenPositionLike {
  entry_date: string
  entry_price: number
  current_price: number | null
}

/** "Duration" — whole days the position has been open (entry_date to
 * today). Part of the standard exit-fields tail (exit today? → exit
 * reason → win/loss if exited today → duration → CAGR) every open-position
 * table should carry, per the app's recommendation/exit column
 * convention. */
export function tradeDurationColumn<TData extends OpenPositionLike>(): ColumnDef<TData, unknown> {
  return {
    id: 'duration_days',
    accessorFn: (row) => daysSince(row.entry_date),
    header: 'Duration',
    size: 75,
    meta: { align: 'right' },
    cell: (i) => `${i.getValue<number>()}d`,
  }
}

/** "CAGR if exited today" — annualized return using `current_price` as a
 * stand-in exit price, computed client-side from fields every open-
 * position table already fetches (entry_date/entry_price/current_price) —
 * no backend change needed. Null until the position has been open at
 * least a day (annualizing a same-day return is not meaningful). */
export function exitTodayCagrColumn<TData extends OpenPositionLike>(): ColumnDef<TData, unknown> {
  return {
    id: 'cagr_if_exited_today',
    accessorFn: (row) => {
      const days = daysSince(row.entry_date)
      if (days < 1 || row.current_price == null || row.entry_price === 0) return null
      const totalReturn = row.current_price / row.entry_price
      return (Math.pow(totalReturn, 365 / days) - 1) * 100
    },
    header: 'CAGR (if exited today)',
    size: 90,
    meta: { align: 'right' },
    cell: (i) => {
      const v = i.getValue<number | null>()
      if (v == null) return '—'
      return <span className={v >= 0 ? 'text-green' : 'text-red'}>{v.toFixed(1)}%</span>
    },
  }
}
