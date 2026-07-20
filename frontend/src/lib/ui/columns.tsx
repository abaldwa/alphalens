import type { ColumnDef } from '@tanstack/react-table'

import { TickerLink } from '@/lib/ui/TickerLink'
import { formatCurrencyINR } from '@/lib/ui/table-utils'

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
 * header always reads "Ticker" (not "Stock"/"Symbol") at a fixed width. */
export function tickerColumn<TData extends { ticker: string }>(): ColumnDef<TData, unknown> {
  return {
    accessorKey: 'ticker',
    header: 'Ticker',
    size: 100,
    cell: (i) => <TickerLink ticker={i.getValue<string>()} />,
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
