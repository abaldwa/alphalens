/**
 * Shared cell alignment/typography classes for DataTable column defs.
 * Formalizes the numeric-right-align / text-left-align + tabular-numeral
 * convention (previously applied ad hoc via `.font-mono-data`) into an
 * importable standard so new tables can't drift from it. Prefer tagging a
 * DataTable column with `meta: { align: 'right' }` instead (DataTable.tsx)
 * — it applies this automatically without touching the `cell` renderer.
 * These raw classes remain for tables built directly on the `Table`
 * primitive (not going through `DataTable`'s ColumnDef/meta system).
 */
export const numericCellClass = 'text-right font-mono-data tabular-nums'
export const textCellClass = 'text-left'

/**
 * Standard price/currency formatting for the whole app — always 2 decimal
 * places, ₹ prefix, en-IN thousands grouping (e.g. `₹4,01,55.00` →
 * `₹40,155.00`). Use this instead of a page-local `fmtPrice`/`fmtMoney` so
 * every price in the app renders identically; null/undefined renders as
 * an em dash rather than "null" or "NaN".
 */
export function formatCurrencyINR(value: number | null | undefined): string {
  if (value == null || Number.isNaN(value)) return '—'
  return `₹${value.toLocaleString('en-IN', { minimumFractionDigits: 2, maximumFractionDigits: 2 })}`
}
