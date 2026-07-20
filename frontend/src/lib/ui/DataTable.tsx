import {
  type Cell,
  type ColumnDef,
  flexRender,
  getCoreRowModel,
  getFilteredRowModel,
  getSortedRowModel,
  type SortingState,
  useReactTable,
} from '@tanstack/react-table'
import * as React from 'react'
import { ArrowDown, ArrowUp, ArrowUpDown, ChevronRight, Search, X } from 'lucide-react'

import { cn } from '@/lib/utils'
import { Table, TableHeader, TableBody, TableRow, TableHead, TableCell } from '@/lib/ui/primitives/table'
import { Skeleton } from '@/lib/ui/primitives/skeleton'
import { Input } from '@/lib/ui/primitives/input'
import { Badge } from '@/lib/ui/primitives/badge'

/** Column-priority hint, read off `columnDef.meta.priority`. Below
 * `COLLAPSE_WIDTH_PX` container width, `low`-priority columns are pulled
 * out of the grid and shown in an expandable per-row disclosure instead of
 * forcing horizontal scroll — the "eliminate horizontal scroll" behavior
 * for 10-12 field tables (fundamental/valuation/forensic screeners). All
 * columns stay registered in the one TanStack Table instance regardless of
 * priority, so `id`-only computed columns (no accessorKey) still render
 * correctly in the collapsed disclosure via their real cell context. */
export type ColumnPriority = 'high' | 'medium' | 'low'

declare module '@tanstack/react-table' {
  interface ColumnMeta<TData, TValue> {
    priority?: ColumnPriority
    /** Groups related low-priority columns onto one line in the collapsed
     * disclosure instead of each getting its own row — e.g. tag a
     * ticker's name/sector columns `group: 'identity'` and its
     * price/support/resistance columns `group: 'price'` so each cluster
     * reads as one line. Columns without a group each still get their own
     * line. This is a framework-level DataTable feature — set it via
     * column meta in any page and every table gets the same behavior. */
    group?: string
    /** Cell text alignment — tag any numeric/price/score column
     * `meta: { align: 'right' }` to right-align it with tabular-numeral
     * spacing, instead of wrapping the cell's own render output in
     * alignment classes by hand. Defaults to left-aligned (the HTML/CSS
     * default), matching the "numeric right, text left" convention
     * documented in table-utils.ts. Use `'center'` for short non-numeric
     * placeholder/status-style values (e.g. an always-blank column) where
     * neither edge alignment reads naturally. Applies in both the main row
     * and the collapsed disclosure line. */
    align?: 'left' | 'right' | 'center'
  }
}

/** Clusters hidden (collapsed) cells that share a `meta.group` onto one
 * disclosure line, in first-appearance order; ungrouped cells each keep
 * their own line. */
function groupHiddenCells<TData>(cells: Cell<TData, unknown>[]): { key: string; cells: Cell<TData, unknown>[] }[] {
  const lines: { key: string; cells: Cell<TData, unknown>[] }[] = []
  const groupIndex = new Map<string, number>()
  for (const cell of cells) {
    const group = cell.column.columnDef.meta?.group
    if (!group) {
      lines.push({ key: cell.id, cells: [cell] })
      continue
    }
    const existingIndex = groupIndex.get(group)
    if (existingIndex == null) {
      groupIndex.set(group, lines.length)
      lines.push({ key: `group:${group}`, cells: [cell] })
    } else {
      lines[existingIndex].cells.push(cell)
    }
  }
  return lines
}

export interface DataTableFacetFilter<TData> {
  /** A key on TData whose distinct values become toggle chips above the
   * table (e.g. 'category') — not a <select>/dropdown, per the app's
   * filter-bar convention: every value is visible and togglable at a
   * glance instead of hidden behind a click-to-open control. */
  columnId: keyof TData & string
  /** Chip-bar label, e.g. "Category". */
  label?: string
  /** Optional display-value formatter for the chip label (e.g. reformat an
   * ISO date to "01-Jul") — the underlying filter still matches on the raw
   * field value, this only changes what's shown on the chip. */
  formatValue?: (raw: string) => string
}

export interface DataTableProps<TData> {
  columns: ColumnDef<TData, unknown>[]
  data: TData[]
  isLoading?: boolean
  emptyMessage?: string
  /** Rendered instead of the table when isLoading is true. */
  skeletonRows?: number
  /** Let users drag column borders to resize. Default true — the shared
   * grid used by every screener/list page in the app, so this applies
   * product-wide without per-page opt-in. Set false to disable for a
   * specific table (e.g. one with only 2-3 fixed-purpose columns). */
  resizableColumns?: boolean
  /** Free-text search box above the table, filtering across every column's
   * rendered text (TanStack's built-in substring match). Default true —
   * every DataTable in the app gets an instant search bar for free. */
  enableSearch?: boolean
  placeholder?: string
  /** Optional toggle-chip facet filter above the table (see
   * DataTableFacetFilter) — an alternative to a dropdown for filtering by
   * a low-cardinality column like category/style/status. */
  facetFilter?: DataTableFacetFilter<TData>
  /** Multiple independent toggle-chip facet filters, each with its own row
   * above the table (e.g. Category + Recommendation Date) — combined with
   * AND semantics across filters, OR within a filter's selected values.
   * Use this instead of `facetFilter` when filtering by more than one
   * field; `facetFilter` remains a shorthand for the single-filter case. */
  facetFilters?: DataTableFacetFilter<TData>[]
}

/**
 * Dense data-grid wrapper around TanStack Table + the shadcn-style Table
 * primitive — the shared building block for every screener/list page.
 * Supports client-side column sorting, column resizing, priority-based
 * column collapse (see `ColumnPriority`), a built-in search box, and an
 * optional toggle-chip facet filter; consumers just pass columns/data.
 */
export function DataTable<TData>({
  columns,
  data,
  isLoading,
  emptyMessage = 'No rows.',
  skeletonRows = 6,
  resizableColumns = true,
  enableSearch = true,
  placeholder = 'Search…',
  facetFilter,
  facetFilters,
}: DataTableProps<TData>) {
  const allFacetFilters = React.useMemo(
    () => facetFilters ?? (facetFilter ? [facetFilter] : []),
    [facetFilters, facetFilter],
  )
  const [sorting, setSorting] = React.useState<SortingState>([])
  const [expandedRows, setExpandedRows] = React.useState<Record<string, boolean>>({})
  const [globalFilter, setGlobalFilter] = React.useState('')
  const [selectedFacetsByColumn, setSelectedFacetsByColumn] = React.useState<Record<string, Set<string>>>({})
  const [containerWidth, setContainerWidth] = React.useState<number | null>(null)
  const resizeObserverRef = React.useRef<ResizeObserver | null>(null)

  // Callback ref (not a plain useRef + one-time useEffect) because the
  // container element only exists once `isLoading` flips to false — a
  // mount-only effect would capture a null ref forever if the table
  // starts out loading, which is the common case for every screener page.
  const containerRef = React.useCallback((el: HTMLDivElement | null) => {
    resizeObserverRef.current?.disconnect()
    if (!el) return
    const observer = new ResizeObserver((entries) => {
      setContainerWidth(entries[0]?.contentRect.width ?? 0)
    })
    observer.observe(el)
    resizeObserverRef.current = observer
  }, [])

  const facetOptionsByColumn = React.useMemo(() => {
    const result = new Map<string, [string, number][]>()
    for (const filter of allFacetFilters) {
      const counts = new Map<string, number>()
      for (const row of data) {
        const raw = row[filter.columnId]
        if (raw == null) continue
        const key = String(raw)
        counts.set(key, (counts.get(key) ?? 0) + 1)
      }
      result.set(filter.columnId, Array.from(counts.entries()).sort(([a], [b]) => a.localeCompare(b)))
    }
    return result
  }, [data, allFacetFilters])

  const facetedData = React.useMemo(() => {
    if (allFacetFilters.length === 0) return data
    return data.filter((row) =>
      allFacetFilters.every((filter) => {
        const selected = selectedFacetsByColumn[filter.columnId]
        if (!selected || selected.size === 0) return true
        return selected.has(String(row[filter.columnId]))
      }),
    )
  }, [data, allFacetFilters, selectedFacetsByColumn])

  const toggleFacet = (columnId: string, value: string) => {
    setSelectedFacetsByColumn((prev) => {
      const next = new Set(prev[columnId] ?? [])
      if (next.has(value)) next.delete(value)
      else next.add(value)
      return { ...prev, [columnId]: next }
    })
  }

  const hasLowPriorityColumns = columns.some((c) => c.meta?.priority === 'low')

  const table = useReactTable({
    data: facetedData,
    columns,
    state: { sorting, globalFilter },
    onSortingChange: setSorting,
    onGlobalFilterChange: setGlobalFilter,
    getCoreRowModel: getCoreRowModel(),
    getSortedRowModel: getSortedRowModel(),
    getFilteredRowModel: getFilteredRowModel(),
    globalFilterFn: 'includesString',
    columnResizeMode: 'onChange',
    enableColumnResizing: resizableColumns,
    defaultColumn: { minSize: 60, size: 160 },
  })

  // Collapse low-priority columns only if the full column set actually
  // overflows the measured container — not an arbitrary breakpoint, since
  // the same column count can fit fine in a full-width page but overflow
  // inside a narrower card/sidebar layout.
  const collapseColumns = hasLowPriorityColumns && containerWidth != null && table.getTotalSize() > containerWidth

  const filterBar =
    enableSearch || allFacetFilters.length > 0 ? (
      <div className="mb-3 flex flex-col gap-2">
        {enableSearch ? (
          <div className="relative w-full max-w-xs">
            <Search className="pointer-events-none absolute left-2.5 top-1/2 size-3.5 -translate-y-1/2 text-muted-foreground" />
            <Input
              value={globalFilter}
              onChange={(e) => setGlobalFilter(e.target.value)}
              placeholder={placeholder}
              className="pl-8"
            />
            {globalFilter ? (
              <button
                type="button"
                aria-label="Clear search"
                onClick={() => setGlobalFilter('')}
                className="absolute right-2 top-1/2 -translate-y-1/2 text-muted-foreground hover:text-foreground"
              >
                <X className="size-3.5" />
              </button>
            ) : null}
          </div>
        ) : null}
        {allFacetFilters.map((filter) => {
          const selected = selectedFacetsByColumn[filter.columnId] ?? new Set<string>()
          const options = facetOptionsByColumn.get(filter.columnId) ?? []
          return (
            <div key={filter.columnId} className="flex flex-wrap items-center gap-1.5">
              {filter.label ? <span className="text-xs text-muted-foreground">{filter.label}:</span> : null}
              {options.map(([value, count]) => {
                const active = selected.has(value)
                return (
                  <button key={value} type="button" onClick={() => toggleFacet(filter.columnId, value)}>
                    <Badge
                      variant={active ? 'default' : 'outline'}
                      className={cn('cursor-pointer select-none', active && 'ring-1 ring-primary')}
                    >
                      {filter.formatValue ? filter.formatValue(value) : value}{' '}
                      <span className="text-[10px] opacity-70">({count})</span>
                    </Badge>
                  </button>
                )
              })}
              {selected.size > 0 ? (
                <button
                  type="button"
                  onClick={() =>
                    setSelectedFacetsByColumn((prev) => ({ ...prev, [filter.columnId]: new Set<string>() }))
                  }
                  className="text-xs text-muted-foreground underline-offset-2 hover:underline"
                >
                  Clear
                </button>
              ) : null}
            </div>
          )
        })}
      </div>
    ) : null

  if (isLoading) {
    return (
      <div className="flex flex-col gap-2">
        {filterBar}
        {Array.from({ length: skeletonRows }).map((_, i) => (
          <Skeleton key={i} className="h-9 w-full" />
        ))}
      </div>
    )
  }

  const isLowPriority = (columnId: string) => table.getColumn(columnId)?.columnDef.meta?.priority === 'low'
  const visibleHeaderGroups = table.getHeaderGroups().map((hg) => ({
    ...hg,
    headers: collapseColumns ? hg.headers.filter((h) => !isLowPriority(h.column.id)) : hg.headers,
  }))
  const columnCount = (visibleHeaderGroups[0]?.headers.length ?? columns.length) + (collapseColumns ? 1 : 0)
  const CHEVRON_COLUMN_WIDTH = 32
  const visibleTotalSize = collapseColumns
    ? CHEVRON_COLUMN_WIDTH +
      table
        .getVisibleLeafColumns()
        .filter((c) => !isLowPriority(c.id))
        .reduce((sum, c) => sum + c.getSize(), 0)
    : table.getTotalSize()

  return (
    <div>
      {filterBar}
      <div ref={containerRef}>
        <Table
          style={
            resizableColumns
              ? { width: visibleTotalSize, minWidth: '100%', tableLayout: 'fixed' }
              : undefined
          }
        >
          <TableHeader>
            {visibleHeaderGroups.map((hg) => (
              <TableRow key={hg.id}>
                {collapseColumns ? <TableHead className="w-8" /> : null}
                {hg.headers.map((header) => {
                  const sorted = header.column.getIsSorted()
                  return (
                    <TableHead
                      key={header.id}
                      className={cn('relative text-center', header.column.getCanSort() && 'cursor-pointer select-none')}
                      style={resizableColumns ? { width: header.getSize() } : undefined}
                    >
                      <span
                        className="flex w-full items-center justify-center gap-1"
                        onClick={header.column.getToggleSortingHandler()}
                      >
                        {header.isPlaceholder ? null : flexRender(header.column.columnDef.header, header.getContext())}
                        {header.column.getCanSort() ? (
                          sorted === 'asc' ? (
                            <ArrowUp className="size-3" />
                          ) : sorted === 'desc' ? (
                            <ArrowDown className="size-3" />
                          ) : (
                            <ArrowUpDown className="size-3 opacity-30" />
                          )
                        ) : null}
                      </span>
                      {resizableColumns && header.column.getCanResize() ? (
                        <div
                          onMouseDown={header.getResizeHandler()}
                          onTouchStart={header.getResizeHandler()}
                          onClick={(e) => e.stopPropagation()}
                          className={cn(
                            'absolute right-0 top-0 h-full w-1.5 cursor-col-resize touch-none select-none hover:bg-primary/40',
                            header.column.getIsResizing() && 'bg-primary',
                          )}
                        />
                      ) : null}
                    </TableHead>
                  )
                })}
              </TableRow>
            ))}
          </TableHeader>
          <TableBody>
            {table.getRowModel().rows.length ? (
              table.getRowModel().rows.map((row) => {
                const isOpen = expandedRows[row.id] ?? false
                const allCells = row.getVisibleCells()
                const visibleCells = collapseColumns ? allCells.filter((c) => !isLowPriority(c.column.id)) : allCells
                const hiddenCells = collapseColumns ? allCells.filter((c) => isLowPriority(c.column.id)) : []
                return (
                  <React.Fragment key={row.id}>
                    <TableRow>
                      {collapseColumns ? (
                        <TableCell className="w-8 py-0">
                          <button
                            type="button"
                            aria-label={isOpen ? 'Collapse row details' : 'Expand row details'}
                            onClick={() => setExpandedRows((e) => ({ ...e, [row.id]: !isOpen }))}
                            className="flex size-6 items-center justify-center text-muted-foreground hover:text-foreground"
                          >
                            <ChevronRight className={cn('size-3.5 transition-transform', isOpen && 'rotate-90')} />
                          </button>
                        </TableCell>
                      ) : null}
                      {visibleCells.map((cell) => (
                        <TableCell
                          key={cell.id}
                          className={cn(
                            'truncate font-mono-data text-sm',
                            cell.column.columnDef.meta?.align === 'right' && 'text-right tabular-nums',
                            cell.column.columnDef.meta?.align === 'center' && 'text-center',
                          )}
                          style={resizableColumns ? { width: cell.column.getSize() } : undefined}
                        >
                          {flexRender(cell.column.columnDef.cell, cell.getContext())}
                        </TableCell>
                      ))}
                    </TableRow>
                    {collapseColumns && isOpen ? (
                      <TableRow>
                        <TableCell colSpan={columnCount} className="bg-muted/30 py-2">
                          <div className="flex flex-col gap-1.5">
                            {groupHiddenCells(hiddenCells).map((line) => (
                              <div key={line.key} className="flex min-w-0 items-baseline gap-x-4">
                                {line.cells.map((cell) => {
                                  const header = cell.column.columnDef.header
                                  return (
                                    <span key={cell.id} className="inline-flex min-w-0 shrink items-baseline gap-1">
                                      <span className="shrink-0 text-xs text-muted-foreground">
                                        {typeof header === 'string' ? header : cell.column.id}:
                                      </span>
                                      <span
                                        className={cn(
                                          'min-w-0 truncate font-mono-data text-sm',
                                          cell.column.columnDef.meta?.align === 'right' && 'tabular-nums',
                                        )}
                                      >
                                        {flexRender(cell.column.columnDef.cell, cell.getContext())}
                                      </span>
                                    </span>
                                  )
                                })}
                              </div>
                            ))}
                          </div>
                        </TableCell>
                      </TableRow>
                    ) : null}
                  </React.Fragment>
                )
              })
            ) : (
              <TableRow>
                <TableCell colSpan={columnCount || 1} className="py-8 text-center text-sm text-muted-foreground">
                  {emptyMessage}
                </TableCell>
              </TableRow>
            )}
          </TableBody>
        </Table>
      </div>
    </div>
  )
}
