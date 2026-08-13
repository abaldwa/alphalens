import {
  type Cell,
  type ColumnDef,
  type ColumnFiltersState,
  flexRender,
  getCoreRowModel,
  getFacetedUniqueValues,
  getFilteredRowModel,
  getSortedRowModel,
  type SortingState,
  useReactTable,
} from '@tanstack/react-table'
import * as React from 'react'
import { ArrowDown, ArrowUp, ArrowUpDown, ChevronDown, ChevronRight, Filter, Search, X } from 'lucide-react'

import { cn } from '@/lib/utils'
import { Table, TableHeader, TableBody, TableRow, TableHead, TableCell } from '@/lib/ui/primitives/table'
import { Skeleton } from '@/lib/ui/primitives/skeleton'
import { Input } from '@/lib/ui/primitives/input'
import { Badge } from '@/lib/ui/primitives/badge'
import {
  DropdownMenu,
  DropdownMenuTrigger,
  DropdownMenuContent,
  DropdownMenuItem,
  DropdownMenuCheckboxItem,
  DropdownMenuLabel,
  DropdownMenuSeparator,
} from '@/lib/ui/primitives/dropdown-menu'

/** Multi-select checkbox filter used by every DataTable column's header
 * dropdown — filterValue is the list of raw (stringified) values the user
 * has checked; an empty/undefined filterValue means "no filter applied". */
function multiSelectFilterFn(row: { getValue: (columnId: string) => unknown }, columnId: string, filterValue: string[]) {
  if (!filterValue || filterValue.length === 0) return true
  return filterValue.includes(String(row.getValue(columnId) ?? ''))
}

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
  /** Caps the table's vertical scroll region so its header can stick to the
   * top of that region while scrolling — the "fix the top row" behavior.
   * Default `'calc(100vh - 320px)'` fits comfortably under a page's title/
   * filter bar; pass a smaller value for a table embedded alongside other
   * content on the same page. Pass `'none'` to disable the scroll region
   * (header scrolls away with the page) for a short table that never needs
   * to scroll internally. */
  maxHeight?: string
  /** Called when a row is activated, by click or by Enter/Space when the row
   * has keyboard focus. Rows become focusable only when this is supplied, so
   * tables without it keep a clean tab order.
   *
   * [2026-08-13] pages/technical/comparison.tsx has been passing this since it
   * was written, but DataTable never accepted it — so the strategy drill-down
   * panel on that page was unreachable: `selected` is set nowhere else, so
   * `{selected?.lump && ...}` never rendered. */
  onRowClick?: (row: TData) => void
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
  maxHeight = 'calc(100vh - 320px)',
  onRowClick,
}: DataTableProps<TData>) {
  const allFacetFilters = React.useMemo(
    () => facetFilters ?? (facetFilter ? [facetFilter] : []),
    [facetFilters, facetFilter],
  )
  const [sorting, setSorting] = React.useState<SortingState>([])
  const [columnFilters, setColumnFilters] = React.useState<ColumnFiltersState>([])
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

  const table = useReactTable({
    data: facetedData,
    columns,
    state: { sorting, globalFilter, columnFilters },
    onSortingChange: setSorting,
    onGlobalFilterChange: setGlobalFilter,
    onColumnFiltersChange: setColumnFilters,
    getCoreRowModel: getCoreRowModel(),
    getSortedRowModel: getSortedRowModel(),
    getFilteredRowModel: getFilteredRowModel(),
    getFacetedUniqueValues: getFacetedUniqueValues(),
    globalFilterFn: 'includesString',
    columnResizeMode: 'onChange',
    enableColumnResizing: resizableColumns,
    defaultColumn: { minSize: 60, size: 160, filterFn: multiSelectFilterFn },
  })

  // Two-stage overflow collapse: hide `low`-priority columns first; if the
  // remaining (high + medium) columns still don't fit the measured
  // container, also hide `medium`-priority columns. This is what lets a
  // dense 10-12 column screener honor "no horizontal scroll" — a single
  // collapse stage isn't always enough once `high`-priority columns alone
  // approach the container width (e.g. a narrower viewport or the sidebar
  // expanded). Never collapses `high`/unset-priority columns — those are
  // the floor a table is allowed to shrink to.
  const lowWidth = columns
    .filter((c) => c.meta?.priority === 'low')
    .reduce((sum, c) => sum + (c.size ?? 160), 0)
  const mediumWidth = columns
    .filter((c) => c.meta?.priority === 'medium')
    .reduce((sum, c) => sum + (c.size ?? 160), 0)
  const hasLowPriorityColumns = lowWidth > 0
  const hasMediumPriorityColumns = mediumWidth > 0
  const fullWidth = table.getTotalSize()
  const collapseLevel: 'none' | 'low' | 'low+medium' =
    containerWidth == null || fullWidth <= containerWidth
      ? 'none'
      : hasLowPriorityColumns && fullWidth - lowWidth <= containerWidth
        ? 'low'
        : hasLowPriorityColumns || hasMediumPriorityColumns
          ? 'low+medium'
          : 'none'
  const collapseColumns = collapseLevel !== 'none'

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

  const isCollapsedPriority = (columnId: string) => {
    const priority = table.getColumn(columnId)?.columnDef.meta?.priority
    if (collapseLevel === 'low+medium') return priority === 'low' || priority === 'medium'
    if (collapseLevel === 'low') return priority === 'low'
    return false
  }
  const visibleHeaderGroups = table.getHeaderGroups().map((hg) => ({
    ...hg,
    headers: collapseColumns ? hg.headers.filter((h) => !isCollapsedPriority(h.column.id)) : hg.headers,
  }))
  const columnCount = (visibleHeaderGroups[0]?.headers.length ?? columns.length) + (collapseColumns ? 1 : 0)
  const CHEVRON_COLUMN_WIDTH = 32
  const visibleTotalSize = collapseColumns
    ? CHEVRON_COLUMN_WIDTH +
      table
        .getVisibleLeafColumns()
        .filter((c) => !isCollapsedPriority(c.id))
        .reduce((sum, c) => sum + c.getSize(), 0)
    : table.getTotalSize()
  const hasActiveColumnFilters = columnFilters.length > 0

  return (
    <div>
      <div className="mb-3 flex items-start justify-between gap-2">
        {filterBar}
        {hasActiveColumnFilters ? (
          <button
            type="button"
            onClick={() => setColumnFilters([])}
            className="mt-1 flex shrink-0 items-center gap-1 text-xs text-muted-foreground underline-offset-2 hover:underline"
          >
            <X className="size-3" /> Clear column filters
          </button>
        ) : null}
      </div>
      <div ref={containerRef} className="overflow-x-hidden overflow-y-auto" style={maxHeight === 'none' ? undefined : { maxHeight }}>
        <Table
          style={
            resizableColumns
              ? {
                  width: visibleTotalSize,
                  minWidth: '100%',
                  maxWidth: containerWidth ?? undefined,
                  tableLayout: 'fixed',
                }
              : undefined
          }
        >
          <TableHeader>
            {visibleHeaderGroups.map((hg) => (
              <TableRow key={hg.id}>
                {collapseColumns ? <TableHead className="w-8" /> : null}
                {hg.headers.map((header) => {
                  const sorted = header.column.getIsSorted()
                  const canSort = header.column.getCanSort()
                  const canFilter = header.column.getCanFilter() && !header.isPlaceholder
                  const activeFilter = (header.column.getFilterValue() as string[] | undefined) ?? []
                  const facetedValues = canFilter ? Array.from(header.column.getFacetedUniqueValues().keys()) : []
                  const filterOptions = facetedValues
                    .filter((v) => v != null && v !== '')
                    .map((v) => String(v))
                    .sort((a, b) => a.localeCompare(b))
                    .slice(0, 50)
                  return (
                    <TableHead
                      key={header.id}
                      className="relative text-center"
                      style={resizableColumns ? { width: header.getSize() } : undefined}
                    >
                      <DropdownMenu>
                        <DropdownMenuTrigger asChild disabled={!canSort && !canFilter}>
                          <button
                            type="button"
                            className={cn(
                              'flex w-full items-center justify-center gap-1',
                              (canSort || canFilter) && 'cursor-pointer',
                            )}
                          >
                            {header.isPlaceholder ? null : flexRender(header.column.columnDef.header, header.getContext())}
                            {canSort ? (
                              sorted === 'asc' ? (
                                <ArrowUp className="size-3" />
                              ) : sorted === 'desc' ? (
                                <ArrowDown className="size-3" />
                              ) : (
                                <ArrowUpDown className="size-3 opacity-30" />
                              )
                            ) : null}
                            {activeFilter.length > 0 ? <Filter className="size-3 text-primary" /> : null}
                            {canSort || canFilter ? <ChevronDown className="size-3 opacity-50" /> : null}
                          </button>
                        </DropdownMenuTrigger>
                        {canSort || canFilter ? (
                          <DropdownMenuContent align="center" className="normal-case">
                            {canSort ? (
                              <>
                                <DropdownMenuItem onSelect={() => header.column.toggleSorting(false)}>
                                  <ArrowUp className="size-3.5" /> Sort ascending
                                </DropdownMenuItem>
                                <DropdownMenuItem onSelect={() => header.column.toggleSorting(true)}>
                                  <ArrowDown className="size-3.5" /> Sort descending
                                </DropdownMenuItem>
                                {sorted ? (
                                  <DropdownMenuItem onSelect={() => header.column.clearSorting()}>
                                    <ArrowUpDown className="size-3.5" /> Clear sort
                                  </DropdownMenuItem>
                                ) : null}
                              </>
                            ) : null}
                            {canSort && canFilter ? <DropdownMenuSeparator /> : null}
                            {canFilter && filterOptions.length > 0 ? (
                              <>
                                <DropdownMenuLabel>Filter</DropdownMenuLabel>
                                <div className="max-h-56 overflow-y-auto">
                                  {filterOptions.map((value) => (
                                    <DropdownMenuCheckboxItem
                                      key={value}
                                      checked={activeFilter.includes(value)}
                                      onSelect={(e) => {
                                        e.preventDefault()
                                        const next = activeFilter.includes(value)
                                          ? activeFilter.filter((v) => v !== value)
                                          : [...activeFilter, value]
                                        header.column.setFilterValue(next.length > 0 ? next : undefined)
                                      }}
                                    >
                                      {value}
                                    </DropdownMenuCheckboxItem>
                                  ))}
                                </div>
                                {activeFilter.length > 0 ? (
                                  <DropdownMenuItem onSelect={() => header.column.setFilterValue(undefined)}>
                                    <X className="size-3.5" /> Clear filter
                                  </DropdownMenuItem>
                                ) : null}
                              </>
                            ) : null}
                          </DropdownMenuContent>
                        ) : null}
                      </DropdownMenu>
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
                const visibleCells = collapseColumns ? allCells.filter((c) => !isCollapsedPriority(c.column.id)) : allCells
                const hiddenCells = collapseColumns ? allCells.filter((c) => isCollapsedPriority(c.column.id)) : []
                return (
                  <React.Fragment key={row.id}>
                    <TableRow
                      {...(onRowClick
                        ? {
                            onClick: () => onRowClick(row.original),
                            // Keyboard parity: a row that responds to a click
                            // must respond to Enter/Space too, and be
                            // reachable by tab. Only applied when the table
                            // is actually interactive, so non-clickable
                            // tables keep a clean tab order.
                            onKeyDown: (e: React.KeyboardEvent<HTMLTableRowElement>) => {
                              if (e.key === 'Enter' || e.key === ' ') {
                                e.preventDefault()
                                onRowClick(row.original)
                              }
                            },
                            tabIndex: 0,
                            role: 'button',
                            className: 'cursor-pointer hover:bg-muted/50 focus-visible:outline-2 focus-visible:-outline-offset-2 focus-visible:outline-ring',
                          }
                        : {})}
                    >
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
