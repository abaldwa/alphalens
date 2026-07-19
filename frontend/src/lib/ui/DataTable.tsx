import {
  type ColumnDef,
  flexRender,
  getCoreRowModel,
  getSortedRowModel,
  type SortingState,
  useReactTable,
} from '@tanstack/react-table'
import * as React from 'react'
import { ArrowDown, ArrowUp, ArrowUpDown } from 'lucide-react'

import { cn } from '@/lib/utils'
import { Table, TableHeader, TableBody, TableRow, TableHead, TableCell } from '@/lib/ui/primitives/table'
import { Skeleton } from '@/lib/ui/primitives/skeleton'

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
}

/**
 * Dense data-grid wrapper around TanStack Table + the shadcn-style Table
 * primitive — the shared building block for every screener/list page.
 * Supports client-side column sorting and column resizing; consumers just
 * pass columns/data.
 */
export function DataTable<TData>({
  columns,
  data,
  isLoading,
  emptyMessage = 'No rows.',
  skeletonRows = 6,
  resizableColumns = true,
}: DataTableProps<TData>) {
  const [sorting, setSorting] = React.useState<SortingState>([])

  const table = useReactTable({
    data,
    columns,
    state: { sorting },
    onSortingChange: setSorting,
    getCoreRowModel: getCoreRowModel(),
    getSortedRowModel: getSortedRowModel(),
    columnResizeMode: 'onChange',
    enableColumnResizing: resizableColumns,
    defaultColumn: { minSize: 60, size: 160 },
  })

  if (isLoading) {
    return (
      <div className="flex flex-col gap-2">
        {Array.from({ length: skeletonRows }).map((_, i) => (
          <Skeleton key={i} className="h-9 w-full" />
        ))}
      </div>
    )
  }

  return (
    <Table style={resizableColumns ? { width: table.getTotalSize(), minWidth: '100%' } : undefined}>
      <TableHeader>
        {table.getHeaderGroups().map((hg) => (
          <TableRow key={hg.id}>
            {hg.headers.map((header) => {
              const sorted = header.column.getIsSorted()
              return (
                <TableHead
                  key={header.id}
                  className={cn('relative', header.column.getCanSort() && 'cursor-pointer select-none')}
                  style={resizableColumns ? { width: header.getSize() } : undefined}
                >
                  <span className="inline-flex items-center gap-1" onClick={header.column.getToggleSortingHandler()}>
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
          table.getRowModel().rows.map((row) => (
            <TableRow key={row.id}>
              {row.getVisibleCells().map((cell) => (
                <TableCell
                  key={cell.id}
                  className="truncate font-mono-data text-sm"
                  style={resizableColumns ? { width: cell.column.getSize() } : undefined}
                >
                  {flexRender(cell.column.columnDef.cell, cell.getContext())}
                </TableCell>
              ))}
            </TableRow>
          ))
        ) : (
          <TableRow>
            <TableCell colSpan={columns.length} className="py-8 text-center text-sm text-muted-foreground">
              {emptyMessage}
            </TableCell>
          </TableRow>
        )}
      </TableBody>
    </Table>
  )
}
