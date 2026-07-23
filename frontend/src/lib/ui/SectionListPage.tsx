import { useQuery } from '@tanstack/react-query'
import type { ColumnDef } from '@tanstack/react-table'

import { AppShell } from '@/lib/ui/AppShell'
import { DataTable } from '@/lib/ui/DataTable'
import { Card, CardContent, CardHeader, CardTitle, CardDescription } from '@/lib/ui/primitives/card'
import { apiGet } from '@/shared/api/client'

/** Turns any JSON scalar into table-friendly text — arrays/objects are
 * JSON-stringified rather than rendered as "[object Object]". */
function renderCell(value: unknown): string {
  if (value === null || value === undefined) return '—'
  if (typeof value === 'number') return Number.isInteger(value) ? String(value) : value.toFixed(4)
  if (typeof value === 'object') return JSON.stringify(value)
  return String(value)
}

function inferColumns(rows: Record<string, unknown>[]): ColumnDef<Record<string, unknown>, unknown>[] {
  if (!rows.length) return []
  return Object.keys(rows[0]).map((key) => ({
    accessorKey: key,
    header: key.replace(/_/g, ' '),
    cell: (info) => renderCell(info.getValue()),
  }))
}

export interface SectionListPageProps {
  /** Page title shown in AppShell's top bar. */
  title: string
  description?: string
  /** API path, e.g. "/api/v1/momentum/summary". */
  endpoint: string
  queryParams?: Record<string, string | number | boolean | undefined>
  /** If the endpoint returns `{ rows: [...] }` (most AlphaLens list
   * endpoints do), set this to "rows"; if the endpoint returns a bare
   * array, leave unset. */
  rowsKey?: string
}

/**
 * Generic "fetch a list endpoint, render it as a DataTable" page — the
 * fast-path composite used for section pages whose data is fundamentally
 * a table (screeners, reports, holdings, catalogs) without bespoke
 * per-column formatting. Built entirely from library primitives
 * (AppShell, DataTable, Card) so every consumer still goes through the
 * `@/lib/ui` barrel.
 */
export function SectionListPage({ title, description, endpoint, queryParams, rowsKey = 'rows' }: SectionListPageProps) {
  const { data, isLoading, error } = useQuery({
    queryKey: [endpoint, queryParams],
    queryFn: () => apiGet<Record<string, unknown>>(endpoint, queryParams),
  })

  const rows: Record<string, unknown>[] = (() => {
    if (!data) return []
    if (Array.isArray(data)) return data as Record<string, unknown>[]
    const val = (data as Record<string, unknown>)[rowsKey]
    return Array.isArray(val) ? (val as Record<string, unknown>[]) : []
  })()

  const columns = inferColumns(rows)

  return (
    <AppShell title={title} description={description}>
      <Card>
        <CardHeader>
          <CardTitle>Results</CardTitle>
          <CardDescription>
            {isLoading ? 'Loading…' : error ? `Failed to load: ${(error as Error).message}` : `${rows.length} rows`}
          </CardDescription>
        </CardHeader>
        <CardContent>
          {error ? (
            <p className="text-sm text-red">Could not reach the API at {endpoint}. Is the backend running?</p>
          ) : (
            <DataTable columns={columns} data={rows} isLoading={isLoading} />
          )}
        </CardContent>
      </Card>
    </AppShell>
  )
}
