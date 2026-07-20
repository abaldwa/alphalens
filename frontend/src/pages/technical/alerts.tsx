import { useState } from 'react'
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query'
import type { ColumnDef } from '@tanstack/react-table'

import { AppShell, Badge, Button, Card, CardContent, CardHeader, CardTitle, DataTable, tickerColumn } from '@/lib/ui'
import { apiGet, apiPost } from '@/shared/api/client'
import type { TATemplateListResponse, TAUserAlertResponse, TAUserAlertRow } from './types'

async function apiDelete(path: string): Promise<void> {
  const { API_BASE_URL } = await import('@/shared/api/client')
  const resp = await fetch(new URL(path, API_BASE_URL), { method: 'DELETE' })
  if (!resp.ok) throw new Error(`${resp.status} ${resp.statusText}`)
}

export function TechnicalAlertsPage() {
  const [ticker, setTicker] = useState('RELIANCE')
  const [template, setTemplate] = useState<string | null>(null)
  const qc = useQueryClient()

  const templates = useQuery({
    queryKey: ['ta-screener-templates'],
    queryFn: () => apiGet<TATemplateListResponse>('/api/v1/ta/screener/templates'),
  })

  const alerts = useQuery({
    queryKey: ['ta-user-alerts'],
    queryFn: () => apiGet<TAUserAlertResponse>('/api/v1/ta/user-alerts'),
  })

  const create = useMutation({
    mutationFn: () => apiPost('/api/v1/ta/user-alerts', { ticker, template_name: template ?? templates.data?.templates[0]?.name }),
    onSuccess: () => {
      setTicker('')
      qc.invalidateQueries({ queryKey: ['ta-user-alerts'] })
    },
  })

  const remove = useMutation({
    mutationFn: (alertId: number) => apiDelete(`/api/v1/ta/user-alerts/${alertId}`),
    onSuccess: () => qc.invalidateQueries({ queryKey: ['ta-user-alerts'] }),
  })

  const columns: ColumnDef<TAUserAlertRow, unknown>[] = [
    tickerColumn<TAUserAlertRow>(),
    { accessorKey: 'template_name', header: 'Template' },
    { accessorKey: 'category', header: 'Category' },
    {
      id: 'status',
      header: 'Status',
      cell: ({ row }) =>
        row.original.triggered_today ? <Badge variant="destructive">Triggered</Badge> : <Badge>Watching</Badge>,
    },
    { accessorKey: 'last_triggered_date', header: 'Last Triggered', cell: (i) => i.getValue<string | null>() ?? '—' },
    {
      id: 'actions',
      header: '',
      cell: ({ row }) => (
        <Button variant="outline" size="sm" onClick={() => remove.mutate(row.original.alert_id)}>
          Delete
        </Button>
      ),
    },
  ]

  return (
    <AppShell title="Technical — Alerts" description="TA-9 Alert Manager: watch a ticker/template combination for triggers.">
      <Card>
        <CardHeader>
          <CardTitle>Create alert</CardTitle>
        </CardHeader>
        <CardContent>
          <div className="flex flex-wrap items-center gap-2">
            <input
              className="h-9 rounded-[var(--radius-token)] border border-border bg-transparent px-3 text-sm"
              placeholder="Ticker (e.g. RELIANCE)"
              value={ticker}
              onChange={(e) => setTicker(e.target.value.toUpperCase())}
            />
            <select
              className="h-9 rounded-[var(--radius-token)] border border-border bg-transparent px-3 text-sm"
              value={template ?? templates.data?.templates[0]?.name ?? ''}
              onChange={(e) => setTemplate(e.target.value)}
            >
              {(templates.data?.templates ?? []).map((t) => (
                <option key={t.name} value={t.name}>
                  {t.name} — {t.description} ({t.category})
                </option>
              ))}
            </select>
            <Button onClick={() => create.mutate()} disabled={!ticker || create.isPending}>
              Create Alert
            </Button>
          </div>
        </CardContent>
      </Card>

      <div className="mt-4">
        <Card>
          <CardHeader>
            <CardTitle>Active alerts</CardTitle>
          </CardHeader>
          <CardContent>
            {alerts.error ? (
              <p className="text-sm text-red">Could not reach GET /api/v1/ta/user-alerts — {(alerts.error as Error).message}</p>
            ) : (
              <DataTable
                columns={columns}
                data={alerts.data?.rows ?? []}
                isLoading={alerts.isLoading}
                emptyMessage="No alerts yet — create one above to start watching a ticker/template."
              />
            )}
          </CardContent>
        </Card>
      </div>
    </AppShell>
  )
}
