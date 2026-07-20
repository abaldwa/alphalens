import { useMemo, useState } from 'react'
import { useQuery } from '@tanstack/react-query'
import type { ColumnDef } from '@tanstack/react-table'

import { AppShell, Badge, Card, CardContent, CardHeader, CardTitle, DataTable, Table, TableHeader, TableBody, TableRow, TableHead, TableCell, tickerColumn } from '@/lib/ui'
import { apiGet } from '@/shared/api/client'
import type { TAScreenerResponse, TAScreenerRow, TAStrategyWinRateResponse, TATemplateListResponse } from './types'

function fmtPct(v: number | null): string {
  return v == null ? '—' : `${(v * 100).toFixed(0)}%`
}

function fmtInterval(lo: number | null, hi: number | null): string {
  if (lo == null || hi == null) return '—'
  return `${(lo * 100).toFixed(0)}–${(hi * 100).toFixed(0)}%`
}

function fmtDelta(v: number | null): string {
  if (v == null) return '—'
  const pct = (v * 100).toFixed(0)
  return v > 0 ? `+${pct}pp` : `${pct}pp`
}

const STYLE_ORDER = ['Momentum', 'Trend Following', 'Mean Reversion', 'Volatility']

const TIER_BADGE_VARIANT: Record<string, 'success' | 'outline' | 'destructive'> = {
  VALIDATED: 'success',
  PRELIMINARY: 'outline',
}

function StrategyWinRates() {
  const winRates = useQuery({
    queryKey: ['ta-strategy-win-rates'],
    queryFn: () => apiGet<TAStrategyWinRateResponse>('/api/v1/ta/strategies/win_rates'),
  })

  if (winRates.error) {
    return <p className="text-sm text-red">Could not reach GET /api/v1/ta/strategies/win_rates — {(winRates.error as Error).message}</p>
  }

  const totalShown = STYLE_ORDER.reduce((n, s) => n + (winRates.data?.styles[s]?.length ?? 0), 0)

  return (
    <div className="flex flex-col gap-3">
      {!winRates.isLoading && (
        <p className="text-xs text-muted-foreground">
          {totalShown === 0
            ? 'No templates have earned enough independent-day/regime history yet to show a win rate — every strategy needs its own track record before a number is displayed. Check back as more real trading days accumulate.'
            : `${totalShown} of 42 templates have cleared the minimum sample to show a preliminary or validated win rate; the rest are still accumulating history.`}
        </p>
      )}
      <div className="grid grid-cols-1 gap-4 lg:grid-cols-2">
        {STYLE_ORDER.map((style) => {
          const rows = winRates.data?.styles[style] ?? []
          return (
            <Card key={style}>
              <CardHeader>
                <CardTitle>{style}</CardTitle>
              </CardHeader>
              <CardContent>
                {winRates.isLoading ? (
                  <p className="text-sm text-muted-foreground">Loading…</p>
                ) : rows.length === 0 ? (
                  <p className="text-sm text-muted-foreground">No {style} templates have earned a shown win rate yet.</p>
                ) : (
                  <Table>
                    <TableHeader>
                      <TableRow>
                        <TableHead>Template</TableHead>
                        <TableHead className="text-right">W / L / Pending</TableHead>
                        <TableHead className="text-right">Win Rate (95% CI)</TableHead>
                        <TableHead className="text-right">vs. Baseline</TableHead>
                        <TableHead className="text-right">Tier</TableHead>
                      </TableRow>
                    </TableHeader>
                    <TableBody>
                      {rows.map((r) => (
                        <TableRow key={r.template_name} title={r.reasons.join('; ')}>
                          <TableCell>
                            <span className="inline-flex items-center gap-1.5">
                              <Badge>{r.category}</Badge>
                              <span>{r.template_name}</span>
                              <span className="text-xs text-muted-foreground">{r.description}</span>
                            </span>
                          </TableCell>
                          <TableCell className="text-right font-mono-data text-xs">
                            {r.wins} / {r.losses} / {r.pending}
                          </TableCell>
                          <TableCell className="text-right font-mono-data">
                            {fmtPct(r.win_rate)}
                            <span className="ml-1 text-xs text-muted-foreground">({fmtInterval(r.wilson_lo, r.wilson_hi)})</span>
                          </TableCell>
                          <TableCell className="text-right font-mono-data text-xs">{fmtDelta(r.delta_vs_baseline)}</TableCell>
                          <TableCell className="text-right">
                            <Badge variant={TIER_BADGE_VARIANT[r.tier] ?? 'outline'}>{r.tier}</Badge>
                          </TableCell>
                        </TableRow>
                      ))}
                    </TableBody>
                  </Table>
                )}
              </CardContent>
            </Card>
          )
        })}
      </div>
    </div>
  )
}

export function TechnicalScreenerPage() {
  const [selected, setSelected] = useState<string | null>(null)

  const templates = useQuery({
    queryKey: ['ta-screener-templates'],
    queryFn: () => apiGet<TATemplateListResponse>('/api/v1/ta/screener/templates'),
  })

  const activeTemplate = selected ?? templates.data?.templates[0]?.name ?? null

  const results = useQuery({
    queryKey: ['ta-screener-run', activeTemplate],
    queryFn: () => apiGet<TAScreenerResponse>(`/api/v1/ta/screener/run/${activeTemplate}`, { limit: 50 }),
    enabled: !!activeTemplate,
  })

  const columns = useMemo<ColumnDef<TAScreenerRow, unknown>[]>(() => {
    const keyCols = results.data?.rows[0] ? Object.keys(results.data.rows[0].key_values) : []
    return [
      tickerColumn<TAScreenerRow>(),
      { accessorKey: 'score', header: 'Score', meta: { align: 'right' }, cell: (i) => i.getValue<number>().toFixed(2) },
      {
        id: 'matched',
        header: 'Matched',
        meta: { align: 'right' },
        cell: ({ row }) => `${row.original.matched_conditions}/${row.original.total_conditions}`,
      },
      ...keyCols.map((k) => ({
        id: k,
        header: k,
        meta: { align: 'right' as const },
        cell: ({ row }: { row: { original: TAScreenerRow } }) => {
          const v = row.original.key_values[k]
          return v == null ? '—' : v.toFixed(2)
        },
      })),
    ]
  }, [results.data])

  return (
    <AppShell title="Technical — Screener" description="Run any of the pre-built TA-D strategy screener templates.">
      <div className="mb-4">
        <StrategyWinRates />
      </div>

      <Card>
        <CardHeader>
          <CardTitle>Template</CardTitle>
        </CardHeader>
        <CardContent>
          <div className="flex items-center gap-2">
            <select
              className="h-9 rounded-[var(--radius-token)] border border-border bg-transparent px-3 text-sm"
              value={activeTemplate ?? ''}
              onChange={(e) => setSelected(e.target.value)}
            >
              {(templates.data?.templates ?? []).map((t) => (
                <option key={t.name} value={t.name}>
                  {t.name} — {t.description} ({t.category})
                </option>
              ))}
            </select>
          </div>
        </CardContent>
      </Card>

      <div className="mt-4">
        <Card>
          <CardHeader>
            <CardTitle>
              Results {results.data ? `(${results.data.count} match${results.data.count === 1 ? '' : 'es'} on ${results.data.date ?? 'latest date'})` : ''}
            </CardTitle>
          </CardHeader>
          <CardContent>
            {results.error ? (
              <p className="text-sm text-red">Could not reach GET /api/v1/ta/screener/run/{'{template}'} — {(results.error as Error).message}</p>
            ) : (
              <DataTable columns={columns} data={results.data?.rows ?? []} isLoading={results.isLoading || templates.isLoading} />
            )}
          </CardContent>
        </Card>
      </div>
    </AppShell>
  )
}
