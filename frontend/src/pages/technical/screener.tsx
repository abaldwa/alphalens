import { useMemo, useState } from 'react'
import { useQuery } from '@tanstack/react-query'
import type { ColumnDef } from '@tanstack/react-table'

import { AppShell, Badge, Card, CardContent, CardHeader, CardTitle, DataTable, TickerLink } from '@/lib/ui'
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
                  <table className="w-full text-sm">
                    <thead>
                      <tr className="text-left text-xs text-muted-foreground">
                        <th className="py-1">Template</th>
                        <th className="py-1 text-right">W / L / Pending</th>
                        <th className="py-1 text-right">Win Rate (95% CI)</th>
                        <th className="py-1 text-right">vs. Baseline</th>
                        <th className="py-1 text-right">Tier</th>
                      </tr>
                    </thead>
                    <tbody>
                      {rows.map((r) => (
                        <tr key={r.template_name} className="border-t border-border" title={r.reasons.join('; ')}>
                          <td className="py-1.5">
                            <span className="inline-flex items-center gap-1.5">
                              <Badge>{r.category}</Badge>
                              <span>{r.template_name}</span>
                              <span className="text-xs text-muted-foreground">{r.description}</span>
                            </span>
                          </td>
                          <td className="py-1.5 text-right font-mono-data text-xs">
                            {r.wins} / {r.losses} / {r.pending}
                          </td>
                          <td className="py-1.5 text-right font-mono-data">
                            {fmtPct(r.win_rate)}
                            <span className="ml-1 text-xs text-muted-foreground">({fmtInterval(r.wilson_lo, r.wilson_hi)})</span>
                          </td>
                          <td className="py-1.5 text-right font-mono-data text-xs">{fmtDelta(r.delta_vs_baseline)}</td>
                          <td className="py-1.5 text-right">
                            <Badge variant={TIER_BADGE_VARIANT[r.tier] ?? 'outline'}>{r.tier}</Badge>
                          </td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
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
      { accessorKey: 'ticker', header: 'Ticker', cell: (i) => <TickerLink ticker={i.getValue<string>()} /> },
      { accessorKey: 'score', header: 'Score', cell: (i) => i.getValue<number>().toFixed(2) },
      {
        id: 'matched',
        header: 'Matched',
        cell: ({ row }) => `${row.original.matched_conditions}/${row.original.total_conditions}`,
      },
      ...keyCols.map((k) => ({
        id: k,
        header: k,
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
