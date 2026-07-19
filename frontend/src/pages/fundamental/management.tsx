import { useState } from 'react'
import { useQuery } from '@tanstack/react-query'
import type { ColumnDef } from '@tanstack/react-table'

import { AppShell, Badge, Button, Card, CardContent, CardHeader, CardTitle, DataTable, InfoTooltip, StatCard } from '@/lib/ui'
import { apiGet } from '@/shared/api/client'
import type { FAScoresResponse, GovernanceResponse, GovernanceRow } from './types'

const pct = (v: number | null | undefined) => (v == null ? '—' : `${(v).toFixed(1)}%`)

const columns: ColumnDef<GovernanceRow, unknown>[] = [
  { accessorKey: 'filing_date', header: 'Filing Date', cell: (i) => i.getValue<string>().slice(0, 10) },
  { accessorKey: 'promoter_pct', header: 'Promoter %', cell: (i) => pct(i.getValue<number | null>()) },
  {
    accessorKey: 'promoter_pledge',
    header: () => (
      <span className="inline-flex items-center gap-1">
        Pledge %
        <InfoTooltip>Promoter Pledge %: share of promoters' holding pledged as collateral for loans. High/rising pledge is a governance red flag — a forced sale on default can crash the stock.</InfoTooltip>
      </span>
    ),
    cell: (i) => pct(i.getValue<number | null>()),
  },
  {
    accessorKey: 'fii_pct',
    header: () => (
      <span className="inline-flex items-center gap-1">
        FII %
        <InfoTooltip>FII (Foreign Institutional Investors): share of the company held by foreign institutional investors (funds, FPIs).</InfoTooltip>
      </span>
    ),
    cell: (i) => pct(i.getValue<number | null>()),
  },
  {
    accessorKey: 'dii_pct',
    header: () => (
      <span className="inline-flex items-center gap-1">
        DII %
        <InfoTooltip>DII (Domestic Institutional Investors): share of the company held by domestic institutions (mutual funds, insurers, banks).</InfoTooltip>
      </span>
    ),
    cell: (i) => pct(i.getValue<number | null>()),
  },
  { accessorKey: 'mf_pct', header: 'MF %', cell: (i) => pct(i.getValue<number | null>()) },
  { accessorKey: 'retail_pct', header: 'Retail %', cell: (i) => pct(i.getValue<number | null>()) },
]

export function FundamentalManagementPage() {
  const [input, setInput] = useState('RELIANCE')
  const [ticker, setTicker] = useState('RELIANCE')

  const governance = useQuery({
    queryKey: ['governance', ticker],
    queryFn: () => apiGet<GovernanceResponse>(`/api/v1/governance/${ticker}`),
    enabled: !!ticker,
  })

  const scores = useQuery({
    queryKey: ['fa-scores', ticker],
    queryFn: () => apiGet<FAScoresResponse>(`/api/v1/fundamentals/${ticker}/scores`),
    enabled: !!ticker,
  })

  const rows = governance.data?.data ?? []
  const latest = [...rows].sort((a, b) => (a.filing_date < b.filing_date ? 1 : -1))[0]
  const sorted = [...rows].sort((a, b) => (a.filing_date < b.filing_date ? -1 : 1))

  return (
    <AppShell title="Fundamental — Management" description="FA-F Management Quality: promoter/shareholding + composite management-quality score.">
      <Card>
        <CardHeader>
          <CardTitle>Ticker</CardTitle>
        </CardHeader>
        <CardContent>
          <div className="flex items-center gap-2">
            <input
              className="h-9 rounded-[var(--radius-token)] border border-border bg-transparent px-3 text-sm"
              value={input}
              onChange={(e) => setInput(e.target.value.toUpperCase())}
              placeholder="Ticker (e.g. RELIANCE)"
            />
            <Button onClick={() => setTicker(input.trim())}>Load</Button>
          </div>
        </CardContent>
      </Card>

      {!rows.length ? (
        <div className="mt-4">
          <Card>
            <CardContent className="pt-4 text-sm text-muted-foreground">
              {governance.error
                ? `Could not reach GET /api/v1/governance/{ticker} — ${(governance.error as Error).message}`
                : governance.isLoading
                  ? 'Loading…'
                  : `No governance data for ${ticker}`}
            </CardContent>
          </Card>
        </div>
      ) : (
        <>
          <div className="mt-4 flex items-center gap-2">
            <span className="text-lg font-semibold">{ticker}</span>
            <Badge>Management Quality {scores.data?.management_quality_score != null ? scores.data.management_quality_score.toFixed(0) : '—'}</Badge>
            <InfoTooltip>Composite score summarizing shareholding-quality signals (promoter stake, pledge level, institutional ownership) into a single 0-100 figure.</InfoTooltip>
            {latest?.superstar_flag ? <Badge variant="secondary">Superstar Investor Tracked</Badge> : null}
          </div>

          <div className="mt-4 grid grid-cols-2 gap-4 sm:grid-cols-4">
            <StatCard label="Promoter %" value={pct(latest?.promoter_pct)} />
            <StatCard
              label={
                <span className="inline-flex items-center gap-1">
                  Promoter Pledge %
                  <InfoTooltip>Share of promoters' holding pledged as collateral for loans. High/rising pledge is a governance red flag — a forced sale on default can crash the stock.</InfoTooltip>
                </span>
              }
              value={pct(latest?.promoter_pledge)}
              tone={(latest?.promoter_pledge ?? 0) > 10 ? 'red' : 'default'}
            />
            <StatCard
              label={
                <span className="inline-flex items-center gap-1">
                  FII %
                  <InfoTooltip>FII (Foreign Institutional Investors): share of the company held by foreign institutional investors.</InfoTooltip>
                </span>
              }
              value={pct(latest?.fii_pct)}
            />
            <StatCard
              label={
                <span className="inline-flex items-center gap-1">
                  DII %
                  <InfoTooltip>DII (Domestic Institutional Investors): share of the company held by domestic institutions (mutual funds, insurers, banks).</InfoTooltip>
                </span>
              }
              value={pct(latest?.dii_pct)}
            />
          </div>

          <div className="mt-4">
            <Card>
              <CardHeader>
                <CardTitle>Shareholding history</CardTitle>
              </CardHeader>
              <CardContent>
                <DataTable columns={columns} data={sorted} isLoading={governance.isLoading} />
              </CardContent>
            </Card>
          </div>
        </>
      )}

      <div className="mt-4">
        <Card>
          <CardHeader>
            <CardTitle>Related-party transactions</CardTitle>
          </CardHeader>
          <CardContent>
            <p className="text-sm text-muted-foreground">
              Related-party-transaction analysis needs a backend that doesn't exist yet — systems/fundamental_analysis/management/ is an empty stub.
            </p>
          </CardContent>
        </Card>
      </div>
    </AppShell>
  )
}
