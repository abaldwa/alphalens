import { useState } from 'react'
import { useQuery } from '@tanstack/react-query'
import type { ColumnDef } from '@tanstack/react-table'

import { AppShell, Badge, Card, CardContent, CardHeader, CardTitle, DataTable, InfoTooltip, TickerLink } from '@/lib/ui'
import { apiGet } from '@/shared/api/client'

interface AnnouncementRow {
  seq_id: string
  ticker: string
  company_name: string | null
  category: string
  subject: string | null
  announcement_text: string | null
  announced_at: string
  exchange_disseminated_at: string | null
  attachment_url: string | null
}
interface AnnouncementResponse {
  data: AnnouncementRow[]
  record_count: number
}

const CATEGORY_LABEL: Record<string, string> = {
  buyback: 'Buyback',
  qip: 'QIP',
  board_change: 'Board Change',
  investigation: 'Investigation',
  insider: 'Insider / SAST',
  credit_rating: 'Credit Rating',
  auditor_change: 'Auditor Change',
  ma: 'M&A',
}

const CATEGORY_VARIANT: Record<string, 'default' | 'secondary' | 'outline' | 'success' | 'warning' | 'destructive'> = {
  buyback: 'default',
  qip: 'default',
  board_change: 'warning',
  investigation: 'destructive',
  insider: 'destructive',
  credit_rating: 'outline',
  auditor_change: 'warning',
  ma: 'success',
}

function makeColumns(): ColumnDef<AnnouncementRow, unknown>[] {
  return [
    { accessorKey: 'announced_at', header: 'Date', cell: (i) => i.getValue<string>().replace('T', ' ').slice(0, 16) },
    { accessorKey: 'ticker', header: 'Ticker', cell: (i) => <TickerLink ticker={i.getValue<string>()} /> },
    { accessorKey: 'company_name', header: 'Company', cell: (i) => i.getValue<string | null>() ?? '—' },
    {
      accessorKey: 'category',
      header: () => (
        <span className="inline-flex items-center gap-1">
          Category
          <InfoTooltip>
            QIP (Qualified Institutional Placement): a fundraise where shares are issued directly to institutional investors. Insider / SAST: filings under SEBI's insider-trading and Substantial
            Acquisition of Shares &amp; Takeovers regulations, covering promoter/insider trades and large stake changes.
          </InfoTooltip>
        </span>
      ),
      cell: (i) => {
        const c = i.getValue<string>()
        return <Badge variant={CATEGORY_VARIANT[c] ?? 'secondary'}>{CATEGORY_LABEL[c] ?? c}</Badge>
      },
    },
    { accessorKey: 'subject', header: 'Subject', cell: (i) => i.getValue<string | null>() ?? '—' },
    {
      id: 'filing',
      header: 'Filing',
      cell: ({ row }) =>
        row.original.attachment_url ? (
          <a className="underline" href={row.original.attachment_url} target="_blank" rel="noopener noreferrer">
            View
          </a>
        ) : (
          '—'
        ),
    },
  ]
}

const CATEGORY_OPTIONS = Object.entries(CATEGORY_LABEL)
const selectClass = 'h-9 rounded-[var(--radius-token)] border border-border bg-transparent px-3 text-sm'
const inputClass = selectClass

export function BigInvestorsAnnouncementsPage() {
  const [days, setDays] = useState('5')
  const [recentCategory, setRecentCategory] = useState('')

  const recent = useQuery({
    queryKey: ['ca-recent', days, recentCategory],
    queryFn: () =>
      apiGet<AnnouncementResponse>('/api/v1/corporate-announcements/recent', {
        days,
        category: recentCategory || undefined,
      }),
  })

  const [company, setCompany] = useState('')
  const [searchCategory, setSearchCategory] = useState('')
  const [searchTerm, setSearchTerm] = useState('')

  const search = useQuery({
    queryKey: ['ca-search', searchTerm, searchCategory],
    queryFn: () =>
      apiGet<AnnouncementResponse>('/api/v1/corporate-announcements/search', {
        company: searchTerm,
        category: searchCategory || undefined,
      }),
    enabled: !!searchTerm,
  })

  const columns = makeColumns()

  return (
    <AppShell title="Big Investors — Announcements" description="Corporate announcements feed (real NSE material-event categories).">
      <Card>
        <CardHeader>
          <CardTitle>Recent</CardTitle>
        </CardHeader>
        <CardContent>
          <div className="mb-3 flex flex-wrap items-center gap-2">
            <select className={selectClass} value={days} onChange={(e) => setDays(e.target.value)}>
              <option value="1">Last 1 day</option>
              <option value="5">Last 5 days</option>
              <option value="10">Last 10 days</option>
              <option value="30">Last 30 days</option>
            </select>
            <select className={selectClass} value={recentCategory} onChange={(e) => setRecentCategory(e.target.value)}>
              <option value="">All categories</option>
              {CATEGORY_OPTIONS.map(([value, label]) => (
                <option key={value} value={value}>
                  {label}
                </option>
              ))}
            </select>
          </div>
          {recent.error ? (
            <p className="text-sm text-red">
              Could not reach GET /api/v1/corporate-announcements/recent — {(recent.error as Error).message}
            </p>
          ) : (
            <DataTable columns={columns} data={recent.data?.data ?? []} isLoading={recent.isLoading} emptyMessage="No material announcements in range." />
          )}
        </CardContent>
      </Card>

      <div className="mt-4">
        <Card>
          <CardHeader>
            <CardTitle>Search by company</CardTitle>
          </CardHeader>
          <CardContent>
            <div className="mb-3 flex flex-wrap items-center gap-2">
              <input
                className={inputClass}
                placeholder="Company name (e.g. Reliance)"
                value={company}
                onChange={(e) => setCompany(e.target.value)}
                onKeyDown={(e) => {
                  if (e.key === 'Enter') setSearchTerm(company.trim())
                }}
              />
              <select className={selectClass} value={searchCategory} onChange={(e) => setSearchCategory(e.target.value)}>
                <option value="">All categories</option>
                {CATEGORY_OPTIONS.map(([value, label]) => (
                  <option key={value} value={value}>
                    {label}
                  </option>
                ))}
              </select>
              <button
                className="h-9 rounded-[var(--radius-token)] border border-border px-3 text-sm"
                onClick={() => setSearchTerm(company.trim())}
              >
                Search
              </button>
            </div>
            {!searchTerm ? (
              <div className="text-sm text-muted-foreground">Enter a company name to search.</div>
            ) : search.error ? (
              <p className="text-sm text-red">
                Could not reach GET /api/v1/corporate-announcements/search — {(search.error as Error).message}
              </p>
            ) : (
              <DataTable columns={columns} data={search.data?.data ?? []} isLoading={search.isLoading} emptyMessage="No material announcements found." />
            )}
          </CardContent>
        </Card>
      </div>
    </AppShell>
  )
}
