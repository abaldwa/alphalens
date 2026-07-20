import { useQuery } from '@tanstack/react-query'
import { Bar, BarChart, CartesianGrid, Tooltip, XAxis, YAxis } from 'recharts'

import { AppShell, Card, CardContent, CardDescription, CardHeader, CardTitle, ResponsiveChartCard, StatCard, NAV_SECTIONS } from '@/lib/ui'
import { apiGet } from '@/shared/api/client'
import type { TAMarketOverviewResponse } from '@/pages/technical/types'

export function HomePage() {
  const sections = NAV_SECTIONS.filter((s) => s.id !== 'home')

  const overview = useQuery({
    queryKey: ['ta-market-overview'],
    queryFn: () => apiGet<TAMarketOverviewResponse>('/api/v1/ta/market_overview'),
  })

  // Sector order comes straight from the backend (already sorted by
  // avg_change_pct descending, get_ta_market_overview in
  // datastore/api/routers/technical.py) — no client-side re-sort.
  const chartData =
    overview.data?.sector_breadth.map((s) => ({
      sector: s.sector,
      advances: s.advances,
      declines: -s.declines,
    })) ?? []

  return (
    <AppShell title="Home" description="Quant research and monitoring across technicals, fundamentals, valuation, forensics, ML signals, and momentum.">
      <div className="grid grid-cols-1 gap-4 sm:grid-cols-2">
        <StatCard label="Advances" value={overview.data?.advances ?? '—'} tone="green" />
        <StatCard label="Declines" value={overview.data?.declines ?? '—'} tone="red" />
      </div>

      <div className="mt-4">
        <ResponsiveChartCard
          title="Sector breadth"
          description={overview.data?.date ? `As of ${overview.data.date}` : overview.error ? 'Failed to load' : 'Loading…'}
          height={280}
        >
          <BarChart data={chartData}>
            <CartesianGrid strokeDasharray="3 3" stroke="var(--border)" />
            <XAxis dataKey="sector" tick={{ fontSize: 11 }} interval={0} angle={-30} textAnchor="end" height={70} />
            <YAxis tick={{ fontSize: 11 }} />
            <Tooltip />
            <Bar dataKey="advances" fill="var(--green)" radius={[3, 3, 0, 0]} />
            <Bar dataKey="declines" fill="var(--red)" radius={[0, 0, 3, 3]} />
          </BarChart>
        </ResponsiveChartCard>
      </div>

      <div className="mt-4 grid grid-cols-1 gap-4 sm:grid-cols-2 lg:grid-cols-3">
        {sections.map((s) => (
          <a key={s.id} href={s.href}>
            <Card className="h-full transition-colors hover:border-primary">
              <CardHeader>
                <CardTitle>{s.label}</CardTitle>
                <CardDescription>Open the {s.label.toLowerCase()} section</CardDescription>
              </CardHeader>
              <CardContent />
            </Card>
          </a>
        ))}
      </div>
    </AppShell>
  )
}
