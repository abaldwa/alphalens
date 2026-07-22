import { useQuery } from '@tanstack/react-query'

import { AppShell, Badge, Card, CardContent, CardDescription, CardHeader, CardTitle, NAV_SECTIONS } from '@/lib/ui'
import { apiGet } from '@/shared/api/client'

interface PillarSummary {
  as_of_date: string | null
  available: boolean
  recommendation_count: number
  avg_expected_return_pct: number | null
  top_strategy: string | null
  top_strategy_success_rate_pct: number | null
}

interface Pillar {
  id: string
  label: string
  href: string
  endpoint: string
}

// The 5 strategy pillars the Home page summarizes — each backed by its own
// lightweight GET .../pillar_summary endpoint (technical.py, fundamentals.py,
// valuation.py, momentum.py, watchlist.py) returning the same shape:
// recommendation_count / avg_expected_return_pct / top_strategy /
// top_strategy_success_rate_pct. Only Technical currently has a real
// strategy/win-rate table (strategy_confidence_summary) — the other 4
// legitimately return null for top_strategy_success_rate_pct rather than
// fabricating a number, per the app's no-stub-data policy.
const PILLARS: Pillar[] = [
  { id: 'technical', label: 'Technical', href: '/technical-watchlist', endpoint: '/api/v1/ta/pillar_summary' },
  { id: 'fundamental', label: 'Fundamental', href: '/fundamental-screener', endpoint: '/api/v1/fundamentals/pillar_summary' },
  { id: 'valuation', label: 'Valuation', href: '/valuation-batch', endpoint: '/api/v1/valuation/pillar_summary' },
  { id: 'momentum', label: 'Momentum', href: '/momentum', endpoint: '/api/v1/momentum/pillar_summary' },
  { id: 'ml', label: 'ML Signals', href: '/ml-signal', endpoint: '/api/v1/watchlist/pillar_summary' },
]

const SECONDARY_SECTION_IDS = new Set(['forensic', 'big_investors', 'backtest', 'ops', 'macro', 'explain'])

function fmtPct(v: number | null): string {
  if (v == null) return '—'
  return `${v >= 0 ? '+' : ''}${v.toFixed(1)}%`
}

function PillarCard({ pillar }: { pillar: Pillar }) {
  // Each card's query is independent — a slow or failed endpoint for one
  // pillar never blocks the other 4 from rendering.
  const summary = useQuery({
    queryKey: ['pillar-summary', pillar.id],
    queryFn: () => apiGet<PillarSummary>(pillar.endpoint),
  })

  return (
    <a href={pillar.href}>
      <Card className="h-full transition-colors hover:border-primary">
        <CardHeader>
          <CardTitle>{pillar.label}</CardTitle>
          <CardDescription>
            {summary.isLoading
              ? 'Loading…'
              : summary.error
                ? 'Failed to load'
                : summary.data?.as_of_date
                  ? `As of ${summary.data.as_of_date}`
                  : 'No data available yet'}
          </CardDescription>
        </CardHeader>
        <CardContent>
          {summary.data?.available ? (
            <div className="flex flex-col gap-2">
              <div className="flex items-baseline justify-between">
                <span className="text-xs text-muted-foreground">Recommendations</span>
                <span className="font-mono-data text-lg font-semibold">{summary.data.recommendation_count}</span>
              </div>
              <div className="flex items-baseline justify-between">
                <span className="text-xs text-muted-foreground">Avg. expected return</span>
                <span
                  className={`font-mono-data text-sm ${
                    summary.data.avg_expected_return_pct != null && summary.data.avg_expected_return_pct >= 0
                      ? 'text-green'
                      : summary.data.avg_expected_return_pct != null
                        ? 'text-red'
                        : 'text-muted-foreground'
                  }`}
                >
                  {fmtPct(summary.data.avg_expected_return_pct)}
                </span>
              </div>
              {summary.data.top_strategy ? (
                <div className="flex items-center justify-between gap-2">
                  <Badge variant="outline" className="max-w-[70%] truncate">
                    {summary.data.top_strategy}
                  </Badge>
                  <span className="font-mono-data text-xs text-muted-foreground">
                    {summary.data.top_strategy_success_rate_pct != null
                      ? `${summary.data.top_strategy_success_rate_pct.toFixed(0)}% win rate`
                      : 'win rate n/a'}
                  </span>
                </div>
              ) : null}
            </div>
          ) : (
            <p className="text-sm text-muted-foreground">
              {summary.error ? (summary.error as Error).message : 'No recommendations for the latest available date.'}
            </p>
          )}
        </CardContent>
      </Card>
    </a>
  )
}

export function HomePage() {
  const secondarySections = NAV_SECTIONS.filter((s) => SECONDARY_SECTION_IDS.has(s.id))

  return (
    <AppShell title="Home" description="Today's outcomes across all 5 strategy pillars — Technical, Fundamental, Valuation, Momentum, and ML Signals.">
      <div className="grid grid-cols-1 gap-4 sm:grid-cols-2 lg:grid-cols-3 xl:grid-cols-5">
        {PILLARS.map((p) => (
          <PillarCard key={p.id} pillar={p} />
        ))}
      </div>

      <div className="mt-6">
        <h2 className="mb-2 text-sm font-semibold text-muted-foreground">More</h2>
        <div className="grid grid-cols-1 gap-4 sm:grid-cols-2 lg:grid-cols-3">
          {secondarySections.map((s) => (
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
      </div>
    </AppShell>
  )
}
