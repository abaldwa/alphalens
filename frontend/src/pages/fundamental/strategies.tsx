import { useQuery } from '@tanstack/react-query'
import { Link } from 'react-router-dom'

import { AppShell, Badge, Card, CardContent, CardHeader, CardTitle } from '@/lib/ui'
import { apiGet } from '@/shared/api/client'
import type { FAStrategyCatalogEntry, FAStrategyCatalogResponse } from './types'

// Investor-style categories, in display order — mirrors technical/screener.tsx's
// STYLE_ORDER-per-Card pattern for grouping a growing strategy list.
const CATEGORY_ORDER = ['Value', 'Quality', 'Growth', 'Governance', 'Contrarian']

const KIND_BADGE_VARIANT: Record<string, 'success' | 'outline' | 'secondary'> = {
  preset: 'success',
  bespoke: 'outline',
  composite_score: 'secondary',
}

const KIND_LABEL: Record<string, string> = {
  preset: 'Screener',
  bespoke: 'Screener',
  composite_score: 'Score only',
}

function StrategyCard({ strategy }: { strategy: FAStrategyCatalogEntry }) {
  const runnable = strategy.kind === 'preset' || strategy.kind === 'bespoke'
  const body = (
    <div className="flex flex-col gap-1 rounded-[var(--radius-token)] border border-border p-3 text-sm">
      <div className="flex items-center justify-between gap-2">
        <span className="font-semibold">{strategy.label}</span>
        <span className="flex items-center gap-1">
          <Badge variant={KIND_BADGE_VARIANT[strategy.kind] ?? 'outline'}>{KIND_LABEL[strategy.kind]}</Badge>
          {/* 2026-07-25: real current backtest status per strategy, not a
              placeholder — none have cleared a backtest yet. */}
          <Badge variant={strategy.backtested ? 'success' : 'warning'}>
            {strategy.backtested ? 'Backtested' : 'Not backtested'}
          </Badge>
        </span>
      </div>
      <span className="text-xs text-muted-foreground">{strategy.description}</span>
    </div>
  )
  if (!runnable) {
    // composite_score strategies aren't run against the universe today —
    // they're computed per-ticker via GET /api/v1/fundamentals/{ticker}/scores
    // (strategy_scores field). No universe-wide screener exists for them
    // yet, so this card is informational rather than a dead link.
    return body
  }
  return (
    <Link to={`/fundamental-screener?preset=${strategy.key}`} className="block transition hover:bg-white/5">
      {body}
    </Link>
  )
}

export function FundamentalStrategiesPage() {
  const catalog = useQuery({
    queryKey: ['fa-strategy-catalog'],
    queryFn: () => apiGet<FAStrategyCatalogResponse>('/api/v1/fundamentals/screener/catalog'),
  })

  if (catalog.error) {
    return (
      <AppShell title="Fundamental — Strategies" description="Investor-style strategy catalog.">
        <p className="text-sm text-red">
          Could not reach GET /api/v1/fundamentals/screener/catalog — {(catalog.error as Error).message}
        </p>
      </AppShell>
    )
  }

  const byCategory = new Map<string, FAStrategyCatalogEntry[]>()
  for (const s of catalog.data?.strategies ?? []) {
    const list = byCategory.get(s.category) ?? []
    list.push(s)
    byCategory.set(s.category, list)
  }

  const strategies = catalog.data?.strategies ?? []
  const backtestedCount = strategies.filter((s) => s.backtested).length

  return (
    <AppShell
      title="Fundamental — Strategies"
      description="26 investor-style screening strategies (Graham, Buffett, Lynch, Agrawal, Kedia, and more) grouped by category. 'Screener' cards link to a live universe scan; 'Score only' cards are per-ticker composite scores available via a symbol's Fundamental tab."
    >
      {!catalog.isLoading && strategies.length > 0 && (
        <div className="mb-4 rounded-[var(--radius-token)] border border-amber/40 bg-amber/10 p-3 text-sm text-amber">
          <strong>{backtestedCount} of {strategies.length} strategies have a validated backtest.</strong>{' '}
          Composite weights are hardcoded from the source formula catalog, not tuned or backtested — treat
          "Not backtested" strategies as research/screening heuristics, not validated trading signals.
        </div>
      )}
      <div className="grid grid-cols-1 gap-4 lg:grid-cols-2">
        {CATEGORY_ORDER.map((category) => {
          const strategies = byCategory.get(category) ?? []
          return (
            <Card key={category}>
              <CardHeader>
                <CardTitle>{category}</CardTitle>
              </CardHeader>
              <CardContent>
                {catalog.isLoading ? (
                  <p className="text-sm text-muted-foreground">Loading…</p>
                ) : strategies.length === 0 ? (
                  <p className="text-sm text-muted-foreground">No strategies in this category.</p>
                ) : (
                  <div className="flex flex-col gap-2">
                    {strategies.map((s) => (
                      <StrategyCard key={s.key} strategy={s} />
                    ))}
                  </div>
                )}
              </CardContent>
            </Card>
          )
        })}
      </div>
    </AppShell>
  )
}
