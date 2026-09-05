/**
 * pages/momentum/band-strategy-ranking.tsx
 *
 * Renamed/rebuilt from pages/backtest/r0-band-analysis.tsx (2026-09-04):
 * that page embedded static R0-era HTML reports via iframe -- R0 itself
 * was retired and split into R14-R17 (see project_r0_split_r14_r17
 * memory), so those reports no longer reflect the current strategy set.
 *
 * This page answers the actual question instead of linking out to a
 * report: for EVERY band, which strategies should we deploy? Ranks every
 * (strategy, config) result by CAGR within its band and shows the top 3.
 *
 * Data via shared/api/framework_backtest.ts only (see that file's
 * docstring on why) -- no direct DB/report-file access from this page.
 */

import { useMemo } from 'react'
import { useQuery } from '@tanstack/react-query'

import { Badge, Card, CardContent, CardHeader, CardTitle } from '@/lib/ui'
import { listFrameworkRuns, type FrameworkRunSummary } from '@/shared/api/framework_backtest'

function fmtPct(v: number | null) {
  return typeof v === 'number' ? `${(v * 100).toFixed(2)}%` : '—'
}
function fmtNum(v: number | null, digits = 2) {
  return typeof v === 'number' ? v.toFixed(digits) : '—'
}
function bandLabel(bandId: number) {
  return `M${String(bandId).padStart(2, '0')}`
}
function configLabel(r: FrameworkRunSummary) {
  const parts = [
    r.top_n != null ? `top${r.top_n}` : null,
    r.lookback_months != null ? `${r.lookback_months}mo` : null,
    r.rebalance_cadence_days != null ? `${r.rebalance_cadence_days}d` : null,
    r.position_sizing ?? null,
  ].filter(Boolean)
  return parts.join(' · ')
}

const TOP_N_PER_BAND = 3

export function MomentumBandStrategyRankingPage() {
  const { data, isLoading } = useQuery({
    queryKey: ['framework-campaign-runs', 'band-ranking'],
    queryFn: () => listFrameworkRuns({ limit: 5000 }),
    refetchInterval: 60_000,
  })

  const runs = useMemo(() => data?.runs ?? [], [data?.runs])

  const byBand = useMemo(() => {
    const groups = new Map<number, FrameworkRunSummary[]>()
    for (const r of runs) {
      if (typeof r.cagr !== 'number') continue // no metric yet — not rankable
      const list = groups.get(r.band_id) ?? []
      list.push(r)
      groups.set(r.band_id, list)
    }
    for (const list of groups.values()) {
      list.sort((a, b) => (b.cagr ?? -Infinity) - (a.cagr ?? -Infinity))
    }
    return [...groups.entries()].sort(([a], [b]) => a - b)
  }, [runs])

  return (
    <div className="space-y-4 p-4">
      <div>
        <h1 className="text-3xl font-bold mb-2">Band Strategy Ranking</h1>
        <p className="text-gray-600">
          For every market-cap band, the top {TOP_N_PER_BAND} (strategy, config) results by CAGR from the
          native-engine campaign — a direct deployment shortlist, not a static report embed. Updates as the
          campaign completes more jobs.
        </p>
      </div>

      {isLoading ? (
        <p className="text-sm text-muted-foreground">Loading…</p>
      ) : byBand.length === 0 ? (
        <p className="text-sm text-muted-foreground">No results with metrics yet.</p>
      ) : (
        byBand.map(([bandId, bandRuns]) => (
          <Card key={bandId}>
            <CardHeader>
              <CardTitle>
                {bandLabel(bandId)} <span className="text-sm font-normal text-muted-foreground">({bandRuns.length} results so far)</span>
              </CardTitle>
            </CardHeader>
            <CardContent>
              <div className="grid gap-3 md:grid-cols-3">
                {bandRuns.slice(0, TOP_N_PER_BAND).map((r, i) => (
                  <div key={r.run_id} className="rounded-[var(--radius-token)] border border-border p-3">
                    <div className="flex items-center justify-between">
                      <Badge variant={i === 0 ? 'default' : 'outline'}>#{i + 1}</Badge>
                      <span className="font-mono-data text-xs text-muted-foreground">{r.strategy_code}</span>
                    </div>
                    <div className="mt-2 text-2xl font-bold">{fmtPct(r.cagr)}</div>
                    <div className="text-xs text-muted-foreground">Sharpe {fmtNum(r.sharpe_ratio)} · {r.trade_count} trades</div>
                    <div className="mt-1 text-xs text-muted-foreground">{configLabel(r)}</div>
                  </div>
                ))}
              </div>
            </CardContent>
          </Card>
        ))
      )}
    </div>
  )
}
