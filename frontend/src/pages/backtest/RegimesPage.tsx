import { useState } from 'react'
import { useQuery } from '@tanstack/react-query'

import { cn } from '@/lib/utils'

import { AppShell, Card, CardContent, CardDescription, CardHeader, CardTitle } from '@/lib/ui'
import { getMarketRegimes } from '@/shared/api/backtest'

const REGIME_COLOR: Record<string, string> = {
  bull: 'bg-green/70',
  bear: 'bg-red/70',
  sideways: 'bg-muted-foreground/40',
}

// Four thresholds the classifier can be run at (systems/regime/
// market_regime.py's threshold_pct param) — 20% is the original/default
// method; 15/10/5% were added so strategies with regime-conditional exits
// can visually compare how sensitive the Bull/Bear/Sideways segmentation
// is to the threshold choice before picking which one (if any) drives
// exit logic. Order here is the stacking order top-to-bottom.
const REGIME_THRESHOLDS = [
  { pct: 20, method: '20pct_threshold_v1', label: '20% threshold (default)' },
  { pct: 15, method: '15pct_threshold_v1', label: '15% threshold' },
  { pct: 10, method: '10pct_threshold_v1', label: '10% threshold' },
  { pct: 5, method: '5pct_threshold_v1', label: '5% threshold' },
] as const

// Shared bar renderer for one threshold's timeline — extracted so the 4
// stacked rows (one per threshold) all render identically; only the
// fetched segments differ.
function RegimeBar({ label, method }: { label: string; method: string }) {
  const regimes = useQuery({
    queryKey: ['market-regimes', 'Nifty 500', method],
    queryFn: () => getMarketRegimes('Nifty 500', method),
  })

  const segments = regimes.data?.segments ?? []
  if (regimes.isLoading) {
    return <div className="text-xs text-muted-foreground">Loading {label}…</div>
  }
  if (!segments.length) {
    return (
      <div className="text-xs text-muted-foreground">
        {label}: no segments backfilled yet (run scripts/backfill_market_regimes.py)
      </div>
    )
  }

  const start = new Date(segments[0].start_date).getTime()
  const end = new Date(segments[segments.length - 1].end_date).getTime()
  const totalMs = Math.max(end - start, 1)

  return (
    <div>
      <div className="mb-1 flex items-center justify-between text-xs font-medium text-foreground">
        <span>{label}</span>
        <span className="text-muted-foreground">
          {segments.length} segment{segments.length === 1 ? '' : 's'}
        </span>
      </div>
      <div className="flex h-8 w-full overflow-hidden rounded-[var(--radius-token)] border border-border">
        {segments.map((s, i) => {
          const segStart = new Date(s.start_date).getTime()
          const segEnd = new Date(s.end_date).getTime()
          const widthPct = (Math.max(segEnd - segStart, 1) / totalMs) * 100
          return (
            <div
              key={i}
              title={`${s.regime} · ${s.start_date} → ${s.end_date}${s.move_pct != null ? ` · ${(s.move_pct * 100).toFixed(1)}%` : ''}`}
              className={cn('h-full border-r border-border/50 last:border-r-0', REGIME_COLOR[s.regime] ?? 'bg-muted')}
              style={{ width: `${widthPct}%` }}
            />
          )
        })}
      </div>
      <div className="mt-1 text-[11px] text-muted-foreground">
        {segments[0].start_date} → {segments[segments.length - 1].end_date}
      </div>
    </div>
  )
}

export function RegimesPage() {
  // All four thresholds visible by default (checked=visible) — the whole
  // point of this comparison view is seeing all of them side by side; the
  // checkboxes let the user narrow in on one or two once they've formed an
  // opinion, rather than having to opt each one in first.
  const [visible, setVisible] = useState<Record<string, boolean>>(() =>
    Object.fromEntries(REGIME_THRESHOLDS.map((t) => [t.method, true])),
  )

  return (
    <AppShell
      title="Market Regimes"
      description="Bull/Bear/Sideways market regime timeline (Nifty 500), moved off the Backtest page — this view doesn't need to reload every time the Runs table does."
    >
      <Card>
        <CardHeader>
          <CardTitle>Market Regime Timeline (Nifty 500)</CardTitle>
          <CardDescription>
            Bull/Bear/Sideways date-range segments (rule-based — see the Backtest Explainer), one row per Bull/Bear
            confirmation threshold. Hover a band for exact dates. Compare how sensitive the classification is to the
            threshold before choosing one for regime-conditional exit logic. The trailing segment on each row is
            provisional and may still extend or reclassify.
          </CardDescription>
        </CardHeader>
        <CardContent className="space-y-4">
          <div className="flex flex-wrap items-center gap-4 text-xs text-muted-foreground">
            <span className="flex items-center gap-1.5">
              <span className="size-2.5 rounded-full bg-green/70" /> Bull
            </span>
            <span className="flex items-center gap-1.5">
              <span className="size-2.5 rounded-full bg-red/70" /> Bear
            </span>
            <span className="flex items-center gap-1.5">
              <span className="size-2.5 rounded-full bg-muted-foreground/40" /> Sideways
            </span>
          </div>
          <div className="flex flex-wrap items-center gap-4 border-b border-border pb-3 text-xs">
            {REGIME_THRESHOLDS.map((t) => (
              <label key={t.method} className="flex cursor-pointer items-center gap-1.5">
                <input
                  type="checkbox"
                  checked={visible[t.method] ?? true}
                  onChange={(e) => setVisible((prev) => ({ ...prev, [t.method]: e.target.checked }))}
                />
                {t.label}
              </label>
            ))}
          </div>
          <div className="space-y-4">
            {REGIME_THRESHOLDS.filter((t) => visible[t.method]).map((t) => (
              <RegimeBar key={t.method} label={t.label} method={t.method} />
            ))}
            {REGIME_THRESHOLDS.every((t) => !visible[t.method]) && (
              <div className="text-xs text-muted-foreground">
                All thresholds hidden — check a box above to show its timeline.
              </div>
            )}
          </div>
        </CardContent>
      </Card>
    </AppShell>
  )
}
