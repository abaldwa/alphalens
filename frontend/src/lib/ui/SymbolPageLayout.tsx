import type { ReactNode } from 'react'

import { Card, CardContent, CardHeader, CardTitle } from '@/lib/ui/primitives/card'
import { SignalBadge } from '@/lib/ui/SignalBadge'

/**
 * Compound layout enforcing the mandated per-symbol visual hierarchy:
 * Signal (upfront verdict) → Rationale (thesis) → Proof (Confidence
 * Matrix) → Validation (backtest results). Any future per-symbol page
 * should compose this instead of hand-rolling its own section order.
 */
function SymbolPageLayout({ children }: { children: ReactNode }) {
  return <div className="flex flex-col gap-6">{children}</div>
}

function Signal({
  ticker,
  direction,
  currentPrice,
  targetPrice,
  chart,
}: {
  ticker: string
  direction: string | null
  currentPrice: number | null
  targetPrice: number | null
  /** Persistent chart widget, mounted by the caller at the route level
   * (see SymbolOverviewPage) so it isn't torn down by re-renders here. */
  chart?: ReactNode
}) {
  return (
    <Card>
      <CardHeader className="flex flex-row flex-wrap items-center justify-between gap-3">
        <CardTitle className="text-xl">{ticker}</CardTitle>
        <SignalBadge direction={direction} label={direction ? direction.toUpperCase() : 'NO SIGNAL'} />
      </CardHeader>
      <CardContent className="flex flex-col gap-4">
        <div className="flex flex-wrap gap-8">
          <div>
            <div className="text-xs uppercase tracking-wide text-muted-foreground">Current Price</div>
            <div className="font-mono-data text-2xl font-semibold">
              {currentPrice != null ? `₹${currentPrice.toLocaleString('en-IN')}` : '—'}
            </div>
          </div>
          <div>
            <div className="text-xs uppercase tracking-wide text-muted-foreground">Target Price</div>
            <div className="font-mono-data text-2xl font-semibold">
              {targetPrice != null ? `₹${targetPrice.toLocaleString('en-IN')}` : '—'}
            </div>
          </div>
        </div>
        {chart}
      </CardContent>
    </Card>
  )
}

function Rationale({ children }: { children: ReactNode }) {
  return (
    <Card>
      <CardHeader>
        <CardTitle>Rationale</CardTitle>
      </CardHeader>
      <CardContent className="text-sm text-muted-foreground">{children}</CardContent>
    </Card>
  )
}

function Proof({ children }: { children: ReactNode }) {
  return <div>{children}</div>
}

function Validation({ children }: { children: ReactNode }) {
  return (
    <Card>
      <CardHeader>
        <CardTitle>Validation — Backtest Results</CardTitle>
      </CardHeader>
      <CardContent>{children}</CardContent>
    </Card>
  )
}

SymbolPageLayout.Signal = Signal
SymbolPageLayout.Rationale = Rationale
SymbolPageLayout.Proof = Proof
SymbolPageLayout.Validation = Validation

export { SymbolPageLayout }
