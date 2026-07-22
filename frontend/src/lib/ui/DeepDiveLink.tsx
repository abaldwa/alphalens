import { Microscope } from 'lucide-react'

export type DeepDivePillar = 'technical' | 'fundamental' | 'valuation' | 'momentum' | 'ml'

const DEEP_DIVE_ROUTE: Record<DeepDivePillar, string | null> = {
  technical: '/technical-deep_dive',
  fundamental: '/fundamental-deep_dive',
  valuation: null,
  momentum: null,
  ml: null,
}

/**
 * Framework-level convention: the microscope icon, wherever it appears,
 * routes to the deep-dive page for the pillar the user is currently in
 * (Technical page → Technical deep dive, Fundamental page → Fundamental
 * deep dive). Same-window navigation — deep dive is page context, not a
 * chart popout (compare TickerLink, which opens a new tab). Renders
 * nothing if the pillar has no deep-dive page yet.
 */
export function DeepDiveLink({ pillar, ticker }: { pillar: DeepDivePillar | undefined; ticker: string | null | undefined }) {
  const route = pillar ? DEEP_DIVE_ROUTE[pillar] : null
  if (!route || !ticker) return null

  return (
    <a
      href={`${route}?ticker=${encodeURIComponent(ticker)}`}
      title="Deep Dive"
      className="text-muted-foreground hover:text-primary"
    >
      <Microscope className="h-3.5 w-3.5" />
    </a>
  )
}
