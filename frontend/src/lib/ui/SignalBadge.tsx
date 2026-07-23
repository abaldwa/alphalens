import { Badge, type badgeVariants } from '@/lib/ui/primitives/badge'
import type { VariantProps } from 'class-variance-authority'

type BadgeVariant = VariantProps<typeof badgeVariants>['variant']

/**
 * Single source of truth for mapping a buy/hold/sell-shaped direction
 * string onto a Badge variant. Previously redefined ad hoc per page
 * (e.g. `directionVariant()` in ml/signal.tsx) — every module should use
 * this instead so the buy=green/sell=red/hold=amber convention can't drift.
 */
export function getSignalVariant(direction: string | null | undefined): BadgeVariant {
  const d = direction?.toLowerCase()
  if (d === 'sell' || d === 'red' || d === 'black') return 'destructive'
  if (d === 'buy' || d === 'green') return 'success'
  if (d === 'hold' || d === 'amber' || d === 'yellow' || d === 'orange') return 'warning'
  return 'outline'
}

export function SignalBadge({ direction, label }: { direction: string | null | undefined; label?: string }) {
  return <Badge variant={getSignalVariant(direction)}>{label ?? direction ?? '—'}</Badge>
}
