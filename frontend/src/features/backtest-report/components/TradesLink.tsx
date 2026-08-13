/**
 * features/backtest-report/components/TradesLink.tsx
 *
 * "Trades is a hyperlink on every strategy" — uniformly, across channels whose
 * trade books are served quite differently: momentum returns a CSV download,
 * the orchestrator channels return JSON from a run-scoped endpoint.
 *
 * The adapters resolve that difference into `tradeBookUrl`, so this component
 * only has to decide how to present it: a CSV endpoint is a download (which
 * must not be a react-router <Link>, or the SPA intercepts the navigation and
 * the file never arrives), anything else opens in a new tab.
 *
 * A strategy with no run has no trade book. That renders as disabled text
 * rather than a dead link, so an unrunnable strategy is visibly different from
 * one whose link merely 404s.
 */

import { cn } from '@/lib/utils'

export interface TradesLinkProps {
  url: string | null
  label?: string
  className?: string
}

export function TradesLink({ url, label = 'Trades', className }: TradesLinkProps) {
  if (!url) {
    return (
      <span
        className={cn('text-muted-foreground', className)}
        title="No trade book: this strategy has no completed run in the current report."
      >
        {label}
      </span>
    )
  }

  const isCsv = /csv|\/trades(\?|$)/.test(url)

  return (
    <a
      href={url}
      {...(isCsv
        ? { download: '' }
        : { target: '_blank', rel: 'noreferrer' })}
      className={cn(
        'text-primary underline-offset-2 hover:underline focus-visible:outline focus-visible:outline-2 focus-visible:outline-offset-2 focus-visible:outline-ring',
        className,
      )}
    >
      {label}
      {isCsv ? <span className="sr-only"> (downloads a CSV)</span> : null}
    </a>
  )
}
