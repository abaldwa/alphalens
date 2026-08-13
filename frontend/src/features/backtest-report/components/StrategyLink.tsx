/**
 * features/backtest-report/components/StrategyLink.tsx
 *
 * Every cell that names a strategy renders through this. Two reasons it is a
 * component rather than an inline <Link>:
 *
 * - The label comes from displayLabel(), so the same strategy reads identically
 *   on the matrix, the sweep, the recommendations and the detail page. The
 *   current mismatch (STRATEGY_LABELS[v.strategy] on the hub vs rowLabel() on
 *   the matrix) is exactly what this removes.
 * - Section-to-section links carry the selected strategy in the URL, so the
 *   chain the report is meant to support stays navigable with the browser's
 *   own back button and is shareable as a link.
 */

import { Link } from 'react-router-dom'

import { cn } from '@/lib/utils'

import { sectionUrl, strategyDetailUrl, type ReportSection } from '../strategyKey'
import type { StrategyKey } from '../types'

export interface StrategyLinkProps {
  strategyKey: StrategyKey
  label: string
  /** Target section. Omitted means the single-strategy detail page. */
  section?: string
  className?: string
}

export function StrategyLink({
  strategyKey,
  label,
  section,
  className,
}: StrategyLinkProps) {
  const to =
    section === undefined
      ? strategyDetailUrl(strategyKey)
      : sectionUrl(section as ReportSection, { strategy: strategyKey })

  return (
    <Link
      to={to}
      className={cn(
        'text-primary underline-offset-2 hover:underline focus-visible:outline focus-visible:outline-2 focus-visible:outline-offset-2 focus-visible:outline-ring',
        className,
      )}
    >
      {label}
    </Link>
  )
}
