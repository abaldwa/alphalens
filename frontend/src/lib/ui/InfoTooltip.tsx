import { Info } from 'lucide-react'

import { Tooltip, TooltipContent, TooltipTrigger } from '@/lib/ui/primitives/tooltip'

export interface InfoTooltipProps {
  /** Explanation text — column/metric meaning, calculation basis, etc. */
  children: React.ReactNode
  className?: string
}

/**
 * Small inline (i) icon that reveals an explanation on hover/focus —
 * the standard way to annotate a column header, StatCard label, or badge
 * whose meaning isn't self-evident (e.g. "Meta Prob", "MB Tier", a score
 * band). Assumes it renders inside AppShell, which already provides
 * TooltipProvider.
 */
export function InfoTooltip({ children, className }: InfoTooltipProps) {
  return (
    <Tooltip>
      <TooltipTrigger asChild>
        <button
          type="button"
          aria-label="More information"
          className={className ?? 'inline-flex text-muted-foreground/60 hover:text-muted-foreground'}
          onClick={(e) => e.preventDefault()}
        >
          <Info className="size-3.5" />
        </button>
      </TooltipTrigger>
      <TooltipContent>{children}</TooltipContent>
    </Tooltip>
  )
}
