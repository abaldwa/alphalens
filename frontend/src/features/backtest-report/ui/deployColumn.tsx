/**
 * features/backtest-report/deploy/deployColumn.tsx
 *
 * The Deploy checkbox column, appended to any section's table.
 *
 * A non-momentum strategy's checkbox is DISABLED with a tooltip naming A91,
 * rather than absent or silently inert. The distinction matters: absent looks
 * like an oversight, inert looks like a bug, and disabled-with-a-reason tells
 * the user the truth — this channel has no deploy path yet.
 */

import type { ColumnDef } from '@tanstack/react-table'

import type { StrategyReport } from '../core/types'
import { deployBlockedReason, isDeployable } from '../core/toConfigForm'
import { useDeploySelection } from '../data/useDeploySelection'

/** Checkbox column, appended to any section's table. */
export function useDeployColumn(): ColumnDef<StrategyReport, unknown> {
  const { isSelected, toggle } = useDeploySelection()

  return {
    id: 'deploy',
    header: 'Deploy',
    enableSorting: false,
    size: 70,
    cell: (i) => {
      const r = i.row.original
      const blocked = deployBlockedReason(r)
      return (
        <input
          type="checkbox"
          checked={isSelected(r.key)}
          disabled={!isDeployable(r)}
          title={blocked ?? `Select ${r.label} for deployment`}
          aria-label={
            blocked ? `${r.label}: ${blocked}` : `Deploy ${r.label}`
          }
          onChange={() => toggle(r.key)}
          className="h-4 w-4 accent-[var(--color-primary)] disabled:cursor-not-allowed disabled:opacity-40"
        />
      )
    },
  }
}
