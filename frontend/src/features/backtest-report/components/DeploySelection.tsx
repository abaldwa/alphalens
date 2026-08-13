/**
 * features/backtest-report/components/DeploySelection.tsx
 *
 * The sticky "N selected → Review & Deploy" bar that carries the deploy
 * selection to the deploy page. The checkbox column lives in
 * ../deploy/deployColumn.tsx.
 */

import { Link } from 'react-router-dom'

import { Button } from '@/lib/ui'

import { prefillParam, useDeploySelection } from '../deploy/useDeploySelection'

export function DeploySelectionBar() {
  const { selected, count, clear } = useDeploySelection()
  if (count === 0) return null

  return (
    <div className="sticky bottom-0 z-20 mt-4 flex flex-wrap items-center justify-between gap-3 border-t border-border bg-card px-4 py-3 shadow-lg">
      <span className="text-sm">
        {count} strateg{count === 1 ? 'y' : 'ies'} selected for deployment
      </span>
      <span className="flex items-center gap-2">
        <Button variant="ghost" size="sm" onClick={clear}>
          Clear
        </Button>
        <Button asChild size="sm">
          <Link to={`/momentum-deploy?prefill=${encodeURIComponent(prefillParam(selected))}`}>
            Review &amp; Deploy
          </Link>
        </Button>
      </span>
    </div>
  )
}
