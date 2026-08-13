import { Link } from 'react-router-dom'

// Back-to-hub link shown at the top of every split-out section page, plus a
// pointer to the unified cross-channel report. These momentum-only pages keep
// detail the unified section does not carry yet (per-FY benchmark comparison,
// the income-mode breakdown), so they are linked from it rather than replaced
// by it — see A83/A88.
export function BackToReportLink({ unifiedSection }: { unifiedSection?: string } = {}) {
  return (
    <div className="mb-4 flex flex-wrap gap-4 text-sm">
      <Link to="/momentum-dynamic-report" className="text-primary underline">
        ← Back to Strategy Report
      </Link>
      <Link
        to={`/backtest-report/${unifiedSection ?? ''}?channel=momentum`}
        className="text-primary underline"
      >
        Compare across all channels in the Backtest Report →
      </Link>
    </div>
  )
}
