/**
 * ValidationDetails.tsx
 *
 * Detailed validation information panel for a backtest result.
 * Shows validation status, reasons, execution timestamp, and recommendations.
 */

import { format } from 'date-fns'
import { Card, CardContent, CardHeader, CardTitle } from '@/lib/ui'
import { ValidationBadge } from './ValidationBadge'

type ValidationStatus = 'valid' | 'alternative_period' | 'flagged' | 'invalid'

interface ValidationDetailsProps {
  isValid: boolean
  validationStatus: ValidationStatus
  markedInvalidReason?: string | null
  runExecutedAt?: string | null
}

export function ValidationDetails({
  isValid,
  validationStatus,
  markedInvalidReason,
  runExecutedAt,
}: ValidationDetailsProps) {
  const executedDate = runExecutedAt ? new Date(runExecutedAt) : null

  return (
    <Card>
      <CardHeader>
        <div className="flex items-center justify-between">
          <CardTitle className="text-sm">Validation Status</CardTitle>
          <ValidationBadge status={validationStatus} reason={markedInvalidReason} />
        </div>
      </CardHeader>

      <CardContent className="space-y-3">
        {/* Status Description */}
        <div className="text-sm text-gray-700 space-y-1">
          {getStatusDescription(validationStatus, markedInvalidReason)}
        </div>

        {/* Execution Timestamp */}
        {executedDate && (
          <div className="text-xs text-gray-600 border-t pt-2">
            <p className="font-medium">Backtest Executed</p>
            <p>{format(executedDate, 'PPpp')}</p>
          </div>
        )}

        {/* Recommendations */}
        <div className="border-t pt-2">
          <p className="text-xs font-medium text-gray-700 mb-1">Recommendation</p>
          <p className="text-xs text-gray-600">{getRecommendation(validationStatus, isValid)}</p>
        </div>
      </CardContent>
    </Card>
  )
}

function getStatusDescription(status: ValidationStatus, reason?: string | null): React.ReactNode {
  switch (status) {
    case 'valid':
      return (
        <>
          <p className="font-medium text-green-900">✅ Valid Result</p>
          <p>Clean backtest using standard 2009-04-01 to 2026-06+ period.</p>
          <p className="text-xs">Ready for production analysis and deployment decisions.</p>
        </>
      )

    case 'alternative_period':
      return (
        <>
          <p className="font-medium text-amber-900">🟡 Alternative Period</p>
          <p>Substantial backtest (&gt;1 year) but using non-standard timing.</p>
          <p className="text-xs">Example: 2010-2025, 2019-2025, etc.</p>
          <p className="text-xs font-medium mt-1">✓ Useful for comparison analysis</p>
        </>
      )

    case 'flagged':
      return (
        <>
          <p className="font-medium text-yellow-900">⚠️ Data Gaps Detected</p>
          <p>Result contains significant data gaps (&gt;50 gaps in period).</p>
          <p className="text-xs">Common in smaller-cap bands with limited trading history.</p>
          <p className="text-xs font-medium mt-1">✓ Valid but use for trend analysis, not exact metrics</p>
        </>
      )

    case 'invalid':
      return (
        <>
          <p className="font-medium text-red-900">❌ Invalid Result</p>
          {reason && <p className="text-xs">{reason}</p>}
          <p className="text-xs font-medium mt-1">✗ Do NOT use for analysis or decisions</p>
        </>
      )

    default:
      return <p className="text-gray-600">Unknown validation status</p>
  }
}

function getRecommendation(status: ValidationStatus, isValid: boolean): string {
  if (!isValid) {
    return 'Skip this result. Use only for reference to understand why this configuration does not work.'
  }

  switch (status) {
    case 'valid':
      return 'Use for all decision-making. This result meets production standards.'

    case 'alternative_period':
      return 'Use for performance comparison. Note the different time period in your analysis.'

    case 'flagged':
      return 'Use for identifying trends, but do not rely on exact CAGR/Sharpe figures due to data gaps.'

    default:
      return 'Consult documentation for usage guidelines.'
  }
}

/**
 * Compact validation info for inline display
 */
export function ValidationInfo({ status, reason, timestamp }: { status: ValidationStatus; reason?: string | null; timestamp?: string | null }) {
  return (
    <div className="flex items-center gap-2 text-xs text-gray-600">
      <ValidationBadge status={status} reason={reason} className="text-xs" />
      {timestamp && <span>• {new Date(timestamp).toLocaleDateString()}</span>}
    </div>
  )
}

/**
 * Validation warning banner for invalid results
 */
export function ValidationWarning({ status, reason, isVisible = true }: { status: ValidationStatus; reason?: string | null; isVisible?: boolean }) {
  if (!isVisible || status !== 'invalid') return null

  return (
    <div className="mb-4 p-3 bg-red-50 border border-red-200 rounded">
      <p className="text-xs font-medium text-red-900 mb-1">⚠️ Invalid Result</p>
      {reason && <p className="text-xs text-red-800">{reason}</p>}
      <p className="text-xs text-red-700 mt-1">
        This result does not meet quality standards and should not be used for analysis or strategy selection.
      </p>
    </div>
  )
}
