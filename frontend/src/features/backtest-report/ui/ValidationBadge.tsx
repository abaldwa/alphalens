import { cn } from '@/lib/utils'

export type ValidationStatus = 'valid' | 'alternative_period' | 'flagged' | 'invalid'

interface ValidationBadgeProps {
  status: ValidationStatus
  reason?: string | null
  className?: string
}

export function ValidationBadge({ status, reason, className }: ValidationBadgeProps) {
  const config = getStatusConfig(status)

  return (
    <div className={cn('group relative inline-block', className)}>
      <div
        className={cn(
          'inline-flex items-center gap-1 px-2 py-1 rounded text-xs font-medium transition-colors',
          config.badgeClass
        )}
        title={config.tooltip}
      >
        <span className="text-sm">{config.icon}</span>
        <span>{config.label}</span>
      </div>

      {reason && (
        <div className="hidden group-hover:block absolute bottom-full left-0 mb-2 p-2 bg-gray-900 text-white text-xs rounded shadow-lg whitespace-nowrap z-10 pointer-events-none">
          {reason}
        </div>
      )}
    </div>
  )
}

function getStatusConfig(status: ValidationStatus) {
  switch (status) {
    case 'valid':
      return {
        icon: '✅',
        label: 'Valid',
        tooltip: 'Clean result (2009-2026 standard period)',
        badgeClass: 'bg-green-100 text-green-900 hover:bg-green-200',
      }

    case 'alternative_period':
      return {
        icon: '🟡',
        label: 'Alternative Period',
        tooltip: 'Substantial period (>1 year) but non-standard timing',
        badgeClass: 'bg-amber-100 text-amber-900 hover:bg-amber-200',
      }

    case 'flagged':
      return {
        icon: '⚠️',
        label: 'Data Gaps',
        tooltip: 'Valid result but contains data gaps (common in smaller caps)',
        badgeClass: 'bg-yellow-100 text-yellow-900 hover:bg-yellow-200',
      }

    case 'invalid':
      return {
        icon: '❌',
        label: 'Invalid',
        tooltip: 'Do not use (leverage used, very short period, or missing metrics)',
        badgeClass: 'bg-red-100 text-red-900 hover:bg-red-200 line-through opacity-50',
      }

    default:
      return {
        icon: '?',
        label: 'Unknown',
        tooltip: 'Unknown validation status',
        badgeClass: 'bg-gray-100 text-gray-900',
      }
  }
}

export function ValidationStatusCell({ status, reason }: { status: ValidationStatus; reason?: string | null }) {
  return <ValidationBadge status={status} reason={reason} />
}

export function ValidationStatusIcon({ status }: { status: ValidationStatus }) {
  const config = getStatusConfig(status)
  return <span title={config.tooltip}>{config.icon}</span>
}
