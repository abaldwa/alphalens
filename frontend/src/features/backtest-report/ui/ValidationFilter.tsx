/**
 * ValidationFilter.tsx
 *
 * Filter controls for backtest results by validation status.
 * Allows users to view only valid results, or include alternative periods,
 * flagged (data gaps), or invalid results for analysis.
 */

import { useState } from 'react'
import { Badge } from '@/lib/ui'

type ValidationStatus = 'valid' | 'alternative_period' | 'flagged' | 'invalid'

interface ValidationFilterState {
  valid: boolean
  alternative_period: boolean
  flagged: boolean
  invalid: boolean
}

interface ValidationFilterProps {
  value: ValidationFilterState
  onChange: (filter: ValidationFilterState) => void
  counts?: Record<ValidationStatus, number>
  showCounts?: boolean
}

export function ValidationFilter({ value, onChange, counts, showCounts = true }: ValidationFilterProps) {
  const filters: Array<{
    key: keyof ValidationFilterState
    label: string
    icon: string
    description: string
  }> = [
    {
      key: 'valid',
      label: 'Valid',
      icon: '✅',
      description: 'Standard 2009-2026 period',
    },
    {
      key: 'alternative_period',
      label: 'Alternative Period',
      icon: '🟡',
      description: 'Other substantial periods (>1 year)',
    },
    {
      key: 'flagged',
      label: 'Data Gaps',
      icon: '⚠️',
      description: 'Valid but with data gaps',
    },
    {
      key: 'invalid',
      label: 'Invalid',
      icon: '❌',
      description: 'Leverage/short period/missing metrics',
    },
  ]

  const handleToggle = (key: keyof ValidationFilterState) => {
    onChange({
      ...value,
      [key]: !value[key],
    })
  }

  const activeCount = Object.values(value).filter(Boolean).length
  const showAll = activeCount === 4
  const showNone = activeCount === 0

  return (
    <div className="space-y-2">
      <div className="flex items-center justify-between">
        <label className="text-sm font-medium">Validation Status</label>
        {showNone && <span className="text-xs text-red-600">No filters selected</span>}
        {!showAll && !showNone && <span className="text-xs text-blue-600">{activeCount} selected</span>}
      </div>

      <div className="flex flex-wrap gap-2">
        {filters.map(({ key, label, icon, description }) => (
          <button
            key={key}
            type="button"
            onClick={() => handleToggle(key)}
            className="group relative"
            title={description}
          >
            <Badge
              variant={value[key] ? 'default' : 'outline'}
              className="cursor-pointer transition-all hover:shadow-md"
            >
              {icon} {label}
              {showCounts && counts && <span className="ml-1 text-xs opacity-75">({counts[key] || 0})</span>}
            </Badge>
            <div className="hidden group-hover:block absolute bottom-full left-0 mb-2 p-2 bg-gray-900 text-white text-xs rounded shadow-lg whitespace-nowrap z-10 pointer-events-none">
              {description}
            </div>
          </button>
        ))}
      </div>

      {/* Quick presets */}
      <div className="flex gap-2 pt-2 border-t">
        <button
          type="button"
          onClick={() => onChange({ valid: true, alternative_period: false, flagged: false, invalid: false })}
          className="text-xs px-2 py-1 rounded bg-green-50 text-green-700 hover:bg-green-100 transition-colors"
        >
          Production Only
        </button>
        <button
          type="button"
          onClick={() => onChange({ valid: true, alternative_period: true, flagged: true, invalid: false })}
          className="text-xs px-2 py-1 rounded bg-blue-50 text-blue-700 hover:bg-blue-100 transition-colors"
        >
          Analysis View
        </button>
        <button
          type="button"
          onClick={() => onChange({ valid: true, alternative_period: true, flagged: true, invalid: true })}
          className="text-xs px-2 py-1 rounded bg-gray-50 text-gray-700 hover:bg-gray-100 transition-colors"
        >
          Show All
        </button>
      </div>
    </div>
  )
}

/**
 * Hook to manage validation filter state
 */
export function useValidationFilter(initialShowInvalid = false) {
  const [filter, setFilter] = useState<ValidationFilterState>({
    valid: true,
    alternative_period: true,
    flagged: true,
    invalid: initialShowInvalid,
  })

  return { filter, setFilter }
}

/**
 * Utility to check if a result should be shown based on filter
 */
export function matchesValidationFilter(status: ValidationStatus, filter: ValidationFilterState): boolean {
  return filter[status] ?? false
}

/**
 * Utility to get active filter description
 */
export function getFilterDescription(filter: ValidationFilterState): string {
  const active = Object.entries(filter)
    .filter(([_, enabled]) => enabled)
    .map(([status, _]) => status)

  if (active.length === 0) return 'No filters selected'
  if (active.length === 4) return 'Showing all results'

  const labels: Record<string, string> = {
    valid: 'Valid',
    alternative_period: 'Alternative Period',
    flagged: 'Data Gaps',
    invalid: 'Invalid',
  }

  return `Showing: ${active.map((s) => labels[s]).join(', ')}`
}
