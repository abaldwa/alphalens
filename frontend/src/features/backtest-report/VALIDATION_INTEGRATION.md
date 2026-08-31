# Backtest Validation Status — Frontend Integration Guide

**Date:** 2026-08-30  
**Status:** Ready for integration

---

## Overview

The API now returns validation status for each backtest result. This guide shows how to integrate validation components into the backtest report UI.

---

## Validation Status Types

| Status | Icon | Meaning | Display | Use For |
|--------|------|---------|---------|---------|
| **valid** | ✅ | Standard 2009-2026 period | Green badge | Production decisions |
| **alternative_period** | 🟡 | Other substantial periods (>1 year) | Amber badge | Comparison analysis |
| **flagged** | ⚠️ | Data gaps detected | Yellow badge | Trend analysis |
| **invalid** | ❌ | Leverage/short period/missing metrics | Red badge, grayed out | Reference only |

---

## Available Components

### 1. ValidationBadge

Displays validation status with hover tooltip.

```typescript
import { ValidationBadge } from '@/features/backtest-report/ui/ValidationBadge'

// Basic usage
<ValidationBadge status="valid" />

// With reason
<ValidationBadge 
  status="invalid" 
  reason="Leverage used (position sizing > 1.0x)" 
/>

// In table cells
<ValidationBadge status={run.validation_status} reason={run.marked_invalid_reason} />
```

### 2. ValidationFilter

Filter controls with presets for common views.

```typescript
import { ValidationFilter, useValidationFilter } from '@/features/backtest-report/ui/ValidationFilter'

export function BacktestRunsTable() {
  const { filter, setFilter } = useValidationFilter()

  const filteredRuns = runs.filter(run => 
    matchesValidationFilter(run.validation_status, filter)
  )

  return (
    <>
      <ValidationFilter 
        value={filter} 
        onChange={setFilter}
        counts={{
          valid: 2002,
          alternative_period: 138,
          flagged: 1535,
          invalid: 103,
        }}
      />
      
      {/* Render filtered results */}
      {filteredRuns.map(run => (
        <RunRow key={run.run_id} run={run} />
      ))}
    </>
  )
}
```

### 3. ValidationDetails

Detailed panel showing validation info for a single result.

```typescript
import { ValidationDetails } from '@/features/backtest-report/ui/ValidationDetails'

export function BacktestRunDetail({ run }) {
  return (
    <div className="grid grid-cols-3 gap-4">
      <div>
        {/* Other metrics... */}
      </div>
      <div>
        <ValidationDetails
          isValid={run.is_valid}
          validationStatus={run.validation_status}
          markedInvalidReason={run.marked_invalid_reason}
          runExecutedAt={run.run_executed_at}
        />
      </div>
    </div>
  )
}
```

### 4. ValidationWarning

Banner warning for invalid results.

```typescript
import { ValidationWarning } from '@/features/backtest-report/ui/ValidationDetails'

export function BacktestRunDetail({ run }) {
  return (
    <>
      <ValidationWarning 
        status={run.validation_status}
        reason={run.marked_invalid_reason}
        isVisible={!run.is_valid}
      />
      
      {/* Rest of details... */}
    </>
  )
}
```

---

## Integration Examples

### Example 1: Add Badge to Runs Table

**File:** `src/features/backtest-report/ui/gridColumns.tsx`

```typescript
import { ValidationBadge } from './ValidationBadge'
import { ColumnDef } from '@tanstack/react-table'

export const validationColumn: ColumnDef<BacktestRun> = {
  accessorKey: 'validation_status',
  header: 'Status',
  size: 120,
  cell: (info) => (
    <ValidationBadge 
      status={info.row.original.validation_status as ValidationStatus}
      reason={info.row.original.marked_invalid_reason}
    />
  ),
}
```

Add to column definitions:
```typescript
const columns = [
  strategyColumn,
  cagrColumn,
  sharpeColumn,
  validationColumn,  // ← Add here
  // ... other columns
]
```

### Example 2: Add Filter to Hub Page

**File:** `src/pages/backtest-report/hub.tsx`

```typescript
import { ValidationFilter, useValidationFilter, matchesValidationFilter } from '@/features/backtest-report/ui/ValidationFilter'

export function BacktestReportHubPage() {
  const page = useReportPage()
  const { filter, setFilter } = useValidationFilter()

  // Calculate filter counts
  const counts = {
    valid: page.strategies.filter(s => s.validation_status === 'valid').length,
    alternative_period: page.strategies.filter(s => s.validation_status === 'alternative_period').length,
    flagged: page.strategies.filter(s => s.validation_status === 'flagged').length,
    invalid: page.strategies.filter(s => !s.is_valid).length,
  }

  // Filter strategies
  const filteredStrategies = page.strategies.filter(s => 
    matchesValidationFilter(s.validation_status, filter)
  )

  return (
    <ReportLayout title="Backtest Report">
      <ValidationFilter 
        value={filter}
        onChange={setFilter}
        counts={counts}
      />

      {/* Show counts */}
      <div className="mb-4 text-sm text-gray-600">
        Showing {filteredStrategies.length} of {page.strategies.length} results
      </div>

      {/* Render recommendations, tables, etc. */}
      {/* ... */}
    </ReportLayout>
  )
}
```

### Example 3: Show Validation Info in Details Page

**File:** `src/pages/backtest-report/strategy-detail.tsx`

```typescript
import { ValidationDetails, ValidationWarning } from '@/features/backtest-report/ui/ValidationDetails'

export function StrategyDetailPage() {
  const run = useQuery(...)

  return (
    <div className="space-y-6">
      <ValidationWarning 
        status={run.validation_status}
        reason={run.marked_invalid_reason}
      />

      <div className="grid grid-cols-2 gap-6">
        <div>
          {/* Equity curve, performance metrics, etc. */}
        </div>
        <div>
          <ValidationDetails
            isValid={run.is_valid}
            validationStatus={run.validation_status}
            markedInvalidReason={run.marked_invalid_reason}
            runExecutedAt={run.run_executed_at}
          />
        </div>
      </div>
    </div>
  )
}
```

---

## Data Model

The API returns these fields in `BacktestRunSummary`:

```typescript
interface BacktestRunSummary {
  run_id: string
  strategy_id: string
  created_at: string
  
  // Validation fields (NEW)
  is_valid: boolean
  validation_status: 'valid' | 'alternative_period' | 'flagged' | 'invalid'
  marked_invalid_reason?: string
  run_executed_at?: string
  
  // ... existing fields
  metrics: BacktestRunMetrics
  integrity_passed?: boolean
  // etc.
}
```

---

## Styling Guidance

Use Tailwind utilities for consistency:

```typescript
// Valid (green)
bg-green-100 text-green-900

// Alternative period (amber)
bg-amber-100 text-amber-900

// Flagged (yellow)
bg-yellow-100 text-yellow-900

// Invalid (red, grayed)
bg-red-100 text-red-900 opacity-50 line-through
```

---

## Common Use Cases

### Show Only Production-Ready Results
```typescript
const validOnly = runs.filter(r => r.validation_status === 'valid')
```

### Show Valid + Alternative (for analysis)
```typescript
const analysisReady = runs.filter(r => 
  r.validation_status === 'valid' || r.validation_status === 'alternative_period'
)
```

### Highlight Results with Issues
```typescript
const problemResults = runs.filter(r => 
  r.validation_status === 'flagged' || !r.is_valid
)
```

### Get Timestamp of Execution
```typescript
const executedDate = new Date(run.run_executed_at)
```

---

## Testing

### Mock Data
```typescript
const mockValidRun = {
  run_id: 'run_123',
  validation_status: 'valid',
  marked_invalid_reason: null,
  run_executed_at: '2026-08-30T12:34:56',
  is_valid: true,
}

const mockInvalidRun = {
  run_id: 'run_456',
  validation_status: 'invalid',
  marked_invalid_reason: 'Leverage used (position sizing > 1.0x)',
  run_executed_at: '2026-08-28T10:00:00',
  is_valid: false,
}
```

### Component Tests
```typescript
import { render, screen } from '@testing-library/react'
import { ValidationBadge } from './ValidationBadge'

test('displays valid badge with correct styling', () => {
  render(<ValidationBadge status="valid" />)
  expect(screen.getByText('Valid')).toHaveClass('bg-green-100')
})

test('displays invalid badge with strikethrough', () => {
  render(<ValidationBadge status="invalid" />)
  expect(screen.getByText('Invalid')).toHaveClass('line-through', 'opacity-50')
})
```

---

## Migration Path

### Phase 1: Add Components (Now)
- Copy ValidationBadge, ValidationFilter, ValidationDetails to your project
- Update API type definitions to include validation fields

### Phase 2: Integrate into Runs Table (This Week)
- Add validation column to grid
- Add filter controls above table
- Test with mock data

### Phase 3: Add to Detail Pages (Next Week)
- Show ValidationDetails panel on strategy page
- Add ValidationWarning for invalid results

### Phase 4: Polish & Optimize (Following Week)
- Add sorting by validation status
- Export/download filtering options
- Analytics dashboard of validation stats

---

## Troubleshooting

**Q: How do I see validation fields in API response?**  
A: Run: `curl http://localhost:8123/api/v1/backtest/runs?limit=1` and check for `is_valid`, `validation_status`, `run_executed_at`

**Q: Component not showing up?**  
A: Make sure you've imported from `@/features/backtest-report/ui/ValidationBadge` (or other component)

**Q: How do I filter results in a table?**  
A: Use the `useValidationFilter` hook + `matchesValidationFilter` utility function

**Q: Can I customize badge colors?**  
A: Yes, update `getStatusConfig()` function in ValidationBadge.tsx

---

## Summary

- ✅ 3 main components ready to integrate
- ✅ Full TypeScript support
- ✅ Tailwind styling
- ✅ Accessible with hover tooltips
- ✅ Production-ready

**Next step:** Copy components into your project and start integrating into pages.
