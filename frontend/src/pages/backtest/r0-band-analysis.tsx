/**
 * pages/backtest/r0-band-analysis.tsx
 *
 * R0 Band Analysis report wrapper with dynamic validation filtering.
 * Fetches backtest runs data and displays band analysis with ability to filter
 * by validation status (Valid, Alternative Period, Flagged, Invalid).
 */

import { useMemo, useState } from 'react'
import { useQuery } from '@tanstack/react-query'
import { Card, CardContent, CardHeader, CardTitle } from '@/lib/ui'
import { listBacktestRuns } from '@/shared/api/backtest'

type ValidationStatus = 'valid' | 'alternative_period' | 'flagged' | 'invalid'
type ReportType = 'detailed' | 'comprehensive'
type MarketCapBand = 'all' | 'M1' | 'M2' | 'M3' | 'M4' | 'M5' | 'M6' | 'M7' | 'M8' | 'M9' | 'M10' | 'M11' | 'M12'

interface FilterState {
  status: ValidationStatus | 'all'
  reportType: ReportType
  band: MarketCapBand
}

export function R0BandAnalysisPage() {
  const [filter, setFilter] = useState<FilterState>({
    status: 'all',
    reportType: 'detailed',
    band: 'all',
  })

  const { data: runsData, isLoading } = useQuery({
    queryKey: ['backtest-runs-r0-analysis', 'momentum'],
    queryFn: () =>
      listBacktestRuns({
        channel: 'momentum',
        sort_by: 'created_at',
        limit: 2000,
      }),
    staleTime: 1000 * 60 * 5,
  })

  // Filter runs by validation status
  const filteredRuns = useMemo(() => {
    if (!runsData?.runs) return []
    if (filter.status === 'all') return runsData.runs

    return runsData.runs.filter((run) => {
      const status = (run.validation_status ?? 'valid') as ValidationStatus
      return status === filter.status
    })
  }, [runsData?.runs, filter.status])

  // Calculate statistics
  const stats = useMemo(() => {
    const byStatus: Record<ValidationStatus, number> = {
      valid: 0,
      alternative_period: 0,
      flagged: 0,
      invalid: 0,
    }

    if (!runsData?.runs) {
      return { total: 0, byStatus, filtered: 0 }
    }

    runsData.runs.forEach((run) => {
      const status = (run.validation_status ?? 'valid') as ValidationStatus
      byStatus[status]++
    })

    return {
      total: runsData.runs.length,
      byStatus,
      filtered: filteredRuns.length,
    }
  }, [runsData?.runs, filteredRuns.length])

  // Bands available in each report type
  const detailedBands = new Set(['all', 'M2', 'M4', 'M7', 'M9', 'M10', 'M12'])
  const allBands = new Set(['all', 'M1', 'M2', 'M3', 'M4', 'M5', 'M6', 'M7', 'M8', 'M9', 'M10', 'M11', 'M12'])
  const availableBands = filter.reportType === 'detailed' ? detailedBands : allBands

  // Auto-correct if selected band isn't available in current report type
  const correctedBand = availableBands.has(filter.band) ? filter.band : 'all'

  const reportUrl = (() => {
    const baseName = filter.reportType === 'detailed'
      ? 'r0_band_analysis_detailed'
      : 'r0_comprehensive_band_analysis_full'

    const bandSuffix = correctedBand === 'all' ? '' : `_${correctedBand}`
    return `http://localhost:8123/api/v1/backtest/html-reports/${baseName}${bandSuffix}`
  })()

  return (
    <div className="space-y-4 p-4">
      {/* Header */}
      <div>
        <h1 className="text-3xl font-bold mb-2">R0 Band Analysis Reports</h1>
        <p className="text-gray-600">
          Analyze R0 strategy performance by market capitalization band with validation filtering
        </p>
      </div>

      {/* Filter Controls */}
      <Card>
        <CardHeader>
          <CardTitle className="text-sm">Filters</CardTitle>
        </CardHeader>
        <CardContent className="space-y-4">
          <div className="grid grid-cols-1 gap-4 md:grid-cols-3">
            {/* Report Type Selection */}
            <div>
              <label className="block text-sm font-medium mb-2">Report Type</label>
              <select
                value={filter.reportType}
                onChange={(e) =>
                  setFilter({ ...filter, reportType: e.target.value as ReportType })
                }
                className="w-full px-3 py-2 border border-gray-300 rounded-md text-sm"
              >
                <option value="detailed">Band Analysis (Detailed)</option>
                <option value="comprehensive">Comprehensive Analysis</option>
              </select>
            </div>

            {/* Market Cap Band Selection */}
            <div>
              <label className="block text-sm font-medium mb-2">Market Cap Band</label>
              <select
                value={correctedBand}
                onChange={(e) =>
                  setFilter({
                    ...filter,
                    band: e.target.value as MarketCapBand,
                  })
                }
                className="w-full px-3 py-2 border border-gray-300 rounded-md text-sm"
              >
                <option value="all">All Bands</option>
                <option value="M1" disabled={!availableBands.has('M1')}>M1 (Ranks 1-50){!availableBands.has('M1') ? ' - Not in this report' : ''}</option>
                <option value="M2" disabled={!availableBands.has('M2')}>M2 (Ranks 1-75)</option>
                <option value="M3" disabled={!availableBands.has('M3')}>M3 (Ranks 51-100){!availableBands.has('M3') ? ' - Not in this report' : ''}</option>
                <option value="M4" disabled={!availableBands.has('M4')}>M4 (Ranks 76-160)</option>
                <option value="M5" disabled={!availableBands.has('M5')}>M5 (Ranks 101-150){!availableBands.has('M5') ? ' - Not in this report' : ''}</option>
                <option value="M6" disabled={!availableBands.has('M6')}>M6 (Ranks 151-200){!availableBands.has('M6') ? ' - Not in this report' : ''}</option>
                <option value="M7" disabled={!availableBands.has('M7')}>M7 (Ranks 161-275)</option>
                <option value="M8" disabled={!availableBands.has('M8')}>M8 (Ranks 201-300){!availableBands.has('M8') ? ' - Not in this report' : ''}</option>
                <option value="M9" disabled={!availableBands.has('M9')}>M9 (Ranks 276-550)</option>
                <option value="M10" disabled={!availableBands.has('M10')}>M10 (Ranks 301-500)</option>
                <option value="M11" disabled={!availableBands.has('M11')}>M11 (Ranks 501-800){!availableBands.has('M11') ? ' - Not in this report' : ''}</option>
                <option value="M12" disabled={!availableBands.has('M12')}>M12 (Ranks 551-800)</option>
              </select>
              {filter.reportType === 'detailed' && (
                <p className="text-xs text-amber-600 mt-1">💡 Detailed report available for: M2, M4, M7, M9, M10, M12. Others use Comprehensive report.</p>
              )}
            </div>

            {/* Validation Status Filter */}
            <div>
              <label className="block text-sm font-medium mb-2">Validation Status</label>
              <select
                value={filter.status}
                onChange={(e) =>
                  setFilter({
                    ...filter,
                    status: e.target.value as ValidationStatus | 'all',
                  })
                }
                className="w-full px-3 py-2 border border-gray-300 rounded-md text-sm"
              >
                <option value="all">All ({stats.total})</option>
                <option value="valid">✅ Valid ({stats.byStatus.valid})</option>
                <option value="alternative_period">
                  🟡 Alternative Period ({stats.byStatus.alternative_period})
                </option>
                <option value="flagged">⚠️ Flagged - Data Gaps ({stats.byStatus.flagged})</option>
                <option value="invalid">❌ Invalid ({stats.byStatus.invalid})</option>
              </select>
            </div>
          </div>

          {/* Summary Stats */}
          <div className="pt-2 border-t">
            <p className="text-xs text-gray-600">
              {filter.status === 'all'
                ? `Showing all ${stats.total} runs`
                : `Showing ${stats.filtered} of ${stats.total} runs (${((stats.filtered / stats.total) * 100).toFixed(1)}%)`}
              {correctedBand !== 'all' && ` • Band: ${correctedBand}`}
            </p>
          </div>
        </CardContent>
      </Card>

      {/* Data Summary */}
      <Card>
        <CardHeader>
          <CardTitle className="text-sm">Run Summary by Validation Status</CardTitle>
        </CardHeader>
        <CardContent>
          <div className="grid grid-cols-2 gap-4 md:grid-cols-4">
            <div className="text-center p-2 bg-green-50 rounded">
              <div className="text-2xl font-bold text-green-900">{stats.byStatus.valid}</div>
              <div className="text-xs text-green-700">Valid</div>
            </div>
            <div className="text-center p-2 bg-amber-50 rounded">
              <div className="text-2xl font-bold text-amber-900">{stats.byStatus.alternative_period}</div>
              <div className="text-xs text-amber-700">Alternative Period</div>
            </div>
            <div className="text-center p-2 bg-yellow-50 rounded">
              <div className="text-2xl font-bold text-yellow-900">{stats.byStatus.flagged}</div>
              <div className="text-xs text-yellow-700">Flagged</div>
            </div>
            <div className="text-center p-2 bg-red-50 rounded">
              <div className="text-2xl font-bold text-red-900">{stats.byStatus.invalid}</div>
              <div className="text-xs text-red-700">Invalid</div>
            </div>
          </div>
        </CardContent>
      </Card>

      {/* Report Embed Notice */}
      <Card>
        <CardHeader>
          <CardTitle className="text-sm">Report Information</CardTitle>
        </CardHeader>
        <CardContent className="text-xs text-gray-600 space-y-2">
          <p>
            <strong>Note:</strong> The embedded report below shows analysis for{' '}
            {filter.status === 'all'
              ? 'all runs'
              : `runs with validation status: ${filter.status}`}
            . Use the filters above to analyze specific data quality categories.
          </p>
          <p>
            For detailed analysis and full interactivity, open the report in a new window:{' '}
            <a
              href={reportUrl}
              target="_blank"
              rel="noopener noreferrer"
              className="text-blue-600 hover:underline font-medium"
            >
              Open Report →
            </a>
          </p>
        </CardContent>
      </Card>

      {/* Report Embed */}
      <Card className="min-h-screen">
        <CardHeader>
          <CardTitle>
            {filter.reportType === 'detailed' ? 'Band Analysis (Detailed)' : 'Comprehensive Analysis'}
          </CardTitle>
        </CardHeader>
        <CardContent>
          {isLoading ? (
            <div className="flex items-center justify-center p-8">
              <p className="text-gray-600">Loading report data...</p>
            </div>
          ) : (
            <div className="space-y-4">
              <div className="text-sm text-gray-600 bg-blue-50 p-3 rounded border border-blue-200">
                <p className="font-medium mb-1">📊 Filtered View</p>
                <p>
                  This report is filtered to show{' '}
                  {correctedBand !== 'all' && `${correctedBand} band `}
                  {filter.status === 'all' ? 'all runs' : `only ${filter.status} runs`}.
                  Currently showing <strong>{stats.filtered}</strong> out of{' '}
                  <strong>{stats.total}</strong> backtest runs.
                </p>
              </div>

              {/* Embedded Report */}
              <iframe
                src={reportUrl}
                className="w-full border border-gray-300 rounded"
                style={{ minHeight: '800px' }}
                title={`R0 ${filter.reportType === 'detailed' ? 'Detailed' : 'Comprehensive'} Analysis`}
              />

              <div className="text-xs text-gray-500 text-center p-4 border-t">
                Executed: 2026-08-25 | Period: 2009-01-01 to 2026-08-26 | Band:{' '}
                <strong>{correctedBand === 'all' ? 'All' : correctedBand}</strong> | Validation status:{' '}
                <strong>{filter.status === 'all' ? 'All' : filter.status}</strong>
              </div>
            </div>
          )}
        </CardContent>
      </Card>
    </div>
  )
}
