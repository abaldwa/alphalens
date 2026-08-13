import { useState } from 'react'
import { useQueryClient } from '@tanstack/react-query'

import { AppShell, Card, CardContent, CardDescription, CardHeader, CardTitle, DataTable } from '@/lib/ui'
import { SweepTriggerButton } from '../SweepTriggerButton'
import { BackToReportLink } from './SectionNav'
import { bandLabel, STRATEGY_LABELS, useDynamicReportData, useSweepColumns } from './shared'
import type { MomentumDynamicReportVariant } from '../types'

export function MomentumStrategySweepPage() {
  const [strategy, setStrategy] = useState<string>('')
  const queryClient = useQueryClient()
  const { report, allRows, bands, strategyOptions, effectiveTopCagrRank } = useDynamicReportData()

  function jumpToYoy(bandId: number) {
    window.location.href = `/momentum-dynamic-report/yoy?band=${bandId}`
  }

  const columns = useSweepColumns(effectiveTopCagrRank, jumpToYoy)

  return (
    <AppShell title="Momentum — Strategy Sweep" description="Every variant across all 7 rank bands. Part of the Momentum Strategy Report.">
      <BackToReportLink />
      <Card>
        <CardHeader>
          <CardTitle>Strategy Sweep</CardTitle>
          <CardDescription>
            {report.isLoading
              ? 'Loading…'
              : report.error
                ? 'Failed to load'
                : `${allRows.length} variants${
                    report.data?.generated_at ? ` — generated ${new Date(report.data.generated_at).toLocaleString()}` : ''
                  }`}
          </CardDescription>
          <div className="mt-2 flex flex-wrap gap-3">
            <SweepTriggerButton
              label="Strategy Report"
              triggerUrl="/api/v1/momentum/dynamic_report/trigger"
              statusUrlPrefix="/api/v1/momentum/dynamic_report/trigger/status"
              onCompleted={() => queryClient.invalidateQueries({ queryKey: ['momentum-dynamic-report'] })}
            />
            <select
              className="h-9 rounded-[var(--radius-token)] border border-border bg-transparent px-3 text-sm"
              value={strategy}
              onChange={(e) => setStrategy(e.target.value)}
            >
              <option value="">All categories</option>
              {strategyOptions.map((s) => (
                <option key={s} value={s}>
                  {STRATEGY_LABELS[s as MomentumDynamicReportVariant['strategy']]}
                </option>
              ))}
            </select>
          </div>
        </CardHeader>
        <CardContent>
          {report.error ? (
            <p className="text-sm text-red">
              Could not reach GET /api/v1/momentum/dynamic_report — {(report.error as Error).message}
            </p>
          ) : (
            bands.map((band) => {
              const bandRows = allRows.filter(
                (r) => r.band_id === band.band_id && (!strategy || r.strategy === strategy),
              )
              return (
                <details key={band.band_id} className="mb-4 rounded-[var(--radius-token)] border border-border">
                  <summary className="cursor-pointer px-3 py-2 text-sm font-semibold">
                    Universe (rank {bandLabel(band.rank_start, band.rank_end)}) — {bandRows.length} variants
                  </summary>
                  <div className="border-t border-border p-2">
                    <DataTable
                      columns={columns}
                      data={bandRows}
                      isLoading={report.isLoading}
                      emptyMessage="No variants for this universe/category yet — run the sweep above."
                    />
                  </div>
                </details>
              )
            })
          )}
        </CardContent>
      </Card>
    </AppShell>
  )
}
