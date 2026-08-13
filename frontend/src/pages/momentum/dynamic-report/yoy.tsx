import { useMemo, useState } from 'react'
import { useSearchParams } from 'react-router-dom'

import { AppShell, Card, CardContent, CardDescription, CardHeader, CardTitle, DataTable } from '@/lib/ui'
import { BackToReportLink } from './SectionNav'
import { bandLabel, useDynamicReportData, useYoyColumns } from './shared'

export function MomentumYoyPage() {
  const [searchParams] = useSearchParams()
  const [yoyBandId, setYoyBandId] = useState<string>(searchParams.get('band') ?? '')
  const { report, allYoyRows, bands } = useDynamicReportData()
  const yoyColumns = useYoyColumns()

  const yoyRows = useMemo(
    () => allYoyRows.filter((r) => !yoyBandId || String(r.band_id) === yoyBandId),
    [allYoyRows, yoyBandId],
  )

  return (
    <AppShell title="Momentum — Year-on-Year" description="Part of the Momentum Strategy Report.">
      <BackToReportLink unifiedSection="consistency" />
      <Card>
        <CardHeader>
          <CardTitle>Year-on-Year (Apr&ndash;Mar)</CardTitle>
          <CardDescription>
            Per-FY return, churn, and Nifty Midcap 150 / Smallcap 250 comparison (benchmark data real from 2023-07
            onward only).
          </CardDescription>
          <div className="mt-2 flex flex-wrap gap-3">
            <select
              className="h-9 rounded-[var(--radius-token)] border border-border bg-transparent px-3 text-sm"
              value={yoyBandId}
              onChange={(e) => setYoyBandId(e.target.value)}
            >
              <option value="">All universes</option>
              {bands.map((b) => (
                <option key={b.band_id} value={b.band_id}>
                  {bandLabel(b.rank_start, b.rank_end)}
                </option>
              ))}
            </select>
          </div>
        </CardHeader>
        <CardContent>
          <DataTable
            columns={yoyColumns}
            data={yoyRows}
            isLoading={report.isLoading}
            emptyMessage="No year-on-year rows yet."
          />
        </CardContent>
      </Card>
    </AppShell>
  )
}
