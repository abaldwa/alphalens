import { AppShell, Card, CardContent, CardDescription, CardHeader, CardTitle, DataTable } from '@/lib/ui'
import { BackToReportLink } from './SectionNav'
import { useDynamicReportData, useRollingColumns } from './shared'

export function MomentumRollingReturnsPage() {
  const { report, recommendedRows } = useDynamicReportData()
  const rollingColumns = useRollingColumns()

  return (
    <AppShell title="Momentum — Rolling Return Consistency" description="Part of the Momentum Strategy Report.">
      <BackToReportLink unifiedSection="consistency" />
      <Card>
        <CardHeader>
          <CardTitle>Rolling Return Consistency</CardTitle>
          <CardDescription>
            2/3/4-year rolling-window CAGR (min / median / max across every window in the backtest period) for each
            Recommended/Most Important pick, by universe and category — pre-tax, same basis as the Strategy Sweep
            CAGR. A tight min-max spread means the strategy's return doesn't depend heavily on when you started
            investing.
          </CardDescription>
        </CardHeader>
        <CardContent>
          <DataTable
            columns={rollingColumns}
            data={recommendedRows}
            isLoading={report.isLoading}
            emptyMessage="No rolling-return data yet — run the sweep above."
          />
        </CardContent>
      </Card>
    </AppShell>
  )
}
