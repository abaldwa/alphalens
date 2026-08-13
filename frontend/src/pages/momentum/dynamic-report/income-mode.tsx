import { AppShell, Card, CardContent, CardDescription, CardHeader, CardTitle, DataTable } from '@/lib/ui'
import { BackToReportLink } from './SectionNav'
import { useDynamicReportData, useIncomeColumns } from './shared'

export function MomentumIncomeModePage() {
  const { report, recommendedRows } = useDynamicReportData()
  const incomeColumns = useIncomeColumns()

  return (
    <AppShell title="Momentum — Income Mode" description="Part of the Momentum Strategy Report.">
      <BackToReportLink />
      <Card>
        <CardHeader>
          <CardTitle>Income Mode (Annual Capital Reset)</CardTitle>
          <CardDescription>
            Each strategy starts every fiscal year at ₹10,00,000. After real YoY tax is paid, any surplus is
            withdrawn as cash back to the investor; a loss year is topped back up to ₹10L instead. "Total Withdrawn"
            is real lifetime cash paid out on this fixed base — not a growth/reinvestment figure, so it isn't
            comparable to CAGR elsewhere in this report. "Years Survived" is the % of fiscal years that didn't need
            a top-up.
          </CardDescription>
        </CardHeader>
        <CardContent>
          <DataTable
            columns={incomeColumns}
            data={recommendedRows}
            isLoading={report.isLoading}
            emptyMessage="No income-mode data yet — run the sweep above."
          />
        </CardContent>
      </Card>
    </AppShell>
  )
}
