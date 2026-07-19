import { useMemo, useState } from 'react'
import { useMutation, useQueries, useQueryClient } from '@tanstack/react-query'
import { CartesianGrid, Line, LineChart, Tooltip, XAxis, YAxis } from 'recharts'

import { AppShell, Button, Card, CardContent, CardHeader, CardTitle, InfoTooltip, ResponsiveChartCard } from '@/lib/ui'
import { apiGet, apiPost } from '@/shared/api/client'

// Mirrors features/real_economy_macro.py's MANUAL_ENTRY_FEATURES (the 8
// series with no free automated source — cement_dispatches_growth/
// power_consumption_growth are excluded since they already have a real
// scraper, see datastore/api/routers/macro.py's module docstring).
const MANUAL_ENTRY_FEATURES = [
  'gst_collection_growth',
  'pmi_manufacturing',
  'pmi_services',
  'iip_growth',
  'auto_monthly_sales_growth',
  'rail_freight_growth',
  'upi_transaction_growth',
  'bank_credit_growth',
]

interface MacroIndicatorRow {
  feature_name: string
  reference_month_end: string
  value: number
  availability_date: string
}
interface MacroIndicatorsResponse {
  rows: MacroIndicatorRow[]
}

function currentMonthEnd(): string {
  const now = new Date()
  const nextMonth = new Date(now.getFullYear(), now.getMonth() + 1, 1)
  const monthEnd = new Date(nextMonth.getTime() - 86400000)
  return monthEnd.toISOString().slice(0, 10)
}

const inputClass = 'h-9 rounded-[var(--radius-token)] border border-border bg-transparent px-3 text-sm'

const FEATURE_TOOLTIP: Record<string, string> = {
  gst_collection_growth: 'GST (Goods and Services Tax): YoY growth in monthly GST collections, a broad proxy for economic activity/consumption in India.',
  pmi_manufacturing: 'PMI (Purchasing Managers\' Index) — Manufacturing: survey-based gauge of manufacturing sector activity; above 50 = expansion, below 50 = contraction.',
  pmi_services: 'PMI (Purchasing Managers\' Index) — Services: survey-based gauge of services sector activity; above 50 = expansion, below 50 = contraction.',
  iip_growth: 'IIP (Index of Industrial Production): YoY growth in India\'s official index of industrial output (manufacturing, mining, electricity).',
  auto_monthly_sales_growth: 'YoY growth in monthly automobile sales — a widely watched consumption/demand indicator.',
  rail_freight_growth: 'YoY growth in railway freight (goods) volumes — a proxy for industrial/logistics activity.',
  upi_transaction_growth: 'UPI (Unified Payments Interface): YoY growth in UPI digital-payment transaction volumes.',
  bank_credit_growth: 'YoY growth in outstanding bank credit (loans) — a proxy for credit demand/economic activity.',
}

export function MacroPage() {
  const queryClient = useQueryClient()
  const [month, setMonth] = useState(currentMonthEnd())
  const [values, setValues] = useState<Record<string, string>>({})
  const [status, setStatus] = useState<{ text: string; ok: boolean } | null>(null)
  const [chartFeature, setChartFeature] = useState(MANUAL_ENTRY_FEATURES[0])

  const historyQueries = useQueries({
    queries: MANUAL_ENTRY_FEATURES.map((f) => ({
      queryKey: ['macro-indicators', f],
      queryFn: () => apiGet<MacroIndicatorsResponse>('/api/v1/macro/indicators', { feature_name: f, limit_months: 12 }),
    })),
  })

  const allRows = useMemo(() => {
    const rows: MacroIndicatorRow[] = []
    for (const q of historyQueries) {
      if (q.data) rows.push(...q.data.rows)
    }
    return rows.sort((a, b) =>
      a.feature_name < b.feature_name ? -1 : a.feature_name > b.feature_name ? 1 : b.reference_month_end.localeCompare(a.reference_month_end),
    )
  }, [historyQueries])

  const isLoadingHistory = historyQueries.some((q) => q.isLoading)
  const anyError = historyQueries.find((q) => q.error)

  const chartData = useMemo(() => {
    return allRows
      .filter((r) => r.feature_name === chartFeature)
      .sort((a, b) => a.reference_month_end.localeCompare(b.reference_month_end))
      .map((r) => ({ month: r.reference_month_end, value: r.value }))
  }, [allRows, chartFeature])

  const submit = useMutation({
    mutationFn: async () => {
      const toWrite = MANUAL_ENTRY_FEATURES.filter((f) => values[f] !== undefined && values[f] !== '')
      let written = 0
      let failed = 0
      for (const feature of toWrite) {
        try {
          await apiPost('/api/v1/macro/indicators', {
            feature_name: feature,
            reference_month_end: month,
            value: parseFloat(values[feature]),
          })
          written += 1
        } catch {
          failed += 1
        }
      }
      return { written, failed }
    },
    onSuccess: ({ written, failed }) => {
      setStatus({ text: failed === 0 ? `Saved ${written} value(s).` : `Saved ${written}, ${failed} failed.`, ok: failed === 0 })
      setValues({})
      MANUAL_ENTRY_FEATURES.forEach((f) => queryClient.invalidateQueries({ queryKey: ['macro-indicators', f] }))
    },
  })

  const handleSubmit = () => {
    if (!month) {
      setStatus({ text: 'Pick a month first.', ok: false })
      return
    }
    const toWrite = MANUAL_ENTRY_FEATURES.filter((f) => values[f] !== undefined && values[f] !== '')
    if (toWrite.length === 0) {
      setStatus({ text: 'Enter at least one value.', ok: false })
      return
    }
    setStatus({ text: 'Saving...', ok: true })
    submit.mutate()
  }

  return (
    <AppShell title="Macro" description="A27 manual macro-entry screen: 8 real-economy series with no free automated source.">
      <Card>
        <CardHeader>
          <CardTitle>Enter this month&apos;s readings</CardTitle>
        </CardHeader>
        <CardContent>
          <div className="mb-3 flex items-center gap-2">
            <span className="text-sm text-muted-foreground">Month (any date in the target month)</span>
            <input className={inputClass} type="date" value={month} onChange={(e) => setMonth(e.target.value)} />
          </div>
          <div className="grid grid-cols-1 gap-2 sm:grid-cols-2">
            {MANUAL_ENTRY_FEATURES.map((feature) => (
              <div key={feature} className="flex items-center justify-between gap-2 rounded-[var(--radius-token)] border border-border px-3 py-2">
                <span className="inline-flex items-center gap-1 text-sm">
                  {feature}
                  {FEATURE_TOOLTIP[feature] && <InfoTooltip>{FEATURE_TOOLTIP[feature]}</InfoTooltip>}
                </span>
                <input
                  className={`${inputClass} w-32`}
                  type="number"
                  step="any"
                  placeholder="value"
                  value={values[feature] ?? ''}
                  onChange={(e) => setValues((v) => ({ ...v, [feature]: e.target.value }))}
                />
              </div>
            ))}
          </div>
          <div className="mt-3 flex items-center gap-2">
            <Button disabled={submit.isPending} onClick={handleSubmit}>
              Save entered values
            </Button>
            {status && <span className={`text-sm ${status.ok ? 'text-green' : 'text-red'}`}>{status.text}</span>}
          </div>
        </CardContent>
      </Card>

      <div className="mt-4">
        <Card>
          <CardHeader>
            <CardTitle>History chart</CardTitle>
          </CardHeader>
          <CardContent>
            <div className="mb-3">
              <select className={inputClass} value={chartFeature} onChange={(e) => setChartFeature(e.target.value)}>
                {MANUAL_ENTRY_FEATURES.map((f) => (
                  <option key={f} value={f}>
                    {f}
                  </option>
                ))}
              </select>
            </div>
            <ResponsiveChartCard title={chartFeature} description="Last 12 months" height={240}>
              <LineChart data={chartData}>
                <CartesianGrid strokeDasharray="3 3" stroke="var(--border)" />
                <XAxis dataKey="month" tick={{ fontSize: 11 }} />
                <YAxis tick={{ fontSize: 11 }} />
                <Tooltip />
                <Line type="monotone" dataKey="value" stroke="var(--blue)" strokeWidth={2} dot={false} />
              </LineChart>
            </ResponsiveChartCard>
          </CardContent>
        </Card>
      </div>

      <div className="mt-4">
        <Card>
          <CardHeader>
            <CardTitle>All entries (last 12 months per indicator)</CardTitle>
          </CardHeader>
          <CardContent>
            {anyError ? (
              <p className="text-sm text-red">Could not reach GET /api/v1/macro/indicators — {(anyError.error as Error).message}</p>
            ) : isLoadingHistory ? (
              <p className="text-sm text-muted-foreground">Loading…</p>
            ) : allRows.length === 0 ? (
              <p className="text-sm text-muted-foreground">No manual entries yet — use the form above to enter this month&apos;s readings.</p>
            ) : (
              <table className="w-full text-sm">
                <thead>
                  <tr className="text-left">
                    <th className="pb-2">Indicator</th>
                    <th className="pb-2">Month</th>
                    <th className="pb-2">Value</th>
                    <th className="pb-2">Available Since</th>
                  </tr>
                </thead>
                <tbody>
                  {allRows.map((r) => (
                    <tr key={`${r.feature_name}-${r.reference_month_end}`}>
                      <td className="py-1">
                        <span className="inline-flex items-center gap-1">
                          {r.feature_name}
                          {FEATURE_TOOLTIP[r.feature_name] && <InfoTooltip>{FEATURE_TOOLTIP[r.feature_name]}</InfoTooltip>}
                        </span>
                      </td>
                      <td className="py-1 font-mono-data">{r.reference_month_end}</td>
                      <td className="py-1 font-mono-data">{r.value}</td>
                      <td className="py-1 font-mono-data text-xs">{r.availability_date}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            )}
          </CardContent>
        </Card>
      </div>
    </AppShell>
  )
}
