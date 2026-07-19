import { useQuery } from '@tanstack/react-query'

import { AppShell, Badge, Card, CardContent, CardHeader, CardTitle, InfoTooltip } from '@/lib/ui'
import { apiGet } from '@/shared/api/client'
import type { ForensicFlaggedResponse, ForensicRow } from './types'
import { flagBadgeVariant } from './types'

function fmtNum(v: number | null | undefined, digits = 2): string {
  return v == null ? '—' : v.toFixed(digits)
}

function HeatCell({ value, digits, isBad }: { value: number | null | undefined; digits: number; isBad: boolean }) {
  if (value == null) {
    return <td className="border border-border px-2 py-1 text-right font-mono-data text-muted-foreground">—</td>
  }
  return (
    <td
      className="border border-border px-2 py-1 text-right font-mono-data"
      style={{
        background: isBad ? 'color-mix(in srgb, var(--red) 22%, transparent)' : 'color-mix(in srgb, var(--green) 18%, transparent)',
        color: isBad ? 'var(--red)' : 'var(--green)',
      }}
    >
      {fmtNum(value, digits)}
    </td>
  )
}

export function HeatmapPage() {
  const flagged = useQuery({
    queryKey: ['forensic-heatmap-flagged'],
    queryFn: () => apiGet<ForensicFlaggedResponse>('/api/v1/signals/ml/forensic/flagged', { flag: 'red,amber' }),
  })

  const tickers = (flagged.data?.rows ?? []).slice(0, 15)

  const details = useQuery({
    queryKey: ['forensic-heatmap-details', tickers.map((t) => t.ticker).join(',')],
    queryFn: () =>
      Promise.all(
        tickers.map((t) =>
          apiGet<ForensicRow | null>(`/api/v1/signals/ml/forensic/${t.ticker}`).catch(() => null),
        ),
      ),
    enabled: tickers.length > 0,
  })

  return (
    <AppShell
      title="Forensic — Heatmap"
      description="Peer forensic heatmap across red/amber-flagged tickers — classical score breakdown per company."
    >
      <Card>
        <CardHeader>
          <CardTitle>Red/Amber Flagged Universe</CardTitle>
        </CardHeader>
        <CardContent>
          {flagged.error ? (
            <p className="text-sm text-red">
              Could not reach GET /api/v1/signals/ml/forensic/flagged — {(flagged.error as Error).message}
            </p>
          ) : flagged.isLoading ? (
            <p className="text-sm text-muted-foreground">Loading…</p>
          ) : !tickers.length ? (
            <p className="text-sm text-muted-foreground">No red/amber flagged tickers</p>
          ) : (
            <div className="overflow-x-auto">
              <table className="w-full border-collapse text-sm">
                <thead>
                  <tr>
                    <th className="border border-border px-2 py-1 text-left">Company</th>
                    <th className="border border-border px-2 py-1 text-left">
                      <span className="inline-flex items-center gap-1">
                        Score
                        <InfoTooltip>
                          Forensic Composite — a 0-100 blended score across the classical forensic scores below.
                          The red/amber flag is true above a fixed block threshold.
                        </InfoTooltip>
                      </span>
                    </th>
                    <th className="border border-border px-2 py-1 text-right">
                      <span className="inline-flex items-center gap-1">
                        Beneish
                        <InfoTooltip>
                          Beneish M-Score — earnings-manipulation risk score. Lower/more-negative values indicate
                          lower risk, per the published Beneish model's convention.
                        </InfoTooltip>
                      </span>
                    </th>
                    <th className="border border-border px-2 py-1 text-right">
                      <span className="inline-flex items-center gap-1">
                        Altman Z
                        <InfoTooltip>Altman Z-Score — bankruptcy-risk score. Higher is safer.</InfoTooltip>
                      </span>
                    </th>
                    <th className="border border-border px-2 py-1 text-right">
                      <span className="inline-flex items-center gap-1">
                        Piotroski F
                        <InfoTooltip>Piotroski F-Score — 0-9 fundamental-strength score. Higher is stronger.</InfoTooltip>
                      </span>
                    </th>
                    <th className="border border-border px-2 py-1 text-right">
                      <span className="inline-flex items-center gap-1">
                        Sloan Accrual
                        <InfoTooltip>
                          Ratio of accounting accruals to total assets. Large positive values are historically
                          associated with lower forward returns (the "accrual anomaly").
                        </InfoTooltip>
                      </span>
                    </th>
                    <th className="border border-border px-2 py-1 text-right">
                      <span className="inline-flex items-center gap-1">
                        Benford MAD
                        <InfoTooltip>
                          Mean absolute deviation of reported-figure leading digits from Benford's Law's expected
                          distribution. Higher suggests a higher chance of manufactured/rounded figures.
                        </InfoTooltip>
                      </span>
                    </th>
                  </tr>
                </thead>
                <tbody>
                  {tickers.map((t, i) => {
                    const d = details.data?.[i] ?? null
                    return (
                      <tr key={t.ticker}>
                        <td className="border border-border px-2 py-1 font-semibold">{t.ticker}</td>
                        <td className="border border-border px-2 py-1">
                          <Badge variant={flagBadgeVariant(t.forensic_flag_label)}>{fmtNum(t.forensic_composite, 0)}</Badge>
                        </td>
                        <HeatCell value={d?.beneish_m} digits={2} isBad={(d?.beneish_m ?? -Infinity) > -1.78} />
                        <HeatCell value={d?.altman_z} digits={2} isBad={(d?.altman_z ?? Infinity) < 1.81} />
                        <HeatCell value={d?.piotroski_f} digits={0} isBad={(d?.piotroski_f ?? Infinity) <= 2} />
                        <HeatCell value={d?.sloan_accrual} digits={3} isBad={(d?.sloan_accrual ?? -Infinity) > 0.1} />
                        <HeatCell value={d?.benford_mad} digits={4} isBad={(d?.benford_mad ?? -Infinity) > 0.03} />
                      </tr>
                    )
                  })}
                </tbody>
              </table>
            </div>
          )}
        </CardContent>
      </Card>
    </AppShell>
  )
}
