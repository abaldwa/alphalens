import { useMemo, useState } from 'react'
import { useQuery } from '@tanstack/react-query'

import { AppShell, Badge, Button, Card, CardContent, CardHeader, CardTitle, InfoTooltip, StatCard } from '@/lib/ui'
import { apiGet } from '@/shared/api/client'
import type { ForensicRow } from './types'
import { flagBadgeVariant } from './types'

function fmtNum(v: number | null | undefined, digits = 2): string {
  return v == null ? '—' : v.toFixed(digits)
}

interface ShapEntry {
  feature: string
  value: number
}

export function ForensicPage() {
  const [tickerInput, setTickerInput] = useState('RELIANCE')
  const [ticker, setTicker] = useState('RELIANCE')

  const query = useQuery({
    queryKey: ['forensic-dashboard', ticker],
    queryFn: () => apiGet<ForensicRow | null>(`/api/v1/signals/ml/forensic/${ticker}`),
  })

  const row = query.data

  const shapEntries = useMemo<ShapEntry[] | null>(() => {
    if (!row?.shap_top5_json) return null
    try {
      const parsed = JSON.parse(row.shap_top5_json) as unknown
      if (Array.isArray(parsed)) {
        return parsed.map((e) => ({ feature: (e as { feature?: string }).feature ?? '—', value: Number((e as { value?: number }).value ?? 0) }))
      }
      return Object.entries(parsed as Record<string, number>).map(([feature, value]) => ({ feature, value: Number(value) }))
    } catch {
      return null
    }
  }, [row])

  const scoreCards: [string, React.ReactNode, string][] = row
    ? [
        [
          'Beneish M-Score',
          "Earnings-manipulation risk score. Lower/more-negative values indicate lower risk, per the published Beneish model's convention.",
          fmtNum(row.beneish_m),
        ],
        ['Altman Z-Score', 'Bankruptcy-risk score. Higher is safer.', fmtNum(row.altman_z)],
        ['Piotroski F-Score', '0-9 fundamental-strength score. Higher is stronger.', fmtNum(row.piotroski_f, 0)],
        [
          'Ohlson O-Score',
          'Bankruptcy-probability score (logistic-regression based). Higher values indicate a higher estimated bankruptcy probability.',
          fmtNum(row.ohlson_o),
        ],
        [
          'Dechow F-Score',
          'Earnings-quality / manipulation-risk score derived from the Dechow-Ge-Larson-Sloan model.',
          fmtNum(row.dechow_f),
        ],
        [
          'Sloan Accrual',
          'Ratio of accounting accruals to total assets. Large positive values are historically associated with lower forward returns (the "accrual anomaly").',
          fmtNum(row.sloan_accrual, 3),
        ],
        [
          'Benford MAD',
          "Mean absolute deviation of reported-figure leading digits from Benford's Law's expected distribution. Higher suggests a higher chance of manufactured/rounded figures.",
          fmtNum(row.benford_mad, 4),
        ],
        [
          'ML Fraud Prob',
          "A separate ML classifier's own probability estimate of forensic risk, distinct from the composite of classical formula-based scores.",
          row.forensic_ml_prob != null ? `${(row.forensic_ml_prob * 100).toFixed(1)}%` : '—',
        ],
      ]
    : []

  return (
    <AppShell
      title="Forensic"
      description="ML forensic-accounting dashboard — classical (Beneish/Altman/Piotroski/Ohlson/Dechow/Sloan/Benford) plus ensemble ML fraud probability, per ticker."
      actions={
        <div className="flex items-center gap-2">
          <input
            className="h-9 rounded-[var(--radius-token)] border border-border bg-transparent px-3 text-sm"
            value={tickerInput}
            onChange={(e) => setTickerInput(e.target.value.toUpperCase())}
            placeholder="Ticker (e.g. RELIANCE)"
          />
          <Button onClick={() => setTicker(tickerInput.trim().toUpperCase())}>Load</Button>
        </div>
      }
    >
      {query.error ? (
        <p className="text-sm text-red">
          Could not reach GET /api/v1/signals/ml/forensic/{ticker} — {(query.error as Error).message}
        </p>
      ) : !row ? (
        <p className="text-sm text-muted-foreground">{query.isLoading ? 'Loading…' : `No forensic score for ${ticker}`}</p>
      ) : (
        <>
          <div className="mb-4 flex items-center gap-4">
            <span className="text-xl font-bold">{ticker}</span>
            <span className="font-mono-data text-2xl font-semibold">{row.forensic_composite != null ? row.forensic_composite.toFixed(0) : '—'}</span>
            <Badge variant={flagBadgeVariant(row.forensic_flag_label)}>{(row.forensic_flag_label ?? 'unscored').toUpperCase()}</Badge>
          </div>

          {row.forensic_flag && (
            <p className="mb-4 rounded-[var(--radius-token)] border border-red/40 bg-red/10 px-4 py-2 text-sm text-red">
              This stock is BLOCKED from buy recommendations — forensic composite above the block threshold
            </p>
          )}

          <div className="mb-4 grid grid-cols-1 gap-4 sm:grid-cols-4">
            {scoreCards.map(([label, tooltip, value]) => (
              <StatCard
                key={label}
                label={
                  <span className="inline-flex items-center gap-1">
                    {label}
                    <InfoTooltip>{tooltip}</InfoTooltip>
                  </span>
                }
                value={value}
              />
            ))}
          </div>

          <Card className="mb-4">
            <CardHeader>
              <CardTitle className="inline-flex items-center gap-1">
                Top Risk Factors (SHAP)
                <InfoTooltip>
                  SHAP (SHapley Additive exPlanations) attribution — each feature's individual contribution to the
                  ML forensic-risk probability above; positive (green) pushes risk up, negative (red) pushes it
                  down.
                </InfoTooltip>
              </CardTitle>
            </CardHeader>
            <CardContent>
              {shapEntries === null ? (
                <p className="text-sm text-muted-foreground">No SHAP attribution available for this ticker</p>
              ) : (
                <div className="flex flex-col gap-2">
                  {shapEntries.map((e) => (
                    <div key={e.feature} className="flex items-center gap-2 text-xs">
                      <span className="w-40 shrink-0 truncate text-muted-foreground">{e.feature}</span>
                      <div className="relative h-3 flex-1 rounded bg-accent">
                        <div
                          className="absolute inset-y-0"
                          style={{
                            width: `${Math.min(Math.abs(e.value) * 100, 50)}%`,
                            background: e.value >= 0 ? 'var(--green)' : 'var(--red)',
                            borderRadius: 3,
                          }}
                        />
                      </div>
                      <span className={`font-mono-data ${e.value >= 0 ? 'text-green' : 'text-red'}`}>{e.value.toFixed(3)}</span>
                    </div>
                  ))}
                </div>
              )}
            </CardContent>
          </Card>

          <Card>
            <CardHeader>
              <CardTitle className="inline-flex items-center gap-1">
                Historical Pattern Match
                <InfoTooltip>
                  Which known historical forensic-failure pattern (if any) this ticker's figures most resemble.
                </InfoTooltip>
              </CardTitle>
            </CardHeader>
            <CardContent>
              {row.pattern_match ? (
                <p className="text-sm">{row.pattern_match}</p>
              ) : (
                <p className="text-sm text-muted-foreground">No pattern match recorded for this ticker</p>
              )}
            </CardContent>
          </Card>
        </>
      )}
    </AppShell>
  )
}
