import { useState } from 'react'
import { useQuery } from '@tanstack/react-query'

import { AppShell, Badge, Button, Card, CardContent, CardHeader, CardTitle, InfoTooltip } from '@/lib/ui'
import { apiGet, API_BASE_URL } from '@/shared/api/client'
import type { ForensicRow } from './types'

function Line({ label, value }: { label: React.ReactNode; value: React.ReactNode }) {
  return (
    <div className="flex flex-wrap gap-1 border-b border-border/50 py-2 text-sm last:border-b-0">
      <span className="font-semibold">{label}:</span>
      <span>{value}</span>
    </div>
  )
}

export function ReportPage() {
  const [tickerInput, setTickerInput] = useState('RELIANCE')
  const [ticker, setTicker] = useState('RELIANCE')

  const query = useQuery({
    queryKey: ['forensic-report', ticker],
    queryFn: () => apiGet<ForensicRow | null>(`/api/v1/signals/ml/forensic/${ticker}`),
  })

  const row = query.data
  const pdfUrl = `${API_BASE_URL}/api/v1/signals/ml/forensic/${ticker}/report/pdf`

  return (
    <AppShell
      title="Forensic — Report"
      description="Investigation report — every line is a real ForensicRow field, not a generative summary. Export as a server-rendered PDF."
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
        <Card>
          <CardHeader>
            <CardTitle>Investigation Report — {ticker}</CardTitle>
          </CardHeader>
          <CardContent>
            <Line label="Ticker" value={ticker} />
            <Line
              label={
                <span className="inline-flex items-center gap-1">
                  Forensic Composite
                  <InfoTooltip>
                    A 0-100 blended score across the classical forensic scores below. The flag/badge is true above
                    a fixed block threshold.
                  </InfoTooltip>
                </span>
              }
              value={`${row.forensic_composite != null ? row.forensic_composite.toFixed(0) : '—'}/100 (${(row.forensic_flag_label ?? 'unscored').toUpperCase()})`}
            />
            {row.beneish_m != null && (
              <Line
                label={
                  <span className="inline-flex items-center gap-1">
                    Beneish M-Score
                    <InfoTooltip>
                      Earnings-manipulation risk score. Lower/more-negative values indicate lower risk, per the
                      published Beneish model's convention.
                    </InfoTooltip>
                  </span>
                }
                value={`${row.beneish_m.toFixed(2)} (manipulator threshold: -1.78)`}
              />
            )}
            {row.altman_z != null && (
              <Line
                label={
                  <span className="inline-flex items-center gap-1">
                    Altman Z-Score
                    <InfoTooltip>Bankruptcy-risk score. Higher is safer.</InfoTooltip>
                  </span>
                }
                value={`${row.altman_z.toFixed(2)} (distress: <1.81, safe: >2.99)`}
              />
            )}
            {row.piotroski_f != null && (
              <Line
                label={
                  <span className="inline-flex items-center gap-1">
                    Piotroski F-Score
                    <InfoTooltip>0-9 fundamental-strength score. Higher is stronger.</InfoTooltip>
                  </span>
                }
                value={`${row.piotroski_f.toFixed(0)} (weak: <=2)`}
              />
            )}
            {row.sloan_accrual != null && (
              <Line
                label={
                  <span className="inline-flex items-center gap-1">
                    Sloan Accrual Ratio
                    <InfoTooltip>
                      Ratio of accounting accruals to total assets. Large positive values are historically
                      associated with lower forward returns (the "accrual anomaly").
                    </InfoTooltip>
                  </span>
                }
                value={`${row.sloan_accrual.toFixed(3)} (high-accrual: >0.10)`}
              />
            )}
            {row.benford_mad != null && (
              <Line
                label={
                  <span className="inline-flex items-center gap-1">
                    Benford MAD
                    <InfoTooltip>
                      Mean absolute deviation of reported-figure leading digits from Benford's Law's expected
                      distribution. Higher suggests a higher chance of manufactured/rounded figures.
                    </InfoTooltip>
                  </span>
                }
                value={`${row.benford_mad.toFixed(4)} (non-conforming: >0.015)`}
              />
            )}
            {row.forensic_ml_prob != null && (
              <Line
                label={
                  <span className="inline-flex items-center gap-1">
                    ML Fraud Probability
                    <InfoTooltip>
                      A separate ML classifier's own probability estimate of forensic risk, distinct from the
                      composite of classical formula-based scores above.
                    </InfoTooltip>
                  </span>
                }
                value={`${(row.forensic_ml_prob * 100).toFixed(1)}%`}
              />
            )}
            {row.pattern_match && (
              <Line
                label={
                  <span className="inline-flex items-center gap-1">
                    Historical Pattern Match
                    <InfoTooltip>
                      Which known historical forensic-failure pattern (if any) this ticker's figures most resemble.
                    </InfoTooltip>
                  </span>
                }
                value={row.pattern_match}
              />
            )}
            <Line
              label="Recommendation"
              value={
                <Badge variant={row.forensic_flag ? 'destructive' : 'success'}>
                  {row.forensic_flag ? 'BLOCKED FROM BUY RECOMMENDATIONS' : 'Not currently blocked'}
                </Badge>
              }
            />

            <div className="mt-4 flex justify-center gap-2">
              <Button asChild>
                <a href={pdfUrl}>Download PDF</a>
              </Button>
              <Button variant="outline" onClick={() => window.print()}>
                Print
              </Button>
            </div>
          </CardContent>
        </Card>
      )}
    </AppShell>
  )
}
