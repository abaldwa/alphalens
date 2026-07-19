import { useState } from 'react'
import { useQuery } from '@tanstack/react-query'

import { AppShell, Badge, Button, Card, CardContent, InfoTooltip } from '@/lib/ui'
import { apiGet } from '@/shared/api/client'
import type { ForensicRow } from './types'

type Severity = 'critical' | 'high' | 'info'

interface Panel {
  severity: Severity
  title: React.ReactNode
  detail: string
}

const SEVERITY_META: Record<Severity, { variant: 'destructive' | 'warning' | 'outline'; label: string; border: string }> = {
  critical: { variant: 'destructive', label: '🔴 CRITICAL', border: 'var(--red)' },
  high: { variant: 'warning', label: '⚠ HIGH', border: 'var(--amber)' },
  info: { variant: 'outline', label: 'ℹ INFO', border: 'var(--blue)' },
}

function FlagPanel({ p }: { p: Panel }) {
  const meta = SEVERITY_META[p.severity]
  return (
    <Card className="mb-3" style={{ borderLeft: `3px solid ${meta.border}` }}>
      <CardContent className="pt-5">
        <div className="flex items-center gap-3">
          <Badge variant={meta.variant}>{meta.label}</Badge>
          <span className="font-semibold">{p.title}</span>
        </div>
        <div className="mt-3 text-xs text-muted-foreground">{p.detail}</div>
      </CardContent>
    </Card>
  )
}

function buildPanels(row: ForensicRow): Panel[] {
  const panels: Panel[] = []
  if (row.beneish_m != null) {
    const flagged = row.beneish_m > -1.78
    panels.push({
      severity: flagged ? 'critical' : 'info',
      title: (
        <span className="inline-flex items-center gap-1">
          {`Beneish M-Score (${row.beneish_m.toFixed(2)})`}
          <InfoTooltip>
            Earnings-manipulation risk score. Lower/more-negative values indicate lower risk, per the published
            Beneish model's convention.
          </InfoTooltip>
        </span>
      ),
      detail: `Manipulator threshold: -1.78 · ${flagged ? 'Above threshold — earnings manipulation risk' : 'Below threshold — within normal range'}`,
    })
  }
  if (row.altman_z != null) {
    const distress = row.altman_z < 1.81
    const safe = row.altman_z > 2.99
    panels.push({
      severity: distress ? 'critical' : safe ? 'info' : 'high',
      title: (
        <span className="inline-flex items-center gap-1">
          {`Altman Z-Score (${row.altman_z.toFixed(2)})`}
          <InfoTooltip>Bankruptcy-risk score. Higher is safer.</InfoTooltip>
        </span>
      ),
      detail: `Distress zone: < 1.81 · Safe zone: > 2.99 · ${distress ? 'In distress zone' : safe ? 'In safe zone' : 'In grey zone'}`,
    })
  }
  if (row.piotroski_f != null) {
    const weak = row.piotroski_f <= 2
    panels.push({
      severity: weak ? 'high' : 'info',
      title: (
        <span className="inline-flex items-center gap-1">
          {`Piotroski F-Score (${row.piotroski_f.toFixed(0)})`}
          <InfoTooltip>0-9 fundamental-strength score. Higher is stronger.</InfoTooltip>
        </span>
      ),
      detail: `Weak threshold: <= 2 · ${weak ? 'Weak fundamental quality' : 'Within normal range'}`,
    })
  }
  if (row.ohlson_o != null) {
    panels.push({
      severity: 'info',
      title: (
        <span className="inline-flex items-center gap-1">
          {`Ohlson O-Score (${row.ohlson_o.toFixed(2)})`}
          <InfoTooltip>
            Bankruptcy-probability score (logistic-regression based). Higher values indicate a higher estimated
            bankruptcy probability.
          </InfoTooltip>
        </span>
      ),
      detail: 'Bankruptcy-risk score — higher values indicate higher risk',
    })
  }
  if (row.dechow_f != null) {
    panels.push({
      severity: 'info',
      title: (
        <span className="inline-flex items-center gap-1">
          {`Dechow F-Score (${row.dechow_f.toFixed(2)})`}
          <InfoTooltip>
            Earnings-quality / manipulation-risk score derived from the Dechow-Ge-Larson-Sloan model.
          </InfoTooltip>
        </span>
      ),
      detail: 'Earnings-misstatement probability score',
    })
  }
  if (row.sloan_accrual != null) {
    panels.push({
      severity: 'info',
      title: (
        <span className="inline-flex items-center gap-1">
          {`Sloan Accrual Ratio (${row.sloan_accrual.toFixed(3)})`}
          <InfoTooltip>
            Ratio of accounting accruals to total assets. Large positive values are historically associated with
            lower forward returns (the "accrual anomaly").
          </InfoTooltip>
        </span>
      ),
      detail: 'Higher values indicate lower earnings quality (more accrual-driven, less cash-driven)',
    })
  }
  if (row.forensic_ml_prob != null) {
    const high = row.forensic_ml_prob > 0.5
    panels.push({
      severity: high ? 'high' : 'info',
      title: (
        <span className="inline-flex items-center gap-1">
          {`ML Fraud Probability (${(row.forensic_ml_prob * 100).toFixed(1)}%)`}
          <InfoTooltip>
            A separate ML classifier's own probability estimate of forensic risk, distinct from the composite of
            classical formula-based scores above.
          </InfoTooltip>
        </span>
      ),
      detail: "Ensemble model's estimated fraud probability",
    })
  }
  if (row.benford_mad != null) {
    panels.push({
      severity: 'info',
      title: (
        <span className="inline-flex items-center gap-1">
          {`Benford MAD (${row.benford_mad.toFixed(4)})`}
          <InfoTooltip>
            Mean absolute deviation of reported-figure leading digits from Benford's Law's expected distribution.
            Higher suggests a higher chance of manufactured/rounded figures.
          </InfoTooltip>
        </span>
      ),
      detail: "Mean absolute deviation from Benford's Law digit distribution — see the Benford screen for detail",
    })
  }
  if (row.pattern_match) {
    panels.push({
      severity: 'high',
      title: (
        <span className="inline-flex items-center gap-1">
          Historical Pattern Match
          <InfoTooltip>
            Which known historical forensic-failure pattern (if any) this ticker's figures most resemble.
          </InfoTooltip>
        </span>
      ),
      detail: row.pattern_match,
    })
  }
  return panels
}

export function RedflagPage() {
  const [tickerInput, setTickerInput] = useState('RELIANCE')
  const [ticker, setTicker] = useState('RELIANCE')

  const query = useQuery({
    queryKey: ['forensic-redflag', ticker],
    queryFn: () => apiGet<ForensicRow | null>(`/api/v1/signals/ml/forensic/${ticker}`),
  })

  const row = query.data
  const panels = row ? buildPanels(row) : []

  return (
    <AppShell
      title="Forensic — Red Flags"
      description="Severity thresholds are the documented classical_scores.py constants (Beneish -1.78, Altman 1.81/2.99, Piotroski <=2)."
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
      ) : !panels.length ? (
        <p className="text-sm text-muted-foreground">No classical forensic scores available for this ticker</p>
      ) : (
        <div>
          {panels.map((p, idx) => (
            <FlagPanel key={idx} p={p} />
          ))}
        </div>
      )}
    </AppShell>
  )
}
