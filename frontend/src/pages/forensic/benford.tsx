import { useMemo, useState } from 'react'
import { useQuery } from '@tanstack/react-query'
import { Bar, BarChart, CartesianGrid, Legend, Tooltip, XAxis, YAxis } from 'recharts'

import { AppShell, Badge, Button, Card, CardContent, CardHeader, CardTitle, InfoTooltip, ResponsiveChartCard } from '@/lib/ui'
import { apiGet } from '@/shared/api/client'
import type { ForensicRow } from './types'

const EXPECTED_DIST = [0.301, 0.176, 0.125, 0.097, 0.079, 0.067, 0.058, 0.051, 0.046]

interface SeriesDetail {
  chi2: number | null
  pValue: number | null
  mad: number | null
  nObs: number | null
  dist: number[] | null
}

export function BenfordPage() {
  const [tickerInput, setTickerInput] = useState('RELIANCE')
  const [ticker, setTicker] = useState('RELIANCE')

  const query = useQuery({
    queryKey: ['forensic-benford', ticker],
    queryFn: () => apiGet<ForensicRow | null>(`/api/v1/signals/ml/forensic/${ticker}`),
  })

  const row = query.data

  const { detail, seriesNames, expected } = useMemo(() => {
    if (!row?.benford_detail_json) return { detail: null as Record<string, unknown> | null, seriesNames: [] as string[], expected: EXPECTED_DIST }
    try {
      const parsed = JSON.parse(row.benford_detail_json) as Record<string, unknown>
      const names = Object.keys(parsed)
        .filter((k) => k.startsWith('benford_') && k.endsWith('_chi2'))
        .map((k) => k.slice('benford_'.length, -'_chi2'.length))
      const exp = (parsed.benford_expected_distribution as number[] | undefined) ?? EXPECTED_DIST
      return { detail: parsed, seriesNames: names, expected: exp }
    } catch {
      return { detail: null, seriesNames: [], expected: EXPECTED_DIST }
    }
  }, [row])

  function seriesDetail(name: string): SeriesDetail {
    if (!detail) return { chi2: null, pValue: null, mad: null, nObs: null, dist: null }
    return {
      chi2: (detail[`benford_${name}_chi2`] as number | null) ?? null,
      pValue: (detail[`benford_${name}_p_value`] as number | null) ?? null,
      mad: (detail[`benford_${name}_mad`] as number | null) ?? null,
      nObs: (detail[`benford_${name}_n_obs`] as number | null) ?? null,
      dist: (detail[`benford_${name}_digit_distribution`] as number[] | null) ?? null,
    }
  }

  return (
    <AppShell
      title="Forensic — Benford"
      description="Benford's Law first-digit distribution analysis per financial series."
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
          <Card className="mb-4">
            <CardHeader>
              <CardTitle className="inline-flex items-center gap-1">
                Overall Mean Absolute Deviation (MAD)
                <InfoTooltip>
                  Mean absolute deviation of this ticker's reported-figure leading-digit distribution from
                  Benford's Law's expected distribution. Higher suggests a higher chance of manufactured/rounded
                  figures.
                </InfoTooltip>
              </CardTitle>
            </CardHeader>
            <CardContent>
              {row.benford_mad != null ? (
                <div className="flex items-center gap-2 text-sm">
                  <strong>MAD = {row.benford_mad.toFixed(4)}</strong>
                  <span className="text-muted-foreground">
                    (non-conforming threshold: 0.015 · significant-deviation threshold: 0.030)
                  </span>
                  <Badge variant={row.benford_mad > 0.03 ? 'destructive' : row.benford_mad > 0.015 ? 'warning' : 'success'}>
                    {row.benford_mad > 0.03 ? 'SIGNIFICANT DEVIATION' : row.benford_mad > 0.015 ? 'NON-CONFORMING' : 'CONFORMS'}
                  </Badge>
                </div>
              ) : (
                <p className="text-sm text-muted-foreground">No Benford MAD score for this ticker</p>
              )}
            </CardContent>
          </Card>

          {!detail ? (
            <p className="text-sm text-muted-foreground">
              No per-digit Benford distribution recorded for this ticker/date yet — re-run the forensic scorer
              (score_forensic.py) to populate benford_detail_json.
            </p>
          ) : !seriesNames.length ? (
            <p className="text-sm text-muted-foreground">No per-series Benford data in benford_detail_json</p>
          ) : (
            <div className="flex flex-col gap-4">
              {seriesNames.map((name) => {
                const d = seriesDetail(name)
                const chartData = expected.map((exp, idx) => ({
                  digit: String(idx + 1),
                  expected: Number((exp * 100).toFixed(2)),
                  observed: d.dist && d.dist.length === 9 ? Number((d.dist[idx] * 100).toFixed(2)) : null,
                }))
                return (
                  <ResponsiveChartCard
                    key={name}
                    title={name.replace(/_/g, ' ')}
                    description={`n=${d.nObs ?? '—'} · χ²=${d.chi2 != null ? d.chi2.toFixed(2) : '—'} · p=${d.pValue != null ? d.pValue.toFixed(4) : '—'} · MAD=${d.mad != null ? d.mad.toFixed(4) : '—'}`}
                    height={220}
                  >
                    <BarChart data={chartData}>
                      <CartesianGrid strokeDasharray="3 3" stroke="var(--border)" />
                      <XAxis dataKey="digit" tick={{ fontSize: 11 }} />
                      <YAxis tick={{ fontSize: 11 }} unit="%" />
                      <Tooltip />
                      <Legend />
                      <Bar dataKey="expected" name="Expected" fill="var(--red)" radius={[3, 3, 0, 0]} />
                      <Bar dataKey="observed" name="Observed" fill="var(--blue)" radius={[3, 3, 0, 0]} />
                    </BarChart>
                  </ResponsiveChartCard>
                )
              })}
            </div>
          )}
        </>
      )}
    </AppShell>
  )
}
