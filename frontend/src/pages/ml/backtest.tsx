import { useState } from 'react'
import { useQuery } from '@tanstack/react-query'

import { AppShell, Badge, Card, CardContent, CardDescription, CardHeader, CardTitle, StatCard } from '@/lib/ui'
import { apiGet } from '@/shared/api/client'
import type { BacktestReport, BacktestReportsResponse } from './types'

function fmtPct(v: unknown) {
  return typeof v === 'number' ? `${(v * 100).toFixed(1)}%` : '—'
}
function fmtNum(v: unknown, digits = 2) {
  return typeof v === 'number' ? v.toFixed(digits) : v == null ? '—' : String(v)
}

function PhaseSection({ phaseKey, phase }: { phaseKey: string; phase: Record<string, unknown> | undefined }) {
  if (!phase) return null
  const agg = (phase.aggregate as Record<string, unknown>) ?? {}
  const integrityOk = Boolean(phase.integrity_passed)
  const detail = (phase.integrity_detail as Record<string, unknown>) ?? {}
  const failures = (detail.critical_failures as unknown[]) ?? []
  const folds = (phase.folds as Record<string, unknown>[]) ?? []
  const foldCols = ['fold_index', 'train_start', 'train_end', 'test_start', 'test_end', 'cagr', 'sharpe', 'max_drawdown', 'win_rate', 'profit_factor', 'n_trades']

  return (
    <Card className="mt-4">
      <CardHeader>
        <div className="flex items-center justify-between">
          <CardTitle>
            {phaseKey} — {String(phase.model_name ?? '')}
          </CardTitle>
          <Badge variant={integrityOk ? 'success' : 'destructive'}>{integrityOk ? 'integrity passed' : 'integrity FAILED'}</Badge>
        </div>
      </CardHeader>
      <CardContent>
        <div className="grid grid-cols-2 gap-3 sm:grid-cols-3">
          <StatCard label="CAGR (mean)" value={fmtPct(agg.cagr_mean)} />
          <StatCard label="Sharpe (mean)" value={fmtNum(agg.sharpe_mean)} />
          <StatCard label="Max Drawdown (worst)" value={fmtPct(agg.max_drawdown_worst)} />
          <StatCard label="Win Rate (mean)" value={fmtPct(agg.win_rate_mean)} />
          <StatCard label="Profit Factor (mean)" value={fmtNum(agg.profit_factor_mean)} />
          <StatCard label="Total Trades" value={String(agg.total_trades ?? '—')} />
        </div>

        {failures.length ? (
          <div className="mt-4 flex flex-col gap-1">
            <span className="text-xs font-semibold uppercase text-muted-foreground">Critical failures</span>
            {failures.map((f, i) => (
              <p key={i} className="text-sm text-red">
                {String(f)}
              </p>
            ))}
          </div>
        ) : null}

        {folds.length ? (
          <div className="mt-4 overflow-x-auto">
            <table className="w-full text-sm">
              <thead>
                <tr>
                  {foldCols.map((c) => (
                    <th key={c} className="p-2 text-left text-xs font-semibold uppercase text-muted-foreground">
                      {c}
                    </th>
                  ))}
                </tr>
              </thead>
              <tbody>
                {folds.map((f, i) => (
                  <tr key={i} className="border-t border-border">
                    {foldCols.map((c) => {
                      let v = f[c]
                      if (['cagr', 'max_drawdown', 'win_rate'].includes(c)) v = fmtPct(v)
                      else if (typeof v === 'number') v = fmtNum(v, c === 'sharpe' || c === 'profit_factor' ? 2 : 0)
                      else if (typeof v === 'string' && v.includes(' 00:00:00')) v = v.slice(0, 10)
                      return (
                        <td key={c} className="p-2 font-mono-data">
                          {v == null ? '—' : String(v)}
                        </td>
                      )
                    })}
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        ) : null}
      </CardContent>
    </Card>
  )
}

export function MlBacktestPage() {
  const reports = useQuery({
    queryKey: ['backtest-reports'],
    queryFn: () => apiGet<BacktestReportsResponse>('/api/v1/backtest/reports'),
  })
  const [selected, setSelected] = useState<string | null>(null)
  const activeName = selected ?? reports.data?.reports.at(-1) ?? null

  const report = useQuery({
    queryKey: ['backtest-report', activeName],
    queryFn: () => apiGet<BacktestReport>(`/api/v1/backtest/reports/${activeName}`),
    enabled: !!activeName,
  })

  return (
    <AppShell title="ML — Backtest" description="Walk-forward backtest reports produced by backtest/engine.py, with integrity checklist and per-fold detail.">
      <Card>
        <CardHeader>
          <CardTitle>Report</CardTitle>
          <CardDescription>
            {reports.data?.reports.length ? `${reports.data.reports.length} report(s) found in backtest/reports/` : 'No backtest reports found in backtest/reports/'}
          </CardDescription>
        </CardHeader>
        <CardContent>
          {reports.data?.reports.length ? (
            <select
              className="h-9 w-full max-w-md rounded-[var(--radius-token)] border border-border bg-transparent px-3 text-sm"
              value={activeName ?? ''}
              onChange={(e) => setSelected(e.target.value)}
            >
              {reports.data.reports.map((name) => (
                <option key={name} value={name}>
                  {name}
                </option>
              ))}
            </select>
          ) : null}
        </CardContent>
      </Card>

      {report.error ? (
        <p className="mt-4 text-sm text-red">
          Could not reach GET /api/v1/backtest/reports/{'{name}'} — {(report.error as Error).message}
        </p>
      ) : report.data ? (
        <>
          <p className="mt-4 text-sm text-muted-foreground">
            Generated {String(report.data.generated_at ?? '—')} · watchlist size {String(report.data.watchlist_size ?? '—')}
          </p>
          {(['phase1', 'phase2', 'phase3'] as const).map((k) => (
            <PhaseSection key={k} phaseKey={k} phase={report.data?.[k] as Record<string, unknown> | undefined} />
          ))}
        </>
      ) : null}
    </AppShell>
  )
}
