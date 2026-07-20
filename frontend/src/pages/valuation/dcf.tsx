import { useState } from 'react'
import { useQuery } from '@tanstack/react-query'

import { AppShell, Badge, Button, Card, CardContent, CardHeader, CardTitle, InfoTooltip, StatCard, Table, TableHeader, TableBody, TableRow, TableHead, TableCell } from '@/lib/ui'
import { apiGet } from '@/shared/api/client'

interface ValuationResult {
  ticker: string
  as_of_date: string | null
  lifecycle_stage: string | null
  intrinsic_value: number | null
  current_price: number | null
  valuation_gap_pct: number | null
  margin_of_safety: number | null
  wacc: number | null
  cost_of_equity: number | null
  dcf_model_type: string | null
  scenario_bull: number | null
  scenario_base: number | null
  scenario_bear: number | null
  mc_probability_undervalued: number | null
}

interface SensitivityCell {
  wacc: number
  terminal_growth: number
  intrinsic_value: number | null
}

interface SensitivityResponse {
  ticker: string
  base_wacc: number
  base_terminal_growth: number
  table: SensitivityCell[]
}

function fmtMoney(v: number | null | undefined): string {
  return v == null ? '—' : `₹${v.toLocaleString('en-IN', { maximumFractionDigits: 2 })}`
}

function fmtPct(v: number | null | undefined): string {
  return v == null ? '—' : `${(v * 100).toFixed(1)}%`
}

function valuationTone(mos: number | null | undefined): 'green' | 'red' | 'amber' | 'default' {
  if (mos == null) return 'default'
  if (mos > 0.15) return 'green'
  if (mos < -0.15) return 'red'
  return 'amber'
}

function valuationLabel(mos: number | null | undefined): string {
  if (mos == null) return 'N/A'
  if (mos > 0.15) return 'Undervalued'
  if (mos < -0.15) return 'Overvalued'
  return 'Fairly Valued'
}

export function DcfPage() {
  const [tickerInput, setTickerInput] = useState('RELIANCE')
  const [ticker, setTicker] = useState('RELIANCE')

  const summary = useQuery({
    queryKey: ['valuation-dcf', ticker],
    queryFn: () => apiGet<ValuationResult>(`/api/v1/valuation/${ticker}`),
  })

  const sensitivity = useQuery({
    queryKey: ['valuation-dcf-sensitivity', ticker],
    queryFn: () => apiGet<SensitivityResponse>(`/api/v1/valuation/${ticker}/sensitivity`),
  })

  const r = summary.data
  const s = sensitivity.data
  const waccVals = s ? Array.from(new Set(s.table.map((c) => c.wacc))).sort((a, b) => a - b) : []
  const growthVals = s ? Array.from(new Set(s.table.map((c) => c.terminal_growth))).sort((a, b) => a - b) : []
  const byKey = new Map<string, number | null>()
  s?.table.forEach((c) => byKey.set(`${c.wacc}|${c.terminal_growth}`, c.intrinsic_value))

  return (
    <AppShell
      title="Valuation — DCF"
      description="Single-ticker Damodaran DCF valuation with WACC × terminal-growth sensitivity."
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
      {summary.error ? (
        <p className="text-sm text-red">
          Could not reach GET /api/v1/valuation/{ticker} — {(summary.error as Error).message}
        </p>
      ) : (
        <>
          <div className="grid grid-cols-1 gap-4 sm:grid-cols-4">
            <StatCard
              label="Overall Valuation"
              value={<Badge variant={valuationTone(r?.margin_of_safety) === 'green' ? 'success' : valuationTone(r?.margin_of_safety) === 'red' ? 'destructive' : 'warning'}>{valuationLabel(r?.margin_of_safety)}</Badge>}
            />
            <StatCard label="CMP" value={fmtMoney(r?.current_price)} />
            <StatCard
              label={
                <span className="inline-flex items-center gap-1">
                  Intrinsic Value <InfoTooltip>DCF-estimated fair value per share, discounting projected future cash flows back at the model's WACC.</InfoTooltip>
                </span>
              }
              value={fmtMoney(r?.intrinsic_value)}
            />
            <StatCard
              label={
                <span className="inline-flex items-center gap-1">
                  % Difference <InfoTooltip>Gap between current market price and DCF intrinsic value, as a percentage of intrinsic value.</InfoTooltip>
                </span>
              }
              value={r?.valuation_gap_pct != null ? `${(-r.valuation_gap_pct * 100).toFixed(1)}%` : '—'}
              tone={r?.valuation_gap_pct != null ? (r.valuation_gap_pct < 0 ? 'green' : 'red') : 'default'}
            />
          </div>

          <div className="mt-4 grid grid-cols-1 gap-4 sm:grid-cols-4">
            <StatCard label="Lifecycle Stage" value={r?.lifecycle_stage ?? '—'} />
            <StatCard
              label={
                <span className="inline-flex items-center gap-1">
                  DCF Model <InfoTooltip>Which DCF variant was used (e.g. standard, distressed, excess-return) — chosen based on the company's lifecycle stage and financial health.</InfoTooltip>
                </span>
              }
              value={r?.dcf_model_type ?? '—'}
            />
            <StatCard
              label={
                <span className="inline-flex items-center gap-1">
                  WACC <InfoTooltip>Weighted Average Cost of Capital — the discount rate applied to projected future cash flows. Higher WACC lowers the intrinsic value estimate.</InfoTooltip>
                </span>
              }
              value={fmtPct(r?.wacc)}
            />
            <StatCard
              label={
                <span className="inline-flex items-center gap-1">
                  Cost of Equity <InfoTooltip>The required return demanded by equity holders, one of the inputs used to derive WACC.</InfoTooltip>
                </span>
              }
              value={fmtPct(r?.cost_of_equity)}
            />
          </div>

          <div className="mt-4">
            <Card>
              <CardHeader>
                <CardTitle>Monte Carlo Scenarios</CardTitle>
              </CardHeader>
              <CardContent>
                {r && (r.scenario_bull != null || r.scenario_base != null || r.scenario_bear != null) ? (
                  <div className="grid grid-cols-1 gap-4 sm:grid-cols-4">
                    <StatCard
                      label={
                        <span className="inline-flex items-center gap-1">
                          Bear (P10) <InfoTooltip>10th percentile intrinsic value across Monte Carlo simulation runs — a pessimistic-case estimate.</InfoTooltip>
                        </span>
                      }
                      value={fmtMoney(r.scenario_bear)}
                    />
                    <StatCard
                      label={
                        <span className="inline-flex items-center gap-1">
                          Base (Median) <InfoTooltip>Median intrinsic value across Monte Carlo simulation runs.</InfoTooltip>
                        </span>
                      }
                      value={fmtMoney(r.scenario_base)}
                    />
                    <StatCard
                      label={
                        <span className="inline-flex items-center gap-1">
                          Bull (P90) <InfoTooltip>90th percentile intrinsic value across Monte Carlo simulation runs — an optimistic-case estimate.</InfoTooltip>
                        </span>
                      }
                      value={fmtMoney(r.scenario_bull)}
                    />
                    <StatCard
                      label={
                        <span className="inline-flex items-center gap-1">
                          P(Undervalued) <InfoTooltip>Share of Monte Carlo simulation runs where intrinsic value came out above the current market price.</InfoTooltip>
                        </span>
                      }
                      value={fmtPct(r.mc_probability_undervalued)}
                    />
                  </div>
                ) : (
                  <p className="text-sm text-muted-foreground">
                    No Monte Carlo scenario data (distressed or excess-return models skip MC).
                  </p>
                )}
              </CardContent>
            </Card>
          </div>
        </>
      )}

      <div className="mt-4">
        <Card>
          <CardHeader>
            <CardTitle className="inline-flex items-center gap-1">
              WACC × Terminal Growth Sensitivity
              <InfoTooltip>
                How the DCF intrinsic value estimate changes across a grid of WACC (discount rate) and terminal
                growth rate (assumed perpetual growth beyond the forecast period) assumptions. The highlighted cell
                is the model's base-case combination.
              </InfoTooltip>
            </CardTitle>
          </CardHeader>
          <CardContent>
            {sensitivity.error ? (
              <p className="text-sm text-red">
                Could not reach GET /api/v1/valuation/{ticker}/sensitivity — {(sensitivity.error as Error).message}
              </p>
            ) : !s ? (
              <p className="text-sm text-muted-foreground">Loading…</p>
            ) : (
              <Table>
                <TableHeader>
                  <TableRow>
                    <TableHead>WACC \ Growth</TableHead>
                    {growthVals.map((g) => (
                      <TableHead key={g} className="font-mono-data normal-case">
                        {(g * 100).toFixed(0)}%
                      </TableHead>
                    ))}
                  </TableRow>
                </TableHeader>
                <TableBody>
                  {waccVals.map((w) => (
                    <TableRow key={w}>
                      <TableHead className="font-mono-data font-semibold normal-case">{(w * 100).toFixed(1)}%</TableHead>
                      {growthVals.map((g) => {
                        const v = byKey.get(`${w}|${g}`)
                        const isBase = Math.abs(w - s.base_wacc) < 1e-6 && Math.abs(g - s.base_terminal_growth) < 1e-6
                        return (
                          <TableCell key={g} className={`text-right font-mono-data ${isBase ? 'bg-accent font-bold' : ''}`}>
                            {v != null ? fmtMoney(v) : '—'}
                          </TableCell>
                        )
                      })}
                    </TableRow>
                  ))}
                </TableBody>
              </Table>
            )}
          </CardContent>
        </Card>
      </div>
    </AppShell>
  )
}
