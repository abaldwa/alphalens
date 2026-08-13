/**
 * pages/backtest-report/strategy-detail.tsx
 *
 * Everything known about one strategy: all five attribute groups, the full
 * channel-specific setup, and the trade book.
 *
 * This page is where the pruned columns went. The decision tables deliberately
 * drop raw run ids, the legacy trading-day CAGR and the outlier-integrity
 * fields so they stop competing with the numbers a decision turns on — but
 * nothing collected is lost, it is all reachable here. That is the difference
 * between pruning a table and discarding data.
 */

import { useParams } from 'react-router-dom'

import {
  Badge,
  AppShell,
  Card,
  CardContent,
  CardDescription,
  CardHeader,
  CardTitle,
} from '@/lib/ui'
import { MatrixTable } from '@/features/backtest-report/components/MatrixTable'
import { TradesLink } from '@/features/backtest-report/components/TradesLink'
import { EM_DASH, days, inr, num, pct, rate, rateDelta } from '@/features/backtest-report/format'
import { useReportData } from '@/features/backtest-report/useReportData'
import type { StrategyReport, StrategySetup } from '@/features/backtest-report/types'

function Field({
  label,
  value,
  hint,
}: {
  label: string
  value: string | number | null | undefined
  hint?: string
}) {
  return (
    <div>
      <dt className="text-xs text-muted-foreground" title={hint}>
        {label}
      </dt>
      <dd className="tabular-nums text-sm">
        {value == null || value === '' ? EM_DASH : value}
      </dd>
    </div>
  )
}

/** The channel-specific half of `setup`. A screener template and a momentum
 * variant genuinely do not share fields, so they are rendered per channel
 * rather than flattened into columns that are empty for three channels out of
 * four. */
function SetupFields({ setup }: { setup: StrategySetup }) {
  switch (setup.channel) {
    case 'momentum':
      return (
        <>
          <Field label="Lookback" value={setup.lookbackMonths ? `${setup.lookbackMonths}mo` : null} />
          <Field label="Rebalance" value={setup.rebalanceFreq} />
          <Field label="Top N" value={setup.topN} />
          <Field
            label="Rank band"
            value={
              setup.rankStart != null && setup.rankEnd != null
                ? `${setup.rankStart}-${setup.rankEnd}`
                : null
            }
          />
          <Field label="Grace cycles" value={setup.graceCycles} />
          <Field label="Category" value={setup.category} />
        </>
      )
    case 'technical':
      return (
        <>
          <Field label="Template" value={setup.templateName} />
          <Field label="Category" value={setup.templateCategory} />
          <Field label="Exit policy" value={setup.exitPolicyVariant} />
          <Field label="Holding horizon" value={setup.holdingHorizon} />
          <Field label="Entry conditions" value={setup.entryConditions.length || null} />
        </>
      )
    case 'fundamental':
      return (
        <>
          <Field label="Preset" value={setup.preset} />
          <Field label="Score function" value={setup.scoreFunction} />
          <Field label="Kind" value={setup.kind} />
          <Field label="Rebalance" value={setup.rebalanceFreq} />
          <Field label="Top N" value={setup.topN} />
          <Field
            label="Excluded sectors"
            value={setup.excludedSectors.join(', ') || null}
          />
        </>
      )
    case 'ml':
      return (
        <>
          <Field label="Model" value={setup.modelName} />
          <Field label="Version" value={setup.modelVersion} />
          <Field label="Horizon" value={setup.horizonDays ? `${setup.horizonDays}d` : null} />
          <Field label="Signal threshold" value={num(setup.signalThreshold)} />
          <Field
            label="Meta-labeler"
            value={setup.metaLabeler == null ? null : setup.metaLabeler ? 'yes' : 'no'}
          />
        </>
      )
  }
}

function PendingNote({ report }: { report: StrategyReport }) {
  const entries = Object.entries(report.pending)
  if (entries.length === 0) return null
  return (
    <Card className="mb-4">
      <CardHeader>
        <CardTitle className="text-sm">Not measured yet</CardTitle>
        <CardDescription>
          These are gaps in the engine, not properties of the strategy. Each
          names the backlog item that will fill it.
        </CardDescription>
      </CardHeader>
      <CardContent>
        <ul className="space-y-1 text-xs">
          {entries.map(([path, p]) => (
            <li key={path}>
              <Badge variant="outline">{p.backlogId}</Badge>{' '}
              <span className="font-mono">{path}</span>{' '}
              <span className="text-muted-foreground">— {p.reason}</span>
            </li>
          ))}
        </ul>
      </CardContent>
    </Card>
  )
}

export function BacktestStrategyDetailPage() {
  const { key = '' } = useParams<{ key: string }>()
  const decoded = decodeURIComponent(key)
  const { strategies, isLoading } = useReportData()
  const report = strategies.find((s) => s.key === decoded)

  if (isLoading) {
    return (
      <AppShell title="Strategy">
        <p className="text-sm text-muted-foreground">Loading…</p>
      </AppShell>
    )
  }

  if (!report) {
    return (
      <AppShell title="Strategy not found">
        <p className="text-sm text-muted-foreground">
          No report carries the strategy <span className="font-mono">{decoded}</span>.
          It may not have been backtested yet, or the link may predate a rename.
        </p>
      </AppShell>
    )
  }

  const { returns, risk, tradeQuality, consistency, setup, income } = report

  const yoyColumns = consistency.yoy.map((y) => ({ key: y.fyLabel, label: y.fyLabel }))
  const yoyRow = {
    key: report.key,
    label: report.label,
    values: Object.fromEntries(consistency.yoy.map((y) => [y.fyLabel, y.returnPct])),
  }

  return (
    <AppShell
      title={report.label}
      description={`${report.channel} strategy — every attribute recorded for this backtest.`}
    >
      <div className="mb-4 flex flex-wrap items-center gap-3">
        <Badge>{report.channel}</Badge>
        <span className="font-mono text-xs text-muted-foreground">{report.key}</span>
        <TradesLink url={report.tradeBookUrl} label="Trade book" />
      </div>

      <div className="grid gap-4 lg:grid-cols-2">
        <Card>
          <CardHeader>
            <CardTitle>Returns</CardTitle>
            <CardDescription>
              Every figure is an annual rate, never a total over the window.
            </CardDescription>
          </CardHeader>
          <CardContent>
            <dl className="grid grid-cols-2 gap-3 sm:grid-cols-3">
              <Field label="CAGR (post-tax)" value={rate(returns.cagrPostTax)} />
              <Field label="CAGR (pre-tax)" value={rate(returns.cagrPreTax)} />
              <Field label="XIRR" value={rate(returns.xirr)} />
              <Field label="SIP XIRR" value={rate(returns.sipXirr)} />
              <Field
                label={`Benchmark${returns.benchmarkIndexName ? ` (${returns.benchmarkIndexName})` : ''}`}
                value={rate(returns.benchmarkCagr)}
                hint={returns.benchmarkCaveat ?? undefined}
              />
              <Field label="Excess" value={rateDelta(returns.excessReturn)} />
              <Field label="Final capital" value={inr(returns.finalCapital)} />
              <Field label="Contributed" value={inr(returns.totalContributed)} />
            </dl>
            {returns.benchmarkCaveat ? (
              <p className="mt-2 text-xs text-amber">{returns.benchmarkCaveat}</p>
            ) : null}
          </CardContent>
        </Card>

        <Card>
          <CardHeader>
            <CardTitle>Risk</CardTitle>
          </CardHeader>
          <CardContent>
            <dl className="grid grid-cols-2 gap-3 sm:grid-cols-3">
              <Field label="Max drawdown" value={pct(risk.maxDrawdown)} />
              <Field label="Sharpe" value={num(risk.sharpe)} />
              <Field label="Sortino" value={num(risk.sortino)} />
              <Field label="Calmar" value={num(risk.calmar)} />
              <Field label="Volatility" value={pct(risk.volatility)} />
            </dl>
          </CardContent>
        </Card>

        <Card>
          <CardHeader>
            <CardTitle>Trade quality</CardTitle>
            <CardDescription>
              Per-trade outcomes, so these are plain percentages — a three-day
              trade has no annual rate.
            </CardDescription>
          </CardHeader>
          <CardContent>
            <dl className="grid grid-cols-2 gap-3 sm:grid-cols-3">
              <Field label="Trades" value={tradeQuality.nTrades} />
              <Field label="Closed" value={tradeQuality.nClosedTrades} />
              <Field label="Open" value={tradeQuality.nOpenTrades} />
              <Field label="Win rate" value={pct(tradeQuality.winRate)} />
              <Field label="Profit factor" value={num(tradeQuality.profitFactor)} />
              <Field label="Avg hold" value={days(tradeQuality.avgHoldDays)} />
              <Field label="Churn/yr" value={num(tradeQuality.churnPerYear, 1)} />
              <Field label="Avg winner" value={pct(tradeQuality.avgWinnerPct)} />
              <Field label="Avg loser" value={pct(tradeQuality.avgLoserPct)} />
              <Field label="Turnover" value={num(tradeQuality.turnoverRatio)} />
            </dl>
          </CardContent>
        </Card>

        <Card>
          <CardHeader>
            <CardTitle>Setup</CardTitle>
            <CardDescription>
              What this strategy actually is — the definition a deployment
              would carry forward.
            </CardDescription>
          </CardHeader>
          <CardContent>
            <dl className="grid grid-cols-2 gap-3 sm:grid-cols-3">
              <Field label="Universe" value={setup.universe} />
              <Field
                label="Window"
                value={
                  setup.window.startDate
                    ? `${setup.window.startDate} → ${setup.window.endDate ?? ''}`
                    : null
                }
              />
              <Field label="Capital" value={inr(setup.capitalDeployed)} />
              <Field label="Capital mode" value={setup.capitalMode} />
              <Field label="Benchmark" value={setup.benchmarkIndexName} />
              <Field label="Exit variant" value={setup.exitCriterion.variant} />
              <SetupFields setup={setup} />
            </dl>
            {setup.filters.length > 0 ? (
              <div className="mt-3 flex flex-wrap gap-1.5">
                {setup.filters.map((f) => (
                  <Badge key={f.filterId} variant="outline">
                    {f.filterId}
                  </Badge>
                ))}
              </div>
            ) : null}
          </CardContent>
        </Card>
      </div>

      {consistency.rolling.length > 0 ? (
        <Card className="mt-4">
          <CardHeader>
            <CardTitle>Rolling windows</CardTitle>
          </CardHeader>
          <CardContent>
            <table className="w-full text-xs">
              <thead className="text-muted-foreground">
                <tr>
                  <th scope="col" className="text-left font-normal">Window</th>
                  <th scope="col" className="text-right font-normal">Worst</th>
                  <th scope="col" className="text-right font-normal">Median</th>
                  <th scope="col" className="text-right font-normal">Best</th>
                  <th scope="col" className="text-right font-normal">Positive</th>
                  <th scope="col" className="text-right font-normal">Windows</th>
                </tr>
              </thead>
              <tbody>
                {consistency.rolling.map((w) => (
                  <tr key={w.window} className="border-t border-border">
                    <td>{w.window}y</td>
                    <td className="text-right tabular-nums">{rate(w.minCagr)}</td>
                    <td className="text-right tabular-nums font-semibold">
                      {rate(w.medianCagr)}
                    </td>
                    <td className="text-right tabular-nums">{rate(w.maxCagr)}</td>
                    <td className="text-right tabular-nums">{pct(w.positiveShare)}</td>
                    <td className="text-right tabular-nums">{w.nWindows ?? EM_DASH}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </CardContent>
        </Card>
      ) : null}

      {consistency.yoy.length > 0 ? (
        <Card className="mt-4">
          <CardHeader>
            <CardTitle>Year on year</CardTitle>
          </CardHeader>
          <CardContent>
            <MatrixTable
              columns={yoyColumns}
              rows={[yoyRow]}
              boundaries={{ red: 0, green: 0.18 }}
              caption={`Year-on-year returns for ${report.label}`}
            />
          </CardContent>
        </Card>
      ) : null}

      {income ? (
        <Card className="mt-4">
          <CardHeader>
            <CardTitle>Regular returns (income mode)</CardTitle>
            <CardDescription>
              Withdrawing the excess each year rather than compounding it.
            </CardDescription>
          </CardHeader>
          <CardContent>
            <dl className="grid grid-cols-2 gap-3 sm:grid-cols-4">
              <Field label="Withdrawn" value={inr(income.totalWithdrawn)} />
              <Field label="Backfilled" value={inr(income.totalInjected)} />
              <Field label="Profitable years" value={pct(income.yearsSurvivedPct)} />
              <Field label="Years" value={income.nYears} />
              <Field
                label="After a losing year"
                value={
                  income.topUpAfterLoss == null
                    ? null
                    : income.topUpAfterLoss
                      ? 'Topped back up'
                      : 'Runs on current capital'
                }
              />
            </dl>
          </CardContent>
        </Card>
      ) : null}

      <div className="mt-4">
        <PendingNote report={report} />
      </div>
    </AppShell>
  )
}
