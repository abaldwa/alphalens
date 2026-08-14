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
import { MatrixTable } from '@/features/backtest-report/ui/MatrixTable'
import { TradesLink } from '@/features/backtest-report/ui/TradesLink'
import { EM_DASH, days, inr, num, pct, rate, rateDelta } from '@/features/backtest-report/core/format'
import { useReportData } from '@/features/backtest-report/data/useReportData'
import { useStrategyDefinition } from '@/features/backtest-report/data/useStrategyDefinition'
import type { RegistryFilter, RegistryStrategy } from '@/shared/api/strategies'
import type { StrategyReport, StrategySetup } from '@/features/backtest-report/core/types'

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

/**
 * The strategy's DEFINITION, straight from strategy_registry (A95).
 *
 * Distinct from the "Setup" card below it, and deliberately so: Setup shows
 * what THIS RUN was configured with, read off the run row. This card shows
 * what the strategy IS, read off the registry. When they disagree, that is a
 * real finding — a run executed against a definition that has since been
 * revised — and it is only visible because the two are fetched from different
 * places rather than one being derived from the other.
 */
function DefinitionCard({
  strategy,
  filters,
  isLoading,
  error,
}: {
  strategy: RegistryStrategy | undefined
  filters: RegistryFilter[]
  isLoading: boolean
  error: Error | null
}) {
  return (
    <Card>
      <CardHeader>
        <CardTitle>Definition</CardTitle>
        <CardDescription>
          From <span className="font-mono text-xs">strategy_registry</span> — the same row the
          backtest and any deployment read, not re-derived from the run.
        </CardDescription>
      </CardHeader>
      <CardContent>
        {isLoading ? (
          <p className="text-sm text-muted-foreground">Loading definition…</p>
        ) : error ? (
          // Surfaced, not swallowed: a missing definition means this run
          // cannot be traced to a declared strategy, which is worth seeing.
          <p className="text-sm text-muted-foreground">
            Definition unavailable — {error.message}
          </p>
        ) : !strategy ? (
          <p className="text-sm text-muted-foreground">
            This run carries no registry entry. Runs recorded before the strategy was migrated
            into <span className="font-mono text-xs">strategy_registry</span> have no definition
            to show.
          </p>
        ) : (
          <div className="space-y-4">
            <dl className="grid grid-cols-2 gap-3 sm:grid-cols-3">
              <Field label="Label" value={strategy.display_label} />
              <Field label="Category" value={strategy.category} />
              <Field
                label="Version"
                value={strategy.version}
                hint="The definition revision. A run must be explained with the version it executed against."
              />
              <Field label="Status" value={strategy.status} />
              <Field label="Valid from" value={strategy.valid_from} />
              <Field label="Valid to" value={strategy.valid_to ?? 'current'} />
            </dl>

            {strategy.description ? (
              <p className="text-sm text-muted-foreground">{strategy.description}</p>
            ) : null}

            <div>
              <h4 className="mb-1 text-xs font-medium text-muted-foreground">
                Entry criterion
                {strategy.entry_criterion.length > 0
                  ? ` (${strategy.entry_criterion.length}, in order)`
                  : ''}
              </h4>
              {strategy.entry_criterion.length === 0 ? (
                <p className="text-sm text-muted-foreground">
                  None declared — this strategy selects by ranking rather than by predicates.
                </p>
              ) : (
                // Rendered in declared order and never sorted: for a composing
                // entry criterion the order is part of the meaning.
                <ol className="space-y-1">
                  {strategy.entry_criterion.map((p, i) => (
                    <li key={i} className="font-mono text-xs tabular-nums">
                      {[p.feature, p.op, p.feature2 ?? (p.value as string | number | undefined)]
                        .filter((x) => x !== undefined && x !== null)
                        .join(' ')}
                    </li>
                  ))}
                </ol>
              )}
            </div>

            <div>
              <h4 className="mb-1 text-xs font-medium text-muted-foreground">Exit criterion</h4>
              <p className="font-mono text-xs">
                {(strategy.exit_criterion?.variant as string | undefined) ?? EM_DASH}
              </p>
            </div>

            <div>
              <h4 className="mb-1 text-xs font-medium text-muted-foreground">
                Filters {filters.length > 0 ? `(${filters.length})` : ''}
              </h4>
              {filters.length === 0 ? (
                <p className="text-sm text-muted-foreground">None declared.</p>
              ) : (
                <ul className="space-y-1">
                  {filters.map((f) => (
                    <li key={f.filter_id} className="text-xs">
                      <span className="font-mono">{f.filter_id}</span>
                      {f.name ? <span className="text-muted-foreground"> — {f.name}</span> : null}
                    </li>
                  ))}
                </ul>
              )}
            </div>

            {strategy.source_ref ? (
              <p className="text-xs text-muted-foreground">
                Source: <span className="font-mono">{strategy.source_ref}</span>
              </p>
            ) : null}
          </div>
        )}
      </CardContent>
    </Card>
  )
}

export function BacktestStrategyDetailPage() {
  const { key = '' } = useParams<{ key: string }>()
  const decoded = decodeURIComponent(key)
  const { strategies, isLoading } = useReportData()
  const report = strategies.find((s) => s.key === decoded)
  // The report's own key IS the registry key (A89 made the engine emit it),
  // so no client-side parsing is needed to look the definition up — which is
  // the whole point of A95.
  const definition = useStrategyDefinition(report?.key)

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
        <DefinitionCard
          strategy={definition.strategy}
          filters={definition.filters}
          isLoading={definition.isLoading}
          error={definition.error}
        />

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
