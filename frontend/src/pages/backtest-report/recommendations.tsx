/**
 * pages/backtest-report/recommendations.tsx
 *
 * The page the whole section exists for: given everything backtested, which
 * strategies should this person deploy?
 *
 * Three personas, each with its own gates and weights. The ranking is shown
 * WITH its reasoning — the gates a strategy had to clear, the weights applied,
 * and for anything rejected, exactly which gate it failed. A ranked list whose
 * criteria are invisible is an oracle, and an oracle is not a decision-support
 * tool: the user has to be able to disagree with it on specifics.
 *
 * Missing data fails a gate rather than passing it. Technical strategies have
 * no rolling or YoY figures yet (T13), and the permissive reading would rank
 * every one of them above every Momentum strategy purely by having less known
 * about them.
 */

import { useMemo, useState } from 'react'

import {
  Badge,
  Card,
  CardContent,
  CardDescription,
  CardHeader,
  CardTitle,
} from '@/lib/ui'
import { StrategyLink } from '@/features/backtest-report/components/StrategyLink'
import { TradesLink } from '@/features/backtest-report/components/TradesLink'
import { ReportLayout } from '@/features/backtest-report/components/ReportLayout'
import { cagrOn } from '@/features/backtest-report/columns'
import { EM_DASH, inr, num, pct, rate } from '@/features/backtest-report/format'
import {
  PERSONAS,
  PERSONA_ORDER,
  recommendAll,
  type PersonaId,
  type Recommendation,
} from '@/features/backtest-report/recommendations'
import { layoutProps } from '@/features/backtest-report/sections'
import { useReportPage } from '@/features/backtest-report/useReportPage'

const TOP_N = 5

function GateList({ rec }: { rec: Recommendation }) {
  return (
    <ul className="space-y-0.5 text-xs">
      {rec.gates.map((g) => (
        <li key={g.id} className="flex items-start gap-1.5">
          <span
            aria-hidden="true"
            className={g.passed ? 'text-green' : 'text-red'}
          >
            {g.passed ? '✓' : '✗'}
          </span>
          <span className={g.passed ? 'text-muted-foreground' : 'text-red'}>
            {g.label}
            <span className="text-muted-foreground"> — {g.describe}</span>
          </span>
        </li>
      ))}
    </ul>
  )
}

function PersonaSection({
  personaId,
  recs,
  basis,
}: {
  personaId: PersonaId
  recs: Recommendation[]
  basis: 'pre_tax' | 'post_tax'
}) {
  const persona = PERSONAS[personaId]
  const [showRejected, setShowRejected] = useState(false)

  const passed = recs.filter((r) => r.passed)
  const rejected = recs.filter((r) => !r.passed)
  const top = passed.slice(0, TOP_N)

  return (
    <Card className="mb-4">
      <CardHeader>
        <CardTitle>{persona.label}</CardTitle>
        <CardDescription>{persona.summary}</CardDescription>
        <div className="mt-2 flex flex-wrap gap-1.5 text-xs">
          {/* The weights are part of the answer, not configuration trivia:
              the same strategy list ranks differently under each persona
              purely because of these numbers. */}
          {Object.entries(persona.weights).map(([metric, w]) => (
            <Badge key={metric} variant="outline">
              {metric} {w > 0 ? '+' : ''}
              {w.toFixed(2)}
            </Badge>
          ))}
        </div>
      </CardHeader>
      <CardContent>
        {top.length === 0 ? (
          <p className="text-sm text-muted-foreground">
            No strategy clears every {persona.label} gate. That is a real
            answer — the honest conclusion is to deploy nothing for this
            profile rather than to relax the gates until something passes.
          </p>
        ) : (
          <ol className="space-y-3">
            {top.map((rec) => (
              <li
                key={rec.report.key}
                className="rounded-[var(--radius-token)] border border-border p-3"
              >
                <div className="flex flex-wrap items-baseline justify-between gap-2">
                  <span className="flex items-center gap-2">
                    <span className="text-sm font-semibold text-muted-foreground">
                      #{rec.rank}
                    </span>
                    <StrategyLink
                      strategyKey={rec.report.key}
                      label={rec.report.label}
                      className="text-sm font-semibold"
                    />
                    <Badge variant="outline">{rec.report.channel}</Badge>
                  </span>
                  <TradesLink url={rec.report.tradeBookUrl} />
                </div>

                <dl className="mt-2 grid grid-cols-2 gap-x-6 gap-y-1 text-xs sm:grid-cols-4">
                  <div>
                    <dt className="text-muted-foreground">
                      CAGR ({basis === 'post_tax' ? 'post-tax' : 'pre-tax'})
                    </dt>
                    <dd className="tabular-nums">
                      {rate(cagrOn(rec.report, basis))}
                    </dd>
                  </div>
                  <div>
                    <dt className="text-muted-foreground">Max drawdown</dt>
                    <dd className="tabular-nums">{pct(rec.report.risk.maxDrawdown)}</dd>
                  </div>
                  <div>
                    <dt className="text-muted-foreground">3y median</dt>
                    <dd className="tabular-nums">
                      {rate(
                        rec.report.consistency.rolling.find((w) => w.window === 3)
                          ?.medianCagr ?? null,
                      )}
                    </dd>
                  </div>
                  <div>
                    <dt className="text-muted-foreground">Capital deployed</dt>
                    <dd className="tabular-nums">
                      {inr(rec.report.setup.capitalDeployed)}
                    </dd>
                  </div>
                </dl>

                <details className="mt-2">
                  <summary className="cursor-pointer text-xs text-muted-foreground">
                    Why it ranks here (score {num(rec.score)})
                  </summary>
                  <div className="mt-2 space-y-2">
                    <GateList rec={rec} />
                    <table className="w-full text-xs">
                      <thead className="text-muted-foreground">
                        <tr>
                          <th scope="col" className="text-left font-normal">Metric</th>
                          <th scope="col" className="text-right font-normal">Value</th>
                          <th scope="col" className="text-right font-normal">z</th>
                          <th scope="col" className="text-right font-normal">Weight</th>
                          <th scope="col" className="text-right font-normal">Contribution</th>
                        </tr>
                      </thead>
                      <tbody>
                        {rec.components.map((c) => (
                          <tr key={c.metric}>
                            <td>{c.metric}</td>
                            <td className="text-right tabular-nums">
                              {c.raw == null ? EM_DASH : num(c.raw, 3)}
                            </td>
                            <td className="text-right tabular-nums">{num(c.z)}</td>
                            <td className="text-right tabular-nums">{num(c.weight)}</td>
                            <td className="text-right tabular-nums">
                              {num(c.contribution)}
                            </td>
                          </tr>
                        ))}
                      </tbody>
                    </table>
                  </div>
                </details>
              </li>
            ))}
          </ol>
        )}

        {rejected.length > 0 ? (
          <div className="mt-4">
            <button
              type="button"
              onClick={() => setShowRejected((v) => !v)}
              className="text-xs text-primary hover:underline"
              aria-expanded={showRejected}
            >
              {showRejected ? 'Hide' : 'Show'} {rejected.length} strateg
              {rejected.length === 1 ? 'y' : 'ies'} that failed a gate
            </button>
            {showRejected ? (
              <ul className="mt-2 space-y-1 text-xs">
                {rejected.map((rec) => (
                  <li key={rec.report.key} className="flex flex-wrap gap-2">
                    <StrategyLink
                      strategyKey={rec.report.key}
                      label={rec.report.label}
                    />
                    <span className="text-red">
                      failed: {rec.failedGates.join(', ')}
                    </span>
                  </li>
                ))}
              </ul>
            ) : null}
          </div>
        ) : null}
      </CardContent>
    </Card>
  )
}

export function BacktestRecommendationsPage() {
  const page = useReportPage()

  const byPersona = useMemo(
    () => recommendAll(page.strategies, page.params.taxBasis),
    [page.strategies, page.params.taxBasis],
  )

  return (
    <ReportLayout
      title="Backtest Report — Recommendations"
      description="Deploy candidates by investor profile. Each list shows the gates a strategy had to clear and the weights that ranked it, so you can disagree with the ranking on specifics."
      {...layoutProps(page)}
    >
      {page.isLoading ? (
        <p className="text-sm text-muted-foreground">Loading…</p>
      ) : page.strategies.length === 0 ? (
        <Card>
          <CardContent className="pt-6 text-sm text-muted-foreground">
            No strategy reports cover the selected window.
          </CardContent>
        </Card>
      ) : (
        PERSONA_ORDER.map((id) => (
          <PersonaSection
            key={id}
            personaId={id}
            recs={byPersona[id]}
            basis={page.params.taxBasis}
          />
        ))
      )}
    </ReportLayout>
  )
}
