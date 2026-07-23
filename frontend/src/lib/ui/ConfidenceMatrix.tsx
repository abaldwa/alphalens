import { useQuery } from '@tanstack/react-query'

import { Card, CardContent, CardHeader, CardTitle } from '@/lib/ui/primitives/card'
import { Skeleton } from '@/lib/ui/primitives/skeleton'
import { SignalBadge } from '@/lib/ui/SignalBadge'
import { getFundamentalScores } from '@/shared/api/fundamentals'
import { getForensicRow } from '@/shared/api/forensic'
import { getValuation } from '@/shared/api/valuation'
import { getTaRecommendations, deriveTechnicalScore } from '@/shared/api/technical'
import { getMlSignals, deriveMlScore } from '@/shared/api/ml'

interface EngineRow {
  engine: string
  score: number | null
  direction: string | null
  stat: string
}

function todayStr() {
  return new Date().toISOString().slice(0, 10)
}

/**
 * Centralized "cross-verification" panel: one row per analytical engine
 * (Technical / ML / Fundamentals / Forensic / Valuation) for a symbol, so
 * a user can compare all five at a glance instead of visiting five pages.
 * Fundamentals, Forensic, and Valuation each have a native composite score
 * from the backend; Technical and ML don't, so those two rows are derived
 * client-side (see technical.ts/ml.ts `derive*Score`) and are not yet
 * authoritative — a future `/api/v1/recommendation/{ticker}` aggregator
 * should replace that client-side math.
 */
export function ConfidenceMatrix({ ticker, date = todayStr() }: { ticker: string; date?: string }) {
  const fundamentals = useQuery({
    queryKey: ['confidence-matrix', 'fundamentals', ticker],
    queryFn: () => getFundamentalScores(ticker),
    enabled: !!ticker,
  })
  const forensic = useQuery({
    queryKey: ['confidence-matrix', 'forensic', ticker],
    queryFn: () => getForensicRow(ticker),
    enabled: !!ticker,
  })
  const valuation = useQuery({
    queryKey: ['confidence-matrix', 'valuation', ticker],
    queryFn: () => getValuation(ticker),
    enabled: !!ticker,
  })
  const technical = useQuery({
    queryKey: ['confidence-matrix', 'technical', ticker],
    queryFn: () => getTaRecommendations(ticker),
    enabled: !!ticker,
  })
  const ml = useQuery({
    queryKey: ['confidence-matrix', 'ml', ticker, date],
    queryFn: () => getMlSignals(ticker, date),
    enabled: !!ticker,
  })

  const isLoading = fundamentals.isLoading || forensic.isLoading || valuation.isLoading || technical.isLoading || ml.isLoading

  const taScore = technical.data ? deriveTechnicalScore(technical.data.rows) : { score: null, direction: null }
  const mlScore = ml.data ? deriveMlScore(ml.data) : { score: null, direction: null }

  const rows: EngineRow[] = [
    {
      engine: 'Technical',
      score: taScore.score,
      direction: taScore.direction,
      stat: technical.data ? `${technical.data.count} matched screens` : '—',
    },
    {
      engine: 'ML Models',
      score: mlScore.score,
      direction: mlScore.direction,
      stat: ml.data?.[0] ? `q50 ${((ml.data[0].q50_return ?? 0) * 100).toFixed(1)}%` : '—',
    },
    {
      engine: 'Fundamentals',
      score: fundamentals.data?.quality_score ?? null,
      direction: scoreToDirection(fundamentals.data?.quality_score),
      stat: fundamentals.data ? `growth ${fundamentals.data.growth_score?.toFixed(0) ?? '—'}` : '—',
    },
    {
      engine: 'Forensic',
      score: forensic.data?.forensic_composite ?? null,
      direction: forensic.data?.forensic_flag_label ?? null,
      stat: forensic.data?.forensic_flag_label ? `flag: ${forensic.data.forensic_flag_label}` : '—',
    },
    {
      engine: 'Valuation',
      score: valuation.data?.margin_of_safety != null ? valuation.data.margin_of_safety * 100 : null,
      direction: scoreToDirection(valuation.data?.margin_of_safety != null ? valuation.data.margin_of_safety * 100 : null),
      stat: valuation.data?.margin_of_safety != null ? `margin of safety ${(valuation.data.margin_of_safety * 100).toFixed(1)}%` : '—',
    },
  ]

  return (
    <Card>
      <CardHeader>
        <CardTitle>Confidence Matrix</CardTitle>
      </CardHeader>
      <CardContent>
        {isLoading ? (
          <div className="flex flex-col gap-2">
            {Array.from({ length: 5 }).map((_, i) => (
              <Skeleton key={i} className="h-10 w-full" />
            ))}
          </div>
        ) : (
          <div className="grid grid-cols-1 gap-2">
            {rows.map((r) => (
              <div key={r.engine} className="grid grid-cols-[1fr_auto_auto_1.5fr] items-center gap-3 border-b border-border py-2 last:border-0">
                <div className="text-sm font-medium">{r.engine}</div>
                <div className="font-mono-data text-sm tabular-nums">{r.score == null ? '—' : r.score.toFixed(0)}</div>
                <SignalBadge direction={r.direction} />
                <div className="truncate text-xs text-muted-foreground">{r.stat}</div>
              </div>
            ))}
          </div>
        )}
      </CardContent>
    </Card>
  )
}

function scoreToDirection(score: number | null | undefined): string | null {
  if (score == null) return null
  if (score >= 60) return 'buy'
  if (score <= 40) return 'sell'
  return 'hold'
}
