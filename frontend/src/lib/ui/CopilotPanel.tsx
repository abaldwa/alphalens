import * as React from 'react'
import { Sparkles } from 'lucide-react'

import { Button } from '@/lib/ui/primitives/button'
import { Card, CardContent, CardHeader, CardTitle } from '@/lib/ui/primitives/card'
import { Badge } from '@/lib/ui/primitives/badge'
import { Sheet, SheetContent, SheetHeader, SheetTitle, SheetTrigger } from '@/lib/ui/primitives/sheet'
import {
  checkDedup,
  queryStrategy,
  runBacktest,
  saveStrategy,
  type BacktestResult,
  type DedupResult,
  type StrategySpec,
} from '@/shared/api/copilot'
import { ApiError } from '@/shared/api/client'

type Step = 'idle' | 'loading' | 'spec' | 'error'

/**
 * Global Co-Pilot entry point — mounted once inside AppShell so it's
 * available on every page. Flow: NL query -> generated StrategySpec
 * (with any `unresolved` fields shown, never hidden) -> optional dedup
 * check against existing screener templates / saved strategies ->
 * backtest -> save. Every number shown comes straight from the backend;
 * any metric the backend couldn't compute is rendered as "not available"
 * with its reason, never blank or zeroed.
 */
export function CopilotPanel() {
  const [open, setOpen] = React.useState(false)
  const [query, setQuery] = React.useState('')
  const [step, setStep] = React.useState<Step>('idle')
  const [error, setError] = React.useState<string | null>(null)
  const [spec, setSpec] = React.useState<StrategySpec | null>(null)
  const [dedupResult, setDedupResult] = React.useState<DedupResult | null>(null)
  const [backtestResult, setBacktestResult] = React.useState<BacktestResult | null>(null)
  const [saveStatus, setSaveStatus] = React.useState<string | null>(null)

  const reset = () => {
    setQuery('')
    setStep('idle')
    setError(null)
    setSpec(null)
    setDedupResult(null)
    setBacktestResult(null)
    setSaveStatus(null)
  }

  const describeError = (err: unknown): string => {
    if (err instanceof ApiError) {
      if (err.status === 503) return 'Co-Pilot is not configured yet — OPENROUTER_API_KEY is missing on the backend.'
      if (err.status === 502) return 'The LLM call failed. Try again, or rephrase the query.'
      return err.message
    }
    return err instanceof Error ? err.message : String(err)
  }

  const handleAsk = async () => {
    if (!query.trim()) return
    setStep('loading')
    setError(null)
    setDedupResult(null)
    setBacktestResult(null)
    setSaveStatus(null)
    try {
      const result = await queryStrategy(query)
      setSpec(result)
      setStep('spec')
    } catch (err) {
      setError(describeError(err))
      setStep('error')
    }
  }

  const handleDedupCheck = async () => {
    if (!spec) return
    try {
      setDedupResult(await checkDedup(spec))
    } catch (err) {
      setError(describeError(err))
    }
  }

  const handleBacktest = async () => {
    if (!spec) return
    try {
      setBacktestResult(await runBacktest(spec))
    } catch (err) {
      setError(describeError(err))
    }
  }

  const handleSave = async () => {
    if (!spec) return
    try {
      const result = await saveStrategy(spec)
      setSaveStatus(`Saved as "${result.name}".`)
    } catch (err) {
      setError(describeError(err))
    }
  }

  return (
    <Sheet
      open={open}
      onOpenChange={(next) => {
        setOpen(next)
        if (!next) reset()
      }}
    >
      <SheetTrigger asChild>
        <Button
          variant="default"
          size="icon"
          aria-label="Open Co-Pilot"
          className="fixed bottom-6 right-6 z-40 size-12 rounded-full shadow-lg"
        >
          <Sparkles className="size-5" />
        </Button>
      </SheetTrigger>
      <SheetContent side="right" className="!w-[420px] !max-w-[90vw] overflow-y-auto bg-background text-foreground">
        <SheetHeader>
          <SheetTitle className="flex items-center gap-2">
            <Sparkles className="size-4" /> Co-Pilot
          </SheetTitle>
        </SheetHeader>

        <div className="flex flex-col gap-3 px-4 pb-6">
          <textarea
            value={query}
            onChange={(e) => setQuery(e.target.value)}
            placeholder="Describe a strategy, e.g. 'large caps with RSI under 30 and ROE over 15%, rebalanced monthly'"
            rows={3}
            className="w-full resize-none rounded-[var(--radius-token)] border border-border bg-card p-2 text-sm outline-none focus-visible:ring-2 focus-visible:ring-ring"
          />
          <Button onClick={handleAsk} disabled={step === 'loading' || !query.trim()}>
            {step === 'loading' ? 'Asking Co-Pilot…' : 'Ask Co-Pilot'}
          </Button>

          {error ? (
            <p className="text-sm text-destructive">{error}</p>
          ) : null}

          {spec ? (
            <Card>
              <CardHeader>
                <CardTitle className="text-sm">{spec.name}</CardTitle>
              </CardHeader>
              <CardContent className="flex flex-col gap-2 text-sm">
                <p className="text-muted-foreground">{spec.description}</p>

                {spec.technical.length > 0 ? (
                  <div>
                    <div className="text-xs font-medium text-muted-foreground">Technical</div>
                    <ul className="list-disc pl-4">
                      {spec.technical.map((c, i) => (
                        <li key={i}>{c.feature} {c.op} {JSON.stringify(c.value)}</li>
                      ))}
                    </ul>
                  </div>
                ) : null}

                {spec.fundamental.length > 0 ? (
                  <div>
                    <div className="text-xs font-medium text-muted-foreground">Fundamental</div>
                    <ul className="list-disc pl-4">
                      {spec.fundamental.map((c, i) => (
                        <li key={i}>{c.feature} {c.op} {JSON.stringify(c.value)}</li>
                      ))}
                    </ul>
                  </div>
                ) : null}

                {spec.valuation.length > 0 ? (
                  <div>
                    <div className="text-xs font-medium text-muted-foreground">Valuation</div>
                    <ul className="list-disc pl-4">
                      {spec.valuation.map((c, i) => (
                        <li key={i}>{c.feature} {c.op} {JSON.stringify(c.value)}</li>
                      ))}
                    </ul>
                  </div>
                ) : null}

                {spec.unresolved.length > 0 ? (
                  <div className="rounded-[var(--radius-token)] border border-amber-500/40 bg-amber-500/10 p-2">
                    <div className="text-xs font-medium text-amber-600">Could not resolve</div>
                    <ul className="list-disc pl-4 text-xs">
                      {spec.unresolved.map((u, i) => (
                        <li key={i}>{u}</li>
                      ))}
                    </ul>
                  </div>
                ) : null}

                <div className="flex flex-wrap gap-2 pt-2">
                  <Button size="sm" variant="outline" onClick={handleDedupCheck}>
                    Check for duplicates
                  </Button>
                  <Button size="sm" variant="outline" onClick={handleBacktest}>
                    Run backtest
                  </Button>
                  <Button size="sm" onClick={handleSave}>
                    Save strategy
                  </Button>
                </div>

                {dedupResult ? (
                  dedupResult.matched ? (
                    <div className="rounded-[var(--radius-token)] border border-border p-2 text-xs">
                      <Badge variant="secondary">Possible duplicate</Badge>
                      <p className="mt-1">
                        This looks similar to <strong>{dedupResult.matched_name}</strong> (
                        {dedupResult.matched_source === 'screener_template' ? 'existing screener template' : 'a saved strategy'}
                        ), {Math.round((dedupResult.similarity ?? 0) * 100)}% condition overlap.
                      </p>
                    </div>
                  ) : (
                    <p className="text-xs text-muted-foreground">No similar existing strategy found.</p>
                  )
                ) : null}

                {backtestResult ? (
                  <div className="rounded-[var(--radius-token)] border border-border p-2 text-xs">
                    {backtestResult.mode === 'unsupported' || backtestResult.reason ? (
                      <p>{backtestResult.reason}</p>
                    ) : (
                      <dl className="grid grid-cols-2 gap-1">
                        <dt className="text-muted-foreground">CAGR</dt>
                        <dd>{backtestResult.cagr != null ? `${(backtestResult.cagr * 100).toFixed(1)}%` : 'not available'}</dd>
                        <dt className="text-muted-foreground">Total return</dt>
                        <dd>{backtestResult.total_return != null ? `${(backtestResult.total_return * 100).toFixed(1)}%` : 'not available'}</dd>
                        <dt className="text-muted-foreground">Rebalances</dt>
                        <dd>{backtestResult.n_rebalances ?? 'not available'}</dd>
                        <dt className="text-muted-foreground">Universe size</dt>
                        <dd>{backtestResult.universe_size ?? 'not available'}</dd>
                      </dl>
                    )}
                    {backtestResult.caveats.length > 0 ? (
                      <ul className="mt-2 list-disc pl-4 text-amber-600">
                        {backtestResult.caveats.map((c, i) => (
                          <li key={i}>{c}</li>
                        ))}
                      </ul>
                    ) : null}
                  </div>
                ) : null}

                {saveStatus ? <p className="text-xs text-muted-foreground">{saveStatus}</p> : null}
              </CardContent>
            </Card>
          ) : null}
        </div>
      </SheetContent>
    </Sheet>
  )
}
