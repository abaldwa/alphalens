/**
 * features/backtest-report/components/ReportControls.tsx
 *
 * The four header controls every section shares: return mode, tax basis,
 * window, benchmark. They are grouped in one file because they are one bar —
 * splitting four ~30-line controls across four files buys nothing.
 *
 * All of them write to the URL via useReportParams, so a section's state is
 * fully described by its link.
 *
 * Two of them carry a warning rather than silently doing the wrong thing:
 * WindowSelector flags a window reaching into pre-2009 history, and
 * BenchmarkSelector marks an index that did not trade across the window (its
 * options come from GET /api/v1/indices, never a hardcoded list, so an index
 * the pipeline starts capturing appears with no frontend change).
 */

import { cn } from '@/lib/utils'

import {
  RELIABLE_FROM,
  WINDOW_LABELS,
  WINDOW_PRESETS,
  crossesUnreliableHistory,
  type ReportParams,
  type WindowPreset,
} from '../useReportParams'
import type { ReturnMode, TaxBasis } from '../types'

type Patch = (patch: Partial<ReportParams>) => void

/** Shared two-or-more-option segmented control. */
function Segmented<T extends string>({
  label,
  value,
  options,
  onChange,
}: {
  label: string
  value: T
  options: Array<{ value: T; label: string; title?: string }>
  onChange: (v: T) => void
}) {
  return (
    <div className="flex items-center gap-2">
      <span className="text-xs font-medium text-muted-foreground">{label}</span>
      <div
        role="radiogroup"
        aria-label={label}
        className="inline-flex rounded-[var(--radius-token)] border border-border p-0.5"
      >
        {options.map((o) => (
          <button
            key={o.value}
            type="button"
            role="radio"
            aria-checked={value === o.value}
            title={o.title}
            onClick={() => onChange(o.value)}
            className={cn(
              'rounded-[var(--radius-token)] px-2.5 py-1 text-xs transition-colors focus-visible:outline focus-visible:outline-2 focus-visible:outline-offset-2 focus-visible:outline-ring',
              value === o.value
                ? 'bg-primary text-primary-foreground'
                : 'text-muted-foreground hover:text-foreground',
            )}
          >
            {o.label}
          </button>
        ))}
      </div>
    </div>
  )
}

export function ModeToggle({
  mode,
  onChange,
}: {
  mode: ReturnMode
  onChange: Patch
}) {
  return (
    <Segmented<ReturnMode>
      label="Mode"
      value={mode}
      onChange={(v) => onChange({ mode: v })}
      options={[
        {
          value: 'long_term_cagr',
          label: 'Long-term CAGR',
          title: 'Everything compounds; nothing is withdrawn.',
        },
        {
          value: 'regular_returns',
          label: 'Regular returns',
          title:
            'Withdraw the excess each year, backfilling a shortfall and running on at current capital after a losing year.',
        },
      ]}
    />
  )
}

export function TaxBasisToggle({
  basis,
  onChange,
}: {
  basis: TaxBasis
  onChange: Patch
}) {
  return (
    <Segmented<TaxBasis>
      label="Basis"
      value={basis}
      onChange={(v) => onChange({ taxBasis: v })}
      options={[
        {
          value: 'post_tax',
          label: 'Post-tax',
          title:
            'STCG/LTCG paid as a cash outflow each financial year. This is the money you keep.',
        },
        {
          value: 'pre_tax',
          label: 'Pre-tax',
          title: 'Before STCG/LTCG. Flatters high-churn strategies.',
        },
      ]}
    />
  )
}

export function WindowSelector({
  window: preset,
  startDate,
  onChange,
}: {
  window: WindowPreset
  startDate: string | null
  onChange: Patch
}) {
  const unreliable = crossesUnreliableHistory(startDate)
  return (
    <div className="flex items-center gap-2">
      <label
        htmlFor="report-window"
        className="text-xs font-medium text-muted-foreground"
      >
        Window
      </label>
      <select
        id="report-window"
        value={preset}
        onChange={(e) => onChange({ window: e.target.value as WindowPreset })}
        className="h-7 rounded-[var(--radius-token)] border border-border bg-background px-2 text-xs"
      >
        {WINDOW_PRESETS.map((w) => (
          <option key={w} value={w}>
            {WINDOW_LABELS[w]}
          </option>
        ))}
      </select>
      {unreliable ? (
        <span
          className="text-xs text-amber"
          title={`Price history before ${RELIABLE_FROM} crosses the 2007-04-02 legacy/Fyers seam and includes unrepaired corporate actions (A99-A102). Returns from that period are not trustworthy.`}
        >
          ⚠ pre-{RELIABLE_FROM.slice(0, 4)} history
        </span>
      ) : null}
    </div>
  )
}

export interface BenchmarkOption {
  indexName: string
  /** Traded across the whole window, as opposed to reaching it only through
   * NSE's retrospective back-computation. */
  live: boolean
  caveat?: string | null
}

export function BenchmarkSelector({
  benchmark,
  options,
  recommended,
  fallbackReason,
  onChange,
}: {
  benchmark: string | null
  options: BenchmarkOption[]
  recommended?: string | null
  fallbackReason?: string | null
  onChange: Patch
}) {
  const selected = benchmark ?? recommended ?? ''
  const selectedOption = options.find((o) => o.indexName === selected)

  return (
    <div className="flex flex-wrap items-center gap-2">
      <label
        htmlFor="report-benchmark"
        className="text-xs font-medium text-muted-foreground"
      >
        Benchmark
      </label>
      <select
        id="report-benchmark"
        value={selected}
        onChange={(e) => onChange({ benchmark: e.target.value || null })}
        className="h-7 rounded-[var(--radius-token)] border border-border bg-background px-2 text-xs"
      >
        {options.length === 0 ? <option value="">No index available</option> : null}
        {options.map((o) => (
          <option key={o.indexName} value={o.indexName}>
            {o.indexName}
            {o.live ? '' : ' (back-computed)'}
          </option>
        ))}
      </select>
      {/* A104: the fallback message is not optional chrome. An excess return
          against a broad index for a size-scoped strategy contains the size
          spread, so the reader has to be told which comparison they are
          actually looking at. */}
      {fallbackReason ? (
        <span className="text-xs text-amber">{fallbackReason}</span>
      ) : selectedOption?.caveat ? (
        <span className="text-xs text-amber">{selectedOption.caveat}</span>
      ) : null}
    </div>
  )
}
