// Shared data-fetching, formatting, and column-def helpers for the
// Momentum Dynamic Report, split (2026-08-12) from one 962-line page into
// a hub page + one page per section (each independently routable/linkable)
// because the single-page version had become too heavy to load/scroll.
import { useMemo } from 'react'
import { useQuery } from '@tanstack/react-query'
import type { ColumnDef } from '@tanstack/react-table'

import { Badge } from '@/lib/ui'
import { API_BASE_URL, apiGet } from '@/shared/api/client'
import type {
  MomentumDynamicReport,
  MomentumDynamicReportVariant,
  MomentumDynamicReportYoyRow,
} from '../types'

export function fmtPct(v: number | null | undefined) {
  return typeof v === 'number' ? `${(v * 100).toFixed(1)}%` : '—'
}
// avg_winner/loser_return_pct and rolling_*y_*_cagr are already percentage
// points (e.g. 33.9 = 33.9%), unlike win_rate/cagr/etc which are fractions -- fmtPct
// would double-apply the *100 scaling on these, so they use this instead.
export function fmtPctPoints(v: number | null | undefined) {
  return typeof v === 'number' ? `${v.toFixed(1)}%` : '—'
}
export function fmtNum(v: number | null | undefined, digits = 1) {
  return typeof v === 'number' ? v.toFixed(digits) : '—'
}
export function fmtInr(v: number | null | undefined) {
  return typeof v === 'number' ? `₹${Math.round(v).toLocaleString('en-IN')}` : '—'
}
export function bandLabel(rankStart: number, rankEnd: number) {
  return `${rankStart}-${rankEnd}`
}

export const STRATEGY_LABELS: Record<MomentumDynamicReportVariant['strategy'], string> = {
  all_risk: 'All Risk',
  balanced: 'Balanced',
  risk_managed: 'Risk-Managed',
  max_defensive: 'Max-Defensive',
}

export const TRADE_BOOK_BASE = '/api/v1/momentum/dynamic_report/trades'

// The report JSON doesn't carry avg_days_held for the currently-loaded
// sweep (generated before that field existed), but every variant's trade
// book CSV already has a per-trade holding_days column — so for the
// (small, ~40-row) Recommended & Most Important summary table, fetch each
// variant's CSV directly and average holding_days across its closed
// trades, instead of waiting on a full sweep re-run.
async function fetchAvgHoldingDays(variantId: string): Promise<number | null> {
  const resp = await fetch(`${API_BASE_URL}${TRADE_BOOK_BASE}/${variantId}`)
  if (!resp.ok) return null
  const text = await resp.text()
  const lines = text.trim().split('\n')
  if (lines.length < 2) return null
  const header = lines[0].split(',')
  const statusIdx = header.indexOf('status')
  const holdingIdx = header.indexOf('holding_days')
  if (statusIdx === -1 || holdingIdx === -1) return null
  let sum = 0
  let count = 0
  for (let i = 1; i < lines.length; i++) {
    const cols = lines[i].split(',')
    if (cols[statusIdx] === 'closed') {
      const days = Number(cols[holdingIdx])
      if (Number.isFinite(days)) {
        sum += days
        count += 1
      }
    }
  }
  return count > 0 ? sum / count : null
}

// [2026-08-18] graceCyclesFromVariantId is gone with the grace period. Current
// variant_ids carry no "_g<N>" suffix (see strategies/migrations/momentum.py's
// variant_name), so there is nothing left to parse.
export function rowLabel(r: MomentumDynamicReportYoyRow): string {
  return `Top${r.top_n} · ${r.lookback_months}mo · ${r.rebalance_period}`
}

export type RagBand = 'red' | 'amber' | 'green'

export function classifyRag(returnPct: number, redBoundary: number, greenBoundary: number): RagBand {
  if (returnPct < redBoundary) return 'red'
  if (returnPct < greenBoundary) return 'amber'
  return 'green'
}

export const RAG_CLASSES: Record<RagBand, string> = {
  red: 'bg-red/15 text-red',
  amber: 'bg-amber/15 text-amber',
  green: 'bg-green/15 text-green',
}

// Central fetch + derived-row hook, shared by every section page so they
// all read from the same react-query cache entry (one network fetch, no
// matter how many of the split pages get visited in a session).
export function useDynamicReportData() {
  const report = useQuery({
    queryKey: ['momentum-dynamic-report'],
    queryFn: () => apiGet<MomentumDynamicReport>('/api/v1/momentum/dynamic_report'),
  })

  const allRows = report.data?.variants ?? []
  const allYoyRows = report.data?.yoy ?? []

  const bands = useMemo(
    () =>
      Array.from(
        new Map(allRows.map((r) => [r.band_id, { band_id: r.band_id, rank_start: r.rank_start, rank_end: r.rank_end }])).values(),
      ).sort((a, b) => a.rank_start - b.rank_start),
    [allRows],
  )

  const strategyOptions = useMemo(() => Array.from(new Set(allRows.map((r) => r.strategy))), [allRows])

  // Client-side fallback for top_cagr_rank: the currently-loaded report was
  // generated before the backend computed this field, but every variant
  // still carries a real `cagr` value, so the top-2-by-CAGR per universe can
  // be derived here directly instead of waiting on a full sweep re-run.
  // Once a fresh report populates the backend's own top_cagr_rank, that
  // value wins (checked first in effectiveTopCagrRank below).
  const clientTopCagrRank = useMemo(() => {
    const byBand = new Map<number, MomentumDynamicReportVariant[]>()
    for (const r of allRows) {
      if (r.cagr == null) continue
      const arr = byBand.get(r.band_id)
      if (arr) arr.push(r)
      else byBand.set(r.band_id, [r])
    }
    const map = new Map<string, number>()
    for (const arr of byBand.values()) {
      const top2 = [...arr].sort((a, b) => (b.cagr ?? -Infinity) - (a.cagr ?? -Infinity)).slice(0, 2)
      top2.forEach((r, idx) => map.set(r.variant_id, idx + 1))
    }
    return map
  }, [allRows])

  function effectiveTopCagrRank(v: MomentumDynamicReportVariant): number | null {
    return v.top_cagr_rank ?? clientTopCagrRank.get(v.variant_id) ?? null
  }

  const recommendedRows = useMemo(
    () =>
      allRows
        .filter((r) => r.is_recommended || effectiveTopCagrRank(r) != null)
        .sort((a, b) => {
          const aRank = effectiveTopCagrRank(a)
          const bRank = effectiveTopCagrRank(b)
          return (
            a.rank_start - b.rank_start ||
            Number(aRank != null) - Number(bRank != null) ||
            a.strategy.localeCompare(b.strategy) ||
            (aRank ?? 0) - (bRank ?? 0)
          )
        }),
    [allRows, clientTopCagrRank],
  )

  const recommendedVariantIds = useMemo(() => recommendedRows.map((r) => r.variant_id), [recommendedRows])

  const avgHoldingQuery = useQuery({
    queryKey: ['momentum-dynamic-report-avg-holding', recommendedVariantIds],
    queryFn: async () => {
      const entries = await Promise.all(
        recommendedVariantIds.map(async (id) => [id, await fetchAvgHoldingDays(id)] as const),
      )
      return new Map(entries)
    },
    enabled: recommendedVariantIds.length > 0,
    staleTime: Infinity,
  })

  function effectiveAvgDaysHeld(v: MomentumDynamicReportVariant): number | null {
    return v.avg_days_held ?? avgHoldingQuery.data?.get(v.variant_id) ?? null
  }

  return {
    report,
    allRows,
    allYoyRows,
    bands,
    strategyOptions,
    clientTopCagrRank,
    effectiveTopCagrRank,
    recommendedRows,
    avgHoldingQuery,
    effectiveAvgDaysHeld,
  }
}

export type DynamicReportData = ReturnType<typeof useDynamicReportData>

// Full per-variant column set used by both the Strategy Sweep (per-band)
// tables and (with the Avg Holding cell overridden) the hub page's
// Recommended & Most Important summary table.
export function useSweepColumns(
  effectiveTopCagrRank: (v: MomentumDynamicReportVariant) => number | null,
  onJumpToYoy: (bandId: number) => void,
) {
  return useMemo<ColumnDef<MomentumDynamicReportVariant, unknown>[]>(
    () => [
      {
        accessorKey: 'strategy',
        header: 'Category',
        size: 210,
        cell: (i) => {
          const v = i.row.original
          const topCagrRank = effectiveTopCagrRank(v)
          return (
            <span className="flex flex-wrap items-center gap-1.5">
              {STRATEGY_LABELS[v.strategy]}
              {v.is_recommended ? <Badge variant="success">Recommended</Badge> : null}
              {v.is_band_most_important ? <Badge variant="default">Most Important</Badge> : null}
              {topCagrRank ? <Badge variant="outline">Top CAGR #{topCagrRank}</Badge> : null}
            </span>
          )
        },
      },
      { accessorKey: 'top_n', header: 'Top N', size: 55, meta: { align: 'right' } },
      {
        accessorKey: 'lookback_months',
        header: 'Lookback',
        size: 70,
        meta: { align: 'right' },
        cell: (i) => `${i.getValue<number>()}mo`,
      },
      { accessorKey: 'rebalance_period', header: 'Rebalance', size: 85 },
      {
        accessorKey: 'cagr',
        header: 'CAGR',
        size: 65,
        meta: { align: 'right' },
        cell: (i) => fmtPct(i.getValue<number | null>()),
      },
      {
        accessorKey: 'post_tax_cagr',
        header: 'Post-Tax CAGR',
        meta: { align: 'right', priority: 'medium' },
        cell: (i) => fmtPct(i.getValue<number | null>()),
      },
      {
        accessorKey: 'total_tax_paid',
        header: 'Tax Paid (YoY)',
        meta: { align: 'right', priority: 'medium' },
        cell: (i) => fmtInr(i.getValue<number | null>()),
      },
      {
        accessorKey: 'avg_winner_return_pct',
        header: 'Avg Gain (Winners)',
        meta: { align: 'right', priority: 'medium' },
        cell: (i) => fmtPctPoints(i.getValue<number | null>()),
      },
      {
        accessorKey: 'avg_loser_return_pct',
        header: 'Avg Loss (Losers)',
        meta: { align: 'right', priority: 'medium' },
        cell: (i) => fmtPctPoints(i.getValue<number | null>()),
      },
      {
        accessorKey: 'sip_cagr',
        header: 'SIP CAGR',
        meta: { align: 'right', priority: 'medium' },
        cell: (i) => fmtPct(i.getValue<number | null>()),
      },
      {
        accessorKey: 'sharpe',
        header: 'Sharpe',
        size: 60,
        meta: { align: 'right' },
        cell: (i) => fmtNum(i.getValue<number | null>(), 2),
      },
      {
        accessorKey: 'sortino',
        header: 'Sortino',
        size: 65,
        meta: { align: 'right' },
        cell: (i) => fmtNum(i.getValue<number | null>(), 2),
      },
      {
        accessorKey: 'max_drawdown',
        header: 'Max DD',
        size: 65,
        meta: { align: 'right' },
        cell: (i) => fmtPct(i.getValue<number | null>()),
      },
      {
        accessorKey: 'win_rate',
        header: 'Win Rate',
        size: 70,
        meta: { align: 'right' },
        cell: (i) => fmtPct(i.getValue<number | null>()),
      },
      {
        accessorKey: 'total_trades',
        header: 'Trades',
        size: 65,
        meta: { align: 'right', priority: 'medium' },
        cell: (i) => i.getValue<number | null>() ?? '—',
      },
      {
        accessorKey: 'avg_days_held',
        header: 'Avg Holding',
        size: 85,
        meta: { align: 'right', priority: 'medium' },
        cell: (i) => {
          const v = i.getValue<number | null>()
          return typeof v === 'number' ? `${v.toFixed(0)}d` : '—'
        },
      },
      {
        accessorKey: 'total_signals',
        header: 'Signals',
        meta: { align: 'right', priority: 'low' },
        cell: (i) => i.getValue<number | null>() ?? '—',
      },
      {
        accessorKey: 'value_10L',
        header: 'Value of 10L',
        meta: { align: 'right', priority: 'medium' },
        cell: (i) => fmtInr(i.getValue<number | null>()),
      },
      {
        accessorKey: 'value_10k_sip',
        header: 'Value of 10K SIP',
        meta: { align: 'right', priority: 'medium' },
        cell: (i) => fmtInr(i.getValue<number | null>()),
      },
      {
        accessorKey: 'score',
        header: 'Score',
        meta: { align: 'right', priority: 'low' },
        cell: (i) => fmtNum(i.getValue<number | null>(), 2),
      },
      {
        id: 'links',
        header: 'Links',
        cell: (i) => (
          <span className="flex items-center gap-2 text-xs">
            <a
              href={`${API_BASE_URL}${TRADE_BOOK_BASE}/${i.row.original.variant_id}`}
              target="_blank"
              rel="noreferrer"
              className="text-primary underline"
            >
              Trades
            </a>
            <button type="button" onClick={() => onJumpToYoy(i.row.original.band_id)} className="text-primary underline">
              YoY
            </button>
          </span>
        ),
      },
    ],
    [effectiveTopCagrRank, onJumpToYoy],
  )
}

// Same columns as the per-band tables, plus a leading "Universe" column —
// this summary table spans all 7 bands at once, so the band needs to be
// spelled out per row instead of being implied by a section header. The
// shared Avg Holding column's cell is overridden here to use the
// per-variant trade-book fetch (effectiveAvgDaysHeld) — this summary
// table only has ~40 rows, so fetching each variant's CSV client-side is
// cheap; the full 1,680-row per-band tables intentionally do NOT get this
// treatment (that many CSV fetches would not be cheap).
export function useRecommendedColumns(
  sweepColumns: ColumnDef<MomentumDynamicReportVariant, unknown>[],
  effectiveAvgDaysHeld: (v: MomentumDynamicReportVariant) => number | null,
  avgHoldingLoading: boolean,
) {
  return useMemo<ColumnDef<MomentumDynamicReportVariant, unknown>[]>(
    () => [
      {
        id: 'universe',
        accessorFn: (row) => row.rank_start,
        header: 'Universe (rank)',
        size: 90,
        cell: (i) => bandLabel(i.row.original.rank_start, i.row.original.rank_end),
      },
      ...sweepColumns.map((col) =>
        'accessorKey' in col && col.accessorKey === 'avg_days_held'
          ? {
              ...col,
              cell: (i: { row: { original: MomentumDynamicReportVariant } }) => {
                const days = effectiveAvgDaysHeld(i.row.original)
                if (typeof days === 'number') return `${days.toFixed(0)}d`
                return avgHoldingLoading ? '…' : '—'
              },
            }
          : col,
      ),
    ],
    [sweepColumns, effectiveAvgDaysHeld, avgHoldingLoading],
  )
}

// 2/3/4-year rolling-window return consistency per (universe, category).
export function useRollingColumns() {
  return useMemo<ColumnDef<MomentumDynamicReportVariant, unknown>[]>(
    () => [
      {
        id: 'universe',
        accessorFn: (row) => row.rank_start,
        header: 'Universe (rank)',
        cell: (i) => bandLabel(i.row.original.rank_start, i.row.original.rank_end),
      },
      { accessorKey: 'strategy', header: 'Category' },
      {
        id: 'rolling_2y',
        header: '2Y Rolling (min / median / max)',
        meta: { align: 'right' },
        cell: (i) => {
          const v = i.row.original
          return `${fmtPctPoints(v.rolling_2y_min_cagr)} / ${fmtPctPoints(v.rolling_2y_median_cagr)} / ${fmtPctPoints(v.rolling_2y_max_cagr)}`
        },
      },
      {
        id: 'rolling_3y',
        header: '3Y Rolling (min / median / max)',
        meta: { align: 'right' },
        cell: (i) => {
          const v = i.row.original
          return `${fmtPctPoints(v.rolling_3y_min_cagr)} / ${fmtPctPoints(v.rolling_3y_median_cagr)} / ${fmtPctPoints(v.rolling_3y_max_cagr)}`
        },
      },
      {
        id: 'rolling_4y',
        header: '4Y Rolling (min / median / max)',
        meta: { align: 'right' },
        cell: (i) => {
          const v = i.row.original
          return `${fmtPctPoints(v.rolling_4y_min_cagr)} / ${fmtPctPoints(v.rolling_4y_median_cagr)} / ${fmtPctPoints(v.rolling_4y_max_cagr)}`
        },
      },
      {
        accessorKey: 'rolling_4y_n_windows',
        header: 'Windows (4Y)',
        meta: { align: 'right', priority: 'low' },
        cell: (i) => i.getValue<number | null>() ?? '—',
      },
    ],
    [],
  )
}

// "Income mode" -- start each strategy at 10L, pay real YoY tax, withdraw
// any surplus back to the investor each FY (or top up a loss year), so
// capital always resets to the same base.
export function useIncomeColumns() {
  return useMemo<ColumnDef<MomentumDynamicReportVariant, unknown>[]>(
    () => [
      {
        id: 'universe',
        accessorFn: (row) => row.rank_start,
        header: 'Universe (rank)',
        cell: (i) => bandLabel(i.row.original.rank_start, i.row.original.rank_end),
      },
      { accessorKey: 'strategy', header: 'Category' },
      {
        accessorKey: 'income_total_withdrawn',
        header: 'Total Withdrawn',
        meta: { align: 'right' },
        cell: (i) => fmtInr(i.getValue<number | null>()),
      },
      {
        accessorKey: 'income_total_injected',
        header: 'Total Injected (top-ups)',
        meta: { align: 'right', priority: 'medium' },
        cell: (i) => fmtInr(i.getValue<number | null>()),
      },
      {
        accessorKey: 'income_avg_annual_yield_pct',
        header: 'Avg Annual Yield',
        meta: { align: 'right' },
        cell: (i) => fmtPctPoints(i.getValue<number | null>()),
      },
      {
        accessorKey: 'income_years_survived_pct',
        header: 'Years Survived (no top-up)',
        meta: { align: 'right' },
        cell: (i) => fmtPctPoints(i.getValue<number | null>()),
      },
      {
        accessorKey: 'income_n_years',
        header: 'FYs',
        meta: { align: 'right', priority: 'low' },
        cell: (i) => i.getValue<number | null>() ?? '—',
      },
    ],
    [],
  )
}

export function useYoyColumns() {
  return useMemo<ColumnDef<MomentumDynamicReportYoyRow, unknown>[]>(
    () => [
      {
        id: 'band',
        header: 'Universe (rank)',
        cell: (i) => bandLabel(i.row.original.rank_start, i.row.original.rank_end),
      },
      { accessorKey: 'fy_label', header: 'FY' },
      { accessorKey: 'top_n', header: 'Top N', meta: { align: 'right', priority: 'medium' } },
      { accessorKey: 'rebalance_period', header: 'Rebalance', meta: { priority: 'medium' } },
      {
        accessorKey: 'return_pct',
        header: 'Return',
        meta: { align: 'right' },
        cell: (i) => {
          const v = i.getValue<number | null>()
          return typeof v === 'number' ? `${v.toFixed(1)}%` : '—'
        },
      },
      {
        accessorKey: 'nifty_midcap_150_return_pct',
        header: 'Midcap 150',
        meta: { align: 'right', priority: 'medium' },
        cell: (i) => {
          const v = i.getValue<number | null>()
          return typeof v === 'number' ? `${v.toFixed(1)}%` : '—'
        },
      },
      {
        accessorKey: 'nifty_smallcap_250_return_pct',
        header: 'Smallcap 250',
        meta: { align: 'right', priority: 'medium' },
        cell: (i) => {
          const v = i.getValue<number | null>()
          return typeof v === 'number' ? `${v.toFixed(1)}%` : '—'
        },
      },
      {
        accessorKey: 'churn',
        header: 'Churn',
        meta: { align: 'right', priority: 'low' },
        cell: (i) => i.getValue<number | null>() ?? '—',
      },
      {
        accessorKey: 'avg_holding_days',
        header: 'Avg Days Held',
        meta: { align: 'right', priority: 'low' },
        cell: (i) => fmtNum(i.getValue<number | null>(), 0),
      },
    ],
    [],
  )
}
