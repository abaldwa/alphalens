// Consolidated Momentum strategy report -- scripts/run_momentum_dynamic_report.py's
// All Risk / Balanced / Risk-Managed / Max-Defensive sweep across all 7 rank
// bands (1-50 through 501-800), replacing the old Recommended Strategies page
// plus the static Backtest Ledger / Year-on-Year Report / Rank-Band Sweep
// links (2026-07-30 user request — one dynamic page instead of four).
import { useMemo, useState } from 'react'
import { useQuery, useQueryClient } from '@tanstack/react-query'
import type { ColumnDef } from '@tanstack/react-table'

import { AppShell, Badge, Card, CardContent, CardDescription, CardHeader, CardTitle, DataTable } from '@/lib/ui'
import { API_BASE_URL, apiGet } from '@/shared/api/client'
import type {
  MomentumDynamicReport,
  MomentumDynamicReportVariant,
  MomentumDynamicReportYoyRow,
} from './types'
import { SweepTriggerButton } from './SweepTriggerButton'

function fmtPct(v: number | null | undefined) {
  return typeof v === 'number' ? `${(v * 100).toFixed(1)}%` : '—'
}
function fmtNum(v: number | null | undefined, digits = 1) {
  return typeof v === 'number' ? v.toFixed(digits) : '—'
}
function fmtInr(v: number | null | undefined) {
  return typeof v === 'number' ? `₹${Math.round(v).toLocaleString('en-IN')}` : '—'
}
function bandLabel(rankStart: number, rankEnd: number) {
  return `${rankStart}-${rankEnd}`
}

const STRATEGY_LABELS: Record<MomentumDynamicReportVariant['strategy'], string> = {
  all_risk: 'All Risk',
  balanced: 'Balanced',
  risk_managed: 'Risk-Managed',
  max_defensive: 'Max-Defensive',
}

const TRADE_BOOK_BASE = '/api/v1/momentum/dynamic_report/trades'

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

// Parses the trailing "_g<N>" grace-cycles suffix that the YoY variant_id
// carries (e.g. "b1_1-50_lb3mo_monthly_top20_g2") -- the YoY row type doesn't
// expose grace_cycles as its own field, so this is the only place it's recoverable.
function graceCyclesFromVariantId(variantId: string): number | null {
  const m = /_g(\d+)$/.exec(variantId)
  return m ? Number(m[1]) : null
}

function rowLabel(r: MomentumDynamicReportYoyRow): string {
  const grace = graceCyclesFromVariantId(r.variant_id)
  return `Top${r.top_n} · ${r.lookback_months}mo · ${r.rebalance_period}${grace != null ? ` · g${grace}` : ''}`
}

type RagBand = 'red' | 'amber' | 'green'

function classifyRag(returnPct: number, redBoundary: number, greenBoundary: number): RagBand {
  if (returnPct < redBoundary) return 'red'
  if (returnPct < greenBoundary) return 'amber'
  return 'green'
}

const RAG_CLASSES: Record<RagBand, string> = {
  red: 'bg-red/15 text-red',
  amber: 'bg-amber/15 text-amber',
  green: 'bg-green/15 text-green',
}

export function MomentumDynamicReportPage() {
  const [strategy, setStrategy] = useState<string>('')
  const [yoyBandId, setYoyBandId] = useState<string>('')
  // Kept as raw strings (not numbers) because a controlled number <input>
  // bound to Number(value) breaks mid-typing on "-" or a trailing "." --
  // e.g. typing "-5" hits Number("-") === NaN on the first keystroke,
  // setState(NaN) makes the input show "NaN", and every further keystroke
  // just appends to that. Parsing happens only where the boundary is used.
  const [redBoundaryInput, setRedBoundaryInput] = useState('0')
  const [greenBoundaryInput, setGreenBoundaryInput] = useState('18')
  const redBoundary = Number(redBoundaryInput)
  const greenBoundary = Number(greenBoundaryInput)
  const [matrixSort, setMatrixSort] = useState<Record<number, { key: string; dir: 'asc' | 'desc' }>>({})
  const queryClient = useQueryClient()

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

  const yoyRows = useMemo(
    () => allYoyRows.filter((r) => !yoyBandId || String(r.band_id) === yoyBandId),
    [allYoyRows, yoyBandId],
  )

  function jumpToYoy(bandId: number) {
    setYoyBandId(String(bandId))
    document.getElementById('yoy-section')?.scrollIntoView({ behavior: 'smooth' })
  }

  // Pivot: per band, one row per strategy variant, one column per fiscal
  // year -- lets you scan consistency across years at a glance, instead of
  // the flat (band, variant, year) table above where each variant's history
  // is spread across many rows.
  const matrixByBand = useMemo(() => {
    const byBand = new Map<
      number,
      { rankStart: number; rankEnd: number; years: string[]; rows: Array<{ variantId: string; label: string; byYear: Map<string, number> }> }
    >()
    for (const r of allYoyRows) {
      if (r.return_pct == null) continue
      let band = byBand.get(r.band_id)
      if (!band) {
        band = { rankStart: r.rank_start, rankEnd: r.rank_end, years: [], rows: [] }
        byBand.set(r.band_id, band)
      }
      if (!band.years.includes(r.fy_label)) band.years.push(r.fy_label)
      let row = band.rows.find((row) => row.variantId === r.variant_id)
      if (!row) {
        row = { variantId: r.variant_id, label: rowLabel(r), byYear: new Map() }
        band.rows.push(row)
      }
      row.byYear.set(r.fy_label, r.return_pct)
    }
    for (const band of byBand.values()) {
      band.years.sort()
      band.rows.sort((a, b) => a.label.localeCompare(b.label))
    }
    return byBand
  }, [allYoyRows])

  function computeCagr(byYear: Map<string, number>, years: string[]): number | null {
    const present = years.filter((y) => byYear.has(y))
    if (present.length === 0) return null
    const growth = present.reduce((acc, y) => acc * (1 + (byYear.get(y) ?? 0) / 100), 1)
    return (Math.pow(growth, 1 / present.length) - 1) * 100
  }

  function matrixRagCounts(byYear: Map<string, number>, years: string[]): Record<RagBand, number> {
    const counts: Record<RagBand, number> = { red: 0, amber: 0, green: 0 }
    for (const y of years) {
      const v = byYear.get(y)
      if (v != null) counts[classifyRag(v, redBoundary, greenBoundary)] += 1
    }
    return counts
  }

  function sortedMatrixRows(
    band: { years: string[]; rows: Array<{ variantId: string; label: string; byYear: Map<string, number> }> },
    bandId: number,
  ) {
    const sort = matrixSort[bandId]
    const rows = [...band.rows]
    if (!sort) return rows.sort((a, b) => a.label.localeCompare(b.label))
    const valueFor = (row: (typeof rows)[number]): number | string | null => {
      if (sort.key === 'label') return row.label
      if (sort.key === 'cagr') return computeCagr(row.byYear, band.years)
      if (sort.key === 'red' || sort.key === 'amber' || sort.key === 'green') {
        return matrixRagCounts(row.byYear, band.years)[sort.key]
      }
      return row.byYear.get(sort.key) ?? null
    }
    rows.sort((a, b) => {
      const av = valueFor(a)
      const bv = valueFor(b)
      if (av == null && bv == null) return 0
      if (av == null) return 1
      if (bv == null) return -1
      const cmp = typeof av === 'string' && typeof bv === 'string' ? av.localeCompare(bv) : Number(av) - Number(bv)
      return sort.dir === 'asc' ? cmp : -cmp
    })
    return rows
  }

  function toggleMatrixSort(bandId: number, key: string) {
    setMatrixSort((prev) => {
      const current = prev[bandId]
      const dir: 'asc' | 'desc' = current?.key === key && current.dir === 'asc' ? 'desc' : 'asc'
      return { ...prev, [bandId]: { key, dir } }
    })
  }

  function sortIndicator(bandId: number, key: string): string {
    const sort = matrixSort[bandId]
    if (!sort || sort.key !== key) return ''
    return sort.dir === 'asc' ? ' ▲' : ' ▼'
  }

  const columns = useMemo<ColumnDef<MomentumDynamicReportVariant, unknown>[]>(
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
        cell: (i) => fmtPct(i.getValue<number | null>()),
      },
      {
        accessorKey: 'avg_loser_return_pct',
        header: 'Avg Loss (Losers)',
        meta: { align: 'right', priority: 'medium' },
        cell: (i) => fmtPct(i.getValue<number | null>()),
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
            <button
              type="button"
              onClick={() => jumpToYoy(i.row.original.band_id)}
              className="text-primary underline"
            >
              YoY
            </button>
          </span>
        ),
      },
    ],
    [clientTopCagrRank],
  )

  // Same columns as the per-band tables, plus a leading "Universe" column —
  // this summary table spans all 7 bands at once, so the band needs to be
  // spelled out per row instead of being implied by a section header. The
  // shared Avg Holding column's cell is overridden here to use the
  // per-variant trade-book fetch (effectiveAvgDaysHeld) — this summary
  // table only has ~40 rows, so fetching each variant's CSV client-side is
  // cheap; the full 1,680-row per-band tables below intentionally do NOT
  // get this treatment (that many CSV fetches would not be cheap).
  const recommendedColumns = useMemo<ColumnDef<MomentumDynamicReportVariant, unknown>[]>(
    () => [
      {
        id: 'universe',
        // accessorFn (not just a display-only `cell`) is required for
        // TanStack Table's getCanSort() to return true — sorts by
        // rank_start numerically (1-50 before 51-100, etc.), independent
        // of the arbitrary band_id ordering.
        accessorFn: (row) => row.rank_start,
        header: 'Universe (rank)',
        size: 90,
        cell: (i) => bandLabel(i.row.original.rank_start, i.row.original.rank_end),
      },
      ...columns.map((col) =>
        'accessorKey' in col && col.accessorKey === 'avg_days_held'
          ? {
              ...col,
              cell: (i: { row: { original: MomentumDynamicReportVariant } }) => {
                const v = i.row.original
                const days = effectiveAvgDaysHeld(v)
                if (typeof days === 'number') return `${days.toFixed(0)}d`
                return avgHoldingQuery.isLoading ? '…' : '—'
              },
            }
          : col,
      ),
    ],
    [columns, avgHoldingQuery.data, avgHoldingQuery.isLoading],
  )

  // 2026-08-09 user request: 2/3/4-year rolling-window return consistency
  // per (universe, category) -- reuses recommendedRows (28 recommended
  // picks + per-band Most Important) so "for each of the categories" is
  // satisfied without a second data fetch.
  const rollingColumns = useMemo<ColumnDef<MomentumDynamicReportVariant, unknown>[]>(
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
          return `${fmtPct(v.rolling_2y_min_cagr)} / ${fmtPct(v.rolling_2y_median_cagr)} / ${fmtPct(v.rolling_2y_max_cagr)}`
        },
      },
      {
        id: 'rolling_3y',
        header: '3Y Rolling (min / median / max)',
        meta: { align: 'right' },
        cell: (i) => {
          const v = i.row.original
          return `${fmtPct(v.rolling_3y_min_cagr)} / ${fmtPct(v.rolling_3y_median_cagr)} / ${fmtPct(v.rolling_3y_max_cagr)}`
        },
      },
      {
        id: 'rolling_4y',
        header: '4Y Rolling (min / median / max)',
        meta: { align: 'right' },
        cell: (i) => {
          const v = i.row.original
          return `${fmtPct(v.rolling_4y_min_cagr)} / ${fmtPct(v.rolling_4y_median_cagr)} / ${fmtPct(v.rolling_4y_max_cagr)}`
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

  const yoyColumns = useMemo<ColumnDef<MomentumDynamicReportYoyRow, unknown>[]>(
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

  return (
    <AppShell
      title="Momentum — Strategy Report"
      description="All Risk / Balanced / Risk-Managed / Max-Defensive strategies across all 7 rank bands (1-50 through 501-800) — scripts/run_momentum_dynamic_report.py."
    >
      <div className="mb-4 rounded-[var(--radius-token)] border border-border bg-accent-soft px-3 py-2 text-xs text-muted-foreground">
        <strong className="text-foreground">All Risk</strong> is the unfiltered baseline.{' '}
        <strong className="text-foreground">Balanced</strong> adds liquidity floor, quality gating, ADTV-capped
        sizing, and a circuit-lock proxy. <strong className="text-foreground">Risk-Managed</strong> adds
        regime-conditional buy-disabling in high-volatility periods.{' '}
        <strong className="text-foreground">Max-Defensive</strong> additionally neutralizes size/beta exposure.
        Within each rank band, the highest-scoring variant per category is marked{' '}
        <Badge variant="success">Recommended</Badge> — score ={' '}
        {report.data?.score_formula ?? '0.30·z(Sharpe) + 0.25·z(Sortino) + 0.25·z(CAGR) − 0.20·z(|Max Drawdown|)'},
        z-scored within each (band, category) group of 60 variants. The single best variant across all 240
        configs in each universe is marked <Badge variant="default">Most Important</Badge> — the strategy
        deployed for that band. The top 2 variants by raw CAGR in each universe (any category/config) are
        marked <Badge variant="outline">Top CAGR #1/#2</Badge> for comparison against the risk-adjusted picks.
      </div>

      <Card className="mb-6">
        <CardHeader>
          <CardTitle>Recommended & Most Important Strategies</CardTitle>
          <CardDescription>
            The highest-scoring variant per (universe, category), the per-universe{' '}
            <Badge variant="default">Most Important</Badge> best strategy, and the per-universe{' '}
            <Badge variant="outline">Top CAGR #1/#2</Badge> variants (any category/config) for comparison
            — {recommendedRows.length} rows across all 7 universes.
          </CardDescription>
        </CardHeader>
        <CardContent>
          <DataTable
            columns={recommendedColumns}
            data={recommendedRows}
            isLoading={report.isLoading}
            emptyMessage="No recommended picks yet — run the sweep above."
          />
        </CardContent>
      </Card>

      <Card className="mt-6" id="rolling-returns-section">
        <CardHeader>
          <CardTitle>Rolling Return Consistency</CardTitle>
          <CardDescription>
            2/3/4-year rolling-window CAGR (min / median / max across every window in the backtest period) for each
            Recommended/Most Important pick, by universe and category — pre-tax, same basis as the Strategy Sweep
            CAGR. A tight min-max spread means the strategy's return doesn't depend heavily on when you started
            investing.
          </CardDescription>
        </CardHeader>
        <CardContent>
          <DataTable
            columns={rollingColumns}
            data={recommendedRows}
            isLoading={report.isLoading}
            emptyMessage="No rolling-return data yet — run the sweep above."
          />
        </CardContent>
      </Card>

      <Card>
        <CardHeader>
          <CardTitle>Strategy Sweep</CardTitle>
          <CardDescription>
            {report.isLoading
              ? 'Loading…'
              : report.error
                ? 'Failed to load'
                : `${allRows.length} variants${
                    report.data?.generated_at ? ` — generated ${new Date(report.data.generated_at).toLocaleString()}` : ''
                  }`}
          </CardDescription>
          <div className="mt-2 flex flex-wrap gap-3">
            <SweepTriggerButton
              label="Strategy Report"
              triggerUrl="/api/v1/momentum/dynamic_report/trigger"
              statusUrlPrefix="/api/v1/momentum/dynamic_report/trigger/status"
              onCompleted={() => queryClient.invalidateQueries({ queryKey: ['momentum-dynamic-report'] })}
            />
            <select
              className="h-9 rounded-[var(--radius-token)] border border-border bg-transparent px-3 text-sm"
              value={strategy}
              onChange={(e) => setStrategy(e.target.value)}
            >
              <option value="">All categories</option>
              {strategyOptions.map((s) => (
                <option key={s} value={s}>
                  {STRATEGY_LABELS[s as MomentumDynamicReportVariant['strategy']]}
                </option>
              ))}
            </select>
          </div>
        </CardHeader>
        <CardContent>
          {report.error ? (
            <p className="text-sm text-red">
              Could not reach GET /api/v1/momentum/dynamic_report — {(report.error as Error).message}
            </p>
          ) : (
            bands.map((band) => {
              const bandRows = allRows.filter(
                (r) => r.band_id === band.band_id && (!strategy || r.strategy === strategy),
              )
              return (
                <details key={band.band_id} className="mb-4 rounded-[var(--radius-token)] border border-border" open>
                  <summary className="cursor-pointer px-3 py-2 text-sm font-semibold">
                    Universe (rank {bandLabel(band.rank_start, band.rank_end)}) — {bandRows.length} variants
                  </summary>
                  <div className="border-t border-border p-2">
                    <DataTable
                      columns={columns}
                      data={bandRows}
                      isLoading={report.isLoading}
                      emptyMessage="No variants for this universe/category yet — run the sweep above."
                    />
                  </div>
                </details>
              )
            })
          )}
        </CardContent>
      </Card>

      <Card className="mt-6" id="yoy-section">
        <CardHeader>
          <CardTitle>Year-on-Year (Apr&ndash;Mar)</CardTitle>
          <CardDescription>
            Per-FY return, churn, and Nifty Midcap 150 / Smallcap 250 comparison (benchmark data real from 2023-07
            onward only). Click a row's "YoY" link above to filter this table to that universe.
          </CardDescription>
          <div className="mt-2 flex flex-wrap gap-3">
            <select
              className="h-9 rounded-[var(--radius-token)] border border-border bg-transparent px-3 text-sm"
              value={yoyBandId}
              onChange={(e) => setYoyBandId(e.target.value)}
            >
              <option value="">All universes</option>
              {bands.map((b) => (
                <option key={b.band_id} value={b.band_id}>
                  {bandLabel(b.rank_start, b.rank_end)}
                </option>
              ))}
            </select>
          </div>
        </CardHeader>
        <CardContent>
          <DataTable
            columns={yoyColumns}
            data={yoyRows}
            isLoading={report.isLoading}
            emptyMessage="No year-on-year rows yet."
          />
        </CardContent>
      </Card>

      <Card className="mt-6" id="yoy-matrix-section">
        <CardHeader>
          <CardTitle>YoY Consistency Matrix</CardTitle>
          <CardDescription>
            Per band: strategies as rows, fiscal years as columns, one return figure per cell — scan for the
            strategy with the most consistent (fewest red, most green) year-on-year returns. CAGR and Red/Amber/Green
            counts are in the trailing columns.
          </CardDescription>
          <div className="mt-2 flex flex-wrap items-center gap-4 text-xs">
            <label className="flex items-center gap-2">
              <span className={`inline-block h-3 w-3 rounded-sm ${RAG_CLASSES.red}`} />
              Red: return &lt;
              <input
                type="text"
                inputMode="decimal"
                value={redBoundaryInput}
                onChange={(e) => setRedBoundaryInput(e.target.value)}
                className="h-7 w-16 rounded-[var(--radius-token)] border border-border bg-background px-1.5 text-xs"
              />
              %
            </label>
            <label className="flex items-center gap-2">
              <span className={`inline-block h-3 w-3 rounded-sm ${RAG_CLASSES.amber}`} />
              Amber: {redBoundaryInput}% –
              <input
                type="text"
                inputMode="decimal"
                value={greenBoundaryInput}
                onChange={(e) => setGreenBoundaryInput(e.target.value)}
                className="h-7 w-16 rounded-[var(--radius-token)] border border-border bg-background px-1.5 text-xs"
              />
              %
            </label>
            <label className="flex items-center gap-2">
              <span className={`inline-block h-3 w-3 rounded-sm ${RAG_CLASSES.green}`} />
              Green: return &ge; {greenBoundaryInput}%
            </label>
            <span className="text-muted-foreground">(global — applies to every band below)</span>
            {Number.isNaN(redBoundary) || Number.isNaN(greenBoundary) ? (
              <span className="text-red">Enter valid numbers for both boundaries.</span>
            ) : greenBoundary <= redBoundary ? (
              <span className="text-red">Green boundary must be greater than the red boundary.</span>
            ) : null}
          </div>
        </CardHeader>
        <CardContent>
          {report.isLoading ? (
            <p className="text-sm text-muted-foreground">Loading…</p>
          ) : matrixByBand.size === 0 ? (
            <p className="text-sm text-muted-foreground">No year-on-year rows yet.</p>
          ) : (
            Array.from(matrixByBand.entries())
              .sort((a, b) => a[1].rankStart - b[1].rankStart)
              .map(([bandId, band]) => (
                <details key={bandId} className="mb-4 rounded-[var(--radius-token)] border border-border" open>
                  <summary className="cursor-pointer px-3 py-2 text-sm font-semibold">
                    Universe (rank {bandLabel(band.rankStart, band.rankEnd)}) — {band.rows.length} strategies
                  </summary>
                  <div className="overflow-x-auto border-t border-border p-2">
                    <table className="w-full border-collapse text-xs">
                      <thead>
                        <tr>
                          <th
                            className="sticky left-0 z-10 cursor-pointer select-none bg-card px-2 py-1.5 text-left font-semibold"
                            onClick={() => toggleMatrixSort(bandId, 'label')}
                          >
                            Strategy{sortIndicator(bandId, 'label')}
                          </th>
                          {band.years.map((y) => (
                            <th
                              key={y}
                              className="cursor-pointer select-none px-2 py-1.5 text-right font-semibold"
                              onClick={() => toggleMatrixSort(bandId, y)}
                            >
                              {y}
                              {sortIndicator(bandId, y)}
                            </th>
                          ))}
                          <th
                            className="cursor-pointer select-none px-2 py-1.5 text-right font-semibold"
                            onClick={() => toggleMatrixSort(bandId, 'cagr')}
                          >
                            CAGR{sortIndicator(bandId, 'cagr')}
                          </th>
                          <th
                            className="cursor-pointer select-none px-2 py-1.5 text-right font-semibold text-red"
                            onClick={() => toggleMatrixSort(bandId, 'red')}
                          >
                            Red{sortIndicator(bandId, 'red')}
                          </th>
                          <th
                            className="cursor-pointer select-none px-2 py-1.5 text-right font-semibold text-amber"
                            onClick={() => toggleMatrixSort(bandId, 'amber')}
                          >
                            Amber{sortIndicator(bandId, 'amber')}
                          </th>
                          <th
                            className="cursor-pointer select-none px-2 py-1.5 text-right font-semibold text-green"
                            onClick={() => toggleMatrixSort(bandId, 'green')}
                          >
                            Green{sortIndicator(bandId, 'green')}
                          </th>
                        </tr>
                      </thead>
                      <tbody>
                        {sortedMatrixRows(band, bandId).map((row) => {
                          const counts = matrixRagCounts(row.byYear, band.years)
                          const cagr = computeCagr(row.byYear, band.years)
                          return (
                            <tr key={row.variantId} className="border-t border-border">
                              <td className="sticky left-0 z-10 whitespace-nowrap bg-card px-2 py-1 font-medium">
                                {row.label}
                              </td>
                              {band.years.map((y) => {
                                const v = row.byYear.get(y)
                                return (
                                  <td
                                    key={y}
                                    className={`px-2 py-1 text-right ${v != null ? RAG_CLASSES[classifyRag(v, redBoundary, greenBoundary)] : ''}`}
                                  >
                                    {v != null ? `${v.toFixed(1)}%` : '—'}
                                  </td>
                                )
                              })}
                              <td className="px-2 py-1 text-right font-semibold">{cagr != null ? `${cagr.toFixed(1)}%` : '—'}</td>
                              <td className="px-2 py-1 text-right text-red">{counts.red}</td>
                              <td className="px-2 py-1 text-right text-amber">{counts.amber}</td>
                              <td className="px-2 py-1 text-right text-green">{counts.green}</td>
                            </tr>
                          )
                        })}
                      </tbody>
                    </table>
                  </div>
                </details>
              ))
          )}
        </CardContent>
      </Card>
    </AppShell>
  )
}
