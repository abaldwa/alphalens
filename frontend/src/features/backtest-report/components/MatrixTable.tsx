/**
 * features/backtest-report/components/MatrixTable.tsx
 *
 * The sortable pivot table the whole section standardises on: strategies down
 * the side, periods across the top, one rate per cell, RAG-shaded.
 *
 * Extracted from pages/momentum/dynamic-report/yoy-matrix.tsx, which had the
 * right idea but was momentum-shaped and hand-sorted. Three changes made on
 * the way out:
 *
 * - It takes fractions, not percentages, because that is what StrategyReport
 *   carries; formatting happens at the edge via format.ts.
 * - Header sort controls are real <button>s with aria-sort, so the table is
 *   operable from the keyboard. The original bound onClick to a bare <th>.
 * - Row labels render through StrategyLink, so every strategy named anywhere
 *   in the section is a link to the same detail page.
 *
 * The summary columns are a CAGR over the visible periods and the RAG counts.
 * The CAGR is a geometric mean of the per-year rates — a rate derived from
 * rates, which is the only thing AGENTS.md's rate rule permits here.
 */

import { useMemo, useState } from 'react'

import { cn } from '@/lib/utils'

import { EM_DASH, rate } from '../format'
import {
  RAG_CLASSES,
  classifyRag,
  periodCagr,
  ragCounts,
  type MatrixColumn,
  type MatrixValues,
  type RagBoundaries,
} from '../matrix'
import type { StrategyKey } from '../types'
import { StrategyLink } from './StrategyLink'

export interface MatrixRow {
  key: StrategyKey
  label: string
  /** Fraction per column key; a missing key renders as an em dash, which is
   * not the same fact as a zero return. */
  values: MatrixValues
}

export interface MatrixTableProps {
  columns: MatrixColumn[]
  rows: MatrixRow[]
  boundaries: RagBoundaries
  /** Highlighted row, from the section-wide ?strategy= parameter. */
  selectedKey?: StrategyKey | null
  /** Which section the row links into; defaults to the detail page. */
  linkSection?: string
  caption?: string
}

type SortKey = string
interface SortState {
  key: SortKey
  dir: 'asc' | 'desc'
}

export function MatrixTable({
  columns,
  rows,
  boundaries,
  selectedKey,
  linkSection,
  caption,
}: MatrixTableProps) {
  const [sort, setSort] = useState<SortState | null>(null)

  const derived = useMemo(
    () =>
      rows.map((row) => ({
        row,
        cagr: periodCagr(row.values, columns),
        counts: ragCounts(row.values, columns, boundaries),
      })),
    [rows, columns, boundaries],
  )

  const sorted = useMemo(() => {
    const out = [...derived]
    if (!sort) {
      out.sort((a, b) => a.row.label.localeCompare(b.row.label))
      return out
    }
    const valueOf = (d: (typeof out)[number]): number | string | null => {
      if (sort.key === 'label') return d.row.label
      if (sort.key === 'cagr') return d.cagr
      if (sort.key === 'red' || sort.key === 'amber' || sort.key === 'green') {
        return d.counts[sort.key]
      }
      return d.row.values[sort.key] ?? null
    }
    out.sort((a, b) => {
      const av = valueOf(a)
      const bv = valueOf(b)
      // Nulls sink regardless of direction: an absent year is not a worst
      // year, and letting it sort as one puts empty rows at the top of a
      // "best consistency" view.
      if (av == null && bv == null) return 0
      if (av == null) return 1
      if (bv == null) return -1
      const cmp =
        typeof av === 'string' && typeof bv === 'string'
          ? av.localeCompare(bv)
          : Number(av) - Number(bv)
      return sort.dir === 'asc' ? cmp : -cmp
    })
    return out
  }, [derived, sort])

  function toggle(key: SortKey) {
    setSort((prev) =>
      prev?.key === key && prev.dir === 'asc'
        ? { key, dir: 'desc' }
        : { key, dir: 'asc' },
    )
  }

  function ariaSort(key: SortKey): 'ascending' | 'descending' | 'none' {
    if (sort?.key !== key) return 'none'
    return sort.dir === 'asc' ? 'ascending' : 'descending'
  }

  function SortButton({ sortKey, label }: { sortKey: SortKey; label: string }) {
    const active = sort?.key === sortKey
    return (
      <button
        type="button"
        onClick={() => toggle(sortKey)}
        className="inline-flex items-center gap-1 rounded-[var(--radius-token)] px-0.5 hover:underline focus-visible:outline focus-visible:outline-2 focus-visible:outline-offset-2 focus-visible:outline-ring"
      >
        {label}
        <span aria-hidden="true" className="text-[0.65rem]">
          {active ? (sort.dir === 'asc' ? '▲' : '▼') : ''}
        </span>
      </button>
    )
  }

  if (rows.length === 0) {
    return <p className="text-sm text-muted-foreground">No rows to compare yet.</p>
  }

  return (
    // The wrapper scrolls, not the page: a 15-year matrix is wider than any
    // viewport and the section must never scroll horizontally as a whole.
    <div className="overflow-x-auto">
      <table className="w-full border-collapse text-xs">
        {caption ? <caption className="sr-only">{caption}</caption> : null}
        <thead>
          <tr>
            <th
              scope="col"
              aria-sort={ariaSort('label')}
              className="sticky left-0 z-10 bg-card px-2 py-1.5 text-left font-semibold"
            >
              <SortButton sortKey="label" label="Strategy" />
            </th>
            {columns.map((c) => (
              <th
                key={c.key}
                scope="col"
                aria-sort={ariaSort(c.key)}
                className="px-2 py-1.5 text-right font-semibold"
              >
                <SortButton sortKey={c.key} label={c.label} />
              </th>
            ))}
            <th
              scope="col"
              aria-sort={ariaSort('cagr')}
              className="px-2 py-1.5 text-right font-semibold"
            >
              <SortButton sortKey="cagr" label="CAGR" />
            </th>
            {(['red', 'amber', 'green'] as const).map((band) => (
              <th
                key={band}
                scope="col"
                aria-sort={ariaSort(band)}
                className={cn('px-2 py-1.5 text-right font-semibold', `text-${band}`)}
              >
                <SortButton
                  sortKey={band}
                  label={band[0].toUpperCase() + band.slice(1)}
                />
              </th>
            ))}
          </tr>
        </thead>
        <tbody>
          {sorted.map(({ row, cagr, counts }) => {
            const selected = selectedKey === row.key
            return (
              <tr
                key={row.key}
                className={cn(
                  'border-t border-border',
                  selected && 'ring-2 ring-inset ring-ring',
                )}
              >
                <th
                  scope="row"
                  className="sticky left-0 z-10 whitespace-nowrap bg-card px-2 py-1 text-left font-medium"
                >
                  <StrategyLink
                    strategyKey={row.key}
                    label={row.label}
                    section={linkSection}
                  />
                </th>
                {columns.map((c) => {
                  const v = row.values[c.key]
                  const known = v != null && Number.isFinite(v)
                  return (
                    <td
                      key={c.key}
                      className={cn(
                        'px-2 py-1 text-right tabular-nums',
                        known && RAG_CLASSES[classifyRag(v as number, boundaries)],
                      )}
                    >
                      {known ? rate(v) : EM_DASH}
                    </td>
                  )
                })}
                <td className="px-2 py-1 text-right font-semibold tabular-nums">
                  {rate(cagr)}
                </td>
                <td className="px-2 py-1 text-right tabular-nums text-red">
                  {counts.red}
                </td>
                <td className="px-2 py-1 text-right tabular-nums text-amber">
                  {counts.amber}
                </td>
                <td className="px-2 py-1 text-right tabular-nums text-green">
                  {counts.green}
                </td>
              </tr>
            )
          })}
        </tbody>
      </table>
    </div>
  )
}
