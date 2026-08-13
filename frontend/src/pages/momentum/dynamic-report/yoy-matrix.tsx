import { useMemo, useState } from 'react'

import { AppShell, Card, CardContent, CardDescription, CardHeader, CardTitle } from '@/lib/ui'
import { BackToReportLink } from './SectionNav'
import { bandLabel, classifyRag, RAG_CLASSES, rowLabel, useDynamicReportData, type RagBand } from './shared'

export function MomentumYoyMatrixPage() {
  const { report, allYoyRows } = useDynamicReportData()

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

  // Pivot: per band, one row per strategy variant, one column per fiscal
  // year -- lets you scan consistency across years at a glance, instead of
  // the flat (band, variant, year) table where each variant's history is
  // spread across many rows.
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

  return (
    <AppShell title="Momentum — YoY Consistency Matrix" description="Part of the Momentum Strategy Report.">
      <BackToReportLink />
      <Card>
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
                <details key={bandId} className="mb-4 rounded-[var(--radius-token)] border border-border">
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
