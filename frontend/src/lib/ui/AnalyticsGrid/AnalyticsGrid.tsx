/**
 * lib/ui/AnalyticsGrid/AnalyticsGrid.tsx
 *
 * One grid-plus-chart workspace, reusable across the application.
 *
 * WHAT IT IS FOR. Screens where the reader compares many rows across many
 * periods: the four backtest report sections, the experiments matrix, any
 * future screener leaderboard. Those all want the same behaviour — a dense
 * sortable grid with conditional shading, a few rows ticked, and those rows
 * drawn as a trend beneath. Building that once means the interaction is
 * learned once.
 *
 * WHY AG GRID HERE AND NOT lib/ui/DataTable. DataTable (TanStack) remains the
 * right tool for an ordinary list: it is lighter and it renders the
 * application's own markup. This component exists for the analytical case
 * DataTable does not cover — grouped headers spanning fiscal tracks, pinned
 * identity columns, drag-to-reorder, shift-click multi-sort, and column state
 * that survives a reload. Both are exported; a caller picks by what the screen
 * is for, and neither is a replacement for the other.
 *
 * WHAT PERSISTS, AND WHY IT IS SCOPED. Filter text, column layout, chart
 * toggles and per-series colours are stored in localStorage under the `id`
 * this component is given. Scoping to `id` rather than a global key is what
 * lets two AnalyticsGrids coexist on one page without the second inheriting
 * the first's column order. Nothing about the DATA is persisted — a stale
 * layout is a nuisance, a stale number is a wrong decision.
 *
 * WHAT IS DELIBERATELY NOT HERE. No PDF library. The print path is real CSS
 * (`@media print`) plus `window.print()`, so the page prints through the
 * browser's own engine at full fidelity, with the controls hidden and the
 * grid allowed to expand past its scroll viewport. A bundled PDF renderer
 * would produce a second, worse rendering of the same page and add ~500 kB to
 * every route that imports this file.
 */

import { AgGridReact } from 'ag-grid-react'
import {
  AllCommunityModule,
  ModuleRegistry,
  colorSchemeDark,
  themeQuartz,
  type ColDef,
  type ColGroupDef,
  type GridApi,
  type GridReadyEvent,
  type SelectionChangedEvent,
} from 'ag-grid-community'
import { useCallback, useEffect, useMemo, useRef, useState } from 'react'

import { Button } from '@/lib/ui/primitives/button'
import { cn } from '@/lib/utils'

import {
  AnalyticsTrendChart,
  DEFAULT_CHART_OPTIONS,
  type TrendChartOptions,
  type TrendPoint,
  type TrendSeries,
} from './AnalyticsTrendChart'

// AG Grid 33+ ships nothing registered by default. Registering the community
// bundle once at module scope (rather than per mount) is the documented
// pattern and keeps repeated mounts free.
ModuleRegistry.registerModules([AllCommunityModule])

/**
 * Series colours, assigned in order as rows are ticked.
 *
 * Chosen for separability rather than for brand fit: adjacent hues are far
 * enough apart to survive both a projector and the ~8% of male readers with a
 * red/green deficiency, which a naive rainbow ramp does not. The reader can
 * override any of them at runtime, so this is a starting point, not a policy.
 */
export const SERIES_PALETTE = [
  '#2563eb',
  '#f59e0b',
  '#10b981',
  '#db2777',
  '#7c3aed',
  '#0891b2',
  '#ea580c',
  '#65a30d',
]

export interface HeatmapOptions {
  /** Value at which a positive cell reaches full green, as the same unit the
   * cell holds. */
  positiveCeiling: number
  /** Magnitude at which a negative cell reaches full red. */
  negativeCeiling: number
}

export interface AnalyticsGridSeries<T> {
  /** The x axis, in order. */
  categories: Array<{ key: string; label: string }>
  /** Value of one row in one category, or null for "no data in that period". */
  value: (row: T, categoryKey: string) => number | null
  label: (row: T) => string
  /** How a value reads in the tooltip and on the y axis. */
  format: (v: number) => string
  /** Shown beside the category in the tooltip header, e.g. "annualised". */
  valueLabel?: string
  chartTitle?: string
  chartDescription?: string
}

export interface AnalyticsGridProps<T> {
  /** Stable identity for persisted layout. Must be unique per screen. */
  id: string
  columns: Array<ColDef<T> | ColGroupDef<T>>
  rows: T[]
  getRowId: (row: T) => string
  /** Omit to render the grid alone, with no chart and no selection column. */
  series?: AnalyticsGridSeries<T>
  heatmap?: HeatmapOptions
  isLoading?: boolean
  emptyMessage?: string
  csvFileName?: string
  /** Heading printed above the grid on paper. A printed table with no title
   * is unusable a week later, and the surrounding page chrome — which carries
   * the title on screen — is deliberately not printed. */
  title?: string
  /** Grid viewport height in px. The print stylesheet overrides it so the
   * whole table prints rather than one screenful. */
  height?: number
  className?: string
}

// ---------------------------------------------------------------------------
// persistence
// ---------------------------------------------------------------------------

function storageKey(id: string, slot: string): string {
  return `alphalens.analyticsGrid.${id}.${slot}`
}

/** localStorage that cannot throw. Private-mode browsers and a full quota
 * both raise here, and neither is a reason for the whole screen to fail. */
function loadJson<T>(key: string, fallback: T): T {
  try {
    const raw = localStorage.getItem(key)
    return raw ? (JSON.parse(raw) as T) : fallback
  } catch {
    return fallback
  }
}

function saveJson(key: string, value: unknown): void {
  try {
    localStorage.setItem(key, JSON.stringify(value))
  } catch {
    /* quota or private mode — the layout simply does not persist */
  }
}

// ---------------------------------------------------------------------------
// conditional shading
// ---------------------------------------------------------------------------

/**
 * Background for one numeric cell, scaled by magnitude.
 *
 * Opacity carries the magnitude and hue carries the sign, so the table can be
 * scanned for shape before any number is read. Two rules matter:
 *
 * - A null is NOT a zero and gets no shading at all. Painting a missing period
 *   the same yellow as a genuinely flat one is how a strategy with no data
 *   starts looking like a strategy with no volatility.
 * - The floor opacity is non-zero, so a small positive is still visibly
 *   positive rather than fading into the row background.
 */
export function heatmapStyle(
  value: unknown,
  { positiveCeiling, negativeCeiling }: HeatmapOptions,
): Record<string, string> | null {
  if (value == null || typeof value !== 'number' || !Number.isFinite(value)) {
    return null
  }
  if (value === 0) {
    return { backgroundColor: 'color-mix(in srgb, var(--color-amber, #f59e0b) 12%, transparent)' }
  }
  const positive = value > 0
  const ceiling = positive ? positiveCeiling : negativeCeiling
  const intensity = ceiling > 0 ? Math.min(Math.abs(value) / ceiling, 1) : 1
  const alpha = Math.round((intensity * 0.55 + 0.12) * 100)
  const hue = positive ? 'var(--color-green, #16a34a)' : 'var(--color-red, #dc2626)'
  return {
    backgroundColor: `color-mix(in srgb, ${hue} ${alpha}%, transparent)`,
    fontVariantNumeric: 'tabular-nums',
  }
}

/** Opt a column into the heatmap: `{ ...col, context: HEATMAP_COLUMN }`. */
export const HEATMAP_COLUMN = { heatmap: true } as const

// ---------------------------------------------------------------------------
// component
// ---------------------------------------------------------------------------

export function AnalyticsGrid<T>({
  id,
  columns,
  rows,
  getRowId,
  series,
  heatmap,
  isLoading = false,
  emptyMessage = 'No rows to show.',
  csvFileName,
  title,
  height = 460,
  className,
}: AnalyticsGridProps<T>) {
  const apiRef = useRef<GridApi<T> | null>(null)
  const [selected, setSelected] = useState<T[]>([])

  const [filterText, setFilterText] = useState<string>(() =>
    loadJson(storageKey(id, 'filter'), ''),
  )
  const [chartOptions, setChartOptions] = useState<TrendChartOptions>(() =>
    loadJson(storageKey(id, 'chart'), DEFAULT_CHART_OPTIONS),
  )
  const [colorOverrides, setColorOverrides] = useState<Record<string, string>>(() =>
    loadJson(storageKey(id, 'colors'), {}),
  )
  const [thresholds, setThresholds] = useState<HeatmapOptions>(() =>
    loadJson(storageKey(id, 'heatmap'), heatmap ?? { positiveCeiling: 1, negativeCeiling: 0.5 }),
  )

  useEffect(() => saveJson(storageKey(id, 'filter'), filterText), [id, filterText])
  useEffect(() => saveJson(storageKey(id, 'chart'), chartOptions), [id, chartOptions])
  useEffect(() => saveJson(storageKey(id, 'colors'), colorOverrides), [id, colorOverrides])
  useEffect(() => saveJson(storageKey(id, 'heatmap'), thresholds), [id, thresholds])

  const isDark =
    typeof document !== 'undefined' &&
    (document.documentElement.classList.contains('dark') ||
      (typeof window !== 'undefined' &&
        window.matchMedia?.('(prefers-color-scheme: dark)').matches))

  // The Theming API rather than the legacy stylesheets: it inherits from the
  // page instead of importing a second design system, and it is the only
  // theming path AG Grid 33+ actually supports.
  const theme = useMemo(
    () =>
      (isDark ? themeQuartz.withPart(colorSchemeDark) : themeQuartz).withParams({
        fontFamily: 'inherit',
        fontSize: 12,
        headerFontWeight: 600,
        rowHeight: 34,
        headerHeight: 36,
        borderRadius: 6,
      }),
    [isDark],
  )

  const defaultColDef = useMemo<ColDef<T>>(
    () => ({
      sortable: true,
      filter: true,
      resizable: true,
      // Drag-to-reorder and drag-to-pin are on by default; individual columns
      // switch them off (the identity column does, since a table whose first
      // column can be dragged away loses its labels).
      suppressMovable: false,
      minWidth: 80,
      // Header tooltips are only useful if they actually appear. AG Grid ships
      // them off-by-default-ish (a 2s delay that most readers never wait out),
      // so the delay is dropped to something a hovering hand reaches.
      //
      // A cell with no explicit tooltipValueGetter falls back to its own
      // formatted text, which matters for the identity column: a 40-character
      // strategy name is truncated at 280px and the tooltip is the only way to
      // read the rest of it.
      tooltipValueGetter: (params) =>
        params.valueFormatted ?? (params.value == null ? null : String(params.value)),
      cellStyle: (params) =>
        heatmap && params.colDef.context?.heatmap
          ? heatmapStyle(params.value, thresholds)
          : null,
    }),
    [heatmap, thresholds],
  )

  /** The checkbox column, present only when there is a chart to feed. */
  const gridColumns = useMemo<Array<ColDef<T> | ColGroupDef<T>>>(() => {
    if (!series) return columns
    const selectionCol: ColDef<T> = {
      colId: '__select',
      headerName: '',
      width: 44,
      maxWidth: 44,
      pinned: 'left',
      lockPinned: true,
      lockPosition: true,
      suppressMovable: true,
      sortable: false,
      filter: false,
      resizable: false,
      // Excluded from CSV: a column of ticks is not data, and it shifts every
      // downstream column by one in a spreadsheet.
      suppressColumnsToolPanel: true,
    }
    return [selectionCol, ...columns]
  }, [columns, series])

  const onGridReady = useCallback(
    (event: GridReadyEvent<T>) => {
      apiRef.current = event.api
      const saved = loadJson<Parameters<GridApi['applyColumnState']>[0]['state'] | null>(
        storageKey(id, 'columnState'),
        null,
      )
      if (saved) {
        event.api.applyColumnState({ state: saved, applyOrder: true })
      }
    },
    [id],
  )

  const persistColumnState = useCallback(() => {
    if (apiRef.current) {
      saveJson(storageKey(id, 'columnState'), apiRef.current.getColumnState())
    }
  }, [id])

  const onSelectionChanged = useCallback((event: SelectionChangedEvent<T>) => {
    setSelected(event.api.getSelectedRows())
  }, [])

  const exportCsv = useCallback(() => {
    apiRef.current?.exportDataAsCsv({
      fileName: `${csvFileName ?? id}.csv`,
      // Export what is on screen: the reader's filter and sort are part of
      // what they are exporting, and silently exporting the unfiltered set is
      // how a shared CSV stops matching the screenshot beside it.
      onlySelectedAllPages: false,
      skipColumnGroupHeaders: false,
      columnKeys: apiRef.current
        ?.getAllDisplayedColumns()
        .map((c) => c.getColId())
        .filter((cid) => cid !== '__select'),
    })
  }, [csvFileName, id])

  /**
   * Print the GRID, not the page.
   *
   * Two problems the old one-line `window.print()` did not solve:
   *
   * 1. It printed the whole route — site navigation, tab strip, control bar,
   *    description cards — and the table came out squeezed onto page two.
   *    `body.analytics-grid-printing` hides everything and the print root
   *    alone is made visible again, so the paper carries the table and its
   *    chart and nothing else.
   * 2. AG Grid virtualises rows: only the ~30 in the viewport exist in the
   *    DOM, so a 400-row table printed 30 rows. `domLayout: 'print'` is the
   *    grid's own answer — it renders every row and drops its internal
   *    scrollers for the duration.
   *
   * Both are undone in a `finally`, and also on the browser's afterprint
   * event, so a cancelled print dialog cannot leave the screen blank.
   */
  const printGrid = useCallback(() => {
    const api = apiRef.current
    const body = document.body
    const restore = () => {
      body.classList.remove('analytics-grid-printing')
      api?.setGridOption('domLayout', undefined)
      window.removeEventListener('afterprint', restore)
    }
    window.addEventListener('afterprint', restore)
    try {
      api?.setGridOption('domLayout', 'print')
      body.classList.add('analytics-grid-printing')
      // One frame for the grid to lay every row out before the print dialog
      // snapshots the document; printing synchronously catches it mid-relayout
      // and clips the last rows.
      requestAnimationFrame(() => {
        window.print()
        restore()
      })
    } catch {
      restore()
    }
  }, [])

  const resetLayout = useCallback(() => {
    apiRef.current?.resetColumnState()
    saveJson(storageKey(id, 'columnState'), null)
  }, [id])

  // --- chart wiring -------------------------------------------------------

  const chartSeries = useMemo<TrendSeries[]>(() => {
    if (!series) return []
    return selected.map((row, index) => {
      const key = getRowId(row)
      return {
        key,
        label: series.label(row),
        color: colorOverrides[key] ?? SERIES_PALETTE[index % SERIES_PALETTE.length],
      }
    })
  }, [selected, series, getRowId, colorOverrides])

  const chartData = useMemo<TrendPoint[]>(() => {
    if (!series) return []
    return series.categories.map((category) => {
      const point: TrendPoint = { category: category.label }
      for (const row of selected) {
        point[getRowId(row)] = series.value(row, category.key)
      }
      return point
    })
  }, [series, selected, getRowId])

  const setOption = <K extends keyof TrendChartOptions>(
    key: K,
    value: TrendChartOptions[K],
  ) => setChartOptions((prev) => ({ ...prev, [key]: value }))

  return (
    <div className={cn('analytics-grid analytics-grid-print-root', className)}>
      <style>{PRINT_CSS}</style>

      {/* Screen-hidden, paper-only: the page heading is not printed, so
          without this the sheet is an unlabelled block of numbers. */}
      <div className="analytics-grid-print-heading" aria-hidden>
        <strong>{title ?? csvFileName ?? id}</strong>
        <span> — {rows.length} rows, printed {new Date().toLocaleDateString('en-IN')}</span>
      </div>

      {/* --- toolbar ------------------------------------------------------ */}
      <div className="analytics-grid-controls mb-3 flex flex-wrap items-end gap-x-4 gap-y-2">
        <div className="flex flex-col gap-1">
          <label
            htmlFor={`${id}-filter`}
            className="text-xs font-medium text-muted-foreground"
          >
            Filter
          </label>
          <input
            id={`${id}-filter`}
            type="search"
            value={filterText}
            placeholder="Search every column…"
            title="Matches across every column at once, including ones currently scrolled out of view. Remembered per tab between visits."
            onChange={(e) => setFilterText(e.target.value)}
            className="h-8 w-56 rounded-[var(--radius-token)] border border-border bg-background px-2 text-xs"
          />
        </div>

        {heatmap ? (
          <>
            <ThresholdInput
              id={`${id}-pos`}
              label="Full green at"
              value={thresholds.positiveCeiling}
              onChange={(v) =>
                setThresholds((prev) => ({ ...prev, positiveCeiling: v }))
              }
            />
            <ThresholdInput
              id={`${id}-neg`}
              label="Full red at −"
              value={thresholds.negativeCeiling}
              onChange={(v) =>
                setThresholds((prev) => ({ ...prev, negativeCeiling: v }))
              }
            />
          </>
        ) : null}

        <div className="ml-auto flex items-end gap-2">
          <Button
            type="button"
            variant="outline"
            size="sm"
            onClick={exportCsv}
            title="Downloads exactly what is on screen — your filter, sort and column order — as CSV. Values export as raw numbers, so a spreadsheet can total them."
          >
            Export CSV
          </Button>
          <Button
            type="button"
            variant="outline"
            size="sm"
            onClick={resetLayout}
            title="Puts the columns back to their original order, width and pinning. Does not touch your filter or the rows you have ticked."
          >
            Reset layout
          </Button>
          <Button
            type="button"
            variant="outline"
            size="sm"
            onClick={printGrid}
            title="Prints the grid and its chart only — no navigation, tabs or control bar. Every row is laid out first, so a long table prints in full rather than the screenful you can see. Choose “Save as PDF” in the print dialog for a PDF."
          >
            Print / PDF
          </Button>
        </div>
      </div>

      <p className="analytics-grid-controls mb-2 text-xs text-muted-foreground">
        Drag a column header to reorder, or onto the pinned area to pin it. Hold{' '}
        <kbd className="rounded border border-border px-1">Shift</kbd> while
        clicking headers to sort on several columns at once.
        {series ? ' Tick rows to chart them.' : ''}
      </p>

      {/* --- grid --------------------------------------------------------- */}
      <div className="analytics-grid-viewport" style={{ height, width: '100%' }}>
        <AgGridReact<T>
          theme={theme}
          columnDefs={gridColumns}
          defaultColDef={defaultColDef}
          rowData={rows}
          getRowId={(params) => getRowId(params.data)}
          quickFilterText={filterText}
          loading={isLoading}
          tooltipShowDelay={250}
          tooltipHideDelay={20000}
          // The grid's own tooltip, not the browser's: a native title attribute
          // cannot wrap, so a two-sentence column explanation renders as one
          // unreadable line running off the screen edge.
          enableBrowserTooltips={false}
          overlayNoRowsTemplate={`<span class="text-sm">${emptyMessage}</span>`}
          // Multi-sort on shift-click; the grid's own default is single-sort,
          // which makes "best Sharpe among the ones that beat the benchmark"
          // impossible to express.
          multiSortKey="ctrl"
          alwaysMultiSort={false}
          rowSelection={
            series
              ? {
                  mode: 'multiRow',
                  checkboxes: true,
                  headerCheckbox: true,
                  enableClickSelection: false,
                }
              : undefined
          }
          selectionColumnDef={{
            pinned: 'left',
            width: 44,
            maxWidth: 44,
            lockPinned: true,
            lockPosition: true,
            suppressMovable: true,
            resizable: false,
          }}
          onSelectionChanged={onSelectionChanged}
          onGridReady={onGridReady}
          onColumnMoved={persistColumnState}
          onColumnPinned={persistColumnState}
          onColumnResized={persistColumnState}
          onColumnVisible={persistColumnState}
          onSortChanged={persistColumnState}
          suppressDragLeaveHidesColumns
        />
      </div>

      {/* --- chart -------------------------------------------------------- */}
      {series ? (
        <section className="mt-5">
          <header className="mb-2 flex flex-wrap items-baseline justify-between gap-2">
            <div>
              <h3 className="text-sm font-semibold">
                {series.chartTitle ?? 'Selected strategies'}
              </h3>
              {series.chartDescription ? (
                <p className="text-xs text-muted-foreground">
                  {series.chartDescription}
                </p>
              ) : null}
            </div>
            <ChartToggles
              id={id}
              options={chartOptions}
              onChange={setOption}
            />
          </header>

          {chartSeries.length ? (
            <div className="analytics-grid-controls mb-2 flex flex-wrap items-center gap-3">
              {chartSeries.map((s) => (
                <label
                  key={s.key}
                  className="flex items-center gap-1.5 text-xs"
                  title={`Change the colour of ${s.label}`}
                >
                  <input
                    type="color"
                    value={s.color}
                    aria-label={`Colour for ${s.label}`}
                    onChange={(e) =>
                      setColorOverrides((prev) => ({
                        ...prev,
                        [s.key]: e.target.value,
                      }))
                    }
                    className="size-5 cursor-pointer rounded border border-border bg-transparent p-0"
                  />
                  <span className="max-w-56 truncate">{s.label}</span>
                </label>
              ))}
            </div>
          ) : null}

          <AnalyticsTrendChart
            data={chartData}
            series={chartSeries}
            options={chartOptions}
            format={series.format}
            valueLabel={series.valueLabel}
          />
        </section>
      ) : null}
    </div>
  )
}

function ThresholdInput({
  id,
  label,
  value,
  onChange,
}: {
  id: string
  label: string
  value: number
  onChange: (v: number) => void
}) {
  return (
    <div className="flex flex-col gap-1">
      <label htmlFor={id} className="text-xs font-medium text-muted-foreground">
        {label}
      </label>
      <input
        id={id}
        type="number"
        step="0.05"
        min="0"
        title="The value at which a shaded cell reaches full colour. Lower it to spread the contrast across a narrower range; raise it when a few outliers are washing everything else out."
        value={value}
        onChange={(e) => {
          const next = Number(e.target.value)
          if (Number.isFinite(next) && next >= 0) onChange(next)
        }}
        className="h-8 w-20 rounded-[var(--radius-token)] border border-border bg-background px-2 text-xs"
      />
    </div>
  )
}

function ChartToggles({
  id,
  options,
  onChange,
}: {
  id: string
  options: TrendChartOptions
  onChange: <K extends keyof TrendChartOptions>(
    key: K,
    value: TrendChartOptions[K],
  ) => void
}) {
  const checkboxes: Array<[keyof TrendChartOptions, string]> = [
    ['showGrid', 'Grid'],
    ['showXAxis', 'X axis'],
    ['showYAxis', 'Y axis'],
    ['showDots', 'Points'],
    ['showLegend', 'Legend'],
  ]
  return (
    <div className="analytics-grid-controls flex flex-wrap items-center gap-3">
      {checkboxes.map(([key, label]) => (
        <label key={key} className="flex items-center gap-1.5 text-xs">
          <input
            type="checkbox"
            checked={Boolean(options[key])}
            onChange={(e) => onChange(key, e.target.checked as never)}
          />
          {label}
        </label>
      ))}
      <label className="flex items-center gap-1.5 text-xs">
        <span className="sr-only">Line style</span>
        <select
          id={`${id}-stroke`}
          value={options.strokeStyle}
          onChange={(e) =>
            onChange('strokeStyle', e.target.value as TrendChartOptions['strokeStyle'])
          }
          className="h-7 rounded-[var(--radius-token)] border border-border bg-background px-1.5 text-xs"
        >
          <option value="solid">Solid</option>
          <option value="dashed">Dashed</option>
          <option value="dotted">Dotted</option>
        </select>
      </label>
    </div>
  )
}

/**
 * The print layout.
 *
 * Two things have to change for a screen grid to become a readable page: the
 * interactive chrome has to go, and the grid has to stop being a fixed-height
 * scroll viewport — otherwise the printer faithfully reproduces one screenful
 * and truncates the other forty rows.
 */
const PRINT_CSS = `
.analytics-grid-print-heading { display: none; }

@media print {
  /* Print the grid, nothing else. Hiding the body and re-showing only the
     print root beats hiding each ancestor by name, which breaks the moment a
     caller nests this component one level deeper. */
  body.analytics-grid-printing { visibility: hidden; }
  body.analytics-grid-printing .analytics-grid-print-root,
  body.analytics-grid-printing .analytics-grid-print-root * { visibility: visible; }
  body.analytics-grid-printing .analytics-grid-print-root {
    position: absolute;
    inset: 0 auto auto 0;
    width: 100%;
    padding: 0;
    margin: 0;
  }

  .analytics-grid-print-heading {
    display: block;
    margin-bottom: 8px;
    font-size: 12px;
  }

  .analytics-grid-controls { display: none !important; }
  .analytics-grid-viewport {
    height: auto !important;
    max-height: none !important;
    overflow: visible !important;
  }
  .analytics-grid .ag-body-viewport,
  .analytics-grid .ag-center-cols-viewport,
  .analytics-grid .ag-body-horizontal-scroll {
    overflow: visible !important;
  }
  .analytics-grid .ag-row { page-break-inside: avoid; }
  /* Repeat the header on every sheet — page four of a table with no column
     headings is a page of anonymous numbers. */
  .analytics-grid .ag-header { position: static !important; }
}
`
