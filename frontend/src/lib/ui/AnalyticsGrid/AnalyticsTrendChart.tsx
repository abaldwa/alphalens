/**
 * lib/ui/AnalyticsGrid/AnalyticsTrendChart.tsx
 *
 * The multi-series area chart under an AnalyticsGrid.
 *
 * It draws the rows the user TICKED, and nothing else. A chart that plots
 * every row in the table is a wall of spaghetti nobody reads; a chart that
 * plots the four the reader chose is the comparison they were making anyway.
 * Selection therefore lives in the grid, and this component is a pure
 * function of it.
 *
 * Every series is a gradient area over a shared category axis, with the fill
 * deliberately faint: overlapping opaque areas hide each other, and the
 * comparison here is between LINES — where they cross, which one recovers
 * first — not between filled volumes.
 */

import { useId } from 'react'
import {
  Area,
  AreaChart,
  CartesianGrid,
  Legend,
  ReferenceLine,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from 'recharts'

export interface TrendSeries {
  /** Stable key — also the dataKey into each point. */
  key: string
  label: string
  color: string
}

export interface TrendPoint {
  category: string
  [seriesKey: string]: number | string | null
}

export interface TrendChartOptions {
  showGrid: boolean
  showXAxis: boolean
  showYAxis: boolean
  showDots: boolean
  showLegend: boolean
  /** solid | dashed | dotted — carried so a printed page still distinguishes
   * series when the colour is lost to a monochrome printer. */
  strokeStyle: 'solid' | 'dashed' | 'dotted'
}

export const DEFAULT_CHART_OPTIONS: TrendChartOptions = {
  showGrid: true,
  showXAxis: true,
  showYAxis: true,
  showDots: true,
  showLegend: true,
  strokeStyle: 'solid',
}

export function strokeDashArray(style: TrendChartOptions['strokeStyle']) {
  if (style === 'dashed') return '6 4'
  if (style === 'dotted') return '2 3'
  return undefined
}

/**
 * Truncation happens in the tooltip only, never in the legend or the export.
 * Strategy names in this application run to 40 characters and the tooltip is
 * the one place where the full name would push the numbers off screen.
 */
function shortLabel(label: string, max = 34): string {
  return label.length > max ? `${label.slice(0, max - 1)}…` : label
}

function ChartTooltip({
  active,
  payload,
  label,
  format,
  valueLabel,
}: {
  active?: boolean
  payload?: Array<{ name?: string; value?: number | string; color?: string }>
  label?: string
  format: (v: number) => string
  valueLabel?: string
}) {
  if (!active || !payload?.length) return null
  // Largest first: with six overlapping series the reader is nearly always
  // asking "who is on top here?", and sorting answers it without hunting.
  const rows = [...payload]
    .filter((p) => typeof p.value === 'number' && Number.isFinite(p.value))
    .sort((a, b) => Number(b.value) - Number(a.value))
  return (
    <div className="rounded-[var(--radius-token)] border border-border bg-popover px-3 py-2 text-xs shadow-md">
      <p className="mb-1.5 border-b border-border pb-1 font-semibold text-foreground">
        {label}
        {valueLabel ? (
          <span className="ml-2 font-normal text-muted-foreground">{valueLabel}</span>
        ) : null}
      </p>
      {rows.map((p) => (
        <div key={p.name} className="flex items-baseline justify-between gap-4 py-0.5">
          <span className="flex items-center gap-1.5" style={{ color: p.color }}>
            <span
              aria-hidden
              className="inline-block size-2 rounded-full"
              style={{ backgroundColor: p.color }}
            />
            {shortLabel(String(p.name))}
          </span>
          <span className="font-semibold tabular-nums text-foreground">
            {format(Number(p.value))}
          </span>
        </div>
      ))}
    </div>
  )
}

export function AnalyticsTrendChart({
  data,
  series,
  options,
  format,
  valueLabel,
  height = 320,
}: {
  data: TrendPoint[]
  series: TrendSeries[]
  options: TrendChartOptions
  format: (v: number) => string
  valueLabel?: string
  height?: number
}) {
  // Gradient ids must be unique per mounted chart, or two charts on one page
  // share a fill and the second silently takes the first one's colours.
  const gradientPrefix = useId().replace(/:/g, '')

  if (!series.length) {
    return (
      <p className="py-10 text-center text-sm text-muted-foreground">
        Tick one or more rows in the table above to chart them.
      </p>
    )
  }

  return (
    <div style={{ width: '100%', height }}>
      <ResponsiveContainer width="100%" height="100%">
        <AreaChart data={data} margin={{ top: 8, right: 16, bottom: 4, left: 4 }}>
          <defs>
            {series.map((s) => (
              <linearGradient
                key={s.key}
                id={`${gradientPrefix}-${s.key}`}
                x1="0"
                y1="0"
                x2="0"
                y2="1"
              >
                <stop offset="5%" stopColor={s.color} stopOpacity={0.35} />
                <stop offset="95%" stopColor={s.color} stopOpacity={0.02} />
              </linearGradient>
            ))}
          </defs>
          {options.showGrid ? (
            <CartesianGrid strokeDasharray="3 3" className="stroke-border" vertical={false} />
          ) : null}
          {options.showXAxis ? (
            <XAxis
              dataKey="category"
              tick={{ fontSize: 11 }}
              className="fill-muted-foreground"
              tickLine={false}
            />
          ) : (
            <XAxis dataKey="category" hide />
          )}
          {options.showYAxis ? (
            <YAxis
              tick={{ fontSize: 11 }}
              className="fill-muted-foreground"
              tickLine={false}
              axisLine={false}
              tickFormatter={(v: number) => format(v)}
              width={70}
            />
          ) : (
            <YAxis hide />
          )}
          {/* Zero is the line that matters on a returns chart — above it the
              strategy made money, below it did not — so it is drawn
              explicitly rather than left to whichever gridline lands nearby. */}
          <ReferenceLine y={0} className="stroke-border" strokeWidth={1.5} />
          <Tooltip
            content={
              <ChartTooltip format={format} valueLabel={valueLabel} /> as never
            }
          />
          {options.showLegend ? (
            <Legend
              wrapperStyle={{ fontSize: 11 }}
              formatter={(value: string) => shortLabel(value, 40)}
            />
          ) : null}
          {series.map((s) => (
            <Area
              key={s.key}
              type="monotone"
              dataKey={s.key}
              name={s.label}
              stroke={s.color}
              strokeWidth={2}
              strokeDasharray={strokeDashArray(options.strokeStyle)}
              fill={`url(#${gradientPrefix}-${s.key})`}
              // A gap is drawn as a gap. Joining across a year the strategy
              // did not exist would draw a straight line through nothing and
              // read as a flat year.
              connectNulls={false}
              dot={options.showDots ? { r: 2.5, strokeWidth: 0 } : false}
              activeDot={{ r: 4 }}
              isAnimationActive={false}
            />
          ))}
        </AreaChart>
      </ResponsiveContainer>
    </div>
  )
}
