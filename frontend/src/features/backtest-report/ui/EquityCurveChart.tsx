/**
 * features/backtest-report/ui/EquityCurveChart.tsx
 *
 * The mark-to-market portfolio value over the run's window (A90).
 *
 * Log Y axis, matching pages/technical/comparison.tsx: over a multi-year
 * window a linear axis compresses the early years into the baseline and hides
 * every drawdown that happened while the capital was small. The axis choice is
 * stated on the chart rather than left for the reader to infer.
 *
 * An empty or still-loading series says which it is instead of rendering an
 * empty chart frame — a blank plot and a portfolio worth nothing look
 * identical otherwise.
 */

import {
  CartesianGrid,
  Line,
  LineChart,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from 'recharts'

import { inr } from '../core/format'
import type { EquityPoint } from '../core/types'

export function EquityCurveChart({
  series,
  isLoading,
  isEmpty,
  initialCapital,
}: {
  series: EquityPoint[]
  isLoading?: boolean
  isEmpty?: boolean
  /** Drawn as a reference level so a curve can be read as above or below the
   * capital it started with without doing arithmetic against the axis. */
  initialCapital?: number | null
}) {
  if (isLoading) {
    return (
      <p className="py-16 text-center text-sm text-muted-foreground">
        Loading equity curve…
      </p>
    )
  }

  if (isEmpty || series.length === 0) {
    return (
      <p className="py-16 text-center text-sm text-muted-foreground">
        This run carries no equity curve. Runs completed before the curve was
        persisted (A90) report their metrics but not the series behind them —
        re-run the strategy to populate it.
      </p>
    )
  }

  return (
    <>
      <div style={{ width: '100%', height: 360 }}>
        <ResponsiveContainer>
          <LineChart
            data={series}
            margin={{ top: 8, right: 16, bottom: 8, left: 8 }}
          >
            <CartesianGrid strokeOpacity={0.15} vertical={false} />
            <XAxis dataKey="date" tick={{ fontSize: 11 }} minTickGap={48} />
            <YAxis
              scale="log"
              domain={['auto', 'auto']}
              tick={{ fontSize: 11 }}
              width={80}
              tickFormatter={(v) => inr(Number(v))}
            />
            <Tooltip
              formatter={(v) => (typeof v === 'number' ? inr(v) : String(v))}
            />
            <Line
              type="monotone"
              dataKey="value"
              name="Portfolio value"
              dot={false}
              stroke="var(--chart-1, #2563eb)"
              strokeWidth={1.8}
            />
          </LineChart>
        </ResponsiveContainer>
      </div>
      <p className="mt-2 text-xs text-muted-foreground">
        Cash plus positions marked to market, the same series the run&apos;s
        CAGR, drawdown and Sharpe are measured from — not the cash balance.
        Log scale, so early drawdowns stay visible.
        {initialCapital != null
          ? ` Started at ${inr(initialCapital)}.`
          : null}
      </p>
    </>
  )
}
