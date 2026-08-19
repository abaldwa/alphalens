/**
 * features/backtest-report/data/useBenchmarkOverride.ts
 *
 * Makes the benchmark selector actually change the comparison.
 *
 * THE BUG. A backtest run stores the index it was scored against at the time
 * it ran — `benchmark_index_name`, almost always "Nifty 500" — together with
 * that index's CAGR and the excess return derived from it. The report header
 * offered a benchmark dropdown, but nothing downstream consumed it: choosing
 * "Nifty 100" changed a label in the URL and left every Benchmark and Excess
 * cell reporting Nifty 500. The user's reading of the screen was therefore
 * correct and the screen was wrong.
 *
 * WHAT THIS DOES. `GET /api/v1/indices/returns` measures any index's
 * buy-and-hold CAGR over an explicit window, using the same first-real-close
 * to last-real-close, calendar-annualised definition the engine's benchmark
 * curve uses. This hook asks for the selected index over each distinct run
 * window on screen and rewrites `benchmarkCagr` / `excessReturn` / the index
 * name on rows whose own benchmark differs.
 *
 * WHAT IT DELIBERATELY DOES NOT DO. It does not re-run the strategy. Only the
 * yardstick changes; the strategy's own CAGR is untouched, which is the whole
 * point of an excess return. And it never substitutes a figure it could not
 * measure: an index with fewer than two real bars in the window leaves the
 * row's benchmark null with the API's own `status` as the caveat, rather than
 * quietly falling back to the stored Nifty 500 number under a Nifty 100
 * heading — which is the exact failure this hook was written to end.
 */

import { useQuery } from '@tanstack/react-query'
import { useMemo } from 'react'

import { apiGet } from '@/shared/api/client'

import { cagrOn } from '../core/cagrOn'
import type { StrategyReport, TaxBasis } from '../core/types'

export interface IndexReturn {
  index_name: string
  start_date: string | null
  end_date: string | null
  start_close: number | null
  end_close: number | null
  cagr: number | null
  n_rows: number
  status: string
  caveat: string | null
}

export interface IndexReturnsResponse {
  returns: IndexReturn[]
}

/** A run's own window, as the cache key for one /indices/returns call. */
function windowKey(r: StrategyReport): string | null {
  const { startDate, endDate } = r.setup.window
  return startDate && endDate ? `${startDate}|${endDate}` : null
}

/**
 * The distinct run windows on screen. In practice a sweep shares one window,
 * so this is normally a single request; it is computed rather than assumed
 * because a mixed table (a 4-year revalidation run beside a 17-year sweep)
 * must not measure the benchmark over one strategy's window and subtract it
 * from another's.
 */
export function distinctWindows(rows: StrategyReport[]): string[] {
  const keys = new Set<string>()
  for (const r of rows) {
    const k = windowKey(r)
    if (k) keys.add(k)
  }
  return [...keys].sort()
}

async function fetchWindow(
  key: string,
  indexName: string,
): Promise<[string, IndexReturn | null]> {
  const [start, end] = key.split('|')
  const qs = new URLSearchParams({
    start_date: start,
    end_date: end,
    index_name: indexName,
  })
  const data = await apiGet<IndexReturnsResponse>(
    `/api/v1/indices/returns?${qs.toString()}`,
  )
  return [key, data.returns.find((r) => r.index_name === indexName) ?? null]
}

/**
 * Rows re-scored against `benchmark`, or the rows unchanged when no benchmark
 * is selected (the run's own stored comparison is then the honest one).
 */
export function useBenchmarkOverride(
  rows: StrategyReport[],
  benchmark: string | null,
  taxBasis: TaxBasis,
) {
  const windows = useMemo(() => distinctWindows(rows), [rows])

  const query = useQuery({
    queryKey: ['index-returns', benchmark, windows],
    enabled: Boolean(benchmark) && windows.length > 0,
    queryFn: async () => {
      const pairs = await Promise.all(
        windows.map((w) => fetchWindow(w, benchmark!)),
      )
      return new Map(pairs)
    },
  })

  const strategies = useMemo(() => {
    if (!benchmark || !query.data) return rows
    const measured = query.data
    return rows.map((r) => {
      // A row already scored against the selected index needs no rewrite —
      // and must not get one, since the engine measured it against the
      // strategy's own equity dates rather than the index's calendar.
      if (r.returns.benchmarkIndexName === benchmark) return r
      const key = windowKey(r)
      const hit = key ? measured.get(key) : null
      const own = cagrOn(r, taxBasis)
      const benchmarkCagr = hit?.cagr ?? null
      return {
        ...r,
        returns: {
          ...r.returns,
          benchmarkIndexName: benchmark,
          benchmarkCagr,
          excessReturn:
            benchmarkCagr != null && own != null ? own - benchmarkCagr : null,
          benchmarkCaveat:
            benchmarkCagr == null
              ? `${benchmark} has no usable history over this run's window (${hit?.status ?? 'not measured'}), so no excess return can be stated.`
              : hit?.caveat ?? null,
        },
      }
    })
  }, [rows, benchmark, query.data, taxBasis])

  return {
    strategies,
    isLoading: query.isLoading,
    error: query.error as Error | null,
  }
}
