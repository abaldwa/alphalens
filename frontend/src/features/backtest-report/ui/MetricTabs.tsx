/**
 * features/backtest-report/ui/MetricTabs.tsx
 *
 * The tab strip over the four metric groups — Returns, Consistency, Risk,
 * Trade quality — plus an All-metrics view that puts every group into one
 * grid.
 *
 * WHY TABS RATHER THAN FOUR UNRELATED PAGES. The four groups are four views
 * of ONE comparison, not four different reports. A reader deciding between
 * strategies moves between them constantly: a strong CAGR sends you to
 * drawdown, a good drawdown sends you to churn. Making that a tab rather than
 * a trip back up to the site navigation keeps the comparison on screen and
 * keeps the reader's place in it.
 *
 * WHY THEY ARE STILL ROUTES. Each tab is a real URL, so the whole query
 * string — window, benchmark, tax basis, mode, selected strategy — travels
 * with it and the browser's Back button walks the path the reader actually
 * took. A tab implemented as local component state would break every link in
 * the report, which is the one thing this section is built around.
 *
 * The site-level strip in ReportLayout stays where it is. It answers a
 * different question ("which part of the report am I in — overview,
 * recommendations, pivot, metrics?"); this one answers "which attribute of
 * the strategies am I looking at?".
 */

import { NavLink, useLocation } from 'react-router-dom'

import { cn } from '@/lib/utils'

export type MetricTabId =
  | 'all'
  | 'returns'
  | 'consistency'
  | 'risk'
  | 'trade-quality'

export interface MetricTab {
  id: MetricTabId
  path: string
  label: string
  /** One line under the tab strip: what this view is FOR, not what is in it. */
  blurb: string
}

/**
 * All-metrics leads deliberately.
 *
 * AG Grid's grouped headers make a single wide table genuinely readable —
 * collapse a group and it folds to its headline column — so the consolidated
 * view is the one that answers most questions, and the focused tabs are there
 * for when a reader wants the noise gone. The old four-separate-tables layout
 * had no way to express that, which is why it did not offer it.
 */
export const METRIC_TABS: MetricTab[] = [
  {
    id: 'all',
    path: '/backtest-report/metrics',
    label: 'All metrics',
    blurb:
      'Every group in one grid. Collapse a header group to fold it down to its headline column, or drag the ones you care about next to each other.',
  },
  {
    id: 'returns',
    path: '/backtest-report/returns',
    label: 'Returns',
    blurb:
      'CAGR and XIRR on the selected basis, against the selected benchmark. Every figure is a rate per year, never a total over the window.',
  },
  {
    id: 'consistency',
    path: '/backtest-report/consistency',
    label: 'Consistency',
    blurb:
      'Rolling windows and year-on-year returns: does the strategy work repeatedly, or did one year carry the whole record?',
  },
  {
    id: 'risk',
    path: '/backtest-report/risk',
    label: 'Risk',
    blurb:
      'Drawdown, volatility and risk-adjusted return. The drawdown is the number that decides whether you would actually have stayed invested.',
  },
  {
    id: 'trade-quality',
    path: '/backtest-report/trade-quality',
    label: 'Trade quality',
    blurb:
      'Churn, holding period and the shape of the average win against the average loss — what the strategy costs to run, and how it makes money.',
  },
]

export function findMetricTab(id: MetricTabId): MetricTab {
  return METRIC_TABS.find((t) => t.id === id) ?? METRIC_TABS[0]
}

export function MetricTabs({ active }: { active: MetricTabId }) {
  const { search } = useLocation()
  const current = findMetricTab(active)

  return (
    <div className="mb-4">
      <div
        role="tablist"
        aria-label="Strategy metric groups"
        className="flex flex-wrap gap-1 rounded-[var(--radius-token)] border border-border bg-muted/40 p-1"
      >
        {METRIC_TABS.map((tab) => (
          <NavLink
            key={tab.id}
            // Carrying the query string is what keeps the window, benchmark,
            // tax basis and selected strategy alive across a tab change.
            to={{ pathname: tab.path, search }}
            role="tab"
            aria-selected={tab.id === active}
            className={cn(
              'rounded-[var(--radius-token)] px-3 py-1.5 text-sm transition-colors focus-visible:outline focus-visible:outline-2 focus-visible:outline-offset-2 focus-visible:outline-ring',
              tab.id === active
                ? 'bg-background font-semibold text-foreground shadow-sm'
                : 'text-muted-foreground hover:text-foreground',
            )}
          >
            {tab.label}
          </NavLink>
        ))}
      </div>
      <p className="mt-2 text-xs text-muted-foreground">{current.blurb}</p>
    </div>
  )
}
