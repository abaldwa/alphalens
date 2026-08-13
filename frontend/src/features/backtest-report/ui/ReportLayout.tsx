/**
 * features/backtest-report/components/ReportLayout.tsx
 *
 * The frame every section shares: title, the sub-navigation across the six
 * sections, and the control bar. Sections differ only in their table.
 *
 * The sub-nav preserves the current query string, which is the mechanism that
 * closes the link chain the report is built around — jumping from Returns to
 * Consistency keeps the selected strategy, window, benchmark and tax basis
 * rather than resetting to defaults halfway through a comparison.
 */

import type { ReactNode } from 'react'
import { NavLink, useLocation } from 'react-router-dom'

import { AppShell, Card, CardContent, CardHeader, CardTitle, CardDescription } from '@/lib/ui'
import { cn } from '@/lib/utils'

import { REPORT_SECTIONS } from './sections'
import type { ReportParams } from '../data/useReportParams'
import type { BenchmarkOption } from '../core/types'
import {
  BenchmarkSelector,
  ModeToggle,
  TaxBasisToggle,
  WindowSelector,
} from './ReportControls'

export interface ReportLayoutProps {
  title: string
  description?: string
  params: ReportParams
  onChange: (patch: Partial<ReportParams>) => void
  benchmarkOptions: BenchmarkOption[]
  recommendedBenchmark?: string | null
  fallbackReason?: string | null
  resolvedStart: string | null
  children: ReactNode
}

export function ReportLayout({
  title,
  description,
  params,
  onChange,
  benchmarkOptions,
  recommendedBenchmark,
  fallbackReason,
  resolvedStart,
  children,
}: ReportLayoutProps) {
  const { search } = useLocation()

  return (
    <AppShell title={title} description={description}>
      <nav aria-label="Backtest report sections" className="mb-4">
        <ul className="flex flex-wrap gap-1 border-b border-border">
          {REPORT_SECTIONS.map((s) => (
            <li key={s.path}>
              <NavLink
                // Carrying the query string is what keeps the selected
                // strategy and settings alive across the chain.
                to={{ pathname: s.path, search }}
                end={s.path === '/backtest-report'}
                className={({ isActive }) =>
                  cn(
                    '-mb-px inline-block border-b-2 px-3 py-2 text-sm',
                    isActive
                      ? 'border-primary font-semibold text-foreground'
                      : 'border-transparent text-muted-foreground hover:text-foreground',
                  )
                }
              >
                {s.label}
              </NavLink>
            </li>
          ))}
        </ul>
      </nav>

      <Card className="mb-4">
        <CardHeader className="pb-3">
          <CardTitle className="text-sm">View</CardTitle>
          <CardDescription>
            Every setting here is carried in the URL, so this exact view is
            shareable and reproducible.
          </CardDescription>
        </CardHeader>
        <CardContent className="flex flex-wrap items-center gap-x-6 gap-y-3">
          <ModeToggle mode={params.mode} onChange={onChange} />
          <TaxBasisToggle basis={params.taxBasis} onChange={onChange} />
          <WindowSelector
            window={params.window}
            startDate={resolvedStart}
            onChange={onChange}
          />
          <BenchmarkSelector
            benchmark={params.benchmark}
            options={benchmarkOptions}
            recommended={recommendedBenchmark}
            fallbackReason={fallbackReason}
            onChange={onChange}
          />
        </CardContent>
      </Card>

      {children}
    </AppShell>
  )
}
