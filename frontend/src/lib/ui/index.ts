// Single import surface for the AlphaLens React UI library. Consumers
// (page code under src/pages/**) should import everything UI-related from
// here — `import { Button, Card, AppShell } from "@/lib/ui"` — rather than
// deep-importing primitives or composites directly.

// shadcn/ui-style primitives (Radix + CVA + Tailwind), generated in the
// standard shadcn output shape under primitives/ and re-exported below.
export * from './primitives/button'
export * from './primitives/card'
export * from './primitives/table'
export * from './primitives/skeleton'
export * from './primitives/badge'
export * from './primitives/sheet'
export * from './primitives/tooltip'
export * from './primitives/input'
export * from './primitives/dropdown-menu'

// Composite components built on top of the primitives.
export * from './AppShell'
export * from './CopilotPanel'
export * from './StatCard'
export * from './DataTable'
export * from './ResponsiveChartCard'
export * from './SectionListPage'
export * from './PriceChart'
export * from './TickerLink'
export * from './InfoTooltip'
export * from './nav'
export * from './SignalBadge'
export * from './ConfidenceMatrix'
export * from './SymbolPageLayout'
export * from './table-utils'
export * from './columns'
export * from './TradingViewWidget'
