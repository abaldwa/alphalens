# AlphaLens UI Framework

`src/lib/ui` is the single source of truth for every visual/structural
element in the frontend — design tokens, primitives, and composites. Page
code (`src/pages/**`) is expected to compose these, not invent its own
markup for things this module already provides.

**Import rule**: always import from the barrel, `@/lib/ui`, never a deep
path like `@/lib/ui/Badge`. This is enforced by the `no-restricted-imports`
oxlint rule scoped to `src/pages/**` and `src/app/**` (see `.oxlintrc.json`)
— a deep import fails `npm run lint`. The reason: a component only
propagates a framework-wide fix (a new variant, a token rename) to every
call site if every call site went through the same export. If you add a
new file under `lib/ui/`, add its `export * from './YourFile'` line to
`index.ts` in the same change, or every consumer will fail lint.

## Design tokens

All color/spacing/radius values live in `src/index.css` as CSS custom
properties (`--bg`, `--tx`, `--teal/blue/purple/green/amber/red`,
`--signal-buy/hold/sell`, `--radius`, `--radius-lg`), with dark-mode
overrides under `.dark`. Tailwind v4's `@theme inline` block re-exposes
them as utility classes (`bg-primary`, `text-signal-sell`, `rounded-[var(--radius-token)]`).
**Never hardcode a hex color or px radius in a component** — add or reuse a
token instead, so a palette change is one edit in `index.css` instead of a
grep-and-replace across the app.

## Primitives (`primitives/`)

shadcn/ui-style building blocks (Radix + `class-variance-authority` +
Tailwind): `Button`, `Card`/`CardHeader`/`CardContent`/`CardTitle`, `Table`
family (`Table`, `TableHeader`, `TableBody`, `TableRow`, `TableHead`,
`TableCell`, `TableCaption`), `Badge`, `Skeleton`, `Sheet`, `Tooltip`, `Input`.

Any table markup in a page — including small key/value or matrix tables —
should use the `Table` family, not a raw `<table>`. `Table` already wraps
its content in an `overflow-x-auto` container and applies consistent
border/spacing/typography; a raw `<table>` silently drifts from that and
was previously found duplicated across 10 pages before being converted.

## Composites

Built on the primitives, one per file, all exported from the barrel:

- **`AppShell`** — the sidebar + top-bar shell every page renders inside. Sidebar structure comes from `NAV_SECTIONS` (`nav.ts`) — edit that, not AppShell, to add/rename a section.
- **`DataTable`** — the shared data grid (TanStack Table): sorting, column resizing, priority-based column collapse, a built-in search box, and an optional facet-chip filter.
  - Tag a column `meta: { priority: 'low' }` to have it collapse into a per-row expandable disclosure instead of forcing horizontal scroll when the table's content is wider than its container (collapse triggers by comparing actual rendered column width — with `table-layout: fixed` enforced — to the measured container, not a fixed breakpoint).
  - Add `meta: { group: 'someKey' }` to cluster related low-priority columns onto one disclosure line instead of each getting its own — e.g. `group: 'identity'` for name+sector, `group: 'price'` for CMP/support/resistance (see `technical/watchlist.tsx`).
  - Add `meta: { align: 'right' }` to right-align a numeric/price/score column with tabular-numeral spacing — applies to both the main row and the collapsed disclosure line automatically; don't hand-wrap cell output in alignment classes. Note this only affects data cells, not the header label (see below).
  - Column header labels are always center-aligned, regardless of a column's `align` meta (which only governs data-cell alignment) — this is fixed in `DataTable` itself, not something a page opts into or overrides.
  - Every `DataTable` gets a free-text search box above it by default (`enableSearch`, default `true`; customize the placeholder via `placeholder`) — filters across every column's rendered text, no page wiring required.
  - Pass `facetFilter={{ columnId: 'category', label: 'Category' }}` to add a toggle-chip filter bar for a low-cardinality column — every distinct value renders as a clickable chip with its row count, multi-selectable, with a "Clear" link. This is the app's standard alternative to a `<select>`/dropdown filter; don't build a page-local dropdown for this.
  - For more than one field, pass `facetFilters={[{ columnId: 'category', label: 'Category' }, { columnId: 'recommendation_date', label: 'Rec. Date', formatValue: fmtDate }]}` instead — each entry gets its own chip row (AND across filters, OR within a filter's selected values). `formatValue` reformats a chip's displayed label (e.g. an ISO date to "01-Jul") without changing what it filters on — the underlying match still runs against the raw field value.
  - Every `DataTable` in the app gets all of this for free; there's nothing to opt into beyond setting the meta/props. For tables built directly on the `Table` primitive (not going through `DataTable`), use `numericCellClass`/`textCellClass` from `table-utils.ts` instead.
  - Right-aligning numeric/price columns (`meta: { align: 'right' }`) and formatting INR currency cells with `formatCurrencyINR` is the app-wide convention — applied to every existing `DataTable` consumer under `pages/`. New pages should follow it by default rather than needing a reminder.
- **`formatCurrencyINR`** (`table-utils.ts`) — the standard price/currency formatter: always 2 decimal places, ₹ prefix, en-IN thousands grouping, `—` for null/undefined. Use this instead of a page-local `fmtPrice`/`fmtMoney` so every price in the app renders identically.
- **`SignalBadge`** / **`getSignalVariant`** — the single buy/hold/sell → Badge-variant mapping. Anywhere a direction, flag, or verdict needs a colored badge, use this instead of writing a local ternary — it was previously duplicated per-page (`directionVariant()` in `ml/signal.tsx`) before being extracted.
- **`ConfidenceMatrix`** — the 5-engine (Technical/ML/Fundamentals/Forensic/Valuation) cross-verification panel for a symbol.
- **`SymbolPageLayout`** — the compound Signal→Rationale→Proof→Validation layout for any per-symbol page. Use `SymbolPageLayout.Signal/.Rationale/.Proof/.Validation`.
- **`TickerLink`** — the only way a ticker symbol should ever be rendered as a link anywhere in the app. Writes to the global ticker store (`@/app/tickerStore`) and routes to `/charts`.
- **`TradingViewWidget`** — persistent TradingView chart, symbol-driven off the ticker store.
- **`ResponsiveChartCard`** — Card-wrapped Recharts container, for any non-price chart.
- **`PriceChart`** — lightweight-charts OHLCV candlestick/volume chart.
- **`StatCard`** — single metric tile (label + value + optional hint/tone).
- **`InfoTooltip`** — the small `(?)`-style hover explainer used throughout for jargon/metric definitions.
- **`SectionListPage`** — generic "give me an endpoint, get a full DataTable page" composite for simple list pages.
- **`CopilotPanel`** — the natural-language strategy builder side panel.

## Adding a new component

1. Create the file under `lib/ui/` (or `primitives/` if it's a low-level styling wrapper).
2. Add `export * from './YourFile'` to `index.ts`.
3. Use existing tokens/primitives inside it rather than new raw values.
4. Import it in pages via `@/lib/ui`, never the file's own path.
