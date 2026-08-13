# backtest-report

The cross-channel backtest decision report. Three layers, checked by
`npm run check:layers`, so the UI can be replaced without touching a single
rule about returns.

```
core/   pure domain logic — no React, no router, no query client, no Tailwind
data/   fetching and URL/session state — react-query and router live here
ui/     React components and Tailwind — may import anything
```

Dependencies point **inwards only**: `ui → data → core`. `core` imports
nothing but `core`.

## core/ — survives a UI rewrite

| file | what it owns |
|---|---|
| `types.ts` | `StrategyReport`, the one shape every screen renders |
| `strategyKey.ts` | canonical `{channel}:{name}` identity, display labels, deep links |
| `adapters/` | each channel's API payload → `StrategyReport` |
| `recommendations.ts` | persona gates and weighted scoring |
| `matrix.ts` | pivot maths: period CAGR, RAG classification |
| `format.ts` | units — `rate()` vs `pct()` (see the rate rule below) |
| `window.ts` | window presets and date arithmetic |
| `toConfigForm.ts` | `StrategyReport` → deploy form fields, and what it cannot supply |
| `prefill.ts` | `?prefill=` encoding |

Nothing here knows a browser exists. `selfcheck.ts` imports **only** `core`,
which is why 155 assertions run in a plain Node process with no test
framework, no DOM and no build step.

## data/ — swappable plumbing

React Query and React Router are confined to these four files. Replacing
either means editing `data/`, not hunting through components.

## ui/ — the disposable layer

Every `.tsx`, every Tailwind class, every `@/lib/ui` import. `ragTheme.ts`
holds the RAG colour classes: `classifyRag` in `core` decides *which band a
return falls in* and outlives any redesign; the class strings are this design
system's and would go with it.

## Rules worth knowing before editing

**A return is always a rate.** XIRR% or CAGR%, never a total over a period —
see `AGENTS.md`. `format.rate()` appends `%/yr`; `format.pct()` deliberately
does not and is for figures that genuinely are not rates (win rate, drawdown,
a single trade's P&L). They are two functions rather than one with a flag
because picking the wrong one is a labelling bug and should look like one.

**Null is not zero.** Every metric is nullable. A missing figure renders as an
em dash carrying the backlog item that will supply it — see
`PENDING_REASONS`. A strategy with no data must never look like a strategy
with no drawdown.

**Missing data fails a persona gate**, it does not pass one. The permissive
reading would rank channels with less known about them above channels with
more.

## Commands

```
npm run check:layers   # the boundary above
npm run selfcheck      # 155 assertions over core/
npm run verify         # layers + tsc + lint + selfcheck
```
