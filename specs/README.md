# AlphaLens Specifications

This directory contains all formally-specified features and components for AlphaLens, organized by domain and pipeline stage.

## Structure

- **`shared/`** — Cross-cutting components shared across stages (Backtest, Paper Trading, Live) and domains (Technical, Momentum, ML, Fundamental, Valuation, Forensic, Big-Investors). See [shared/REGISTRY.md](shared/REGISTRY.md) for the complete list.
- **`technical/`** — Technical analysis domain (screeners, backtests, signals)
- **`momentum/`** — Momentum strategies (universe filters, position sizing, signal generation)
- **`ml/`** — Machine learning models and signals
- **`fundamental/`** — Fundamental analysis and scoring
- **`valuation/`** — Valuation models (Damodaran, etc.)
- **`forensic/`** — Forensic accounting signals
- **`big-investors/`** — Big investor tracking and positioning
- **`frontend/`** — Dashboard and UI specifications

## Spec-First Workflow

For **large initiatives** (new strategy domains, architecture changes, cross-module work):

1. Run `/speckit-specify` to create `spec.md` with requirements
2. Run `/speckit-clarify` (optional) to resolve ambiguities
3. Run `/speckit-plan` to create `plan.md` with architecture
4. Run `/speckit-analyze` (optional) to verify cross-artifact consistency
5. Run `/speckit-tasks` to generate ordered `tasks.md`
6. Run `/speckit-implement` to execute tasks

For **small work** (bug fixes, routine backlog items): Use the scrum-master agent's lighter path (no spec required unless it's a large initiative).

## Spec-ID Convention

Each spec has a unique SPEC-ID prefix:

- `SPEC-TECH` — Technical analysis
- `SPEC-MOMENTUM` — Momentum strategies
- `SPEC-ML` — Machine learning
- `SPEC-FUNDAMENTAL` — Fundamental analysis
- `SPEC-VALUATION` — Valuation models
- `SPEC-FORENSIC` — Forensic accounting
- `SPEC-BIGINV` — Big investor tracking
- `SPEC-UI` — Frontend/dashboard
- `SPEC-BT` — Backtest engine (shared)
- `SPEC-PT` — Paper trading (shared)
- `SPEC-SYS` — System-level governance (shared)

## Creating a New Spec

1. Choose a domain folder above.
2. Create `<SPEC_ID>-<title>.md` with frontmatter:
   ```yaml
   ---
   spec_id: SPEC-MOMENTUM-015
   title: Skip-Month Momentum Variant
   domain: momentum
   stages: [backtest, paper, live]
   status: draft
   ---
   ```
3. Write requirements, acceptance criteria, open questions.
4. If large, proceed through the Spec-First workflow.
5. Reference this spec in your backlog entry and commits.

## Related Docs

- [Constitution](.specify/memory/constitution.md) — Non-negotiable project principles
- [Feature Backlog](../FeatureBacklog.md) — Feature requests and in-flight work
- [CLAUDE.md](../CLAUDE.md) — Project guide and operational rules
