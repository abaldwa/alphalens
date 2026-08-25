# AlphaLens Constitution

A quantitative trading system for the Indian equity market. This constitution codifies the non-negotiable principles that guide architecture, testing, operations, and data integrity decisions.

## Core Principles

### I. Lazy Efficiency — YAGNI First
Before writing any code, climb the ladder: (1) Does it need to exist? (2) Does it exist already? (3) Is it stdlib? (4) Is it platform-native? (5) Is it an installed dependency? (6) Can it be one line? Only then implement the minimum that works. Shortest correct diff wins. Deletion over addition. Boring over clever.

**Applied to AlphaLens:** Reuse adapters and shared components in `backtest/core/`, never duplicate. No new strategy definitions outside `strategy_registry`. No filters outside `filter_registry`. One line if it works.

### II. Architecture Is Auditable — Registry-Driven Strategies & Signals
Every strategy is declared once in `strategy_registry`, versioned point-in-time, and executed identically in backtest, paper trading, and live. Every generated signal is persisted to `strategy_signals`. A trade without a traceable signal is not auditable. A mutated registry row silently invalidates historical results — mutations are forbidden; only append.

**Binding backlog items:** A92–A95 (strategy/filter registry mandatory); A94 (signal persistence across all channels); A95 (same definition in backtest and deployment).

### III. Backtest Numbers Are Trustworthy — Correctness Rules
Learned from real defects that produced plausible-looking wrong numbers. Each rule exists because violations caused data loss or hidden errors.

- **Universe ranking is point-in-time.** Ranking on present-day snapshots and applying retroactively is lookahead (A84).
- **Tax is a per-financial-year cash outflow**, not balance subtraction — otherwise tax compounds for the backtest life (A86).
- **Pre-2017 price is legacy-sourced and unrepaired.** Backtests start 2026-04-01; before that crosses the 2007-04-02 legacy/Fyers seam with 960+ known corporate-action gaps (A99–A102).
- **Regime and benchmark indices are independent parameters.** Conflating them means a report change alters which regimes a strategy traded in (A98).
- **Returns are always rates (XIRR% or CAGR%), never totals.** This is the unit of measurement everywhere — tables, gates, recommendations. A "3-year return of 33%" is meaningless next to "5-year return of 61%"; as rates (10%/yr vs 10%/yr) they are identical.
  - Never annualise a figure already annualised.
  - Trade-level P&L is not covered by this rule — a single trade's 3-day return is a trade outcome, not period performance.
- **One metric name means one definition across channels (Technical, Momentum, ML, Fundamental).** Verify against the code that writes the number, never infer from summary (T13).

### IV. Data Integrity Under Concurrency — Single Writer, Isolated Tests
DuckDB is single-writer only. Route all writes through `defer_db_writes`. Never write synthetic or test rows into the real DuckDB, even temporarily — use an isolated/in-memory DB for verification. Tests run in batches (unit → integration → ML-heavy) to avoid OOM and lock contention. Pytest holds the write lock; never run concurrent pipeline steps during testing.

**Binding rules:**
- Never restart `alphalens-api.service` while a backtest queue is running (kills running jobs).
- Never edit source files mid-queue — jobs launch as fresh subprocesses and pick up edits.
- Scheduler can silently hold the write lock for hours if hung; check status before blaming data.

### V. Feature Ingestion Is Wholesale, Not Patchwork
Hybrid Stage 2 feature ingestion overwrites date partitions wholesale. Never run on a ticker subset — it will corrupt other tickers in those date partitions. No synthetic data in tests; `tests/quality/` enforces zero stub rows in the real DB.

### VI. Reports Live In-App, Not As Artifacts
All HTML reports (backtest/momentum/etc.) must publish within the application itself. Never publish as Claude Artifacts. Reports are gitignored but force-added to repo; pre-commit stash-rollback silently restores them — never `git add -f` report output directly.

### VII. Code Style: Clarity Over Cleverness
- No abstractions that weren't explicitly requested.
- No new dependencies if avoidable.
- No boilerplate nobody asked for.
- Comments only explain *why* — well-named identifiers handle *what*. Only document hidden constraints, workarounds, or surprising behavior.
- Validation only at system boundaries (user input, external APIs). Trust internal code and framework guarantees.
- Three similar lines are better than a premature abstraction.
- Plan before large systems (new domains, architecture changes). Reviewed plan required before code for systems touching multiple modules.

## Quality Gates & Testing

- **No Artifact reports** — publish in-app only.
- **No stub/synthetic data** in real DuckDB (tests/quality/ enforces).
- **No mid-queue source edits** — kills fresh subprocesses.
- **Backtest batching order:** unit → integration → ML-heavy (avoid early OOM).
- **Spec-first for large initiatives** — new domains, architecture changes, cross-module work requires `/speckit-specify` before implementation.

## Operational Constraints

- **Data sources:**
  - Fyers coverage: 2007-04-02 onward (2007+ effective); 69/473 tickers have >2x jumps at legacy→Fyers seam.
  - Backtests start 2026-04-01; before that, legacy data dominates.
  - NSE holidays (e.g., 2026-06-26) → empty matrix expected; not a bug.
  - Trendlyne/Tijori use USERNAME/PASSWORD (not API_KEY); read from .env.
- **Scheduler & systemd:**
  - Scheduler can hold write lock silently for hours if hung.
  - systemd-oomd kills processes under memory pressure; add `ManagedOOMPreference=omit` to service files.
  - Check status: `systemctl --user status alphalens-scheduler.service`.
  - View logs: `journalctl --user -u alphalens-scheduler.service --since "5 min ago" --no-pager`.
- **Background jobs:**
  - nohup requires explicit PYTHONPATH: `PYTHONPATH=. nohup python3 script.py &`.
  - Checkpoint cache keys don't include lookback_days/universe; clear before rescoping.

## Governance

This constitution supersedes all other practices. Amendments require documentation (rationale, migration plan) and approval. All code reviews must verify compliance with the architectural invariants (sections II–III, especially registry-driven strategies and audit trails).

**Version:** 1.0 | **Ratified:** 2026-08-25 | **Last Amended:** 2026-08-25
