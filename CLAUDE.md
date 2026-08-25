# AlphaLens Project Guide

**Last updated:** 2026-08-19  
**Phase:** Phase 3 (feature delivery); paper trading: 0 days  
**Main branch:** main | **Dev branch:** feature/unified-backtest-report-ui

## Project Overview

AlphaLens is a quantitative trading system for the Indian equity market. It ingests OHLCV data (Fyers primary since 2017, legacy data pre-2017), computes technical/fundamental/ML features, backtests strategies, and aims to paper-trade live signals.

**Core metrics:** Phase gate 6/9 pass; backtest module stable; frontend mostly delivered (Technical screener + Valuation screens 4 still need wiring).

---

## Directory Structure

### Core Ingestion & Data
- **datastore/** — DuckDB schema, API server (FastAPI), integrity checks, routing
  - `schema/create_normalised.py` — defines all tables (ohlcv_adjusted, fundamentals, delivery, etc.)
  - `api/routers/` — endpoint handlers
  - `integrity/checks.py` — data quality gates; run before loading new data
- **ingestion/** — scrapers and backfills
  - `scheduler/daily_pipeline.py` — orchestrates daily ingest (Fyers bhavcopy, fundamentals, etc.)
  - `scrapers/fyers_backfill.py` — historical OHLCV fills (2007+ coverage)
  - `scrapers/*_nse_xbrl.py` — fundamentals scrapers (BSE data)
- **config/** — runtime settings
  - `settings.py` — data paths, credentials (read from .env)
  - `benchmarks.py` — benchmark index definitions
  - `backtest_exclusions.py` — tickers to skip in backtests only (never ingestion)

### Feature Engineering & ML
- **features/** — feature computation
  - `momentum_universe.py` — momentum signal + universe filtering
  - `*_bands.py`, `*_overlay.py` — technical feature families
  - Feature store: date-partitioned Parquet under `feature_store/hybrid/`
- **baselines/** — model checkpoints (CatBoost, Ridge, etc.)
- **backtest/** — backtesting engine
  - `core/engine.py` — Portfolio simulation + trade execution
  - `core/metrics.py` — Sharpe, Calmar, drawdown, returns calculations
  - `core/overfit_checks.py` — robustness validation (walk-forward, bootstrap)
  - `core/integrity_checker.py` — trade-level validation
  - `run_*.py` — orchestrators (strategy queue, sweep, single-run)

### Dashboard & Frontend
- **frontend/** — React/TypeScript, Vite build
  - `src/features/backtest-report/` — backtest result viewer (new unified UI)
  - `src/pages/backtest-report/` — report page entry point
  - `src/shared/api/backtest.ts` — API adapter for runs/results
- **dashboard/** — legacy Flask dashboard (Technical screener, alerts)
- **paper_trading/** — live signal paper-trading stub (not active)

### Testing & Scripts
- **tests/unit/** — pytest suite (integration tests too)
  - Key concern: pytest holds write lock on prod DuckDB; can block concurrent pipeline steps
  - Strategy: run before/after pipeline steps, not during
- **scripts/** — one-off backfills and experiments
- **execution_logs/** — pipeline execution history

---

## Critical Gotchas & Policies

### DuckDB Concurrency
- **Single writer only** — only one process can hold write lock at a time
- Scheduler can hold lock for hours silently if hung; check `fuser` before force-runs
- Pytest write-locks the prod DB; concurrent pipeline steps fail silently
- **Snapshot parquets** — use read-only snapshots in `feature_store/snapshots/` during big backfills to avoid lock contention
- **Solution:** stop scheduler (`systemctl --user stop alphalens-scheduler.service`) or use pre-snapshotted data

### Test Suite
- **Run in batches** — full suite OOMs; use `--cov-append` flag
- **Heavy ML tests last** — batch order: unit → integration → ML-heavy (to avoid early OOM)
- **Example:** `pytest tests/unit/test_*core*.py tests/quality/ -q` then `pytest tests/unit/test_*ml*.py -q`

### Feature Ingestion & Backfill
- **No stub/synthetic data** — tests/quality/ enforces zero test stubs in real DB
- **Hybrid Stage 2 overwrites date partitions wholesale** — never run on a ticker subset; will corrupt others
- **No synthetic DB writes for verification** — use isolated/in-memory DB instead of inserting test rows
- **Fyers token expiry silent fallback** — expired token → bhavcopy-only mode (row counts look fine, check `source` column)
- **Corp-action discontinuities** — 960+ OHLCV gaps from backward-adjustment bug; never modify OHLC prices for corp actions

### Backtest & Artifacts
- **No Artifact reports** — all HTML reports (backtest/momentum/etc.) must publish within the app itself, never as Claude Artifacts
- **Reports are gitignored but tracked** — `backtest/reports/` is in .gitignore but was force-added; never `git add -f` output; pre-commit's stash-rollback silently restores gitignored files

### Background Jobs & Processes
- **nohup needs explicit PYTHONPATH** — e.g., `PYTHONPATH=. nohup python3 script.py &`
- **Checkpoint cache keys** — gainer experiment checkpoints don't key on lookback_days/universe; clear before rescoping
- **Never edit source during queue runs** — jobs are fresh subprocesses; mid-queue edits killed 22 jobs on 2026-08-09
- **API restart kills running queue** — `systemctl restart alphalens-api.service` while run_strategy_queue is active will orphan jobs

### Scheduler & Systemd
- **Scheduler can hold write lock** — silently for hours if hung; always check status before blaming the data
- **systemd-oomd kills processes** — 2026-07-21 scheduler killed by memory pressure; add `ManagedOOMPreference=omit` to .service file
- **Pressure thresholds** — VSCode got killed too; raised user@.service pressure threshold to 85%/30s
- **Check service status:** `systemctl --user status alphalens-scheduler.service`
- **View logs:** `journalctl --user -u alphalens-scheduler.service --since "5 min ago" --no-pager`

### Data Sources
- **Fyers coverage:** 2007-04-02 onward (2007+ effective); 69/473 tickers jump >2x at legacy→Fyers seam
- **Backtests start 2026-04-01** — before that, legacy data dominates and discontinuities affect results
- **Trading calendar:** NSE holidays (e.g., 2026-06-26) → empty matrix expected; don't treat as bug
- **Credentials:** Trendlyne/Tijori use USERNAME/PASSWORD (not API_KEY); read from .env
- **API endpoints:** 4 undeclared fundamental presets in registry; they work but are undocumented

### Code Style & Architecture
- **No over-engineering** — Three similar lines is better than premature abstraction
- **Comments only for WHY** — well-named identifiers handle WHAT; only document hidden constraints, workarounds, or surprising behavior
- **Validation at boundaries only** — trust internal code and framework guarantees; only validate at system entry (user input, external APIs)
- **Plan before large systems** — user prefers reviewed build plan before code on large new systems (TA templates, Damodaran)

---

## Model & Agent Selection

**Model routing:**
- **Haiku (default):** Bug fixes, small features, focused implementation (~120K context)
- **Sonnet 5:** Planning, architecture, coordination (~200K context)
- **Opus 5:** Complex numeric (backtesting metrics), multi-subsystem design (rare)

**Agent parallelization rule:** Launch `/model-review` (auto-parallelizes 4-6 agents) when:
1. **High-stakes decision** (strategy gate, live trading, major model change), AND
2. **Independent agents** (ml-rigor, domain-expert, backtest-reviewer don't depend on each other), AND
3. **Time justified** (serial review >10 min; parallel saves ~7 min)

**Agent roster (15 total):**

**Core validation (4 agents):**
- `ml-rigor-reviewer` — Statistical rigor, leakage, overfitting
- `domain-expert` — Indian equity market mechanics, sector validity
- `backtest-reviewer` — Engine correctness, trade validation, metrics
- `backend-data-engineer` — DuckDB schema, point-in-time, ingestion

**Strategy audit (3 agents):**
- `momentum-strategy-audit` — Parameter ranges, lookback selection, universe definition
- `technical-strategy-audit` — Indicator thresholds, signal logic, regime compatibility
- `fundamental-strategy-audit` — PIT ranking, financial metric PIT, forecast lag

**Data & signal (3 agents):**
- `data-audit-agent` — Auto-audit before backtest completion; sources, lineage, versioning
- `signal-parity-agent` — Live-vs-backtested signal generation parity (side-by-side)
- `ml-model-audit-agent` — Model training rigor, generalization, cross-validation

**Infrastructure & execution (2 agents):**
- `memory-management-agent` — Real-time OOM monitoring during backtest; 85% pressure threshold
- `enhanced-backtesting-agent` — Orchestrate 12 integrity checks, recommend optimizations (ticker-by-ticker), decision mechanism switching

**Code quality (2 agents):**
- `code-reviewer` — Correctness bugs, simplification
- `frontend-a11y-reviewer` — Accessibility, responsive, light/dark theme

**Product & testing (1 agent):**
- `skeptic-tester` — Failure modes, policy violations, edge cases

---

## Common Commands & Workflows

### Data & Ingestion
```bash
# Check pipeline status (last 60 checkpoints)
sqlite3 datastore/pipeline/pipeline_log.db "SELECT date, step, status FROM pipeline_checkpoints ORDER BY date DESC, step LIMIT 60;"

# View scheduler logs (last 5 minutes)
journalctl --user -u alphalens-scheduler.service --since "5 min ago" --no-pager

# Restart scheduler
systemctl --user restart alphalens-scheduler.service

# Check DuckDB lock holders (if hung)
fuser ~/.local/share/AlphaLens/data/*.duckdb

# Verify data integrity after ingestion
python3 datastore/integrity/checks.py
```

### Testing
```bash
# Run core tests (non-ML, fast)
pytest tests/unit/test_*core*.py tests/quality/ -q

# Run with coverage (batch mode)
pytest tests/unit/ --cov-append -q

# Run heavy ML tests last
pytest tests/unit/test_*ml*.py tests/unit/test_*backtest*.py -q
```

### Backtesting
```bash
# Single-run backtest
python3 backtest/run_orchestrator_backtest.py --symbol SBIN --start 2010-01-01 --end 2025-12-31

# Strategy queue (orchestrator-managed)
python3 backtest/run_strategy_queue.py backtest/queues/momentum_full.json

# Check active runs
python3 -c "
import duckdb
db = duckdb.connect('~/.local/share/AlphaLens/data/backtest.duckdb', read_only=True)
print(db.execute('SELECT run_id, symbol, strategy, status FROM runs ORDER BY created_at DESC LIMIT 10').fetchall())
"
```

### Frontend & API
```bash
# Start API server (port 8123)
.venv/bin/python -m uvicorn datastore.api.main:app --host 127.0.0.1 --port 8123

# Build & serve frontend (from frontend/)
npm run dev  # dev server with HMR
npm run build  # production build
```

---

## Library Documentation — Use MCP Context Server

When asking about library/framework documentation (React, Tailwind, TanStack Query, DuckDB, ag-grid, Recharts, etc.), invoke the context7 MCP server:

```bash
# Automatically fetches latest library docs (bypasses stale training data)
# Available for: React, Next.js, Tailwind, TanStack, DuckDB, ag-grid, Radix UI, Recharts, lightweight-charts, and 100+ other libraries
# Use instead of web search for up-to-date syntax, migration guides, config, and CLI tool usage.
```

---

## Code Graph & Data Catalogue — Before Planning Features/Refactors

Before designing new features or refactoring, read **[docs/codegraph/SUMMARY.md](docs/codegraph/SUMMARY.md)** for:
- **Subsystems overview** — module organization, responsibilities, and role
- **Data catalogue** — DuckDB tables + JSON configuration files
- **Dead code candidates** — modules with no incoming imports (often refactor opportunities)

The full graph (`docs/codegraph/module_graph.json`) includes all 4000+ nodes/edges (modules, tables, JSON files); the summary is the token-efficient default. For drill-down into specific functions/call chains, request the relevant `docs/codegraph/call_graph_<entry_point>.json` file.

**Regenerate:**
```bash
# Full rebuild (updates all outputs, ~5-10s)
python3 .claude/skills/code-graph/scripts/build_summary.py --full

# Incremental (runs automatically via post-commit hook after each commit)
python3 .claude/skills/code-graph/scripts/build_summary.py --incremental $(git diff --name-only HEAD~1 HEAD)
```

See `.claude/skills/code-graph/SKILL.md` for full skill documentation.

---

## Known Issues & Workarounds

| Issue | Workaround | Status |
|-------|-----------|--------|
| Fyers OHLCV discontinuities (960+ gaps) | Added trade_cagr overflow guard as mitigation; real data fix pending | Unfixed |
| Scheduler DuckDB lock hang (silent for ~20h) | Restart scheduler; root cause unknown | Unfixed |
| panel_staging flaky test (byte-identical, passes elsewhere) | Known non-regression; don't chase | By design |
| Fundamentals `announcement_date` wrong type (VARCHAR, should be DATE) | Working around in delivery code | Unfixed |
| --force sentinel bug in backfill orchestrator | Minor; rarely triggered | Unfixed |
| A25 rollout had backfill_runner.py bug | Fixed 2026-07-09; pilot + dry-run verified | Fixed |

---

## Frontend Rules & High-Velocity Protections

AlphaLens frontend (React 19 + TypeScript + Vite) uses consolidated rule files in `.claude/rules/`.

**Rule file (loaded automatically for `frontend/src/**`):**
- [.claude/rules/frontend-patterns.md](.claude/rules/frontend-patterns.md) — Covers state (TanStack Query v5, Zustand v5), styling (Tailwind v4, cva, Radix), and data (ag-grid v36, TanStack Table v8, Recharts v3, lightweight-charts v5)

### High-Velocity Token Protections

**ag-grid type bloat:** Never search grid internals (multi-megabyte types):
> "Assume standard ag-grid v36 types. Ask for specific behavior (e.g., 'disable sorting?') rather than searching internal type definitions."

**shadcn/ui caveat:** AlphaLens does NOT use shadcn. Never tell Claude to "look at the shadcn component directory." Target specific component files directly:
> "@components/ui/dialog.tsx" (specific file), never "look at components/ui/" (directory)

**Lint execution:** Always use oxlint (ultra-fast), never eslint:
```bash
npm run lint              # runs oxlint
npm run verify            # full check: tsc + lint + selfcheck
```

**Vite build traces:** If `npm run dev` outputs 100+ lines of dependency warnings, clear the terminal or pipe to `/tmp/` so build noise doesn't bloat history:
```bash
npm run dev > /tmp/vite.log 2>&1 &  # background, logs to file
tail -f /tmp/vite.log               # watch if needed
```

---

## Related Docs

- **FeatureBacklog.md** — prioritized work queue (Phase 3 focus)
- **BacktestUmbrellaPlan.md** — backtest module architecture & design decisions
- **AGENTS.md** — specialist agent capabilities (ML rigor, domain expert, backtest reviewer)
- **README.md** — high-level quickstart
