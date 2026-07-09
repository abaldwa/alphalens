
Improvement ideas surfaced during the truthful-mode "Explain-Me Walkthrough"
series (PHASE X, prompts X.0–X.10) in `CLAUDE_CODE_PROMPTS.md`, plus the
2026-07-04 architecture review and subsequent 2026-07-05/08 sessions.
Originally reorganized 2026-07-04 by code area; **reorganized again
2026-07-08** into the 8 areas below, ordered to match the sequence the
items will actually be worked through (Architectural → Technical →
Fundamental → Big Investors → Damodaran → Forensic → Corporate
Announcements → Machine Learning), rather than by discovery date. Several
near-duplicate entries created by reused item numbers across sessions
(the emergency-recompute/Stage-2/model-retrain story in particular) have
been merged into single consolidated items. No priority ranking implied
by order within a section.

## Status Matrix

Legend: ✅ Done · 🔧 In Progress · ⏳ Not Started · 🚫 Blocked (external dep or explicit design-pass needed)

### Architectural

| ID | Item | Area | Status | Blocked On |
|---|---|---|---|---|
| A1 | US market overnight correlation (Nasdaq/Dow/S&P) | Data Layer / Scheduler | ✅ | Folded into A3 (implemented there); GIFT NIFTY still has no known free source |
| A2 | Dollar Index (DXY) feature | Data Layer / Scheduler | ✅ | — |
| A3 | Morning Catch-Up redesign (scope fix + new indicators + PIT timing shift) | Scheduler / Macro / Features | ✅ | — |
| A4 | DataStore API Console (freshness rollup) | Ops / API | ✅ | — |
| A5 | Ops Portal: surface weekend job schedules | Ops / API | ✅ | — |
| A6 | Move `/features`, `/models`, `/pipeline/status` into routers | API | ✅ | — |
| A7 | `SIGNAL_THRESHOLD`/`META_THRESHOLD` as fallback values | ML Inference | ✅ | — |
| A8 | AF-1: DuckDB connection-lifecycle audit + fix | Data Layer | ✅ | — |
| A9 | AF-2: Pipeline output sanity gate | Scheduler / Ops | ✅ | — |
| A10 | AF-3: Feature-store query path partition/index | Data Layer / API | ✅ | — |
| A11 | AF-4: Reconcile/remove orphaned test schema | Data Layer / Tests | ✅ | — |
| A12 | AF-5: Fundamentals range/sanity validation gate | Data Layer / Ingestion | ✅ | — |
| A13 | AF-6: Daily off-machine backup | Ops | ✅ | User signup (Backblaze) still pending — job safely no-ops until then |
| A14 | Blank company names (1,817 tickers) — export list | Data Layer / Config | ✅ | — |
| A15 | Scheduler durability: systemd `--user` service + linger | Scheduler / Ops | ✅ | — |
| A16 | 30-min CPU/memory monitor with training-safe throttling + Ops Monitor UI panel | Scheduler / Ops | ✅ | — |
| A17 | Cross-process `daily_pipeline` double-fire race condition | Scheduler | ✅ | — |
| A18 | Model-retrain script map fixed (nonexistent scripts + bare-path invocation) | ML Signal Engine / Scheduler | ✅ | — |
| A19 | `signal_63d` + multibagger given real periodic-retrain entry points | ML Signal Engine | ✅ | — |
| A20 | Data Integrity Checker (corporate actions, nulls, holiday/parquet leakage, random 5yr spot-check) | Data Layer / Ops / Scheduler | ✅ | 2026-07-09: implemented as a standalone `data_integrity_check` scheduler step (see A20 entry below) |
| A21 | Pipeline Health Checker (weekly job-completeness audit + catch-up plan) | Ops / Scheduler | ✅ | 2026-07-09: implemented as a new weekly `job_health_check` scheduler job (see A21 entry below) |
| A22 | Remote/mobile access to dashboard (password-protected) | Ops / Dashboard | ⏳ | Design proposed below (Tailscale) — needs user to install/approve tooling |
| A23 | Job run-time/memory benchmark history + weekday/weekend schedule optimization | Ops / Scheduler | 🔧 | 2026-07-09: `job_run_log` now records `duration_seconds`/`peak_rss_mb` for every job (all 13 scheduled job wrappers instrumented) — see writeup below. Schedule-rebalancing pass itself still blocked on weeks of accumulated real data, as originally scoped. |
| A24 | UI refactor for responsive layout (mobile/tablet) | Dashboard (all) | ⏳ | Ties into A22 — needed for mobile-access use case to be usable |
| A25 | Write-audit-publish architecture for DuckDB ingestion (raw landing → validate → atomic publish, N=7 rollback) | Data Layer / Ingestion / Scheduler | ✅ | 2026-07-09: pilot + full rollout both landed and dry-run verified — see writeup below |
| A26 | Expand `_SANITY_KNOWN_SPARSE_COLUMNS` with remaining confirmed-unsourceable columns; finish 2026-07-03/06/07 recompute+re-run | Scheduler / Data Layer | 🔧 | 2026-07-09: audit found 13 of the "remaining ~12" list were already exempted; only `capex_to_assets`/`noncash_assets_ratio` were actually missing — added, with tests. 2026-07-03/06/07 `step_compute_features` recompute + `sanity_check`/`paper_trade` re-run still outstanding (needs an explicit Ops force-run, not run this session) |
| A27 | Real-economy macro: 8 of 10 series remain genuinely blocked | Data Layer / Ingestion | 🚫 | No free structured source found (PMI licensed, GST/rail-freight freeform PIB text, others bot-blocked); IIP unblockable via `data.gov.in` API key signup |
| A28 | Emergency feature recompute + 8-model retrain (post corporate-action fix) — consolidated | Data Layer / ML Signal Engine / Scheduler | 🔧 | 2026-07-09: (f)/(g) resolved by log/code audit — see A37; 7/8 models confirmed correctly trained on corrected data, `signal_63d` needs one real `retrain_phase2.py` run (blocker was A37's masked crash, now fixed); Stage 2 parquet recompute itself turned out not to be a retrain dependency, still separately unfinished for `datastore/features/daily/` consumers |
| A29 | `shareholding`/`governance` GET endpoints 500 on NULL numeric fields | API / Data Layer | ✅ | Fixed 2026-07-08 |
| A30 | Multi-day-missed pipeline runs: backfill works but UI doesn't flag "backfilled" vs "live" | Scheduler | ✅ | Implemented 2026-07-09 — see writeup below |
| A31 | `download_index_ohlcv` failing repeatedly on backfill (BSE/NSE 404s) | Ingestion | ✅ | Fixed 2026-07-09 — real causes were not scraper/URL related, see writeup below |
| A32 | `scripts/model_training_status.py` not yet run end-to-end | Ops / Tooling | ✅ | Implemented 2026-07-09 — see writeup below |
| A33 | Regression test for the `model_training` overdue-check union fix | Tests | ✅ | Implemented 2026-07-09 — see writeup below |
| A34 | `step_download_fno` may share A31's unwrapped-DB-write gap | Scheduler | ⏳ | Not confirmed live; found while fixing A31 — its DB write also sits outside the step's non-critical try/except, same pattern (step_download_macro checked, it's a no-op, not affected) |
| A35 | screener source can't join A25 staged publish without an architecture change | Data Layer / Ingestion | ⏳ | Found during A25 full rollout (2026-07-09) — see writeup below |
| A36 | `fundamentals` table has 4 writers with inconsistent upsert-conflict precedence | Data Layer / Ingestion | ⏳ | Found during A25 full rollout (2026-07-09) — see writeup below |
| A37 | `retrain_all_when_free.sh` logged false `exit=0` for crashed stages | Scheduler / ML Signal Engine / Tests | ✅ | Fixed 2026-07-09 — root cause of A28(f)/(g) confusion, see writeup below |
| A38 | T5's "18 advanced TA features unused" is only half right — TFT/BiLSTM already consume them, but neither has ever been trained | ML Signal Engine / Data Layer / Scheduler | 🔧 | 2026-07-09: registry.json write-through + scheduler wiring landed and tested; first-ever real training run (smoke test, then full) still pending — see writeup below |
| A39 | `ExitSignalModel` will crash the entire daily inference pipeline the first time paper trading opens a position | ML Signal Engine / Ops | ✅ Fixed 2026-07-09 | `_step_exit` now falls back to `RuleBasedExitPolicy` when no trained model exists, matching `run_daily_paper_trading.py`'s pattern; real trainer still doesn't exist (needs closed-trade data) — see writeup below |
| A40 | `StackingEnsemble` is fully dormant and its one real training attempt died silently mid-run | ML Signal Engine | ⏳ | Found 2026-07-09 model audit — `scripts/train_stacking.py`'s 2026-07-02 run stopped after loading TFT OOF folds with no error, no completion marker, no saved weights; never invoked from `daily_inference.py` or referenced by any dashboard screen — see writeup below |
| A41 | Orphaned pre-A38 TFT/BiLSTM `.pt` checkpoints (2026-06-24/06-30/07-01) sit unregistered outside the current save-path convention | ML Signal Engine / Data Layer | ⏳ | Found 2026-07-09 model audit — 8 files directly under `datastore/models/*.pt` (not `datastore/models/{tft,bilstm}/`), predate A38's registry wiring, never consumed by anything except A40's dead `train_stacking.py` run — see writeup below |
| A42 | Verify which of the 16 `ALL_FEATURE_COLUMNS` categories TFT/BiLSTM actually learn from, and decide a path for categories no serving model uses | ML Signal Engine / Features | ⏳ | Found 2026-07-09 model audit — TFT/BiLSTM take every parquet column by construction, but that's unverified in practice (no completed run, no feature-importance check); most of the 16 categories have no other production ML consumer besides the Technical screener/feature API — see writeup below |
| A43 | Daily Insights / ML signal screens don't surface A30's per-signal backfilled-vs-live flag | Dashboard / API | ⏳ | Found 2026-07-09 while implementing A30 — Ops dashboard now shows it, but a user reading `ml_signals` rows on the Daily Insights/signal screens still has no cue; needs a join from `ml_signals` back to `pipeline_checkpoints` by date, out of scope for this session |

### Technical

| ID | Item | Area | Status | Blocked On |
|---|---|---|---|---|
| T1 | Docstring says "76 core" indicators, code computes 70 | Dashboard (Technical) | ⏳ | Docstring/code mismatch, needs correction |
| T2 | Phantom equity trading data on real holidays: root cause dig deeper? | Ingestion | ⏳ | Two-layer fix landed; whether other scrapers share the underlying NSE archive quirk is unconfirmed |
| T3 | No charting library on Technical > Chart screen | Dashboard (Technical) | ⏳ | Needs a vendored charting lib or custom canvas/SVG renderer |
| T4 | Watchlist screen wiring status unresolved | Dashboard (Technical) | ⏳ | Needs a follow-up truthful-mode pass to confirm real backend wiring |
| T5 | 18 "advanced" TA features computed but unused by any ML training pipeline | ML Signal Engine / Data Layer | ⏳ | Decide: wire into Phase 2 training, or stop computing to save run-time/storage |

### Fundamental

| ID | Item | Area | Status | Blocked On |
|---|---|---|---|---|
| F1 | Sector screen: "Sector-Unique Metrics" sub-panel is a hardcoded empty state | Dashboard (Fundamental) / Features | ⏳ | Needs per-sector metric design (bank GNPA, pharma ANDA approvals, etc.) — no existing data source |
| F2 | Management screen: "Related-Party Transactions" sub-panel is a hardcoded empty state | Dashboard (Fundamental) / Features | ⏳ | `systems/fundamental_analysis/management/` is an empty stub — needs RPT data source + parsing |
| F3 | `systems/fundamental_analysis/{growth,management,peers,quality,sector,thesis}/` — all 6 subpackages are dead stubs | Architecture / Fundamental | ⏳ | Real logic already lives in `features/fundamental_composites.py` — decide: delete stubs, or backfill and refactor composites in |
| F4 | Thesis Builder has no PDF/export feature | Dashboard (Fundamental) | ⏳ | Needs a PDF lib (reportlab/weasyprint) + export endpoint if actually wanted |
| F5 | `scripts/ingest_external_fundamentals.py`'s "write" path never persists | Ingestion / Fundamentals | ⏳ | `DataStoreClient.write_fundamentals` was never implemented (own comment admits it) |
| F6 | Valuation Accuracy screen has zero backend/frontend | Dashboard (Valuation) / API | ⏳ | Verified 2026-07-05: `accuracy.html` calls `renderEmptyState` with literal "Not yet built.", no fetch call exists |

### Big Investors

| ID | Item | Area | Status | Blocked On |
|---|---|---|---|---|
| BI1 | Daily NSE/BSE bulk/block-deal history is still only 1 real day deep for non-superstar participants | Ingestion / Data Layer | ⏳ | No historical-backfill source found outside the 62-family Trendlyne superstar list |
| BI2 | Non-equity Trendlyne deals (InvITs, REITs, etc.) are silently dropped from the bulk-deal backfill | Ingestion | ⏳ | Correct for an equities dashboard today; would need `stock_master`/ticker-map extension if InvIT/REIT coverage becomes in-scope |
| BI3 | Trendlyne bulk-block-deals page pagination not verified across all 62 investors | Ingestion | ⏳ | Only verified for 1 of 62 investors; check raw HTML for pagination if a re-run looks suspiciously capped |
| BI4 | No automated test coverage for Big Investor Activity changes | Tests | ⏳ | All changes verified manually; needs a seeded DuckDB fixture per this repo's no-stub testing policy |
| BI5 | `holding_pct_of_company` / shares-outstanding estimate is a back-derivation, not a real share count | API / Data Layer | ⏳ | Reasonable approximation, not cross-checked against real `fundamentals.shares_outstanding` |
| BI6 | "unmapped:" family ↔ Trendlyne holder-name matching is a string-normalization heuristic | Data Layer | ⏳ | Exact-string matching only, no fuzzy/alias handling; manual `investor_family` seed growth not automated |

### Damodaran

| ID | Item | Area | Status | Blocked On |
|---|---|---|---|---|
| D1 | 3 failing `test_damodaran.py` sector-alias tests | ML / Valuation Tests | ⏳ | Stale test expectations vs. the 2026-07-04 classifier fix — decide whether to alias sector strings or update tests |
| D2 | No router-level tests for `datastore/api/routers/valuation.py` | Valuation / Tests | ⏳ | Endpoint wiring (param validation, error responses, peer-group edge cases) currently unverified by tests |

### Forensic

| ID | Item | Area | Status | Blocked On |
|---|---|---|---|---|
| FO1 | Altman Z-Score structurally NaN in production | ML Signal Engine / Forensic / Data Layer | ⏳ | Needs real market cap, retained earnings, EBIT, current assets/liabilities ingested |
| FO2 | Dechow F-Score always called with `{}` — permanently NaN | ML Signal Engine / Forensic | ⏳ | Needs employee-count, share-issuance, book-to-market data — no existing source |
| FO3 | Beneish M-Score's AQI term permanently NaN | ML Signal Engine / Forensic / Data Layer | ⏳ | Needs `current_assets`/PPE columns backfilled from a live scraper |
| FO4 | Forensic Group C fields hardcoded `np.nan` | ML Signal Engine / Forensic | ⏳ | Needs a data-source decision (GST filings, revenue-concentration inputs) before scoping |
| FO5 | Benford's Law screen only surfaces one aggregate MAD float | Dashboard (Forensic) / API | ⏳ | Chi-square + per-digit frequencies already computed internally — needs API/schema extension + more series wired in |
| FO6 | Investigation Report has no PDF/report-builder backend | Dashboard (Forensic) | ⏳ | Needs a PDF lib + export endpoint, mirroring F4's Thesis Builder gap |
| FO7 | Universe Scan has no on-demand trigger | Dashboard (Forensic) / API | ⏳ | Needs a "run scan now" endpoint wrapping `score_forensic.py`'s full-universe loop |
| FO8 | Several forensic/governance columns unavailable even from NSE XBRL (`contingent_liability_ratio`, etc.) | Data Layer / Ingestion | 🚫 | Only present as freeform "Textual Information" in NSE's template — needs NLP/text extraction |
| FO9 | `altman_z` still NaN for a real subset of tickers | ML Signal Engine / Data Layer | ⏳ | Depends on `shares_outstanding` availability (pre-FY2023-24 filings, implausible-value rejections); full-universe gap size not yet measured |

### Corporate Announcements

| ID | Item | Area | Status | Blocked On |
|---|---|---|---|---|
| CA1 | Triage 174 likely-missing-split tickers vs Fyers, backfill `corporate_actions` | Data Layer / Ingestion | ✅ | 70/174 fixed, rest reclassified — see writeup below |
| CA2 | KANSAINER/AJOONI non-monotonic price-ratio investigation | Data Layer / Ingestion | ⏳ | Not a simple missing-split case; needs dedicated look |
| CA3 | Assess 152 higher-cv Fyers-mismatch tickers | Data Layer / Ingestion | ⏳ | Suspected dividend-adjustment-convention gap, not verified |
| CA4 | Corporate-action validation tracking (`corporate_actions_validation`) | Data Layer / Ingestion | ✅ | 967/967 rows processed; still needs cross-reference against CA1-CA3 triage lists + schema migration entry |
| CA5 | Corporate Announcements "insider" category is an approximation | Ingestion / Data Layer | ⏳ | No dedicated NSE insider-trading-disclosure endpoint found; needs a real dedicated source |
| CA6 | Real NSE filing endpoints found but not yet built into a pipeline (BRSR, QIP, shareholding, RPT, governance) | Ingestion | ⏳ | Endpoints identified live, RPT/governance need a secondary lookup param not yet found |

### Machine Learning

| ID | Item | Area | Status | Blocked On |
|---|---|---|---|---|
| ML1 | URGENT: wire multibagger/forensic/21d/63d/conformal into daily scheduler | ML Signal Engine | ✅ | — |
| ML2 | Daily Insights row fusion (Meta/Interval/P&D/Regime always empty) | ML Signal Engine / API | ✅ | — |
| ML3 | SHAP explainability at inference time | ML Signal Engine | ✅ | — |
| ML4 | 5-day recommendation history table + Sell rationale | Dashboard (ML) | ✅ | — |
| ML5 | Top Buy Signals — remove 5-cap, sortable columns | Dashboard (ML) | ✅ | — |
| ML6 | `mb_tier` relabeled to probability bands | Dashboard (ML) | ✅ | — |
| ML7 | View All for 21d/63d recommendations | Dashboard (ML) | ✅ | — |
| ML8 | Redesign Signal Deep Dive as sortable full-universe list | Dashboard (ML) | ✅ | — |
| ML9 | Indian numbering (`fmtInt`) audit across all apps | Dashboard (all) | ✅ | — |
| ML10 | Dedicated Exit Urgency page | Dashboard (ML) | ✅ | — |
| ML11 | Upload-current-portfolio (external holdings) page | Dashboard (ML) / API | ✅ | — |
| ML12 | Daily sector rotation report | Features / ML Signal Engine | 🔧 | Data source unblocked; sector-index mapping, `features/sector_rotation.py`, API + dashboard still not built |
| ML13 | Multibagger tier change-log / "first appeared" date | ML Signal Engine | ⏳ | ML1's scheduled job just landed — needs a few real weekly runs of history to accumulate |
| ML14 | Multibagger survival-curve labeling fix (flat at 100%) | ML Signal Engine | ✅ | — |
| ML15 | RuleBasedExitPolicy: volatility-scaled target/stop | ML Signal Engine | ✅ | — |
| ML16 | Backdated Entry — relocate off main Paper Trading page | Dashboard (Paper Trading) | ✅ | — |
| ML17 | Unified backtest strategy (per-horizon, Nifty benchmark) | Backtest | 🔧 | Benchmark data source unblocked, but benchmark equity curve not wired into `backtest/engine.py`; per-horizon restructuring unscoped |

---

## Architectural

### A1/A2/A3 — Morning Catch-Up redesign (macro capture + scheduling)
`ingestion/scheduler/pipeline_scheduler.py:770-823`'s `schedule_morning_catchup`
currently re-runs the same gap-backfill-then-today logic as the 18:00 job,
which always 404s on "today" at 07:30 IST since NSE hasn't published today's
bhavcopy yet. Needs a backward-only variant (walks "today minus 1" back, never
attempts "today"). Bundled with this fix: capture GIFT NIFTY (blocked — no
free source found), Nasdaq/Dow/S&P 500/Nikkei/Hang Seng (via Yahoo
Finance/stooq) once daily at 07:30 IST, and a PIT semantics shift moving
VIX/FII-DII/USD-INR capture from the 18:00 job to 07:30 — verify
`compute_features` still joins macro rows by the same trading-day key after
the shift. DXY (A2) needs its own data-source decision before scoping.

### A4 — DataStore API Console (consolidated health page)
Extend `datastore/api/routers/ops.py` with a freshness-rollup endpoint
(last-write timestamp + row count per table: `ohlcv_adjusted`, `fundamentals`,
`macro_indicators`, `ml_signals`, `ta_signals`, `mf_holdings`) + a new Ops page
consuming it.

### A5 — Ops Portal: surface weekend job schedules
`weekend_feature_backfill` (Sat 09:00), `weekend_fundamentals` (Sat 10:30),
and the twice-monthly `mf_holdings_ingestion` job have no visible status on
the Ops page. Confirm via heartbeat store whether `weekend_fundamentals` has
ever fired, then add next-run-time + last-run-status for all three.

### A6 — Move inline routes into router files
`/api/v1/features`, `/api/v1/models`, `/api/v1/pipeline/status` are defined
inline in `datastore/api/main.py` (lines 172/258/336) instead of
`datastore/api/routers/`. Pure refactor, same paths/behavior.

### A7 — `SIGNAL_THRESHOLD`/`META_THRESHOLD` fallback wiring
Per user decision 2026-07-04: wire these currently-dead settings in as the
fallback threshold when a loaded model has no saved tuned threshold
(corrupted/incomplete artifact, or bootstrap before first real training run).

### A8 — AF-1: DuckDB connection-lifecycle audit + fix — IMPLEMENTED 2026-07-04
Every `get_duckdb_connection(...)` call site under `datastore/api/routers/`
now passes explicit `persist=False` + `read_only=` (was previously relying
on the unsafe `persist=True, read_only=False` default in `alerts.py`,
`regime.py`, `multibagger.py`, and also missing one or both kwargs in
`signals.py`, `forensic.py`, `watchlist.py`, `technical.py`,
`shareholding.py`, `fundamentals.py`). Added
`tests/quality/test_duckdb_connection_discipline.py` — an AST-based static
check (same style as `test_no_stub_or_synthetic_data.py`) that fails CI if
any future router call site omits either kwarg.

### A9 — AF-2: Pipeline output sanity gate
`run_models` silently produced no real signals for 10 consecutive trading
days before a user noticed by coincidence. Add `step_sanity_check(run_date)`
to `daily_pipeline.py` after `step_write_signals`: hard floors on
`ml_signals` row count, non-empty `top_buys`, no all-NaN feature columns. On
failure: raise (checkpoint records "failed") + loud alert. Surface
`sanity_check_passed` on `/api/v1/ops/runs`.

### A10 — AF-3: Feature-store query path partition/index
`datastore/api/main.py`'s `/api/v1/features/{ticker}` opens one Parquet file
per calendar day in a date-range loop (4,792+ files). Recommended fix:
Option A — register a DuckDB view over the `daily/*.parquet` glob and let
DuckDB's own metadata pruning handle range filtering (zero writer-side
change). Option B (defer) — Hive-partition the writer.

### A11 — AF-4: Reconcile/remove orphaned test schema — IMPLEMENTED 2026-07-04
`datastore/api/db.py`'s `init_duckdb()`/`init_sqlite()` defined a fake
schema that didn't match production. Audit found their only consumers were
the `test_duckdb`/`test_sqlite` fixtures in `tests/conftest.py` — and
neither fixture was actually used by any test in the suite (dead code, no
migration needed). Deleted `init_duckdb`/`init_sqlite` from `db.py`, the
two dead fixtures from `conftest.py`, and their exports from
`datastore/api/__init__.py`.

### A12 — AF-5: Fundamentals range/sanity validation gate
Two independent unit-scaling bugs already found by hand (margins stored as
0-100 instead of 0-1; ROE reading ~4% for financial-sector tickers). Add a
plausible-range table per ratio field + a validation pass in the fundamentals
ingestion path that flags (not silently writes) out-of-range rows, with an
explicit low-revenue allowance for genuine micro-cap outliers.

### A13 — AF-6: Daily off-machine backup — IMPLEMENTED 2026-07-04
`scripts/backup_to_b2.py` + scheduler job built and verified (cleanly records
`"skipped"` heartbeat with no credentials set). Needs real user action before
it does anything: install `rclone`, sign up at backblaze.com, set
`BACKBLAZE_KEY_ID`/`BACKBLAZE_APPLICATION_KEY`/`BACKBLAZE_BUCKET` +
`BACKUP_ENABLED=true` in `.env`. Follow-up: surface backup heartbeat on Ops
page (ties to A9).

### A14 — Blank company names (1,817 tickers) — 1,126 ENRICHED 2026-07-04
All 1,817 are tier-6, genuinely tradeable (confirmed live via real pending
paper-trading actions). CSV export (`config/tickers_missing_company_name.csv`)
regenerated with `is_nifty500`/`is_fno_eligible` prioritization columns.

**Enrichment**: `scripts/enrich_missing_company_metadata.py` resolves
company_name/sector via screener.in's public company-search API (no login
needed — verified live that its "Peer comparison" breadcrumb's 2nd-level
link matches this project's exact sector taxonomy, e.g. "Oil Gas &
Consumable Fuels", "Information Technology", "Financial Services").
Resumable/checkpointed (writes to `config/company_metadata_enrichment_progress.csv`
incrementally). Ran to completion against all 1,817 tickers:
**1,126 resolved (62%)**, 691 unresolved (delisted/renamed/no screener
match — logged to `config/company_metadata_enrichment_unresolved.csv`).
`scripts/apply_company_metadata_enrichment.py` merged the 1,126 resolved
rows into `config/nifty500_universe.csv` and regenerated the
now-691-ticker missing-names CSV.

**Follow-up for the remaining 691**: try Tijori/Trendlyne (both need
login, both already have working scrapers in `ingestion/scrapers/`) as a
fallback pass — not attempted this session since screener alone resolved
the majority and a second data source adds real complexity (auth, rate
limits, a different response shape to parse).

### A15 — Scheduler durability: systemd `--user` service + linger
Survives a closed Claude Code session/VS Code — IMPLEMENTED.

### A16 — 30-min CPU/memory monitor with training-safe throttling
Defers restart if a step is mid-run; Ops Monitor UI panel added — IMPLEMENTED.

### A17 — Cross-process `daily_pipeline` double-fire race condition
Checkpoints showed success, heartbeat showed failed since 2026-06-22 — fixed.

### A18 — Model-retrain script map fixed
All 6 phase-1 models pointed at nonexistent scripts, and once repointed,
still invoked as a bare file path (`ModuleNotFoundError`) instead of
`python -m <module>` — fixed.

### A19 — `signal_63d` + multibagger given real periodic-retrain entry points
`retrain_phase2.py`, new `train_multibagger.py` — no model left silently
unretrainable except Phase-3 `tft`/`bilstm`.

### A20 — Data Integrity Checker
**Integration point (2026-07-09, from A25's pilot build):** A25 built the
staging/validation-gate skeleton this item is meant to plug into —
`datastore/staging/gate.py::stage_dataframe(conn, table_name, df,
validators)` takes a list of validator callables
(`(df) -> (passed_df, rejected_df_with_reason)`); A20's four checks below
should each become one validator in that list, registered against the
tables they cover (`ohlcv_adjusted`/`fno_data` today; extend to
`fundamentals`/`corporate_actions`/etc. as those sources migrate onto
A25). Rejected rows already land, visible, in `staging.rejected_rows`
(`source_table`, `reason`, `row_json`, `staged_at`) — A20's "open
question" below about where RCA/fix-proposal output should live can build
on top of that existing table rather than a new one from scratch.

New scheduled job, run **before** the daily Feature Engineering and Model
Run steps (per user decision), so a bad ingest never propagates into a
day's features/signals. Four checks, one job:

**a. Corporate-action cross-check.** For every `corporate_actions` row
actioned in the trailing 7 days (dividend/rights/split/bonus), re-pull the
same ticker/date window from Fyers and diff. Reuses the same comparison
method already proven out in CA1's triage
(`scripts/detect_missing_split_reconstruction.py`) and the existing
`corporate_actions_validation` schema (CA4) — this item is effectively that
proposal, turned into a recurring job instead of an ad-hoc script.

**b. Null/NaN sweep.** Scan `features/*` output and the ingested source
tables (`ohlcv_adjusted`, `fundamentals`, `macro_indicators`) for
unexpected null/NaN rates per column, against a per-column baseline
(flag columns that are *structurally* always-NaN, e.g. FO1/FO2/FO3/FO4's
already-known forensic gaps, so the checker doesn't re-alert on known,
accepted gaps every run).

**c. Holiday/non-trading-day leakage check.** Cross-reference
`config/nse_holidays.py` (rebuilt authoritative 2005-2026 calendar per
T2) against `ohlcv_adjusted` and any written Parquet feature-store
partitions — any row or file dated on a real NSE holiday is a signal of
the same failure mode T2 already found and fixed at the scraper layer;
this becomes the recurring detection net for it, not the one-time fix.

**d. Random 5-year spot-check.** Sample ~100 random (ticker, date)
combinations across the last 5 years' `ohlcv_adjusted` history, cross-
check adjusted close against both Fyers and Yahoo Finance. Two
independent sources catch cases where our data agrees with neither
(real bug) vs. disagrees with only one (that source's own data-quality
issue, not ours) — a single-source check can't distinguish these.

**Alerting/remediation flow (per user decision):** on any mismatch, the
job immediately (1) alerts (same channel as existing Ops alerts), (2) runs
a root-cause pass — reusing the same NSE-corporate-actions-API
cross-check and dividend-convention-gap logic already built for CA1/CA3 —
and (3) proposes a concrete fix (e.g. "insert corporate_actions row:
ticker=X, ex_date=Y, ratio=Z, confirmed via NSE API"). The fix is queued
for manual approval, never auto-applied — matches this project's existing
no-silent-write discipline (A12, the null-flagging-not-fixing pattern).

**Open question (resolved 2026-07-09):** RCA+fix-proposal output lives in
a new `data_integrity_findings` table (see "Implemented" below) —
approve/reject via the Ops dashboard, mirroring A9's
`sanity_check_passed` surfacing, per user decision during scoping.

**Implemented (2026-07-09).** Shipped as a **standalone scheduler step**,
not `gate.py` validators — the four checks audit already-published
production tables (`ohlcv_adjusted`, `fundamentals`, `corporate_actions`,
feature Parquet) after the fact, and most ingestion sources still default
to `--publish-mode direct` (not through A25's staging gate), so live
validator wiring would rarely execute today. Wiring these checks into
`gate.py` as real validators is a documented follow-up once more sources
migrate to staged publish mode — this was an explicit user decision
during scoping, not an oversight.

1. **`datastore/integrity/` module:**
   - `findings.py` — `Finding` dataclass + `insert_finding`/`list_findings`/
     `approve_finding`/`reject_finding` against the new
     `data_integrity_findings` table (DDL in
     `datastore/schema/create_normalised.py`). Findings always land as
     `status='pending'`; `approve_finding` is the only code path that
     executes a `proposed_fix_sql` against production data, and only when
     explicitly called (never automatic) — matches A12/A25's "flag,
     don't silently write" discipline.
   - `checks.py` — the four checks:
     - `check_corporate_actions` reuses `classify_factor`/
       `CANDIDATE_FACTORS`/the jump-detection method directly from
       `scripts/detect_missing_split_reconstruction.py` (imported, not
       duplicated), cross-checked against Fyers.
     - `check_null_sweep` scans `ohlcv_adjusted`/`fundamentals`/
       `macro_indicators`/that day's feature Parquet, skipping columns in
       `ingestion.scheduler.daily_pipeline._SANITY_KNOWN_SPARSE_COLUMNS`
       (imported, same list `step_sanity_check` uses) so it never
       re-alerts on already-accepted gaps.
     - `check_holiday_leakage` cross-references
       `config.nse_holidays.is_nse_holiday` against `ohlcv_adjusted` rows
       and feature Parquet partition filenames (`FEATURES_DAILY_DIR`).
     - `check_spot_check` samples random (ticker, date) pairs across 5
       years, cross-checks against Fyers + a thin `yfinance`-based Yahoo
       Finance lookup; only flags when both independent sources disagree
       with us AND agree with each other (two-source logic per the
       original spec, to avoid flagging a single source's own
       data-quality issue as our bug).
   - `runner.py` — `run_integrity_checks(conn, as_of_date)` orchestrates
     all four, inserts every finding, and isolates a single check's own
     exception (e.g. a Fyers outage) so it doesn't take down the other
     three.
2. **Scheduler wiring:** new `data_integrity_check` step in
   `checkpoint.py`'s `STEPS`, placed between `adjust_prices` and
   `compute_features` (which now hard-depends on it) — per user decision,
   so a bad ingest never propagates into a day's features/signals.
   Backfillable (deterministic, no model inference, same class as
   `check_ta_alerts`). `daily_pipeline.py::step_data_integrity_check`
   raises only on a `critical` finding (failing the checkpoint, same
   pattern as `step_sanity_check`'s hard-floor raise); `warning`/`info`
   findings are recorded but don't block the pipeline.
3. **Ops dashboard surfacing:** `data_integrity_check` automatically
   shows up in the existing generic `failed_steps` mechanism
   (`datastore/api/routers/ops.py::get_ops_runs`) with no schema change
   needed there. New endpoints `GET /api/v1/ops/integrity-findings`,
   `POST /api/v1/ops/integrity-findings/{id}/approve`,
   `POST /api/v1/ops/integrity-findings/{id}/reject`. New "Data Integrity
   Findings" panel in `dashboard/static/ops/index.html` /
   `dashboard/static/ops/js/index.js` listing pending findings with
   approve/reject buttons.
4. **Tests:** `tests/unit/test_integrity_findings.py`,
   `test_integrity_checks.py`, `test_integrity_runner.py` — all against a
   private in-memory DuckDB via `create_normalised.create_schema(in_memory=True)`
   + `get_duckdb_connection(None)` (never the real `alphalens.duckdb`);
   Fyers/Yahoo fetches are injected fakes, no live network calls. 17
   tests, all passing.
5. **Live smoke-tested (2026-07-09):** ran `run_integrity_checks` against
   an in-memory DB seeded with a deliberately-injected holiday-dated
   OHLCV row (2026-01-26, Republic Day) — correctly flagged as a critical
   `holiday_leakage` finding. The `null_sweep` check, run read-only
   against the real `datastore/features/daily/2026-02-01.parquet` (never
   written to), surfaced a **known follow-up**: several columns already
   documented as genuinely unsourceable in A26's "~19 more columns" list
   (`altman_z`, `insider_selling_flag`, `audit_qualification_flag`,
   `asset_inflation_flag`, `capex_to_assets`, etc.) are NOT yet in
   `_SANITY_KNOWN_SPARSE_COLUMNS`, so `check_null_sweep` currently
   over-flags them as critical. Not a bug in A20 itself — it's the same
   gap A26 already tracks for `step_sanity_check`; once A26's exemption
   list is expanded, `check_null_sweep` inherits the fix for free since it
   imports the same list.

**Known follow-up, not blocking:** (1) wire the four checks into
`gate.py` as real `Validator`s once more ingestion sources migrate off
`--publish-mode direct` (A25's own "Known follow-up" already flags a
related `tests/quality/` fitness-function for this). (2) `check_null_sweep`
noise depends on A26's exemption-list backfill (see smoke-test note
above) — not an A20 defect. (3) `check_spot_check`/`check_corporate_actions`
were tested only against injected fakes in this session (no live Fyers/
Yahoo calls) — first real scheduled run should be watched for API
rate-limit/latency behavior before trusting its findings volume.

### A21 — Pipeline Health Checker
New scheduled job, also run **before** Feature Engineering/Model Run
(same ordering decision as A20) so a missed upstream job is caught before
downstream steps run on stale/incomplete data. Reads the existing
heartbeat store (same one A5, A9's `sanity_check_passed`, and A23's
benchmark data all key off) to confirm every job that was supposed to run
in the trailing 7 days actually recorded a `success` heartbeat —
including the weekend jobs (A5: `weekend_feature_backfill`,
`weekend_fundamentals`, `mf_holdings_ingestion`) that currently have no
visible completeness tracking at all.

On any gap: highlight the specific job + missed date(s), and propose a
catch-up plan (which backfill script covers that job, in what order,
respecting dependencies — e.g. don't queue a Feature Engineering catch-up
before its upstream ingestion catch-up has actually completed). Surface
on the Ops page as a "missed jobs" panel with an approve-to-run
catch-up action, consistent with A20's approve-before-apply pattern.

**Implemented (2026-07-09).**

**Scoping deviation from the spec's literal wording (user decision during
scoping):** shipped as a **new standalone weekly job** (Sunday 11:00 IST,
after the weekend batch + Sunday scoring jobs have had a chance to record
their own history), not wired as a `daily_pipeline` STEP before
`compute_features`. A21 audits *other jobs'* weekly/weekly-ish
completeness (did `weekend_feature_backfill` run this week) — that
question doesn't have a meaningful daily answer, so re-checking it before
every day's `compute_features` would either be a no-op six days out of
seven or force an artificial "only check on Mondays" special case inside
the STEPS list. A20 (Data Integrity Checker), which genuinely audits
*that day's own* data, correctly stays a daily STEP; A21 doesn't.

**Key gap found during implementation:** `scheduler_heartbeats`
(`ingestion/scheduler/pipeline_scheduler.py::_record_heartbeat`) only
ever upserts the single *latest* attempt per job — no per-date history
existed for the weekly/weekend jobs, so "did `weekend_feature_backfill`
succeed 7 days ago" could not be answered from it. Resolved (user
decision) by adding a new append-only `job_run_log` DuckDB table,
populated by extending `_record_heartbeat` itself (line ~444) — no
call-site changes needed across its ~12 existing callers. Like A23's
benchmark history, this needs a few real weeks to accumulate before it's
fully useful; that's expected, not a bug.

1. **`datastore/health/` module:**
   - `job_registry.py` — static cadence table (`JOB_REGISTRY`) for every
     registered job (`daily_pipeline`, `weekend_feature_backfill`,
     `weekend_fundamentals`, `nse_xbrl_fundamentals`,
     `mf_holdings_ingestion`, `multibagger_scoring`, `forensic_scoring`,
     `daily_backup`) plus each one's catch-up action/params.
     `model_training` is deliberately **not** registered — it's
     demand-driven (skips cleanly when no model is currently overdue), so
     a `skipped` heartbeat is a normal, compliant outcome for it, not a
     missed job; flagging it on a calendar cadence would just be noise.
     `expected_dates(job_id, window_start, window_end)` computes which
     calendar dates a job should have fired on, from its cadence.
   - `checks.py::check_job_completeness` diffs `expected_dates` against
     `job_run_log`'s `status='success'` rows over a true 7-calendar-day
     inclusive window, grouping consecutive gaps into one `Finding` per
     job (`critical` if 2+ missed dates, `warning` if exactly 1).
   - `findings.py` — same `Finding`/`insert_finding`/`list_findings`/
     `reject_finding` shape as A20's `datastore/integrity/findings.py`,
     with `proposed_catchup_action`/`proposed_catchup_params` instead of
     a SQL fix (a missed job isn't a bad row to correct, it's work that
     never happened). Also exposes `begin_approve`/`complete_approve` as
     a split two-phase version of `approve_finding` — see the Ops
     endpoint note below for why.
   - `catchup.py` — the executor registry `approve_finding`/
     `complete_approve` dispatches to: `force_run_daily_pipeline` (reuses
     the exact `checkpoint.py` STEPS dependency-respecting walk, so "don't
     queue Feature Engineering before ingestion" is inherited for free —
     see the `force_run.py` extraction below), `rerun_script` (subprocess
     re-invocation of the job's own script, mirroring how the scheduler
     itself calls it), `rerun_mf_holdings` (calls `_execute_mf_holdings_job`
     directly — idempotent/merge-not-overwrite, safe to re-run).
   - `runner.py::run_job_health_check` — same shape as A20's
     `IntegrityCheckResult` orchestration, isolates a single check's own
     exception.
2. **`ingestion/scheduler/force_run.py`** (new) — the STEPS-walking/
   dependency-respecting core of `datastore/api/routers/ops.py`'s
   `force_run_step` endpoint, extracted into a plain synchronous
   `force_run_date_sync` function so A21's `force_run_daily_pipeline`
   catch-up action reuses the identical logic instead of reimplementing
   it. `ops.py`'s `_force_run_step_locked` is now a thin async wrapper
   (`asyncio.to_thread`) around it — no behavior change to that existing
   endpoint (verified against `tests/unit/test_scheduler.py`,
   `test_daily_pipeline.py` — no regressions).
3. **Bug caught and fixed before shipping:** the Ops approve endpoint's
   first draft held a single DuckDB write connection open across the
   *entire* catch-up run (including a possibly-hours-long weekend script
   re-run), which would have locked the whole database for that whole
   time. Fixed by splitting `approve_finding` into `begin_approve`
   (read+validate, short-lived connection) → run the catch-up with **no**
   DuckDB connection held → `complete_approve` (write final status,
   short-lived connection) — `datastore/api/routers/ops.py`'s
   `approve_missed_job_finding` endpoint uses the split version; the
   convenience one-call `approve_finding` (fine for fast/synchronous
   callers — tests, a CLI) still exists for callers that don't need the
   split.
4. **Scheduler wiring:** `ingestion/scheduler/pipeline_scheduler.py::
   schedule_job_health_check` + `_execute_job_health_check_job`
   (Sunday 11:00 IST, `JOB_HEALTH_CHECK_DAY_OF_WEEK`/
   `JOB_HEALTH_CHECK_SCHEDULE_TIME` in `config/settings.py`), registered
   in `daily_pipeline.py::main()` alongside the other weekly jobs.
   Heartbeats itself as `job_id="job_health_check"` (added to
   `scheduler_status.py`'s `HEARTBEAT_STALE_AFTER`), so it also logs its
   own `job_run_log` row — self-monitoring for free.
5. **Ops dashboard:** new `GET /api/v1/ops/missed-jobs`,
   `POST /api/v1/ops/missed-jobs/{id}/approve`,
   `POST .../{id}/reject` endpoints; new "Missed Jobs" panel in
   `dashboard/static/ops/index.html`/`ops/js/index.js` (approve/reject
   buttons disabled while a catch-up is in flight, since it can take a
   while).
6. **Tests:** `tests/unit/test_job_health_registry.py`,
   `test_job_health_findings.py`, `test_job_health_checks.py`,
   `test_job_health_runner.py`, `test_job_health_catchup.py`,
   `test_record_heartbeat_job_run_log.py` — 28 tests, all against a
   private in-memory DuckDB / temp SQLite path (never the real DB files),
   catch-up executors exercised only against mocked
   subprocess/force-run/mf-holdings calls. Caught and fixed two real bugs
   during test-writing: (a) `pandas.DataFrame.df()` surfaces a DuckDB
   `DATE` column as `pandas.Timestamp`, not `datetime.date` — a
   `d in success_dates` set-membership check silently always failed until
   normalized with `.date()`; (b) `lookback_days=7` computed via
   `as_of_date - timedelta(days=7)` produced an 8-day inclusive window
   (two occurrences of `as_of_date`'s own weekday), not a true 7-day
   window — fixed to `timedelta(days=lookback_days - 1)`. Full regression
   suite (`test_scheduler.py`, `test_daily_pipeline.py`,
   `test_integrity_*.py`, `test_schema.py` — 89 tests) and
   `tests/quality/` (DuckDB connection discipline) pass with no
   regressions from the `_record_heartbeat`/`force_run.py` changes.
7. **Live smoke-tested (2026-07-09):** ran `run_job_health_check` against
   an empty in-memory `job_run_log` — correctly flagged all 8 registered
   jobs as missing their expected trailing-7-day occurrences (2 critical
   — `daily_pipeline`, `daily_backup`; 6 warning), confirming `Finding`s
   are produced end-to-end without ever touching the real DB.

**Known follow-up, not blocking:** `job_run_log` has zero real history
until this ships and a few weeks pass — the first several Sunday runs
will likely over-report gaps for anything not yet re-run since
deployment. Worth a one-time note/dashboard banner ("history still
accumulating") if that first-week noise proves confusing in practice —
deferred rather than guessed at up front.

### A22 — Remote/mobile access to dashboard
**Design recommendation: Tailscale.** The dashboard currently only binds
to localhost/LAN with no auth layer. Rather than exposing a port to the
public internet (which then requires real hardening — TLS cert
management, brute-force protection, a proper auth/session system — to be
safe), put the laptop on a private [Tailscale](https://tailscale.com)
tailnet and install the Tailscale app on the phone/iPad. This gives:
- Access to the dashboard from anywhere (not just home LAN) via the
  laptop's stable tailnet IP/hostname — no port-forwarding, no public
  exposure, no DNS/cert management.
- The "password protection" requirement is naturally satisfied by
  tailnet membership (only your own authenticated devices can reach the
  laptop at all) — an *additional* lightweight app-level login (e.g. a
  single shared password via HTTP basic auth or a simple session cookie
  in front of `datastore/api/main.py`) is still worth adding as
  defense-in-depth, in case the laptop is ever on a shared tailnet.
- No changes needed to how the FastAPI/dashboard app runs today — it's
  a network-layer solution, not an application rewrite.

Trade-off to flag: this requires installing Tailscale (free tier is
sufficient for single-user) on both the laptop and the phone/iPad, and
implicitly trusts Tailscale's coordination service. If that's not
acceptable, the fallback is a self-hosted WireGuard tunnel (same idea,
no third-party coordination service, more setup effort). Needs sign-off
on Tailscale specifically before this is scoped further.

### A23 — Job run-time/memory benchmark history + schedule optimization
Extend the existing heartbeat store (used by A5/A9/A15/A17/A21) with
per-run `duration_seconds` and `peak_rss_mb` fields, written by the same
job-runner wrapper that already records success/failure — no new
storage system, just wider rows on what's already there. Accumulate a
few weeks of real data, then use it to:
1. Identify jobs whose actual runtime/memory footprint no longer matches
   their scheduled slot (e.g. a job that's grown to overlap the next
   scheduled job's start time — the same class of bug A16/A17 already
   fixed reactively for `daily_pipeline`, this makes it visible
   proactively).
2. Rebalance weekday vs. weekend job placement — e.g. move memory-heavy
   jobs (multibagger full-universe scoring, ~2GB peak per ML14's note)
   to weekend slots with more idle headroom, based on measured data
   rather than guesswork.
Depends on A21's heartbeat read path already existing; the optimization
pass itself is explicitly gated on having enough real weeks of history to
act on (not implementable meaningfully on day one).

**Implemented 2026-07-09 (storage + instrumentation half; see BuildLog.md
for the full writeup):**
- `job_run_log` (DuckDB, A21's per-invocation history table) gained two
  nullable columns: `duration_seconds DOUBLE`, `peak_rss_mb DOUBLE` —
  added via both the `CREATE TABLE IF NOT EXISTS` DDL and an idempotent
  `ALTER TABLE ... ADD COLUMN IF NOT EXISTS` migration (same pattern
  already used for `fundamentals`/`shareholding`/`public_shareholders`),
  so the real on-disk DuckDB self-heals the next time `create_schema()`
  runs — no manual migration step.
- `ingestion/scheduler/pipeline_scheduler.py::_record_heartbeat` gained
  two optional kwargs (`duration_seconds`, `peak_rss_mb`), written
  through to the new columns. Both default to `None` so any
  not-yet-instrumented caller keeps working unchanged.
- New helpers `_job_timer_start()`/`_job_timer_stats(start)`: wall-clock
  duration via `time.monotonic()`, plus an approximate peak RSS via
  `resource.getrusage(RUSAGE_SELF)` + `RUSAGE_CHILDREN` (KB → MB). All 13
  scheduled job wrappers (`daily_pipeline`, `morning_catchup`,
  `backfill_catchup`, `mf_holdings_ingestion`, `model_training`,
  `weekend_feature_backfill`, `weekend_fundamentals`, `daily_backup`,
  `job_health_check`, `multibagger_scoring`, `forensic_scoring`,
  `nse_xbrl_fundamentals`, `emergency_recompute`) now call these around
  their real work and pass the result into every `_record_heartbeat` call
  site (success, failure, timeout, and skip branches alike).
- **Known limitation, by design:** `ru_maxrss` is a process-lifetime
  high-water mark the OS never resets, not a precise per-run delta — in
  this long-lived scheduler process, a job that runs shortly after an
  even memory-heavier one will under-report its own peak. Acceptable
  because A23's own stated use (weekday vs. weekend relative-footprint
  comparison once weeks of data accumulate) doesn't need per-run
  precision, and this avoids standing up a new out-of-process
  measurement system (e.g. `psutil` polling a child PID) for a ticket
  that's explicitly not actionable until real history accumulates anyway.
- **Not yet built (deliberately, per this ticket's own scope note):** the
  actual rebalancing/optimization pass (items 1 and 2 above) and any
  read-side API/dashboard surface for the accumulated history — both
  need real weeks of `duration_seconds`/`peak_rss_mb` rows to be
  meaningful, which only start accumulating from this point forward.
- Tests: `tests/unit/test_record_heartbeat_job_run_log.py` (new cases for
  duration/peak-RSS round-tripping through `job_run_log`, NULL when not
  passed, and the timer helper pair), `tests/unit/test_scheduler.py`
  (updated `_execute_backfill_catchup`/`_execute_mf_holdings_job`
  heartbeat-call assertions for the new kwargs).

### A24 — UI refactor for responsive layout
All 5 dashboard apps currently render fixed desktop-width layouts. Needed
specifically to make A22's remote/mobile access actually usable — SSH'ing
in via Tailscale to a phone browser that renders a desktop-width table is
not a real solution to the "check the dashboard from my phone" ask. Scope
TBD (breakpoint strategy, whether tables collapse to cards on narrow
widths, touch target sizing) — flagged here as a dependency of A22 rather
than designed in detail yet.

### A25 — Write-audit-publish architecture for DuckDB ingestion
**Problem:** scrapers write straight into the same DuckDB tables that
Feature Engineering/Model Run read — a bad parse, a NaN, or a stale/
duplicate source response (the T2 class of bug) becomes production data
instantly, with no checkpoint between "HTTP response landed" and "this is
trusted enough to train on."

**Design — three stages:**

1. **Raw landing zone, immutable.** Every ingestion source writes first to
   append-only Parquet/JSON under `datastore/raw/<source>/`, partitioned
   by date — never directly into DuckDB. Most sources already do this
   informally (`raw/fno` 12GB, `raw/nse_xbrl_filings` 1.4GB, `raw/screener`
   509MB, `raw/amfi_holdings` 214MB, `raw/trendlyne` 38MB — measured
   2026-07-08). The one confirmed gap: `raw/bhavcopy` is only 5MB today,
   meaning full daily OHLCV bhavcopy history isn't retained raw. Scope
   includes backfilling that and making raw retention a required part of
   every ingestion path, not an incidental side effect.

2. **Validation gate, staging schema.** A step between landing and
   production reads the raw files and runs the batch through the existing
   sanity checks (A12's range-validation gate, A20's null/NaN and
   holiday-leakage checks) before anything reaches DuckDB. Passing rows go
   into a `staging` schema; failing rows go to a `rejected`/`quarantine`
   table with the reason attached — visible, not silently dropped, per
   this project's existing "flag don't silently write" discipline.
   Staging tables are transient (dropped/vacuumed after each successful
   publish), so steady-state disk cost is ~zero; the only spike is during
   a full-refresh job (e.g. a multibagger panel rebuild), which stages at
   most one table's worth of data, not the whole DB.

3. **Atomic publish + N=7 rollback snapshots.** Promote `staging` →
   production via `CREATE OR REPLACE TABLE ... AS SELECT` / view-swap
   (single atomic operation, avoids the partial-update-mid-ingestion state
   that contributed to the cross-process lock race fixed in commit
   `8147579`), then retain the **last 7 daily snapshots** for rollback.

**Storage budget: 15GB total, confirmed available.** A naive N=7 policy of
full 3.5GB DB copies would cost ~25GB — over budget. To fit inside 15GB,
snapshots must be **incremental/differential, not full copies**:
- Only Parquet-export tables that actually changed since the prior
  snapshot get a new file each day; unchanged tables (most of the schema
  — `fundamentals`, `shareholding`, `corporate_actions`, etc. only change
  on their own cadence, not daily) are hard-linked/referenced from the
  prior snapshot rather than re-copied.
- `fno_data` (121M rows, the dominant table) and `ohlcv_adjusted` (7.3M
  rows) are the two tables that change daily and drive most of the real
  incremental cost — budget headroom is sized around these two, not a
  full-DB multiplier.
- Rough sizing check: raw landing completion (~1-3GB one-time) + staging
  (near-zero steady-state) + 7 incremental daily deltas should land
  comfortably under 15GB; exact per-day delta size needs to be measured
  once the raw-landing gap (bhavcopy) is backfilled and the first real
  incremental snapshot is taken — treat the 15GB figure as the hard cap
  to design against, not a number to re-derive from scratch.
- If measured deltas run over budget, first lever is reducing N (e.g. 5
  instead of 7) before considering compression or dropping any table from
  the snapshot scope.

**Dependency:** this is the foundation A20 (Data Integrity Checker) should
be built on top of — A20's checks belong at the validation-gate stage
(step 2 above) rather than as a separate after-the-fact scan, so build
A25's landing/staging/publish skeleton first, then wire A20's checks into
the gate rather than building A20 standalone and retrofitting later.

**2026-07-09 — pilot slice landed.** Scoped to `fno_data`/`ohlcv_adjusted`
(the two tables the storage-budget design above centers on); remaining
sources (screener, trendlyne, xbrl, amfi, corporate_actions) keep writing
direct for now and migrate later, table-by-table, onto the same library.

1. **Raw landing:** `scripts/backfill_bhavcopy_raw.py` backfills the
   confirmed `raw/bhavcopy` gap (resumable, skips dates already on disk,
   uses `ingestion.scheduler.gap_detector.is_trading_day`). No format
   change — `ingestion/scrapers/bhavcopy.py::_save_raw()`'s existing
   one-CSV-per-date write was already a valid immutable landing zone, it
   had just never been backfilled historically.
2. **Staging + gate:** `datastore/staging/gate.py` —
   `stage_dataframe(conn, table_name, df, validators)` lands a batch into
   DuckDB schema `staging`, running it through a list of validator
   callables; rejects go to `staging.rejected_rows` (source_table, reason,
   row_json, staged_at) rather than being silently dropped.
   `null_check_validator(columns)` is the generic first validator; A20's
   fuller checks plug in here later (see A20's entry above). Staging
   tables are dropped after a successful publish.
3. **Atomic publish:** `datastore/staging/publish.py::publish_table` —
   single `CREATE OR REPLACE TABLE ... AS SELECT` against the same
   already-open writable connection (never a second, independently-opened
   one — the exact failure mode commit `8147579` fixed). Guarded by
   `publish_run_lock()`, an `fcntl.flock` cross-process lock mirroring
   `pipeline_run_lock()` (`config.settings.PUBLISH_RUN_LOCK_PATH`).
4. **Snapshots + restore:** `datastore/staging/snapshot.py` —
   `take_snapshot`/`prune_snapshots`/`restore_snapshot`. Incremental via
   content-hash comparison + `os.link` (hard link unchanged tables to the
   prior day's parquet instead of re-exporting), `SNAPSHOT_RETENTION_N`
   (default 7, `config.settings`). Full restore is implemented, not just
   snapshot creation — `scripts/restore_snapshot.py --date YYYY-MM-DD
   [--table ...]` prompts for confirmation and always takes a pre-restore
   safety snapshot first, so a bad restore is itself reversible.
5. **Wired in:** `scripts/insert_fno_files.py` and
   `ingestion/backfill_runner.py` both gained an opt-in
   `--publish-mode staged` (default stays `direct`, the original
   DELETE+INSERT/upsert path, unchanged). The daily pipeline gained a new
   backfillable `publish_and_snapshot` step
   (`ingestion/scheduler/daily_pipeline.py::step_publish_and_snapshot`,
   registered in `ingestion/scheduler/checkpoint.py::STEPS`) that takes/
   prunes the daily rollback snapshot regardless of which write path
   (direct or staged) produced that day's data — so every day still gets
   an N=7 rollback point even before every source has migrated onto
   staged publish.
6. **Tests:** `tests/unit/test_staging_gate.py`,
   `test_publish.py`, `test_snapshot.py`, `test_backfill_bhavcopy_raw.py`
   — all against private in-memory DuckDB / pytest `tmp_path`, never the
   real `alphalens.duckdb`.

**2026-07-09 — full rollout landed.** `datastore/staging/merge.py`
(`coalesce_merge`/`partition_replace_merge`/`insert_ignore_merge`) wired
into the remaining sources, all opt-in via `--publish-mode staged`
(default stays `direct`): `scripts/backfill_fundamentals_trendlyne.py`,
`scripts/backfill_fundamentals_nse_xbrl.py`,
`ingestion/scrapers/amfi_holdings.py::sync_duckdb_table`,
`ingestion/scrapers/corporate_actions.py::upsert_corporate_actions_staged`.
`datastore/staging/gate.py::stage_via_sql` added as the large-table
variant of `stage_dataframe` — merges entirely inside DuckDB via SQL
`UNION ALL` against the on-disk table instead of round-tripping the whole
production table through pandas (a live pandas-path attempt against
`fno_data`, 121M rows, pushed the process to 8GB+ RSS and into swap before
this fix); wired into `scripts/insert_fno_files.py` and
`ingestion/backfill_runner.py`'s staged paths.

**Live dry-run verification (2026-07-09):** exercised
`stage_via_sql`/`publish_table`/`take_snapshot`/`prune_snapshots`/
`restore_snapshot` end-to-end against a small synthetic scratch DuckDB
(real schema via `create_normalised.create_schema`, a handful of rows —
never the production file, never even opened it for writing; the real
`alphalens.duckdb`'s md5 was confirmed unchanged throughout). Found and
fixed a real bug in the process: `ingestion/backfill_runner.py`'s staged
path built its batch from `FYERSBackfill.download_history()`'s 7-column
output + `adj_factor`, but `stage_via_sql`'s merge SQL UNION-ALLs that
against `SELECT * FROM ohlcv_adjusted` (11 columns) — DuckDB requires
equal column counts for `UNION ALL`, so every staged-mode backfill run
would have raised `duckdb.BinderException` the first time it was actually
exercised. Fixed by padding the staged batch to the full column set
(`delivery_qty`/`delivery_pct` NULL, `vol_adj_factor` 1.0, matching the
direct-mode INSERT's own defaults) before staging. Regression test added:
`tests/unit/test_fyers_backfill.py::test_staged_publish_mode_matches_ohlcv_adjusted_full_schema`.
All 57 staging/publish/snapshot/backfill unit tests pass.

**Known follow-up, not blocking:** the two-full-table-rewrite cost of
`stage_via_sql` (stage once, publish once — each a full scan+copy of the
production table) is disproportionate for a single date's worth of new
rows on `fno_data` (121M rows); worth a `memory_limit` PRAGMA or a
cheaper merge strategy if staged mode becomes the default rather than
opt-in. A `tests/quality/` fitness-function check ("no direct
scraper→production write bypassing staging") once staged mode becomes
the default; A20's actual checks wired into the gate as validators.

### A26 — Expand sanity_check exemption list + finish backfilling 2026-07-03/06/07

2026-07-08 session (see BuildLog.md for the full fix) added
`_SANITY_KNOWN_SPARSE_COLUMNS` to `daily_pipeline.py`'s `step_sanity_check`,
exempting 38 permanently-unsourceable feature columns from the all-NaN
failure check, and backfilled real `YIELD_10YR`/`YIELD_3M` data for
2026-07-03/06 (the wiring gap that made them NaN for exactly those 3 dates).
2026-07-08 itself is fully fixed and verified (`sanity_check`/`paper_trade`
both `success`).

Not yet done:
- `features/deep_forensic.py`'s own 2026-07-07 real-data-availability audit
  already confirms ~19 more columns (`goodwill_ratio`,
  `contingent_liability_ratio`, `subsidiary_count`, `loans_to_related`,
  `capex_to_assets`, `intangibles_growth`, `off_balance_sheet_proxy`,
  `noncash_assets_ratio`, `promoter_pledge`, `promoter_pledge_change_qoq`,
  `pledge_spiral_risk`, `audit_qualification_flag`, `auditor_change_flag`,
  `cfo_tenure_months`, `board_independence`, `director_resignation_count_4q`,
  `whistle_blower_policy`, `salary_to_pat`, `rpt_intensity`,
  `buyback_acceptance_estimated`) as genuinely unsourceable from any free
  structured source today — same category as the 38 already exempted, just
  not folded into `_SANITY_KNOWN_SPARSE_COLUMNS` yet.
- 2026-07-03/06/07 need `step_compute_features` re-run (was in progress,
  still running at 2026-07-08 session end) to pick up the yield backfill
  plus several already-landed 2026-07-07 fixes (`cwip_ratio`,
  `asset_inflation_flag`, `insider_selling_flag`, `peer_outlier_score`,
  `tax_rate_anomaly`, `ipo_lockin_expiry_proximity`,
  `ipo_listing_age_months`), then need `sanity_check`/`paper_trade`
  re-run via the Ops force-run endpoint once the exemption list above is
  expanded — without it, `sanity_check` will still fail on the ~19
  remaining columns even after the recompute.

**[UPDATE 2026-07-08, later same-day session]** The "~19 more columns"
list above is now partially stale — a separate same-day session (see
BuildLog.md "58-column NSE-sourced fundamentals wiring effort") found
real NSE sources for several of them: `goodwill_ratio`,
`promoter_pledge`, `promoter_pledge_change_qoq`, `pledge_spiral_risk`,
`audit_qualification_flag`, `ipo_lockin_expiry_proximity`, and
`ipo_listing_age_months` are now genuinely populated (NSE Integrated
Filing IndAS + `corporate-pledgedata-sast3132` + `public-past-issues`).
Before expanding `_SANITY_KNOWN_SPARSE_COLUMNS`, re-check which of the
remaining ~12 (`contingent_liability_ratio`, `subsidiary_count`,
`loans_to_related`, `capex_to_assets`, `intangibles_growth`,
`off_balance_sheet_proxy`, `noncash_assets_ratio`, `auditor_change_flag`,
`cfo_tenure_months`, `board_independence`, `director_resignation_count_4q`,
`whistle_blower_policy`, `salary_to_pat`, `rpt_intensity`,
`buyback_acceptance_estimated`) are still genuinely blocked — see FO8 for
the confirmed-still-blocked list and CA6 for newer real leads (RPT/
governance endpoints found but not yet wired) that could close a few
more of these.

**[UPDATE 2026-07-09]** Re-checked the "remaining ~12" list above against
the live `_SANITY_KNOWN_SPARSE_COLUMNS` in `ingestion/scheduler/
daily_pipeline.py`: 13 of the 15 named columns (everything except
`capex_to_assets`/`noncash_assets_ratio`) were **already** in the
exemption list from the earlier 2026-07-08 pass — this write-up's "not yet
folded in" framing had gone stale. Only `capex_to_assets` and
`noncash_assets_ratio` (FO8: NSE's own disclosure template renders these
inputs as freeform "Textual Information", same permanently-unstructured
gap as `contingent_liability_ratio`/`subsidiary_count`/`loans_to_related`/
`off_balance_sheet_proxy`) were genuinely missing — added to
`_SANITY_KNOWN_SPARSE_COLUMNS`, with new unit tests
(`tests/unit/test_daily_pipeline.py::TestSanityKnownSparseColumns`,
`TestStepSanityCheck`) covering both the exemption membership and
`step_sanity_check`'s actual pass/fail behavior around it (no prior test
coverage existed for `step_sanity_check` at all).

The 6 CA6-tracked columns (`auditor_change_flag`, `board_independence`,
`cfo_tenure_months`, `director_resignation_count_4q`,
`whistle_blower_policy`, `rpt_intensity`) plus `salary_to_pat` are
deliberately **left in** the exemption list as before, but are not
"confirmed-unsourceable" in the permanent sense FO8's columns are — CA6
found real, working NSE endpoints for all of them, just blocked on an
undiscovered secondary lookup param (`recId`/`seqNum`). Worth revisiting
if CA6 is ever picked up, rather than treating their exemption as
permanent.

**Still outstanding (not done this session):** the 2026-07-03/06/07
`step_compute_features` recompute and subsequent `sanity_check`/
`paper_trade` re-run via the Ops force-run endpoint — this is a live
re-run against the real feature store/signals DB, not a code change, and
wasn't executed in this session; needs an explicit operator-approved
Ops force-run.

### A27 — Real-economy macro: 8 of 10 series remain genuinely blocked

2026-07-08 session (BuildLog.md) confirmed live (not guessed) that
`gst_collection_growth`, `pmi_manufacturing`, `pmi_services`, `iip_growth`,
`auto_monthly_sales_growth`, `rail_freight_growth`, `upi_transaction_growth`,
`bank_credit_growth` have no free, structured, programmatically-fetchable
source as of this session:
- PMI is commercially licensed by S&P Global — no free path, ever, short
  of a paid subscription.
- IIP: a `data.gov.in` API key would unblock it (free to obtain, requires
  account signup this environment couldn't perform) — same ~45-day MOSPI
  lag already modeled in `features/real_economy_macro.py`.
- GST/rail-freight: PIB press releases only (freeform prose), no
  structured feed — would need fragile regex-scraping of unversioned
  release text, deliberately not built (see `ingestion/scrapers/
  macro_real_economy.py`'s module docstring for the full per-series
  research log).
- Auto sales (SIAM), UPI (NPCI), bank credit (RBI DBIE): server-rendered
  dashboards or active bot-blocking, no accessible API found.

`cement_dispatches_growth`/`power_consumption_growth` ARE now real
(DPIIT's Index of Eight Core Industries `.xlsx`) — see BuildLog.md. If
picking this up again, start with the `data.gov.in` API key path for IIP
(the one credentials-only, not source-availability, gap) before
re-attempting the others.

### A28 — Emergency feature recompute + 8-model retrain (consolidated)

This item merges several separate write-ups from the 2026-07-05/07/08
sessions that all describe the same underlying corrective effort (the
post-corporate-action-fix full recompute and subsequent model retrain),
since the original doc had reused numbers/split them across sections in
a way that fragmented one continuous story.

**(a) Stage 1 + Stage 2 recompute.** The 2026-07-05 emergency recompute
job (Stage 1 batched recompute → Stage 2 daily-parquet rebuild → 8-model
retrain) got through all 17 Stage-1 batches (after a resume fix for a
DuckDB lock collision — see BuildLog.md), but Stage 2 then hit the job's
8-hour subprocess timeout and never finished; `models_done` sat empty —
none of `signal_5d/21d/63d, tft, bilstm, multibagger, hmm_market,
pnd_detector` had been retrained on the corrected price history. The
machine has since rebooted and no part of this job was running as of
that check. Needs: investigate why Stage 2 took >8h (first attempt
crashed quickly on a transient lock, so the real single-run duration is
still unmeasured — may just need a longer timeout, or further per-chunk
batching like Stage 1 got); resume via
`_execute_emergency_recompute_job(start_stage="stage2")` (or add a
`start_stage="retrain"` if Stage 2 turns out to already be complete on
re-check); once retrained, cross-reference against the 77-ticker
`needs_retrain` list from CA4.

**(b) Multibagger precompute cache bug — FIXED 2026-07-06.**
`run_stage2`'s multibagger precompute cache silently produced 0 cached
dates every chunk (newest-first `pending_dates[0]`/`[-1]` date-ordering
bug, same class as a prior `run_stage2_chunked` fix), forcing every date
onto a ~15-25s/date fallback instead of a ~15-25s/chunk fast path. Fixed
2026-07-06 (`scripts/feature_backfill_hybrid.py`, `min()`/`max()` of
`pending_dates`) and verified standalone (150/150 dates now cache-hit vs
0/150 before). Fix was not yet run against the live recompute job as of
that check (pending relaunch per (a)).

**(c) `retrain_phase2.py` OOM + ticker-chunking fix.** A manual re-run of
`retrain_phase2.py` on 2026-07-07 (the 3rd manual re-run attempt that
morning) was kernel-OOM-killed after ~3h52m, peaking at 9.4GB RSS on a
~15GB box. Root cause: `compute_technical_features()` was called on the
whole ~2317-ticker universe at once (~6-7GB float64 feature matrix), once
per horizon (5d/21d/63d) in `HORIZON_CONFIGS`, with nothing freed between
iterations. Fixed same session: added `DEFAULT_TICKER_CHUNK_SIZE=400`
ticker-batched processing to `_compute_phase2_panel` and a new
`_build_training_dataset_chunked`, float64→float32 downcasting
(`_downcast_floats`), explicit `del`+`gc.collect()` between horizon
iterations, and a `--chunk-size` CLI flag. Verified: module imports
cleanly, helper functions behave correctly on toy inputs, and
`CORE_TECHNICAL_FEATURES`/`PHASE2_FEATURES` were confirmed to have no
cross-sectional (cross-ticker) features (those live in
`features/multibagger.py`, unused by this script) — so ticker-batching
doesn't change any computed value, only peak memory. **Not done**: no
actual end-to-end re-run of the full retrain (a multi-hour job) to
confirm peak RSS actually stays bounded on the real ~2317-ticker
universe. Recommend a `--chunk-size 200 --quick` (or similar reduced)
smoke run with RSS monitored before trusting this on the next full
retrain, and consider lowering `DEFAULT_TICKER_CHUNK_SIZE` further if the
smoke run still shows high peak memory per batch.

**(d) Forced retrain of all 8 models per explicit user request.**
`signal_5d/21d/63d, tft, bilstm, multibagger, hmm_market, pnd_detector`.
Blocked on (a)/(b) — `_execute_emergency_recompute_job`'s retrain loop
only fires after Stage 2 fully completes; `models_done: []` as of the
last check.

**(e) 2026-07-06 still missing steps.** During the 2026-07-07 OOM
investigation, a stale `daily_pipeline` process (PID 1966732, orphaned
since the previous evening's failed run) was found holding the DuckDB
file lock and was killed; a second, legitimately-running backfill
process then caught `2026-07-03` up fully and got `2026-07-06` through
`check_ta_alerts`, but `2026-07-06`'s `run_models`/`write_signals`/
`sanity_check`/`paper_trade` are still sitting at `skipped` in
`pipeline_checkpoints` (SQLite) from the original failed run. The first
three ARE backfillable under the current `checkpoint.py::STEPS` (see
A30), so a `force_run_step` (or another
`run_steps_for_date(date(2026,7,6))` call once nothing else holds the DB
lock) should complete them — `paper_trade` should correctly stay
skipped for that date (non-backfillable by design). Not done as of the
last check; needs a follow-up force-run.

**(f) RESOLVED 2026-07-09 — which recompute pass actually fed the
2026-07-06 retrain.** Verified without re-running the multi-hour job, by
reading `systems/ml_signal_engine/inference/train_all_phase1.py`,
`retrain_phase2.py`, and `train_multibagger.py`: none of the 8 model
trainers read `datastore/features/daily/` (the Stage 1/2 parquet cache)
at all — they all call `load_ohlcv_from_db()` and compute features live
from the `ohlcv_adjusted` DuckDB table. That table was corrected by
`scripts/run_price_adjuster.py` on 2026-06-25 (`logs/price_adjuster.log`:
"Price adjuster complete", 430 tickers with `adj_factor != 1.0`,
including several of the CA-affected tickers), i.e. *before* the
2026-07-06 retrain ran. So the Stage 1/2 recompute job's completion
status was never actually a dependency for the retrain step at all — the
original A28(a)/(f) framing conflated two independently-fed pipelines.
`logs/retrain_all_20260706.log` confirms `train_all_phase1` and
`train_multibagger` genuinely loaded real rows from the corrected
`ohlcv_adjusted` table and completed successfully — those 7 models'
2026-07-06 artifacts are trustworthy.

**(g) RESOLVED 2026-07-09 — why `signal_63d` didn't update.** Not a
legitimate "didn't improve" outcome. Both 2026-07-06 attempts to run
`retrain_phase2.py` (which trains `signal_63d`) actually crashed —
first on a DuckDB lock conflict, then on a `TypeError:
_build_training_dataset() missing 1 required positional argument:
'benchmark'` (a bug in the pre-chunking-fix version of the script that
predates A28(c)'s `_build_training_dataset_chunked` refactor, which
already fixed this call signature). Both crashes were masked by a bug in
the wrapper, `scripts/retrain_all_when_free.sh`: it logged
`echo "... $(date -Iseconds) exit=$?"`, and bash expands the `$(date
...)` command substitution *before* `$?` is read, so `$?` always
reflected `date`'s (always-zero) exit code instead of the python
command's. Every stage showed `exit=0` in the log regardless of outcome.
Filed and fixed as **A37**. `signal_63d` needs `retrain_phase2.py`
actually re-run (current code already carries the `benchmark`-arg fix
via A28(c)'s chunking rewrite, but that has never been exercised
end-to-end) to pick up a real 2026-07-06-or-later training date.

### A29 — `shareholding`/`governance` GET endpoints 500 on NULL numeric fields (FIXED 2026-07-08)

`datastore/api/routers/shareholding.py::get_shareholding` and
`governance.py::get_governance` crashed with HTTP 500 for any ticker/
quarter where `fii_pct`/`dii_pct`/etc. was NULL in DuckDB — NULL becomes
NaN in a pandas float64 column, and `ShareholdingRow`'s
`Optional[float] = Field(ge=0, le=100)` rejects `float('nan')` outright
(Pydantic v2: `Optional` only catches `None`, not NaN). `fundamentals.py`
already had the correct fix (`df.astype(object).where(df.notna(), None)`
before `to_dict`) in two places; these two routers were missing it — now
fixed identically. This had been silently causing hundreds of per-ticker
500s during `compute_features` (visible as `GET /api/v1/shareholding/...
500` floods in scheduler logs) and was the likely direct cause of a
2026-07-07 `sanity_check` failure's "58 all-NaN columns" (many are
shareholding/corp-action-derived features). See BuildLog.md 2026-07-08
entry for verification.

### A30 — Multi-day pipeline gaps: signals now backfilled, UI now flags "backfilled" vs "live" (IMPLEMENTED 2026-07-09)

2026-07-08 session (see BuildLog.md) made `run_models`/`write_signals`/
`sanity_check` backfillable in `ingestion/scheduler/checkpoint.py`'s
`STEPS`, per explicit user decision: even if the laptop is off across
multiple 18:00 runs, every missed trading day's EOD signal should still
be computed and persisted whenever the process next comes up — a stock
recommended at ₹100 that's now at ₹95 on day 5 of a 21-day window is
still actionable. `paper_trade` deliberately stays non-backfillable
(Phase 3 Gate 7 counts `paper_trading/executions/{date}.csv` files as
forward-time days — auto-trading a backfilled day would corrupt that
count).

**Implemented (2026-07-09).** `pipeline_checkpoints` gained an
`is_backfill BOOLEAN NOT NULL DEFAULT 0` column
(`ingestion/scheduler/checkpoint.py`), migrated onto existing DB files via
`ALTER TABLE ... ADD COLUMN` wrapped in try/except (SQLite has no `ADD
COLUMN IF NOT EXISTS`, unlike DuckDB's — confirmed the syntax error with a
throwaway repro before picking the try/except approach).
`CheckpointManager.save_checkpoint` takes a new `is_backfill: bool = False`
parameter; `run_steps_for_date` (`ingestion/scheduler/pipeline_scheduler.py`)
— which already carried an `is_backfill` flag for its
backfillable-step-skip logic — now also passes that same flag into every
`save_checkpoint` call, so each checkpoint row correctly records whether it
came from a live run or a `run_backfill`/`run_morning_catchup_sequence`
catch-up.

Surfaced via `GET /api/v1/ops/steps` (`OpsStepRow.is_backfill`, per step
per date) and `GET /api/v1/ops/runs` (`OpsRunRow.is_backfill`, True if any
step for that run's date was backfilled). Ops dashboard
(`dashboard/static/ops/js/index.js`) Steps and Runs tables both gained a
"Run Type" column with a `BACKFILLED` (amber) / `LIVE` (green) badge,
reusing the existing `b-amber`/`b-green` CSS classes. `paper_trade`'s
absence from a backfilled date's checkpoints (it's skipped entirely during
backfill, unchanged from before) is itself the signal that no paper trade
happened that day — consistent with the original non-backfillable design.

Tests: `tests/unit/test_checkpoint_backfill_flag.py` (4 tests, in-memory
SQLite) — `save_checkpoint` defaults/records `is_backfill` correctly, a
full live `run_steps_for_date` run marks every step `is_backfill=False`, a
full backfill run marks backfillable steps `is_backfill=True` and confirms
`paper_trade` has no checkpoint row at all. All 4 pass. Full
`test_scheduler.py`/`test_daily_pipeline.py` regression clean except one
pre-existing DuckDB lock-conflict failure unrelated to this change (see
BuildLog.md 2026-07-09 entry).

Daily Insights / ML signal screens (not the Ops dashboard) still don't
surface this per-signal — left as future work (A43) since it needs a join
from `ml_signals` back to `pipeline_checkpoints` by date, out of scope for
this session's Ops-panel-focused fix.

### A31 — `download_index_ohlcv` failing repeatedly on backfill (BSE/NSE 404s) — FIXED 2026-07-09

Originally suspected as a scraper/URL/upstream-format problem (the "BSE
BULK/BLOCK... Expecting value" pattern seen for `large_deals`). Root
cause was actually unrelated to NSE/BSE at all — `logs/daily_pipeline.log`
showed two distinct, already-known infra bugs:

- `2026-07-03`: `Catalog Error: Table with name index_ohlcv does not
  exist!` — the `index_ohlcv` table had been added to `_ALL_TABLES` but
  nothing called `create_schema()` against the live DB outside manual/
  ad-hoc invocation. Already fixed 2026-07-07 (`daily_pipeline.py`'s
  `run_scheduler_service()` now calls `create_schema()` at startup,
  idempotent via `CREATE TABLE IF NOT EXISTS`).
- `2026-07-06`: `IO Error: Could not set lock on file
  "alphalens.duckdb"` — a cross-process DuckDB write-lock conflict
  (same family as the `check_ta_alerts`/`signals.duckdb` race fixed in
  the 2026-07-02 commit, but on `DUCKDB_PATH` instead). `get_duckdb_connection`
  already retries with backoff (`SPEC-SCHED-013`, ~3.5s budget), but
  `step_download_index_ohlcv`'s `try/except` only wrapped the scraper
  fetch — the DB write sat outside it, so once the retry budget was
  exhausted the exception escaped and failed the whole step, even though
  its own docstring documents it as always-non-critical
  ("Returns None — Always").

**Fix**: widened the `try/except` in
`ingestion/scheduler/daily_pipeline.py::step_download_index_ohlcv` to
cover the row-build + DB write, not just the scraper fetch — any failure
now logs a warning and returns, matching `step_download_fno`'s and
`step_download_macro`'s "mark unavailable, never raise" contract.
Verified live: `ingestion.scrapers.nse_indices.download_index_ohlcv`
fetched and parsed `2026-07-03`'s real CSV successfully in isolation
(200 OK, valid NSE data) — confirming the scraper itself was never
broken.

New unit tests in `tests/unit/test_daily_pipeline.py::TestStepDownloadIndexOhlcv`
(scraper-failure caught, DB-write-failure caught, successful persist,
same-date upsert-not-duplicate) — 4/4 pass; full `test_daily_pipeline.py`
(22/22) and `test_scheduler_resume.py` (2/2) still pass.

Found but out of scope for this fix: `step_download_fno` has the same
unwrapped-DB-write gap (its DB write also sits outside its non-critical
try/except) — not confirmed to have failed live, tracked as A34.

### A32 — `scripts/model_training_status.py` run to completion (IMPLEMENTED 2026-07-09)

New CLI status script (`scripts/model_training_status.py`, added per user
request) reports per-model training status against
`datastore/models/registry.json` and `_MODEL_TRAINING_SCRIPT_MAP`, plus
the `model_training` job's `scheduler_heartbeats` row and next-scheduled
run time.

**Root cause of "not executed to completion":** the script's own usage
docstring says `python scripts/model_training_status.py`, but unlike
every other script in `scripts/` it had no `sys.path.insert(0, ...)`
shim — running it exactly as documented failed immediately with
`ModuleNotFoundError: No module named 'config'` before any status logic
ran. Fixed by adding the same shim used elsewhere in `scripts/`.

**Real run output (2026-07-09):** 10 models tracked — `bilstm`/`tft`
never trained (expected, A38/A40 still pending real training runs), the
other 8 all `OK` (0 overdue). `model_training` scheduler job section
correctly reports no heartbeat recorded yet and a real
`next_scheduled: 2026-07-10T12:00:00+05:30`. Table and heartbeat/next-run
section both read real values, not placeholders — confirmed working
end-to-end.

### A33 — Regression test for the `model_training` overdue-check union fix (IMPLEMENTED 2026-07-09)

`_execute_model_training_job`'s overdue-check loop (`ingestion/scheduler/
pipeline_scheduler.py`) was changed to iterate the union of
`registry.json` keys and `_MODEL_TRAINING_SCRIPT_MAP`'s non-`None` keys,
so a mapped-but-never-registered model (multibagger's original bug) is
always caught as "never trained" rather than silently skipped.

New `tests/unit/test_model_training_overdue_union.py` (2 tests, not in
`test_scheduler.py` — kept as its own focused file since it needs its own
`MODELS_DIR`/`PIPELINE_LOG_DB_PATH` monkeypatch fixture): seeds a
`registry.json` in `tmp_path` missing one `_MODEL_TRAINING_SCRIPT_MAP`-mapped
model and asserts `_execute_model_training_job` still calls
`_trigger_model_retrain` (monkeypatched to a list-append, no real
subprocess) for it; a second test checks the union's other direction — a
registry-only model with no script mapping is still queued. Both pass.

### A34 — `step_download_fno` may share A31's unwrapped-DB-write gap

Found while fixing A31: `step_download_fno` (`ingestion/scheduler/
daily_pipeline.py`) follows the same shape as `step_download_index_ohlcv`
did before the A31 fix — the scraper fetch (`fno.download_fno_bhavcopy`)
is wrapped in a `try/except` that catches and logs, but the subsequent
`DELETE FROM fno_data` + `executemany` DB write happens outside that
`try/except`. If that write ever hits a DuckDB lock conflict (same
`SPEC-SCHED-013` cross-process race that caused A31's `2026-07-06`
failure), it would fail the whole step despite the docstring documenting
it as always-non-critical ("Returns None — Always... failures are caught
and logged, never raised"). Not confirmed to have failed live — no
matching error seen in `logs/daily_pipeline.log` for `download_fno` —
so left as a backlog item rather than fixed speculatively.

### A35 — screener source can't join A25 staged publish without an architecture change

Found while doing A25's full rollout (2026-07-09) to the remaining raw
sources. Every other source that gained `--publish-mode staged`
(trendlyne, nse_xbrl, amfi, corporate_actions) is a batch script: it
already accumulates many rows in memory before writing, so switching the
final write from many small SQL statements to one merge + one atomic
`CREATE OR REPLACE TABLE` swap is a mechanical change with the same
net effect.

`screener` is architecturally different: `ingestion/scrapers/
screener.py::batch_export()` writes **one ticker at a time**, over HTTP,
via `datastore/client.py`'s `write_fundamentals()` → the DataStore API's
`POST /fundamentals/write` (`datastore/api/routers/fundamentals.py`),
which does a single-row `INSERT ... ON CONFLICT DO UPDATE` per call. This
is a live, request-per-row design, not a script that owns a DuckDB
connection for a whole batch. Two ways to bring it under A25, neither
free:

1. **Client-side batching**: `batch_export()` accumulates every ticker's
   parsed record in memory across the whole run, then does one staged
   merge + publish at the end (same shape as the other 4 sources) —
   loses the current "each ticker lands the moment it's fetched" property
   (a crash partway through a multi-hour screener run currently still
   keeps whatever was written so far; batching would lose that unless
   combined with periodic staged-publish checkpoints, adding complexity
   back).
2. **API-level staging**: the `POST /fundamentals/write` endpoint itself
   writes into `staging.fundamentals` instead of production, with a
   separate `POST /fundamentals/publish` call the batch script triggers
   once done. Preserves per-ticker crash-resilience but requires new API
   surface, not just a script change like the other 4 sources.

Not designed further here — flagged as a gap in A25's "full rollout"
rather than one of the four already-landed sources, since it needs its
own short design decision (see A22-style precedent for design-before-code
on architecture questions), not a mechanical port.

### A36 — `fundamentals` table has 4 writers with inconsistent upsert-conflict precedence

Found while writing `datastore/staging/merge.py::coalesce_merge` for
A25's rollout — reproducing each source's existing conflict-resolution
policy in pandas required reading all 4 writers' SQL side by side, which
surfaced that they don't agree with each other:

- **kaggle** (`scripts/load_kaggle_fundamentals.py`): own `_write_batch`,
  gated via `features/fundamental_quality_gate.py::validate_and_annotate`.
- **trendlyne** (`scripts/backfill_fundamentals_trendlyne.py`): existing
  DB value wins on conflict (`COALESCE(fundamentals.col, excluded.col)`)
  — gated via `validate_and_annotate`.
- **nse_xbrl** (`scripts/backfill_fundamentals_nse_xbrl.py`): **new**
  value wins on conflict (`COALESCE(excluded.col, fundamentals.col)`) —
  the *opposite* precedence from trendlyne — **not** gated via
  `validate_and_annotate` (confirmed via grep: the symbol never appears
  in this file).
- **screener** (`datastore/api/routers/fundamentals.py`'s
  `POST /fundamentals/write`): new value wins on conflict, same direction
  as nse_xbrl — **not** gated via `validate_and_annotate` either.

Net effect: which of two disagreeing sources "wins" for the same
`(ticker, fiscal_year, quarter)` depends on which pair of sources is
involved and which ran more recently, not on a single documented
priority order (e.g. "regulatory NSE XBRL data should always outrank a
scraped screener figure" — today that's true only sometimes, by
accident of COALESCE direction, not by design). And 2 of the 4 writers
(nse_xbrl, screener) bypass A12's range-validation gate entirely, so a
NSE XBRL parse bug or a screener passthrough bug (the exact failure mode
`features/fundamental_quality_gate.py`'s docstring documents as already
having happened twice) has no automated check today. Not fixed here —
this is a data-quality/precedence design question (what should the real
source-priority order be? does nse_xbrl/screener need
`validate_and_annotate` wired in?) that deserves its own decision, not a
silent behavior change bundled into the A25 rollout.

### A37 — `retrain_all_when_free.sh` logged false `exit=0` for crashed stages — FIXED 2026-07-09

Found while verifying A28(f)/(g) without re-running the multi-hour retrain
job. `scripts/retrain_all_when_free.sh` logged each stage's outcome as:

```bash
echo "=== train_all_phase1 END $(date -Iseconds) exit=$? ==="
```

Bash expands `$(date -Iseconds)` (running `date`, which always exits 0)
*before* it expands `$?` later in the same string — so `$?` always
reflected `date`'s exit status, never the preceding python command's.
Every one of the 3 retrain stages logged `exit=0` in
`logs/retrain_all_20260706.log` regardless of what actually happened.
This directly caused the confusion in A28(f)/(g): `retrain_phase2.py`
(which trains `signal_63d`) crashed on both its 2026-07-06 attempts (a
DB lock conflict, then a since-fixed `TypeError` in the pre-chunking-fix
code), but the log made both look like clean successes, making
`signal_63d`'s stale `last_trained_date` look like a legitimate
"didn't improve" outcome instead of "never actually ran."

**Fix**: capture `$?` into a variable (`rc=$?`) immediately after each
python invocation, before any other command (including the `$(date
...)` substitution in the log line) can overwrite it; log `exit=$rc`
instead of `exit=$?`.

**Tests**: `tests/unit/test_retrain_all_when_free_script.py` — exercises
the fixed logging idiom against both passing and failing commands
(including a real nonzero-exit python subprocess) and statically asserts
the buggy `$(...)....exit=$?` pattern isn't reintroduced.

### A38 — T5's "18 advanced TA features unused" is only half right (found 2026-07-09)

Surfaced while scoping how to improve `signal_63d` (see A28). T5 (below,
under Technical) describes `features/advanced_technical.py`'s 18
features (wavelet, Hurst, entropy, fracdiff, complexity) as "computed but
unused by any ML training pipeline" — true for 6 of the 8 registry
models, but not for all of them, and the actual gap is more specific
than "wire them in or stop computing them."

**Two training paths genuinely diverge on this:**
- `train_all_phase1.py` (`hmm_market`, `pnd_detector`, `signal_5d`,
  `signal_21d`, `meta_labeler`, `conformal_signal5d`) and
  `retrain_phase2.py` (`signal_63d` + refreshed `signal_5d`/`signal_21d`)
  both use an explicit column allowlist — `CORE_TECHNICAL_FEATURES` (70)
  and `PHASE2_FEATURES` (70 + corporate-action/F&O/fundamental/
  governance/MF-holdings features) — by deliberate design
  (`train_all_phase1.py:20-23`'s docstring explicitly says "not the full
  102-column `features.matrix_builder.ALL_FEATURE_COLUMNS`"). The 18
  advanced features are excluded here on purpose, and T5's framing is
  correct for these 6 models plus `signal_63d`.
- `systems/ml_signal_engine/models/deep/{tft,bilstm}_model.py` do **not**
  use an allowlist — both derive `feature_cols` as literally every
  non-id column found in `datastore/features/daily/*.parquet`
  (`tft_model.py:757-759`, `bilstm_model.py:642-644`), and
  `features/matrix_builder.py::ALL_FEATURE_COLUMNS` already includes
  `ADVANCED_TECHNICAL_FEATURES` in that parquet. So TFT/BiLSTM already
  consume all 18 features today, exactly matching
  `advanced_technical.py`'s own docstring ("Consumers: ...
  models/deep").

**The actual gap**: `tft`/`bilstm` are **not** in `datastore/models/
registry.json` at all (`None` for both) and have no
`datastore/models/{tft,bilstm}/` directory — neither has ever been
trained, despite being 2 of the "8 models" A28(d) calls for. So in
practice the 18 features are pure compute/storage overhead today, not
because of a wiring gap (they're already wired into their one real
consumer) but because that consumer has never been run. T5's binary
framing ("wire in vs. stop computing") missed this — the real decision
is "run TFT/BiLSTM for the first time" (a substantial new deep-learning
training job, unproven, likely a multi-hour+ effort of its own) vs.
leaving them uncomputed for the 6+1 models that don't use them.

**Cheaper near-term experiment worth trying once `signal_63d` has a real
post-A37 baseline**: add a small subset — `hurst_exp_63d`,
`wavelet_regime_signal`, and the `fracdiff_*` features — to
`PHASE2_NEW_FEATURES` for `signal_63d` specifically. These are
regime/persistence signals designed for exactly the longer-horizon
structure a 63-day target has, unlike `signal_5d`/`signal_21d`, and
would be a much smaller change than standing up TFT/BiLSTM training from
scratch. Not implemented — needs a decision on whether to spend the
Optuna/validation cycles testing this before committing to full
TFT/BiLSTM training runs.

**2026-07-09 follow-up — wiring landed, first real run still pending.**
Per explicit user request ("trigger any pending modelling and plug it
into the schedule"), closed the wiring gap without yet running the
actual multi-hour training:

- `systems/ml_signal_engine/models/deep/tft_model.py` and
  `bilstm_model.py`'s `schedule_overnight_training()` now return `
  {"folds_trained": int, "last_model_path": str|None}` instead of `None`
  (previously silent about what it had actually done).
- `systems/ml_signal_engine/inference/train_deep_models.py` gained
  `_update_registry()`, which reads/merges/writes
  `datastore/models/registry.json` from that return value — same
  read-merge-write convention as `train_all_phase1.py::_save_model()`
  (`last_trained_date`, `training_interval_days`, plus `folds_trained`/
  `horizon_days`). No-ops (does not touch the file) when
  `folds_trained == 0`, so a run that trained nothing can't stomp a real
  prior `last_trained_date`.
- `ingestion/scheduler/pipeline_scheduler.py::_MODEL_TRAINING_SCRIPT_MAP`:
  `"tft"`/`"bilstm"` now map to
  `systems.ml_signal_engine.inference.train_deep_models` (previously
  `None`, "Phase 3, not built yet" — stale once the CLI existed). Both
  keys intentionally share one module string so
  `_execute_model_training_job`'s dedup-by-script loop still invokes
  only one subprocess per cycle even if both are overdue, same pattern
  `train_all_phase1` already uses for its 6 registry keys.

**Known residual risk, not fixed this session**: `_trigger_model_retrain`
hard-caps each subprocess at 8 hours
(`timeout=3600*8`), but `train_deep_models --model all`'s default trains
`tft` then `bilstm` sequentially, each independently documented at 4-6h
— a combined worst case of 8-12h could hit the timeout mid-`bilstm`. Not
unsafe (each model's registry entry is only written after that model's
own training completes, so a mid-`bilstm` kill leaves `tft`'s already-
written entry intact and `bilstm` correctly still "needs training" next
cycle — no corruption), but worth splitting into two separate
`--model tft` / `--model bilstm` script-map entries or raising the
timeout if the real full run turns out to need it.

**Tests**: `tests/unit/test_train_deep_models_registry.py` (7 tests —
`_update_registry`'s write/no-op/merge/overwrite behavior, plus
`_train_tft`/`_train_bilstm` calling it with the right model key) and
`tests/unit/test_model_training_script_map.py` (4 tests — tft/bilstm no
longer map to `None`, both resolve via `importlib.util.find_spec` the
same way `_trigger_model_retrain` checks, and both share one module
string for the dedup guarantee).

**Not done**: the actual first-ever training run. Plan (per user
decision): a `--quick` smoke test first (2 epochs, ~30s/model) to
confirm the now-wired pipeline works end-to-end on real
`datastore/features/daily/` parquet, once the currently in-flight
`signal_63d` retrain (A28/A37) finishes and releases the DuckDB write
lock and CPU headroom — then a real `--folds 5` run as an explicitly
separate, approved step before relying on it being auto-picked-up by the
Saturday `model_training` job.

**2026-07-09 follow-up #2 — training tft/bilstm still produces zero
visible output; UI rendering is a separate, unstarted gap.** Traced how
each of the 8 models actually reaches the dashboard:

- `signal_5d`/`signal_21d`/`signal_63d`/`meta_labeler`/`pnd_detector`/
  `hmm_market`: `systems/ml_signal_engine/inference/daily_inference.py`'s
  per-ticker loop calls each model's `.predict()`/`.predict_signals()`
  and writes a `(date, ticker, model_name)` row to `ml_signals`.
  `dashboard/static/ml/js/signal.js`'s "Model Scores" table on the ML
  Signal Deep Dive screen is schema-driven off
  `GET /api/v1/signals/ml/{ticker}/{date}` — it renders one row per
  `model_name` returned, with no per-model allowlist. So `signal_63d`
  (and `signal_21d`) already render today with no further work once
  they have fresh predictions — the one cosmetic gap is `signal.js`'s
  `MODEL_LEGEND` dict only has description text for `signal_5d`/
  `meta_labeler`/`pnd_detector`; `signal_21d`/`signal_63d` rows render
  with no legend line.
- `multibagger`: has its own dedicated screen/endpoint
  (`dashboard/static/ml/js/multibagger.js`,
  `/api/v1/signals/ml/multibagger/{ticker}`), already wired, unaffected
  by any of this.
- **`tft`/`bilstm`: will render nowhere, even after a real training run
  completes.** `daily_inference.py`'s per-ticker loop only ever
  constructs `Signal21DModel`/`Signal63DModel` — it never touches
  TFT/BiLSTM, and this isn't a simple omission to copy-paste around:
  TFT/BiLSTM consume a 63-day windowed *sequence* tensor per ticker
  (built directly from `datastore/features/daily/*.parquet`), not the
  same flat per-day feature row `signal_5d/21d/63d` take, so they can't
  be dropped into that existing loop as-is. There is also a
  `StackingEnsemble` (`systems/ml_signal_engine/models/deep/stacking.py`)
  purpose-built to blend multiple models' outputs into one combined
  score — but `grep -rl "StackingEnsemble\|AdaptiveWeightManager"` across
  `systems/ml_signal_engine/inference/` and `dashboard/` returns nothing:
  it is fully dormant, never invoked from `daily_inference.py` and never
  referenced by any dashboard screen.

Net effect: this session's registry/scheduler wiring makes TFT/BiLSTM
*trainable and tracked*, but a completed training run alone still
produces no dashboard-visible output. Serving them requires a distinct,
not-yet-scoped integration: either (a) a new per-ticker sequence-scoring
pass in `daily_inference.py` that writes `model_name="tft"/"bilstm"`
`ml_signals` rows directly (mirroring the existing pattern), or (b)
wiring `StackingEnsemble` in to blend TFT/BiLSTM into an existing
signal's score rather than exposing them as standalone rows. Not
implemented — needs a decision on which approach before either is built,
and should wait until the smoke test above confirms training itself
works.

### A39 — `ExitSignalModel` will crash daily inference the first time a position is open

Found during a 2026-07-09 audit of every model in `systems/ml_signal_engine/
models/` against 5 questions: is it trained, is it scheduled, is it
rendered, is its feature engineering complete, are its features used
somewhere. `exit_signal` failed the first question in the worst possible
way — not "never trained, harmlessly idle" like tft/bilstm, but "never
trained, and the code that depends on it doesn't know that."

`systems/ml_signal_engine/inference/daily_inference.py::_step_exit` is
called unconditionally from `run_daily_inference` whenever
`position_context` is non-empty, and does
`_load_model(ExitSignalModel, EXIT_MODEL_NAME, models_dir)` with no
try/except of its own — the caller wraps it in
`try: ... except Exception as exc: log_pipeline_step(...); raise`, i.e.
the exception is logged and then **re-raised**, halting the entire day's
`run_models` step (and therefore `write_signals`/`sanity_check`
downstream). `find datastore/models -iname "*exit_signal*"` returns
nothing — no `.pkl` has ever been saved for this model, and there is no
standalone trainer for it anywhere in the codebase (unlike every other
model, which has either `train_all_phase1.py`, `retrain_phase2.py`,
`train_multibagger.py`, or now `train_deep_models.py`).

This is currently silent only by accident: `position_context` has always
been empty because paper trading has had 0 real trading days (see
project memory). `scripts/run_daily_paper_trading.py` already knows this
model doesn't exist —
`_load_exit_policy()` defaults to `RuleBasedExitPolicy` and its `"model"`
branch explicitly raises a friendly `FileNotFoundError` with "Run with
--exit-policy rule_based until enough closed trades accumulate to train
one." `daily_inference.py`'s own inference loop was never given the same
guard or fallback.

**Fixed 2026-07-09** — added `_load_exit_model(models_dir)` in
`daily_inference.py`, which checks whether `{EXIT_MODEL_NAME}_current.pkl`
exists before loading: if present, loads the real `ExitSignalModel` as
before; if absent, logs a warning and returns `RuleBasedExitPolicy()` (the
exact same no-arg, drop-in `predict_full()` implementation
`run_daily_paper_trading.py::_load_exit_policy()` already falls back to).
`_step_exit` now calls `_load_exit_model` instead of the generic
`_load_model` it previously shared with pnd/signal/longer-horizon models
(those are unaffected — still use `_load_model` unchanged). Covered by
5 new tests in `tests/unit/test_daily_inference_exit_fallback.py`:
`_load_exit_model` returns a `RuleBasedExitPolicy` instance and never
raises `FileNotFoundError` when no model file exists, still loads a real
model when one is present (via a monkeypatched fake), and an end-to-end
`_step_exit` call with a populated `position_context` and no trained
model completes without raising (the exact scenario that used to halt
`run_daily_inference`).

Not done this session: a real trainer for `ExitSignalModel` still doesn't
exist — it needs closed-trade outcomes to learn from, which don't exist
yet either (0 real paper-trading days, see project memory). Until then,
`RuleBasedExitPolicy` isn't a stopgap fallback, it's the only viable exit
policy in production — this fix makes that explicit and safe instead of
accidental (previously silent only because `position_context` had always
been empty).

### A40 — `StackingEnsemble` is dormant; its one real run died silently

`systems/ml_signal_engine/models/deep/stacking.py`'s `StackingEnsemble`/
`StackingMetaLearner` has a real `train()` method and a real driver
script (`scripts/train_stacking.py`), but
`grep -rl "StackingEnsemble\|StackingMetaLearner\|AdaptiveWeightManager"`
across `systems/ml_signal_engine/inference/` and `dashboard/` returns
nothing outside `stacking.py` and `train_stacking.py` themselves — it is
never invoked from the daily pipeline and never referenced by any
dashboard screen.

`logs/train_stacking.log` shows one real attempt, 2026-07-02: it
resolved the universe, trained/loaded P&D, ran `BacktestEngine` OOF
collection for `signal_5d`/`signal_21d`/`signal_63d` (each taking
5-35 minutes), then at `09:10:58` began "Scoring TFT (M-11) on the
aligned OOF (date, ticker) rows..." and loaded 3 TFT fold checkpoints
(`tft_signal_21d_v20260701_fold{0,1,2}.pt`) — and the log simply stops
there. No error, no traceback, no "complete," no stacking weights file
anywhere under `datastore/models/`. Whether it was killed by something
external (OOM, machine sleep/reboot, manual kill) or hung indefinitely
is unknown from the log alone — nothing in
`scripts/train_stacking.py` catches and logs a clean failure reason, so
a silent kill and a silent hang look identical after the fact.

**Not fixed this session.** Needs: (1) figure out why the 2026-07-02 run
died (check `dmesg`/OOM logs from that window if still available, or
just re-run with more logging around the TFT/BiLSTM scoring step); (2)
decide whether `StackingEnsemble` is still wanted at all given A39/A42's
findings about how little of the model roster is actually proven to
work end-to-end — building an ensemble on top of `signal_5d/21d/63d` +
TFT + BiLSTM only makes sense once each input model has a real,
completed, registered training run to draw from.

### A41 — Orphaned pre-A38 TFT/BiLSTM checkpoints outside the current save convention

`datastore/models/*.pt` (flat, not under `datastore/models/tft/` or
`datastore/models/bilstm/`) contains real fold checkpoints predating
A38's registry wiring:
```
bilstm_signal_21d_v20260630_fold0.pt   (Jul 1 03:21)
bilstm_signal_21d_v20260701_fold{0,1,2}.pt   (Jul 1, 12:09-12:38)
tft_signal_21d_v20260624_fold0.pt      (Jun 24 18:52)
tft_signal_21d_v20260630_fold0.pt      (Jul 1 04:34)
tft_signal_21d_v20260701_fold{0,1,2}.pt   (Jul 1, 10:55-11:54)
```
These prove TFT/BiLSTM training has worked before (contradicts this
session's earlier working assumption that they'd "never been trained" —
that was a registry.json-only check; these files predate registry
tracking entirely). They were never consumed by anything except A40's
dead `train_stacking.py` run, and A38's new `_update_registry()` won't
retroactively register them (it only fires at the end of a fresh
`schedule_overnight_training()` call).

**Not fixed this session** — needs a decision before the A38 smoke test
runs: delete these as stale/unregistered, or treat the most recent set
(`*_v20260701_fold{0,1,2}.pt`) as a legitimate prior training round and
manually backfill a `registry.json` entry for it so
`_execute_model_training_job`'s overdue-check has an accurate
`last_trained_date` (2026-07-01, not "never") the first time it
considers `tft`/`bilstm` post-A38.

### A42 — Verify TFT/BiLSTM's actual feature usage; decide fate of categories no serving model touches

Closes the loop A38 opened: TFT/BiLSTM take *every* column in
`datastore/features/daily/*.parquet` by construction (no allowlist —
see A38), which nominally means all 16 `ALL_FEATURE_COLUMNS` categories
(`features/matrix_builder.py`) feed them. But this has never been
empirically confirmed — no completed training run exists to check
(A38/A41), and "the code takes all columns" is not the same claim as
"the model learns anything useful from all of them." Separately, of the
16 categories, most have **no other production ML consumer**: `signal_
5d/21d/63d`, `hmm_market`, `pnd_detector`, `meta_labeler` all compute
their own features in-process directly from OHLCV/DB (confirmed A28(f))
and never read the parquet at all. The parquet's only confirmed
production consumers today are the Technical screener
(`systems/technical_analysis/screener/engine.py`), the generic
feature-browsing API (`datastore/api/routers/features.py`),
`score_multibagger.py`'s fallback path, and TFT/BiLSTM.

**New feature requested (2026-07-09, explicit user ask)**: once A38's
training pipeline is proven (smoke test, then a real run), check TFT/
BiLSTM's actual learned feature importance/attention weights per
category (`advanced_technical`, `pattern_scores`, `real_economy_macro`,
`deep_forensic`, `calendar`, `intraday`, `macro`, `hmm_regime`,
`pnd`, `multibagger`, plus the ones already confirmed used by Phase1/2:
`core_technical`, `corporate_action`, `fno`, `fundamental`,
`governance`, `mf_holdings`) — determine which categories TFT/BiLSTM
are actually drawing signal from versus carrying as dead weight.

For any category confirmed unused by *every* model (TFT/BiLSTM
included), the user has asked for two build options to be scoped rather
than just "stop computing it":
1. **Build a new dedicated model that consumes it and feeds A40's
   ensemble** — e.g. a model specifically trained on
   `real_economy_macro`/`pattern_scores`/`deep_forensic` columns,
   scored alongside `signal_5d`/TFT/BiLSTM as another `StackingEnsemble`
   input.
2. **Build it as an independent "AlphaLens_Technical" model/screen**
   instead — not blended into the ensemble at all, standing alone the
   way `multibagger` and the Forensic scores already do, with its own
   dashboard surface.

Not scoped further yet — genuinely needs A38 (a working training run)
and A40 (a decision on whether the ensemble is still wanted) resolved
first, since "which categories are actually dead" can't be answered
without a completed TFT/BiLSTM run to inspect, and "build a new model to
feed the ensemble" presumes the ensemble itself is worth investing in.

### A43 — Daily Insights / ML signal screens don't surface A30's per-signal backfilled-vs-live flag

A30 (this session, 2026-07-09) added `is_backfill` to
`pipeline_checkpoints` and surfaced it on the Ops dashboard's Steps/Runs
tables. It does not reach the Daily Insights or ML signal screens
(`dashboard/static/ml/*`), which read `ml_signals` rows directly — a user
looking at a specific stock's signal there still has no cue that it was
computed days after its own trading day rather than live. Needs a join
from `ml_signals.date` back to `pipeline_checkpoints.date` (matching on
the `write_signals` step's `is_backfill` value for that date) at the API
layer, then a small badge on the signal card/table — same visual pattern
as A30's Ops badges. Not scoped further — left as backlog since it wasn't
part of A30's original Ops-panel-focused ask.

---

## Technical

### T1 — Docstring says "76 core" indicators, code computes 70
`datastore/api/routers/technical.py` docstring claims "76 core" technical
indicators; `CORE_TECHNICAL_FEATURES` in `features/technical.py` (asserted
`== 70`) is the actual, verified count. Needs the docstring corrected to
match code (or vice versa, if 6 indicators were genuinely intended but
never added).

### T2 — Phantom equity trading data on real holidays: root cause dig deeper?
Root cause of the 2026-07-05 data-corruption investigation was fixed at two
layers: (1) `config/nse_holidays.py` rebuilt with a full 2005-2026 holiday
calendar from authoritative sources, and (2) `ingestion/scrapers/bhavcopy.py`
now validates the raw `DATE1` column against the requested date and raises
if NSE's archive serves a stale/duplicate file. This closes the known gap,
but whether the underlying NSE archive quirk (HTTP 200 with stale data
instead of 404) needs further defensive investigation elsewhere in the
ingestion pipeline (e.g. other scrapers hitting NSE's archive with a
similar date-request pattern) was raised as an open question and not yet
confirmed or declined by the user.

### T3 — No charting library on the Technical > Chart screen
`dashboard/static/technical/js/chart.js` itself documents this: no
candlestick/time-series charting library exists in this zero-build-tooling
app, so `chart.html` only shows a snapshot panel (latest price + curated
indicator/pattern list), not an actual chart. A real chart would need a
vendored charting library (the app avoids external CDN dependencies) or a
lightweight custom canvas/SVG renderer.

### T4 — Watchlist screen wiring status unresolved
`dashboard/static/technical/watchlist.html` was flagged in the original
screen-by-screen walkthrough as the newest of the six Technical screens;
its backing endpoint/wiring status was not conclusively verified before
the session moved on to the indicator-persistence and data-integrity
investigation. Needs a follow-up truthful-mode pass to confirm whether it
is fully wired to a real backend endpoint or still partial/stubbed.

### T5 — 18 "advanced" TA features computed but unused by any ML training pipeline

2026-07-08 audit (BuildLog.md) confirmed `features/advanced_technical.py`'s
`ADVANCED_TECHNICAL_FEATURES` (wavelet_trend/noise/energy_ratio/regime_signal,
hurst_exp_21d/63d, approx/sample/permutation/spectral entropy,
fractal_dimension, fracdiff_d_optimal/price/volume, lyapunov_exponent_proxy,
rqa_rec_rate, time_series_complexity, nonlinear_trend_strength — 18 cols) are
computed and persisted into the feature matrix on every run via
`features/matrix_builder.py`, but neither `train_all_phase1.py` nor
`retrain_phase2.py` includes them in the training feature set (both use
`CORE_TECHNICAL_FEATURES`, 70 cols, by explicit design per
`train_all_phase1.py:20-23`'s docstring). No train/predict mismatch results
(inference re-derives columns from each model's trained `_feature_names`),
but the compute and storage cost of these 18 features is currently pure
overhead from the model's perspective. Needs a decision: wire them into
Phase 2 (or a future phase) training, or stop computing them to save
pipeline run-time/storage.

**Superseded 2026-07-09 — see A38 (Architectural).** This framing is
only accurate for `train_all_phase1.py`/`retrain_phase2.py`'s 7 models.
`tft`/`bilstm` already consume all 18 features (no allowlist — they take
every parquet column), matching `advanced_technical.py`'s own
"Consumers: ... models/deep" docstring; the real gap is that neither
model has ever been trained at all (`registry.json`: both `None`), not
that the features are unwired.

---

## Fundamental

Gaps surfaced during the 2026-07-05 truthful-mode walkthrough of the 6
Fundamental dashboard screens (dashboard/peers/sector/screener/thesis/
management) against `features/fundamental*.py`, `systems/fundamental_analysis/`,
`ingestion/fundamentals/`, and `datastore/api/routers/fundamentals.py`.

### F1/F2 — Hardcoded empty-state sub-panels (Sector, Management)
`sector.js:16-19`'s "Sector-Unique Metrics" panel and `management.js:20-23`'s
"Related-Party Transactions" panel both call `renderEmptyState(...)`
unconditionally, before any network request — not a loading/error state,
a permanent stub. Matches `alphalens_docs/CLAUDE.md:492`'s documented "one
empty-stated sub-panel each" claim exactly; confirmed accurate, not stale.

### F3 — `systems/fundamental_analysis/*` are dead stub packages
All six subpackages (`growth`, `management`, `peers`, `quality`, `sector`,
`thesis`) are 8-line docstrings with no functions, and nothing imports them
(`grep -rn "import systems.fundamental_analysis"` returns zero hits). Every
real composite score, peer-selection, and quality/growth calc that was
"meant" to live there was instead built directly in
`features/fundamental_composites.py` (which says as much in its own
docstring). Decide: delete the empty directories, or actually backfill them
and move the composites logic in for real module boundaries.

### F4 — Thesis Builder has no PDF export
Zero matches for "PDF" or `print(` in `thesis.html`/`thesis.js`. The "Build"
button only renders templated Strengths/Risks text from real z-score
threshold crossings — no export/download path of any kind exists today.

### F5 — `ingest_external_fundamentals.py` doesn't actually write
The script's write branch only calls `logger.info("Writing: ...")` and
increments a counter; its own comment (~line 124) admits
`DataStoreClient.write_fundamentals` was never implemented. Anyone treating
this script as a working ingestion path is being misled by its log output.

### F6 — Valuation Accuracy screen has zero backend/frontend
Needs an endpoint that backtests past `valuation_signals` predictions
against realized price outcomes, plus a real `accuracy.js`. Verified
2026-07-05: `accuracy.html` calls `renderEmptyState` with literal "Not
yet built.", no fetch call exists.

### Verified NOT a gap (correcting an earlier draft claim)
An earlier pass of this walkthrough claimed Peer Comparison's market-cap-
proximity ranking (`fundamental_composites.py:102-141`) was a dead no-op
because `market_cap_cr` was "hardcoded to 0 universe-wide." Checked directly
against `config/nifty500_universe.csv`: only 363/2,317 tickers (16%) are
still at 0 — the other 1,954 (84%) have real scraped values via
`config/build_universe.py`'s `recompute_market_cap()` /
`backfill_market_cap_from_screener_cache()`. Peer ranking by market-cap
proximity is real for the large majority of the universe. No issue filed.

---

## Big Investors

Gaps surfaced during the 2026-07-08 Big Investor Activity build session
(Trendlyne bulk/block-deal backfill, superstar-investor tracking).

### BI1 — Daily NSE/BSE bulk/block-deal history is still only 1 real day deep for non-superstar participants

The 2026-07-08 session's Trendlyne backfill (`scripts/
backfill_bulk_deals_trendlyne.py`) only backfills deals belonging to the
~62 named `SUPERSTAR_INVESTORS` — real trade dates/prices going back to
2010 for *those* families. `large_deals`' regular daily NSE/BSE ingestion
path (`ingestion/scrapers/large_deals.py`) still only has one real date
loaded (today), since NSE/BSE's live endpoints don't offer a historical
date range (see that module's docstring) and no other historical-backfill
source has been found for bulk/block deals from participants outside the
superstar list. The Big Investor Entries/Exits table will therefore look
much deeper for a superstar-investor family than for anyone else. Worth
searching for a general historical bulk/block-deal archive (NSE's own
archives, a paid data vendor, or another public site) if broader
non-superstar coverage becomes a priority.

### BI2 — Non-equity Trendlyne deals (InvITs, REITs, etc.) are silently dropped from the bulk-deal backfill

`TrendlyneScraper.export_bulk_deals_history` drops any deal row whose
`company_name` doesn't match a `stock_master` ticker (same per-holding
isolation as the existing holdings export). For Rakesh Jhunjhunwala and
Associates specifically, only 73 of 131 scraped deals matched (the rest
were instruments like NDR InvIT Trust that fall outside the equity
universe this DB tracks). This is correct behavior for an equities
dashboard, but if InvIT/REIT-level big-investor activity ever becomes
in-scope, `stock_master`/the ticker-resolution map would need extending
first — not attempted this session.

### BI3 — Trendlyne bulk-block-deals page pagination not verified across all 62 investors

`_parse_bulk_block_deals_table` assumes the entire deal history is
server-rendered in one page load with no pagination/AJAX — verified true
for one investor (Rakesh Jhunjhunwala and Associates: 131 rows,
2010-02-02 through 2026-05-14, one fetch). Not verified for the other 61
superstar investors, some of whom may have a longer or more active
trading history that Trendlyne truncates or paginates differently. If a
future backfill re-run for a given investor looks suspiciously capped
(e.g. exactly 100 or 200 rows, or missing known-old deals), check that
investor's raw HTML directly for a pagination control before trusting the
result as complete.

### BI4 — No automated test coverage for Big Investor Activity changes

Nothing in `tests/` was added or updated for: `_position_and_
wac_asof`'s merged bulk-deal/Trendlyne replay logic (partial-sale
true-up, undisclosed-purchase cost estimate, `_MATERIALITY_HOLDING_PCT`
filtering), the new `_parse_bulk_block_deals_table` parser and
`backfill_bulk_deals_history`'s dedup anti-join, or the MF Holdings
movers' new `scheme_count_change` computation. All changes were verified
manually (live curl against the running dev server, real-data spot
checks) rather than via `tests/unit/` or `tests/integration/` — worth
adding real coverage before this logic is trusted unattended, per this
repo's no-stub/synthetic-data testing policy (would need a small seeded
DuckDB fixture, not mocks).

### BI5 — `holding_pct_of_company` / shares-outstanding estimate is a market-cap/price back-derivation, not a real share count

`_position_row_to_dict` (`datastore/api/routers/big_investors.py`)
computes `shares_outstanding_est = market_cap_cr * 1e7 / cmp` rather than
reading `fundamentals.shares_outstanding` directly, since that field is
PIT-gated per fiscal quarter and only ~9% populated project-wide. This is
a reasonable approximation but assumes `stock_master.market_cap_cr` is
itself freshly derived from a recent price × real share count — not
verified this session. Worth cross-checking against real
`fundamentals.shares_outstanding` for a sample of tickers where both
exist, to quantify how far the estimate can drift.

### BI6 — "unmapped:" family ↔ Trendlyne holder-name matching is a string-normalization heuristic, not a real identity match

`_position_and_wac_asof` matches a `bulk_deal_positions.family_id` of the
form `"unmapped:<normalized name>"` to Trendlyne `public_shareholders.
holder_name` rows by re-normalizing the holder name with the same
`normalize_client_name` used to build the `unmapped:` id — exact-string
matching only, no fuzzy/alias handling. A family whose bulk-deal client
name and Trendlyne holder name differ even slightly (abbreviation,
punctuation, a missing "AND ASSOCIATES" suffix) will silently fail to
match and lose the Trendlyne cross-check for that family/ticker. This
echoes `bulk_deal_reconciliation.py`'s existing note that a corrected gap
lining up with an "unmapped:" client's trades should eventually grow the
`investor_family` seed automatically — not implemented, still manual.

---

## Damodaran

### D1 — 3 failing `test_damodaran.py` sector-alias tests
`test_financial_services_banking/nbfc/insurance` expect
`"Banking"/"NBFC"/"Insurance"` to classify as `FINANCIAL_SERVICES`, but the
classifier only matches the literal `"Financial Services"`. Stale test
expectations vs. the 2026-07-04 classifier fix — decide whether to alias
sector strings or update the tests.

### D2 — No router-level tests for `datastore/api/routers/valuation.py`
Only the underlying `systems/damodaran_valuation/` library is tested via
`test_damodaran.py`. Endpoint wiring (param validation, error responses,
peer-group edge cases) is currently unverified by tests.

---

## Forensic

Gaps surfaced during the 2026-07-05 truthful-mode walkthrough of the 7
Forensic dashboard screens (dashboard/redflag/benford/cashflow/heatmap/
report/universe) against `systems/ml_signal_engine/models/forensic/classical_scores.py`,
`features/forensic_classical.py`, and `datastore/api/routers/forensic.py`.
`alphalens_docs/CLAUDE.md:495` labels this app flatly "Real" — verified
that overstates it; closer to Fundamental's "4 Real, 2 Partial" framing.

### FO1 — Altman Z-Score structurally NaN in production
Formula itself is correct (`classical_scores.py:207-213`, verified against
published 1.2/1.4/3.3/0.6/1.0 weights), but `mktcap` (book equity proxy),
`re` (book equity proxy), and `ebit` (EBITDA proxy) are all substitutes for
real inputs (`forensic_classical.py:505-512`), and `current_assets`/
`current_liabilities` are never populated by the live scraper — any NaN
term zeroes the whole score (`classical_scores.py:202`). Needs real market
cap, retained earnings, EBIT, and current assets/liabilities ingested.

### FO2 — Dechow F-Score always NaN
`forensic_classical.py:541` calls `dechow_f_score({})` — an empty dict,
unconditionally, every time. The formula (`classical_scores.py:365-380`) is
correct but has never received a real input in production. Needs
employee-count, share-issuance, and book-to-market data — no existing
source ingests these today.

### FO3 — Beneish M-Score's AQI term permanently NaN
`current_assets`/PPE columns exist in the schema but the live scraper
doesn't populate them (documented gap, `forensic_classical.py:37-49`), so
AQI is always NaN (`:176-184`) and drags into the overall M-Score.

### FO4 — Forensic "Group C" fields hardcoded NaN
`unbilled_revenue_ratio`, `cash_revenue_ratio`, `revenue_vs_gst_proxy`, and
`revenue_concentration` are hardcoded `np.nan` (`forensic_classical.py:356-359`),
never computed. Needs a data-source decision (GST filing data, revenue-
concentration inputs) before this can even be scoped.

### FO5 — Benford's Law screen exposes far less than it computes
`benford_analysis()` (`classical_scores.py:441-502`) does real math —
`scipy.stats.chisquare` (`:489`) plus a genuine per-digit MAD (`:491`) — but
only `revenue` is ever passed in (`forensic_classical.py:545-546`; no
expense/other line items), and only the single aggregate `benford_mad`
float reaches the API (`datastore/api/routers/forensic.py:56`) and UI
(`benford.js:52` — the frontend's own comment admits the chi-square result
and per-digit histogram are computed internally but never persisted or
exposed). Needs schema + API extended to expose the full distribution, and
more financial series wired into `series_dict`.

### FO6 — Investigation Report has no PDF/report-builder backend
`report.js` fetches one ticker's forensic row and fills an HTML template;
"export" is literally `window.print()` (`report.js:38`). No server-side PDF
generation or guided-report endpoint exists in
`datastore/api/routers/forensic.py`. Same shape as F4's Thesis Builder gap
— needs a PDF lib (reportlab/weasyprint) + export endpoint if this is
actually wanted as a real deliverable.

### FO7 — Universe Scan has no on-demand trigger
`universe.js` only reads the last offline batch's rows via `/summary`
(`:9`) and `/flagged` (`:31`) — there's no "run scan now" button or
endpoint. The real full-universe iteration exists only as the standalone
CLI `score_forensic.py`, which does correctly load the full, non-hardcoded
`config/nifty500_universe.csv` via `config/universe.py:get_tickers()` and
loop per-ticker — it's just unreachable from the dashboard/API. If
`score_forensic.py` has never been run, `/summary` returns
`available: False` and the whole screen is an empty state. Needs an
endpoint wrapping that script's loop (with the same batching/memory
discipline as A28(c)'s chunking fix) plus a UI trigger.

### FO8 — Several forensic/governance columns remain unavailable even from NSE XBRL

`contingent_liability_ratio`/`subsidiary_count`/`loans_to_related`/
`capex_to_assets`/`intangibles_growth`/`off_balance_sheet_proxy`/
`noncash_assets_ratio`. The 2026-07-08 NSE XBRL Integrated Filing pipeline
(BuildLog.md) resolved `goodwill_ratio`/`cwip_ratio`/many other balance-
sheet fields from the same real regulatory filing, but live-verified that
"Disclosure of notes on assets and liabilities" — where contingent
liabilities, subsidiary counts, and related-party loan amounts would
appear — renders as freeform "Textual Information" in NSE's own template,
not a structured numeric field. Same gap as Screener/Trendlyne, not
resolved by switching sources. Would need actual NLP/text extraction
against unversioned freeform disclosure text to close, a materially
different (and much more fragile) effort than the rest of this session's
JSON/structured-HTML work.

### FO9 — `altman_z` still NaN for a real subset of tickers

Even after 2026-07-08's `shares_outstanding` fix (BuildLog.md), `altman_z`
stays NaN when:
- The ticker has no NSE Integrated Filing at all yet (pre-FY2023-24
  quarters, or an entity type/exchange segment NSE's regime doesn't cover).
- The `shares_outstanding` plausibility check (`ingestion/scrapers/
  nse_xbrl_financials.py::_parse_shares_outstanding`) finds neither the
  Lakh-scaled nor raw-rupee interpretation plausible for that filing —
  returns `None` rather than guessing, per this session's no-fabrication
  discipline, but means `market_cap` (and therefore `altman_z`'s X4 term)
  stays unavailable for those specific filings.
No further investigation done on how large this remaining gap is at the
full-universe level — the 2026-07-08 session's DB-wide integrity sweep
only checked for *implausibly large* values, not a count of genuinely
`NULL` `shares_outstanding` rows after the fix.

---

## Corporate Announcements

### CA1 — Triage 174 likely-missing-split tickers vs Fyers, backfill `corporate_actions` — RESOLVED 2026-07-06 (70/174 fixed, rest reclassified)
The 12-date x full-universe Fyers comparison (`full_day_comparison_20260705.csv`,
28,609 rows) found 326 tickers with >15% mismatch between our `adj_close`
and Fyers' close on the same date. Split by coefficient-of-variation of the
mismatch ratio across the 12 dates: **174 tickers with cv<0.15** (near-constant
ratio — the signature of one missing split/bonus not yet in `corporate_actions`)
saved to `followup_missing_splits_20260705.csv` (`likely_missing_split=True`).

**Method used** (`scripts/detect_missing_split_reconstruction.py`): for each
ticker, pulled full Fyers history, found the date where our/Fyers close ratio
jumps, derived the implied price factor — then, critically, cross-checked
every candidate against **NSE's own live corporate-actions API**
(`ingestion/scrapers/corporate_actions.py`, the same source this pipeline
already scrapes) rather than trusting the implied ratio or screener.in. NSE
confirmation before any insert caught several cases where the Fyers-implied
factor was flatly wrong (dividends, demergers, non-equity bonus debentures
masquerading as price jumps).

**Result: 70 of 174 tickers fixed**, each backed by an NSE-confirmed
ex_date/ratio, inserted into `corporate_actions`, run through
`adjust_for_corporate_actions()`, and re-confirmed via the Fyers validator
(`corporate_actions_validation`). DB-wide status is now
`confirmed=788, mismatch=76, no_fyers_data=2` (the 76 mismatches are
unchanged from before this pass — see below).

**Four real parser bugs found and fixed in `_parse_purpose()`
(`ingestion/scrapers/corporate_actions.py`)** during triage, all now fixed
in code (not just patched in the DB), so future NSE ingestion won't repeat
them:
1. The face-value-split regex failed on `"From Rs 10/- Per Share To Rs 5/-
   Per Share"` (text between the two values broke the pattern) — ~15 tickers.
2. `DIVIDEND` was checked before `SPLIT`/`BONUS`, so any compound purpose
   spelling out the full word "Dividend" alongside a real bonus/split (e.g.
   TCS's `"Bonus 1:1 /Dividend- Rs 29 Per Share"`) silently dropped the
   price-relevant action. Reordered so SPLIT/BONUS/RIGHTS are checked first.
3. Compound single-string purposes with **both** a split and a bonus (e.g.
   `"Bonus 1:2 And Face Value Split From Rs.10 To Rs.2"`) only ever captured
   one action type — not fixed in the parser (would need multi-action
   support), handled as manual two-row inserts for the 3 tickers found
   (RAJTV, HINDCOMPOS, INDOCO).
4. The `AGM` check runs first and swallows any compound purpose containing
   "General Meeting", e.g. NAUKRI's `"...Dividend-Re.1/- Per
   Share/Bonus 1:1"` — a real bonus, discarded. Not restructured (AGM
   priority is intentional for the common case); handled case-by-case.

**Left untouched, split into categories** (104 tickers):
- **16 same-date collisions**: ticker already had a `corporate_actions` row
  at the exact candidate date with a *different* ratio than what NSE/the
  empirical factor suggested (from earlier "Inferred SPLIT from
  price-discontinuity scan" work this session, or genuinely ambiguous
  compound events). Needs manual reconciliation, not a blind overwrite:
  BANCOINDIA, JAYBARMARU, HERITGFOOD, GPTINFRA, GULPOLY, IMPAL, FILATEX,
  INDIANHUME, JINDWORLD, JAMNAAUTO, KABRAEXTRU, NESCO, LGBBROSLTD,
  MUNJALAU, PLASTIBLEN, AMRUTANJAN, TPLPLASTEH.
- **4 mistyped pre-existing rows** hit by bug #2/#4 above but left
  as-is per explicit choice not to touch already-approved history this
  session: HINDPETRO (2017-07-11), JINDALSTEL (2009-09-14), WIPRO
  (2010-06-15), ZENSARTECH (2010-07-21). Low-risk future cleanup: re-run
  `download_corporate_actions()` for these exact dates with the fixed
  parser and `UPDATE` the stale row.
- **~28 tickers with no NSE match at the candidate date**: SKFINDIA,
  ADVANIHOTR, KAUSHALYA, NAGREEKEXP, TIMETECHNO, SURYAROSNI, TCI, JBMA,
  ORIENTALTL, AHLEAST, KANANIIND, PGIL, PTL, MAZDA, MINDTECK, DTIL, IIFL,
  JYOTHYLAB, KSB, ADROITINFO, DCMSRIND, GEEKAYWIRE, NCC, NDL, NDRAUTO,
  RAMCOIND, SUPRAJIT, WEBELSOLAR — the Fyers jump is real but NSE's API
  shows nothing that day; needs a wider date search or a different source.
- **~30 tickers reclassified as not a CA1 case at all**: dividend-only
  dates, demergers (SIEMENS), a scheme of arrangement (SURANAT&P), and a
  non-equity bonus debenture (BRITANNIA) — these explain the original
  Fyers mismatch but aren't a missing split/bonus, so no `corporate_actions`
  row was needed. Several (PETRONET, REDINGTON, GRAPHITE, BAJAJ-AUTO,
  GLAXO, MARICO, CASTROLIND, CRISIL, etc.) look like CA3's
  dividend-adjustment-convention gap instead.
- NEULANDLAB: the 2014-08-13 RIGHTS row NSE confirmed already exists in
  `corporate_actions` with the right ratio — not missing, just still
  subject to the general RIGHTS-has-no-adjuster-formula limitation.

**RIGHTS handling**: 8 RIGHTS actions surfaced during this triage (NDTV,
PATELENG, ASHIMASYN, MAHLIFE, ASAL, BAJAJHIND, MURUDCERA, MAHAPEXLTD, VSSL —
9 total incl. one from a prior pass) — all inserted into `corporate_actions`
and fixed via the same empirical `ratio_post/ratio_pre` rescale documented in
`price_adjuster.py` from the 2026-07-05 RIGHTS work, since `_action_factors()`
still has no formula for RIGHTS.

The unchanged `mismatch=76` count is the original out-of-scope SPLIT/BONUS
data-quality set from the initial corporate-actions audit (JINDWORLD,
KANSAINER, FCL, BLKASHYAP, KELLTONTEC, etc.) — confirmed still untouched by
this pass, consistent with the collision list above (several of the same
tickers appear in both lists).

### CA2 — KANSAINER/AJOONI non-monotonic price-ratio investigation
Both tickers showed a mismatch-ratio pattern across the 12 comparison dates
that isn't a constant offset (ruling out a single missing split) and isn't
random noise either — flagged during the CA1 analysis but not enough
signal yet to hypothesize a cause (candidates: multiple overlapping
corporate actions, a ticker-symbol change/reuse conflating two different
underlying instruments, or a demerger not modeled by the current three
action types). Needs a dedicated look at each ticker's full corporate-action
history and raw Fyers series before attempting any fix.

### CA3 — Assess 152 higher-cv Fyers-mismatch tickers
The other half of the 326-ticker mismatch list from CA1 (cv≥0.15, i.e. the
mismatch ratio drifts across dates rather than staying constant) is
suspected to be explained by this project's existing known dividend-
adjustment convention gap rather than a missing corporate action, but this
hasn't been verified — needs spot-checking a sample against Fyers before
concluding no action is needed.

### CA4 — Corporate-action validation tracking — ✅ IMPLEMENTED 2026-07-05
Built as `corporate_actions_validation` (keyed on `ticker, ex_date,
action_type`; columns `validation_status`, `needs_retrain`, `pct_diff`,
`fyers_validated_at`) plus `scripts/validate_corporate_actions_fyers.py`,
which checks ratio-consistency (`our_close/fyers_close` before vs after
`ex_date`, since Fyers' `history` endpoint returns already-adjusted prices —
a raw jump is not a valid signal there). Resumable and budget-capped.
As of 2026-07-08 all 967 rows are processed: 859 confirmed, 77
`needs_retrain=TRUE` (mismatch), 29 insufficient_window, 2 no_fyers_data —
see BuildLog.md's 2026-07-05/08 entry for the full needs_retrain ticker
list. **Still open**: this 77-ticker list has not yet been cross-referenced
against CA1/CA2/CA3's own mismatch lists to produce one reconciled retrain
scope; and the table itself is only in the live DB, not yet added to
`datastore/schema/create_normalised.py` (a rebuild-from-scratch would
silently lose it).

### CA5 — Corporate Announcements "insider" category is an approximation

`ingestion/scrapers/nse_corporate_announcements.py` (built 2026-07-08,
BuildLog.md) maps NSE's real `desc` categories to a coarse taxonomy. No
dedicated NSE "insider trading disclosure" endpoint was found live (only
`/api/corporate-announcements` exists; a plausible `/api/corporate-
insider-trading` guess 404'd) — the `insider` category is approximated by
including `Trading Plan under PIT` and `Disclosure under SEBI Takeover
Regulations` (SAST/substantial-shareholding-change disclosures, not
narrowly "insider sale"), which is why it's a comparatively high-volume
category (126+ rows in initial testing) relative to the others. If a
narrower "insider sale specifically" signal is needed, this category
needs a real dedicated source, not yet found.

### CA6 — Real NSE endpoints found but not yet built into a pipeline

Found live during the 2026-07-08 session (grepping NSE's own loaded
`corporate-filings.js` bundle for real, undocumented API paths — see
BuildLog.md for the technique) but not chased further, each a real,
confirmed-working lead:
- **Sustainability/BRSR reports**: `api/corporate-bussiness-sustainabilitiy`
  — real XBRL XML filings, live-verified against RELIANCE. Not currently
  needed by any of the 58 originally-audited columns; would be a new,
  standalone dataset if the operator wants it ingested.
- **Rich QIP deal data**: `api/corporate-further-issues-qip` — real issue
  price, dates, allottee counts, dilution %. Currently `qip_dilution_impact`
  is only shallowly touched via the Corporate Announcements `qip` category
  (an announcement exists, but no dilution math is derived from it) — this
  endpoint would give the real number.
- **`mf_pct`/`mf_change_qoq`**: `api/shareholding-patterns-sdd` finds the
  real filing index per ticker, but the actual promoter/FII/DII/MF %
  breakdown is embedded in a linked iXBRL HTML document, not returned as
  JSON directly — needs an XBRL/iXBRL parser, not just a JSON response
  handler like the rest of this session's work. More effort than the
  other items here.
- **Related-party transactions** (`rpt_intensity`): `api/related-party-
  transactions-details` is real but requires a `seqNum` param sourced from
  a separate master/list endpoint that wasn't found this session (a
  `?symbol=` query alone returns "No Data Found" even for tickers with
  real known RPT filings).
- **Governance/board composition** (`board_independence`,
  `audit_qualification_flag` — note: audit qualification is now solved via
  the NSE XBRL pipeline instead, see BuildLog.md — `auditor_change_flag`,
  `cfo_tenure_months`, `director_resignation_count_4q`,
  `whistle_blower_policy`): `api/corporate-governance` is real but requires
  a `recId` param, same undiscovered-secondary-lookup problem as RPT above.

---

## Machine Learning

### ML1 — URGENT: wire multibagger/forensic/21d/63d/conformal into daily scheduler
`score_multibagger.py`/`score_forensic.py` are operator-CLI only, never
scheduled. `daily_inference.py` only scores `signal_5d` daily — 21d/63d/
conformal are trained but never invoked. Add scheduled jobs (multibagger
weekly per its own docstring, forensic likewise), add 21d/63d scoring calls
to the per-ticker loop, add conformal scoring after signal_5d, add an
"as of {date}" staleness indicator matching the existing top_buys pattern.

### ML2 — Daily Insights row fusion
Each model writes its own row keyed by `(date, ticker, model_name)`; Daily
Insights only reads the `signal_5d` row, so Meta/Interval/P&D/Regime always
render empty. Stopgap explanation banner already added (`hub.js`). Real fix:
either read-time join across model rows in `top_buys`/`get_ml_signals`, or a
fused summary row written at the end of `daily_inference.py`'s loop.

### ML3 — SHAP explainability (never implemented)
`shap_top5_json` exists in the schema/API contract but nothing computes it.
Add a `shap.TreeExplainer` step to `daily_inference.py`'s `signal_5d` loop,
top-5 |value| features serialized per ticker/date. Bundle with ML1.

### ML4 — 5-day recommendation history + Sell rationale
Rolling scorecard of `signal_5d`'s last 10 calls (recommended date/price,
expected return, CMP, current return) + explicit Sell Recommendation with
rationale via `RuleBasedExitPolicy`'s `exit_type` vocabulary.

### ML5 — Top Buy Signals — remove 5-cap, sortable columns
Implemented — done.

### ML6 — `mb_tier` relabeled to probability bands
Implemented — done.

### ML7 — View All for 21d/63d
Matches Multibagger's existing `view-all` link. Was blocked on ML1 —
nothing to view until 21d/63d are actually scored daily; now done.

### ML8 — Redesign Signal Deep Dive
Replace the ticker+date lookup form (user: "unable to understand the purpose
of this screen") with a sortable full-universe table for the latest date,
double-click to drill into the existing detail view.

### ML9 — `fmtInt` numeric audit
`fmtMoney()` in `dashboard/static/js/api.js` already does `en-IN` grouping;
raw numeric displays that bypass it don't. Project-wide audit across all 5
apps' JS for a new shared `fmtInt` helper.

### ML10 — Dedicated Exit Urgency page
`exit_urgency`/`exit_type` currently only surface as alert banners and Signal
Deep Dive columns. New screen: all held positions ranked by `exit_urgency`,
`exit_type` shown as stated reason.

### ML11 — Upload-current-portfolio page
Read-only "monitor my real holdings" view: user uploads real (non-paper)
holdings, gets daily signal_5d/exit-urgency/P&D against those tickers,
explicitly excluded from training/backtest data.

### ML12 — Daily sector rotation report — DATA SOURCE UNBLOCKED 2026-07-05/06, report itself not yet built
The step-1 data-source blocker is cleared: NSE's `ind_close_all` indices-
close archive (`https://archives.nseindia.com/content/indices/ind_close_all_{ddmmyyyy}.csv`)
was live-verified as a real, unauthenticated, session-cookie-only endpoint
(same priming pattern as `bhavcopy.py`) that returns same-day OHLC for
Nifty 50, Nifty 500, and every NSE sector index in one CSV. Built and
live in production:
1. **Done** — `ingestion/scrapers/nse_indices.py`: `download_index_ohlcv(date)`,
   filtered to a `TRACKED_INDICES` allowlist of 15 indices (Nifty 50/500 +
   13 sector indices), raw CSV retained under `datastore/raw/nse_indices/`.
2. **Done** — `index_ohlcv` table (`datastore/schema/create_normalised.py`,
   `date, index_name, open/high/low/close/volume`, PK `(date, index_name)`).
3. **Done** — daily scheduled step `download_index_ohlcv` (independent
   downloader, no hard deps, same pattern as `download_fno`/`download_macro`
   — see `checkpoint.py`/`daily_pipeline.py::step_download_index_ohlcv`).
   Confirmed live in the production DB: 15 indices/day landing since
   2026-07-05 via the real scheduled job (a separate session also found and
   fixed a `create_schema()`-never-called bug that initially made this step
   fail with `Catalog Error` — see BuildLog 2026-07-07/08 entry). A one-off
   `scripts/backfill_index_ohlcv.py` was written (day-by-day, since NSE's
   archive has no range/batch endpoint, only one CSV per date) but **not
   yet run** — history before 2026-07-05 is not backfilled.
4. **Not started** — `config/sector_index_map.py` (project's `sector`
   taxonomy → tracked index name; only 8 of ~21 sector values have a
   matching NSE sector index, several — Power, Construction, Textiles,
   Diversified, etc. — have no matching index and would need explicit
   exclusion from the ranking rather than a guessed substitute).
5. **Not started** — `features/sector_rotation.py` (trailing-21d relative
   strength per sector vs Nifty 500, ranked, joined back to
   `ml_signals`/`ml_multibagger` for each in-favor sector's top stocks).
6. **Not started** — API endpoint (`datastore/api/routers/sector_rotation.py`)
   and dashboard screen.

Remaining work (4-6) is a normal build, not blocked on anything.

### ML13 — Multibagger tier change-log
Needs ML1 (multibagger on a real schedule) first, so there's history to
report a "first appeared / last changed" date from. First real scoring run
completed live 2026-07-04 (see ML14's memory-fix note below) — real
`ml_multibagger` data now exists (4,664 rows dated 2026-07-04, e.g.
EIFFL/GOODLUCK/BHAGYANGR at mb_probability ≈0.9999, "10x" tier) to seed
the first data point ahead of the next scheduled Sunday run. A change-log
needs at least 2 runs of history to say anything meaningful, so this item
itself is still not implementable yet — but the blocker (no real data at
all) is now cleared.

**Real production bug found while doing this**: `_execute_multibagger_scoring_job`
(the Sunday 09:30 IST scheduled job wired in ML1) invokes
`python -m systems.ml_signal_engine.inference.score_multibagger` with no
`--limit` — i.e. the exact full-universe (~2,300 tickers), no-cached-model
path that was live-verified to exhaust host memory (see ML14's note). This
was a real risk to the production job, not just a manual-run inconvenience.
Fixed in `score_multibagger.py`'s `main()`: new `--batch-size` flag
(default 300) trains the model once, then scores the universe in bounded
chunks instead of materializing the full ticker list's OHLCV panel at
once.

### ML14 — Multibagger survival-curve labeling fix — IMPLEMENTED 2026-07-04
`load_multibagger_training_data_from_db()`'s old `_duration_months()`
measured the fixed 3-year observation window, not the actual 2x-crossing
date — `duration_months` clustered at 36.5–41.3 for every row, so the RSF
structurally couldn't produce non-trivial survival estimates below ~36
months. Fixed: `load_multibagger_training_data_from_db()` now calls
`build_binary_labels()` over the full historical `(ticker, date)` panel
(many labeled snapshots per ticker, subsampled every `snapshot_stride_days`
after labeling) instead of one row per ticker, with real P&D scores scored
across the panel (`_score_pnd_panel()`) instead of all-NaN. Gained an
optional `tickers=` filter parameter for training-set scope control.

**Memory note (2026-07-04):** the full-universe panel (~2,300 tickers x 5yrs
of OHLCV, with rolling feature computation and PnD panel scoring all held in
memory at once) was large enough to exhaust host memory when exercised by
this repo's own test suite (`tests/unit/test_multibagger.py`,
`test_score_multibagger.py`, `tests/regression/test_multibagger_historical.py`
all called `load_multibagger_training_data_from_db()` with no ticker filter,
i.e. full production scale, inside a test fixture). This was not a leak —
it was a correctly-sized production default being exercised at full scale
by tests that didn't need that scale to validate behavior. Fixed by having
each test pass a real, bounded `tickers=` sample (15 large-caps for the two
unit-test files; a market-cap-diversified ~150-ticker sample plus the
regression tickers themselves for the calibration regression test, since an
18-ticker all-large-cap sample distorted that test's cross-sectional
percentile features enough to push two of three known historical
multibaggers below `REGRESSION_THRESHOLD` — not a real model regression).
Peak RSS for all three files together dropped to ~3.2GB / 31s.

**A second, separate memory bug found running the real full-universe
scoring job (2026-07-04)**: even with the test fixtures fixed above,
running `score_multibagger.py` for real against the full ~2,300-ticker
universe still exhausted host memory — twice, at nearly the same ~7GB RSS
peak regardless of whether `RandomSurvivalForest`'s `n_jobs` was capped or
left at its `-1` default. Root cause wasn't joblib worker duplication —
it was `min_samples_leaf=5` (tuned for the old ~1,138-row training set)
letting 200 trees grow to ~11,000+ leaves each on the new ~57,448-row
training set the ML14 fix produces, each leaf storing its own survival
curve. Fixed by scaling `min_samples_leaf` with the actual training row
count (`max(5, len(X_imputed) // 1000)`) instead of a fixed small
constant. Verified live: the same full-universe run that previously grew
unboundedly to ~7GB and was killed by an external memory monitor completed
training with peak RSS staying under 2GB throughout. First real full-
universe scoring run (with both the batching fix from ML13's note and this
fix) kicked off 2026-07-04 to seed real `ml_multibagger` data under the
corrected labeling.

### ML15 — RuleBasedExitPolicy: volatility-scaled target/stop
Currently flat +15%/-7.5% (intentional bootstrap stand-in — `ExitSignalModel`
needs ≥200 real closed positions, only 2 trading days of paper-trading
history exist). Make the interim policy itself ATR/volatility-scaled, and
add a hit/miss/timeout metric per closed trade for future retraining
evaluation.

### ML16 — Backdated Entry relocation
Feature itself is wanted (documented Gate 7 trade-off), just feels out of
place on the main Paper Trading screen — move under a "Tools"/"Historical
Review" section.

### ML17 — Unified backtest strategy — benchmark data unblocked, benchmark curve + restructuring both still unbuilt
Scope was explicitly split 2026-07-05/06 into two independent pieces:

**(a) Real Nifty benchmark curve for backtests** — data-source blocker
cleared (same `index_ohlcv` table as ML12, live in production since
2026-07-05). Not yet wired into the backtest engine: `_fetch_real_benchmark()`
in `run_phase1_backtest.py` still only reads the 3 ETF-ticker proxies
(`NIFTYBEES`/`NIF100BEES`/`MONIFTY500`) from `ohlcv_adjusted`, and
`backtest/engine.py` computes no benchmark equity curve at all (only
`portfolio.equity_curve` for the strategy itself) — `FoldResult`/
`aggregate`/the 3 scripts' JSON reports have no `benchmark_cagr`/
`benchmark_sharpe`/`excess_return` fields yet. This remains a normal,
unblocked build: read Nifty 500 from `index_ohlcv`, build a parallel
buy-and-hold curve alongside `portfolio.equity_curve` inside
`run_full_backtest`, extend `compute_fold_metrics`'s output.

**(b) "One backtest per horizon model, unified cadence" restructuring**
of the 3 existing `run_phase{1,2,3}_backtest.py` scripts — still
unscoped, independent of (a), not attempted.
