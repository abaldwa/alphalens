
Improvement ideas surfaced during the truthful-mode "Explain-Me Walkthrough"
series (PHASE X, prompts X.0–X.10) in `CLAUDE_CODE_PROMPTS.md`, plus the
2026-07-04 architecture review. Reorganized 2026-07-04 by the code area each
item localizes to, so related items sit together regardless of which prompt
surfaced them. No priority ranking implied by order within a section.

## Status Matrix

Legend: ✅ Done · 🔧 In Progress · ⏳ Not Started · 🚫 Blocked (external dep or explicit design-pass needed)

| # | Item | Area | Status | Blocked On |
|---|---|---|---|---|
| 1 | US market overnight correlation (Nasdaq/Dow/S&P) | Data Layer / Scheduler | ✅ | Folded into #3 (implemented there); GIFT NIFTY still has no known free source |
| 2 | Dollar Index (DXY) feature | Data Layer / Scheduler | ✅ | — |
| 3 | Morning Catch-Up redesign (scope fix + new indicators + PIT timing shift) | Scheduler / Macro / Features | ✅ | — |
| 4 | DataStore API Console (freshness rollup) | Ops / API | ✅ | — |
| 5 | Ops Portal: surface weekend job schedules | Ops / API | ✅ | — |
| 6 | Move `/features`, `/models`, `/pipeline/status` into routers | API | ✅ | — |
| 7 | `SIGNAL_THRESHOLD`/`META_THRESHOLD` as fallback values | ML Inference | ✅ | — |
| 8 | AF-1: DuckDB connection-lifecycle audit + fix | Data Layer | ✅ | — |
| 9 | AF-2: Pipeline output sanity gate | Scheduler / Ops | ✅ | — |
| 10 | AF-3: Feature-store query path partition/index | Data Layer / API | ✅ | — |
| 11 | AF-4: Reconcile/remove orphaned test schema | Data Layer / Tests | ✅ | — |
| 12 | AF-5: Fundamentals range/sanity validation gate | Data Layer / Ingestion | ✅ | — |
| 13 | AF-6: Daily off-machine backup | Ops | ✅ | User signup (Backblaze) still pending — job safely no-ops until then |
| 14 | Wire multibagger/forensic/21d/63d/conformal into daily scheduler | ML Signal Engine | ✅ | — |
| 15 | Daily Insights row fusion (Meta/Interval/P&D/Regime always empty) | ML Signal Engine / API | ✅ | — |
| 16 | SHAP explainability at inference time | ML Signal Engine | ✅ | — |
| 17 | 5-day recommendation history table + Sell rationale | Dashboard (ML) | ✅ | — |
| 18 | Top Buy Signals — remove 5-cap, sortable columns | Dashboard (ML) | ✅ | — |
| 19 | `mb_tier` relabeled to probability bands | Dashboard (ML) | ✅ | — |
| 20 | View All for 21d/63d recommendations | Dashboard (ML) | ✅ | — |
| 21 | Redesign Signal Deep Dive as sortable full-universe list | Dashboard (ML) | ✅ | — |
| 22 | Indian numbering (`fmtInt`) audit across all apps | Dashboard (all) | ✅ | — |
| 23 | Dedicated Exit Urgency page | Dashboard (ML) | ✅ | — |
| 24 | Upload-current-portfolio (external holdings) page | Dashboard (ML) / API | ✅ | — |
| 25 | Daily sector rotation report | Features / ML Signal Engine | 🔧 | Data source unblocked 2026-07-05/06 — real NSE index ingestion (`nse_indices.py`, `index_ohlcv` table, daily scheduled job) live in production; sector-index mapping, `features/sector_rotation.py`, API endpoint, and dashboard screen still not built |
| 26 | Multibagger tier change-log / "first appeared" date | ML Signal Engine | ⏳ | #14's scheduled job just landed — needs a few real weekly runs of history to accumulate before this is meaningful |
| 27 | Multibagger survival-curve labeling fix (flat at 100%) | ML Signal Engine | ✅ | — |
| 28 | RuleBasedExitPolicy: volatility-scaled target/stop | ML Signal Engine | ✅ | — |
| 29 | Backdated Entry — relocate off main Paper Trading page | Dashboard (Paper Trading) | ✅ | — |
| 30 | Unified backtest strategy (per-horizon, Nifty benchmark) | Backtest | 🔧 | Benchmark data source unblocked (shares #25's `index_ohlcv`), but the real benchmark equity curve is not yet wired into `backtest/engine.py`/the 3 scripts; per-horizon script restructuring itself still unscoped |
| 31 | Blank company names (1,817 tickers) — export list | Data Layer / Config | ✅ | — |
| 32 | Triage 174 likely-missing-split tickers vs Fyers, backfill `corporate_actions`, 2nd recompute pass | Data Layer / Ingestion | ⏳ | Deferred to let 2026-07-05 overnight recompute finish first |
| 33 | KANSAINER/AJOONI non-monotonic price-ratio investigation | Data Layer / Ingestion | ⏳ | Not a simple missing-split case; needs dedicated look |
| 34 | Assess 152 higher-cv Fyers-mismatch tickers (dividend-convention gap vs real issue) | Data Layer / Ingestion | ⏳ | — |
| 35 | Scheduler durability: systemd `--user` service + linger (survives closed Claude Code session/VS Code) | Scheduler / Ops | ✅ | — |
| 36 | 30-min CPU/memory monitor with training-safe throttling (defers restart if a step is mid-run) + Ops Monitor UI panel | Scheduler / Ops | ✅ | — |
| 37 | Cross-process `daily_pipeline` double-fire race condition (checkpoints showed success, heartbeat showed failed since 2026-06-22) | Scheduler | ✅ | — |
| 38 | Model-retrain script map fixed: all 6 phase-1 models pointed at nonexistent scripts, and once repointed, still invoked as a bare file path (`ModuleNotFoundError`) instead of `python -m <module>` | ML Signal Engine / Scheduler | ✅ | — |
| 39 | `signal_63d` + multibagger given real periodic-retrain entry points (`retrain_phase2.py`, new `train_multibagger.py`) — no model left silently unretrainable except Phase-3 `tft`/`bilstm` | ML Signal Engine | ✅ | — |
| 40 | Sector screen: "Sector-Unique Metrics" sub-panel is a hardcoded empty state (GNPA/ANDA-style metrics never computed) | Dashboard (Fundamental) / Features | ⏳ | Needs per-sector metric design (bank GNPA, pharma ANDA approvals, etc.) — no existing data source |
| 41 | Management screen: "Related-Party Transactions" sub-panel is a hardcoded empty state | Dashboard (Fundamental) / Features | ⏳ | `systems/fundamental_analysis/management/` is an empty stub — needs RPT data source + parsing |
| 42 | `systems/fundamental_analysis/{growth,management,peers,quality,sector,thesis}/` — all 6 subpackages are 8-line docstring-only stubs, never imported anywhere | Architecture / Fundamental | ⏳ | Real logic already lives in `features/fundamental_composites.py` instead — decide: delete the dead stub dirs, or backfill them and refactor composites in |
| 43 | Thesis Builder has no PDF/export feature — no code path produces one (only on-screen templated text) | Dashboard (Fundamental) | ⏳ | Needs a PDF lib (e.g. reportlab/weasyprint) + export endpoint if this is actually wanted |
| 44 | `scripts/ingest_external_fundamentals.py`'s "write" path never persists — logs `"Writing: ..."` and increments a counter, no real DB write function exists | Ingestion / Fundamentals | ⏳ | Own comment (line ~124) admits `DataStoreClient.write_fundamentals` was never implemented |
| 45 | Valuation Accuracy screen has zero backend/frontend — needs an endpoint that backtests past `valuation_signals` predictions against realized price outcomes, plus a real `accuracy.js` | Dashboard (Valuation) / API | ⏳ | Verified 2026-07-05: `accuracy.html` calls `renderEmptyState` with literal "Not yet built.", no fetch call exists |
| 46 | 3 failing `test_damodaran.py` sector-alias tests (`test_financial_services_banking/nbfc/insurance`) expect `"Banking"/"NBFC"/"Insurance"` to classify as `FINANCIAL_SERVICES`, but classifier only matches literal `"Financial Services"` | ML / Valuation Tests | ⏳ | Stale test expectations vs. the 2026-07-04 classifier fix — decide whether to alias sector strings or update the tests |
| 47 | No router-level tests for `datastore/api/routers/valuation.py` — only the underlying `systems/damodaran_valuation/` library is tested via `test_damodaran.py` | Valuation / Tests | ⏳ | Endpoint wiring (param validation, error responses, peer-group edge cases) is currently unverified by tests |
| 48 | Altman Z-Score structurally NaN in production — `mktcap`/`re`/`ebit` are proxies and `current_assets`/`current_liabilities` are never populated | ML Signal Engine / Forensic / Data Layer | ⏳ | Needs real market cap, retained earnings, EBIT, current assets/liabilities ingested — schema columns exist, live scraper doesn't populate them |
| 49 | Dechow F-Score always called with `{}` — permanently NaN, dead in production | ML Signal Engine / Forensic | ⏳ | Needs employee-count, share-issuance, and book-to-market data ingestion — no existing source |
| 50 | Beneish M-Score's AQI term permanently NaN | ML Signal Engine / Forensic / Data Layer | ⏳ | Needs `current_assets`/PPE columns (exist in schema, unpopulated) backfilled from a live scraper |
| 51 | Forensic Group C fields hardcoded `np.nan` (`unbilled_revenue_ratio`, `cash_revenue_ratio`, `revenue_vs_gst_proxy`, `revenue_concentration`) | ML Signal Engine / Forensic | ⏳ | Never computed — needs a data-source decision (GST filings, revenue-concentration inputs) before scoping |
| 52 | Benford's Law screen only surfaces one aggregate MAD float — no per-digit histogram, no chi-square result, only `revenue` tested (not expenses/other line items) | Dashboard (Forensic) / API | ⏳ | `benford_analysis()` already computes chi-square + per-digit frequencies internally (`classical_scores.py:441-502`) — needs the API/schema extended to persist and expose them, and additional financial series wired into `series_dict` |
| 53 | Investigation Report has no PDF/report-builder backend — "export" is `window.print()` over a client-side template | Dashboard (Forensic) | ⏳ | Needs a PDF lib (reportlab/weasyprint) + export endpoint, mirroring #43's Thesis Builder gap |
| 54 | Universe Scan has no on-demand trigger — UI only reads the last offline `score_forensic.py` batch run's DB rows | Dashboard (Forensic) / API | ⏳ | Needs a "run scan now" endpoint wrapping `score_forensic.py`'s full-universe loop (already iterates the real `config/nifty500_universe.csv`, just not reachable from the API) |
| 55 | Docstring says "76 core" indicators, code computes 70 | Dashboard (Technical) | ⏳ | Docstring/code mismatch, needs correction |
| 56 | Phantom equity trading data on real holidays: root cause dig deeper? | Ingestion | ⏳ | Two-layer fix landed; whether the underlying NSE archive quirk affects other scrapers is unconfirmed |
| 57 | No charting library on Technical > Chart screen | Dashboard (Technical) | ⏳ | Needs a vendored charting lib or custom canvas/SVG renderer |
| 58 | Watchlist screen wiring status unresolved | Dashboard (Technical) | ⏳ | Needs a follow-up truthful-mode pass to confirm whether it is fully wired to a real backend endpoint or still partial/stubbed |
| 59 | Data Integrity Checker (corporate actions, nulls, holiday/parquet leakage, random 5yr Fyers/Yahoo spot-check) | Data Layer / Ops / Scheduler | ⏳ | Needs new scheduler job + RCA/fix-suggestion workflow design |
| 60 | Pipeline Health Checker (weekly job-completeness audit + catch-up plan) | Ops / Scheduler | ⏳ | Depends on existing heartbeat store; needs catch-up scheduling logic |
| 61 | Remote/mobile access to dashboard (password-protected) | Ops / Dashboard | ⏳ | Design proposed below (Tailscale) — needs user to install/approve tooling |
| 62 | Job run-time/memory benchmark history + weekday/weekend schedule optimization | Ops / Scheduler | ⏳ | Extend existing heartbeat store with duration/peak-RSS fields; optimization pass blocked on a few weeks of accumulated data |
| 63 | UI refactor for responsive layout (mobile/tablet) | Dashboard (all) | ⏳ | Ties into #61 — needed for the mobile-access use case to actually be usable |
| 64 | Write-audit-publish architecture for DuckDB ingestion (raw landing → validate → atomic publish, N=7 rollback snapshots, 15GB budget) | Data Layer / Ingestion / Scheduler | ⏳ | Needs incremental/diff snapshot design to fit budget; foundation for #59 |
| 65 | Expand `_SANITY_KNOWN_SPARSE_COLUMNS` with ~19 more confirmed-unsourceable forensic/governance columns; recompute + re-run `sanity_check`/`paper_trade` for 2026-07-03/06/07 | Scheduler / Data Layer | ⏳ | 38-column exemption + yield backfill landed 2026-07-08 (see BuildLog); remaining ~19 columns already confirmed genuinely unsourceable by `deep_forensic.py`'s 2026-07-07 audit but not yet added — 3 historical dates still show `sanity_check failed`/`paper_trade skipped` until this lands |
| 66 | Real-economy macro: 8 of 10 series remain genuinely blocked | Data Layer / Ingestion | 🚫 | No free structured source found |
| 67 | Real NSE endpoints found but not yet built into a pipeline | Ingestion | ⏳ | Endpoints identified, not yet wired |
| 68 | 7 forensic/governance columns remain unavailable even from NSE XBRL (`contingent_liability_ratio` etc.) | Data Layer / Ingestion | 🚫 | Only present as freeform "Textual Information" in NSE's own template — would need NLP/text extraction |
| 69 | `altman_z` still NaN for a real subset of tickers | ML Signal Engine / Data Layer | ⏳ | Depends on `shares_outstanding` availability (pre-FY2023-24 filings, implausible-value rejections); full-universe gap size not yet measured |
| 70 | Corporate Announcements "insider" category is an approximation | Ingestion / Data Layer | ⏳ | No dedicated NSE insider-trading-disclosure endpoint found; needs a real dedicated source for a narrower signal |
| 71 | 18 "advanced" TA features computed but unused by any ML training pipeline | ML Signal Engine / Data Layer | ⏳ | Decide: wire into Phase 2 training, or stop computing to save run-time/storage |
| 72 | `shareholding`/`governance` GET endpoints 500 on any NULL numeric field (Pydantic `le=100` rejects NaN) | API / Data Layer | ✅ | Fixed 2026-07-08 — see BuildLog |
| 73 | Multi-day-missed pipeline runs (laptop off across more than one 18:00) still permanently lose that day's signals | Scheduler | ⏳ | 2026-07-08 session made `run_models`/`write_signals`/`sanity_check` backfillable for any number of missed days (see BuildLog) — remaining open question is whether the Ops dashboard/Daily Insights UI clearly surfaces "this day's signal was backfilled N days late, never auto-traded" to avoid confusion with a live signal |
| 74 | Emergency feature recompute (post corporate-action fix, 2,487 tickers × ~4,845 dates) needs to actually finish running | Data Layer / ML Signal Engine | ⏳ | Stage 1 done (17/17 batches); Stage 2 died at chunk 3/33 after an 8h timeout under the pre-fix slow code (see row #75) — needs relaunch resuming from date 301/4845 with the fix in place |
| 75 | `run_stage2`'s multibagger precompute cache silently produced 0 cached dates every chunk (newest-first `pending_dates[0]`/`[-1]` date-ordering bug, same class as a prior `run_stage2_chunked` fix), forcing every date onto a ~15-25s/date fallback instead of a ~15-25s/chunk fast path | ML Signal Engine / Data Layer | ✅ | Fixed 2026-07-06 (`scripts/feature_backfill_hybrid.py`, `min()`/`max()` of `pending_dates`) and verified standalone (150/150 dates now cache-hit vs 0/150 before) — see BuildLog. Fix not yet run against the live recompute job (pending relaunch, row #74) |
| 76 | Forced retrain of all 8 models (signal_5d/21d/63d, tft, bilstm, multibagger, hmm_market, pnd_detector) per explicit user request | ML Signal Engine | ⏳ | Blocked on #74/#75 — `_execute_emergency_recompute_job`'s retrain loop only fires after Stage 2 fully completes; `models_done: []` so far |

---

## Platform Architecture & Data Layer

### #1/#2/#3 — Morning Catch-Up redesign (macro capture + scheduling)
`ingestion/scheduler/pipeline_scheduler.py:770-823`'s `schedule_morning_catchup`
currently re-runs the same gap-backfill-then-today logic as the 18:00 job,
which always 404s on "today" at 07:30 IST since NSE hasn't published today's
bhavcopy yet. Needs a backward-only variant (walks "today minus 1" back, never
attempts "today"). Bundled with this fix: capture GIFT NIFTY (blocked — no
free source found), Nasdaq/Dow/S&P 500/Nikkei/Hang Seng (via Yahoo
Finance/stooq) once daily at 07:30 IST, and a PIT semantics shift moving
VIX/FII-DII/USD-INR capture from the 18:00 job to 07:30 — verify
`compute_features` still joins macro rows by the same trading-day key after
the shift. DXY (#2) needs its own data-source decision before scoping.

### #4 — DataStore API Console (consolidated health page)
Extend `datastore/api/routers/ops.py` with a freshness-rollup endpoint
(last-write timestamp + row count per table: `ohlcv_adjusted`, `fundamentals`,
`macro_indicators`, `ml_signals`, `ta_signals`, `mf_holdings`) + a new Ops page
consuming it.

### #5 — Ops Portal: surface weekend job schedules
`weekend_feature_backfill` (Sat 09:00), `weekend_fundamentals` (Sat 10:30),
and the twice-monthly `mf_holdings_ingestion` job have no visible status on
the Ops page. Confirm via heartbeat store whether `weekend_fundamentals` has
ever fired, then add next-run-time + last-run-status for all three.

### #6 — Move inline routes into router files
`/api/v1/features`, `/api/v1/models`, `/api/v1/pipeline/status` are defined
inline in `datastore/api/main.py` (lines 172/258/336) instead of
`datastore/api/routers/`. Pure refactor, same paths/behavior.

### #7 — `SIGNAL_THRESHOLD`/`META_THRESHOLD` fallback wiring
Per user decision 2026-07-04: wire these currently-dead settings in as the
fallback threshold when a loaded model has no saved tuned threshold
(corrupted/incomplete artifact, or bootstrap before first real training run).

### #8 — AF-1: DuckDB connection-lifecycle audit + fix — IMPLEMENTED 2026-07-04
Every `get_duckdb_connection(...)` call site under `datastore/api/routers/`
now passes explicit `persist=False` + `read_only=` (was previously relying
on the unsafe `persist=True, read_only=False` default in `alerts.py`,
`regime.py`, `multibagger.py`, and also missing one or both kwargs in
`signals.py`, `forensic.py`, `watchlist.py`, `technical.py`,
`shareholding.py`, `fundamentals.py`). Added
`tests/quality/test_duckdb_connection_discipline.py` — an AST-based static
check (same style as `test_no_stub_or_synthetic_data.py`) that fails CI if
any future router call site omits either kwarg.

### #9 — AF-2: Pipeline output sanity gate
`run_models` silently produced no real signals for 10 consecutive trading
days before a user noticed by coincidence. Add `step_sanity_check(run_date)`
to `daily_pipeline.py` after `step_write_signals`: hard floors on
`ml_signals` row count, non-empty `top_buys`, no all-NaN feature columns. On
failure: raise (checkpoint records "failed") + loud alert. Surface
`sanity_check_passed` on `/api/v1/ops/runs`.

### #10 — AF-3: Feature-store query path partition/index
`datastore/api/main.py`'s `/api/v1/features/{ticker}` opens one Parquet file
per calendar day in a date-range loop (4,792+ files). Recommended fix:
Option A — register a DuckDB view over the `daily/*.parquet` glob and let
DuckDB's own metadata pruning handle range filtering (zero writer-side
change). Option B (defer) — Hive-partition the writer.

### #11 — AF-4: Reconcile/remove orphaned test schema — IMPLEMENTED 2026-07-04
`datastore/api/db.py`'s `init_duckdb()`/`init_sqlite()` defined a fake
schema that didn't match production. Audit found their only consumers were
the `test_duckdb`/`test_sqlite` fixtures in `tests/conftest.py` — and
neither fixture was actually used by any test in the suite (dead code, no
migration needed). Deleted `init_duckdb`/`init_sqlite` from `db.py`, the
two dead fixtures from `conftest.py`, and their exports from
`datastore/api/__init__.py`.

### #12 — AF-5: Fundamentals range/sanity validation gate
Two independent unit-scaling bugs already found by hand (margins stored as
0-100 instead of 0-1; ROE reading ~4% for financial-sector tickers). Add a
plausible-range table per ratio field + a validation pass in the fundamentals
ingestion path that flags (not silently writes) out-of-range rows, with an
explicit low-revenue allowance for genuine micro-cap outliers.

### #13 — AF-6: Daily off-machine backup — IMPLEMENTED 2026-07-04
`scripts/backup_to_b2.py` + scheduler job built and verified (cleanly records
`"skipped"` heartbeat with no credentials set). Needs real user action before
it does anything: install `rclone`, sign up at backblaze.com, set
`BACKBLAZE_KEY_ID`/`BACKBLAZE_APPLICATION_KEY`/`BACKBLAZE_BUCKET` +
`BACKUP_ENABLED=true` in `.env`. Follow-up: surface backup heartbeat on Ops
page (ties to #9).

### #31 — Blank company names (1,817 tickers) — 1,126 ENRICHED 2026-07-04
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

### #32 — Triage 174 likely-missing-split tickers vs Fyers, backfill `corporate_actions` — RESOLVED 2026-07-06 (70/174 fixed, rest reclassified)
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
- **~30 tickers reclassified as not a #32 case at all**: dividend-only
  dates, demergers (SIEMENS), a scheme of arrangement (SURANAT&P), and a
  non-equity bonus debenture (BRITANNIA) — these explain the original
  Fyers mismatch but aren't a missing split/bonus, so no `corporate_actions`
  row was needed. Several (PETRONET, REDINGTON, GRAPHITE, BAJAJ-AUTO,
  GLAXO, MARICO, CASTROLIND, CRISIL, etc.) look like #34's
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

### #33 — KANSAINER/AJOONI non-monotonic price-ratio investigation
Both tickers showed a mismatch-ratio pattern across the 12 comparison dates
that isn't a constant offset (ruling out a single missing split) and isn't
random noise either — flagged during the #32 analysis but not enough
signal yet to hypothesize a cause (candidates: multiple overlapping
corporate actions, a ticker-symbol change/reuse conflating two different
underlying instruments, or a demerger not modeled by the current three
action types). Needs a dedicated look at each ticker's full corporate-action
history and raw Fyers series before attempting any fix.

### #34 — Assess 152 higher-cv Fyers-mismatch tickers
The other half of the 326-ticker mismatch list from #32 (cv≥0.15, i.e. the
mismatch ratio drifts across dates rather than staying constant) is
suspected to be explained by this project's existing known dividend-
adjustment convention gap rather than a missing corporate action, but this
hasn't been verified — needs spot-checking a sample against Fyers before
concluding no action is needed.

### Corporate-action validation tracking — ✅ IMPLEMENTED 2026-07-05
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
against the #32/#33/#34 triage's own mismatch lists below to produce one
reconciled retrain scope; and the table itself is only in the live DB, not
yet added to `datastore/schema/create_normalised.py` (a rebuild-from-scratch
would silently lose it).

### #40 — Emergency recompute Stage 2 + model retrain never completed (timed out)
The 2026-07-05 emergency recompute job (Stage 1 batched recompute → Stage 2
daily-parquet rebuild → 8-model retrain) got through all 17 Stage-1 batches
(after a resume fix for a DuckDB lock collision — see BuildLog.md), but
Stage 2 then hit the job's 8-hour subprocess timeout and never finished;
`models_done` is still empty — none of `signal_5d/21d/63d, tft, bilstm,
multibagger, hmm_market, pnd_detector` have been retrained on the corrected
price history yet. The machine has since rebooted and no part of this job
is currently running. Needs: (a) investigate why Stage 2 took >8h this run
(first attempt crashed quickly on a transient lock, so the real single-run
duration is still unmeasured — may just need a longer timeout, or further
per-chunk batching like Stage 1 got), (b) resume via
`_execute_emergency_recompute_job(start_stage="stage2")` (or add a
`start_stage="retrain"` if Stage 2 turns out to already be complete on
re-check), (c) once retrained, cross-reference against the 77-ticker
`needs_retrain` list above.

---

## AlphaLens.ML (Signal Engine + Dashboard)

### #14 — URGENT: wire multibagger/forensic/21d/63d/conformal into daily scheduler
`score_multibagger.py`/`score_forensic.py` are operator-CLI only, never
scheduled. `daily_inference.py` only scores `signal_5d` daily — 21d/63d/
conformal are trained but never invoked. Add scheduled jobs (multibagger
weekly per its own docstring, forensic likewise), add 21d/63d scoring calls
to the per-ticker loop, add conformal scoring after signal_5d, add an
"as of {date}" staleness indicator matching the existing top_buys pattern.

### #15 — Daily Insights row fusion
Each model writes its own row keyed by `(date, ticker, model_name)`; Daily
Insights only reads the `signal_5d` row, so Meta/Interval/P&D/Regime always
render empty. Stopgap explanation banner already added (`hub.js`). Real fix:
either read-time join across model rows in `top_buys`/`get_ml_signals`, or a
fused summary row written at the end of `daily_inference.py`'s loop.

### #16 — SHAP explainability (never implemented)
`shap_top5_json` exists in the schema/API contract but nothing computes it.
Add a `shap.TreeExplainer` step to `daily_inference.py`'s `signal_5d` loop,
top-5 |value| features serialized per ticker/date. Bundle with #14.

### #17 — 5-day recommendation history + Sell rationale
Rolling scorecard of `signal_5d`'s last 10 calls (recommended date/price,
expected return, CMP, current return) + explicit Sell Recommendation with
rationale via `RuleBasedExitPolicy`'s `exit_type` vocabulary.

### #20 — View All for 21d/63d
Matches Multibagger's existing `view-all` link. Blocked on #14 — nothing to
view until 21d/63d are actually scored daily.

### #21 — Redesign Signal Deep Dive
Replace the ticker+date lookup form (user: "unable to understand the purpose
of this screen") with a sortable full-universe table for the latest date,
double-click to drill into the existing detail view.

### #22 — `fmtInt` numeric audit
`fmtMoney()` in `dashboard/static/js/api.js` already does `en-IN` grouping;
raw numeric displays that bypass it don't. Project-wide audit across all 5
apps' JS for a new shared `fmtInt` helper.

### #23 — Dedicated Exit Urgency page
`exit_urgency`/`exit_type` currently only surface as alert banners and Signal
Deep Dive columns. New screen: all held positions ranked by `exit_urgency`,
`exit_type` shown as stated reason.

### #24 — Upload-current-portfolio page
Read-only "monitor my real holdings" view: user uploads real (non-paper)
holdings, gets daily signal_5d/exit-urgency/P&D against those tickers,
explicitly excluded from training/backtest data.

### #25 — Daily sector rotation report — DATA SOURCE UNBLOCKED 2026-07-05/06, report itself not yet built
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

### #26 — Multibagger tier change-log
Needs #14 (multibagger on a real schedule) first, so there's history to
report a "first appeared / last changed" date from. First real scoring run
completed live 2026-07-04 (see #27's memory-fix note below) — real
`ml_multibagger` data now exists (4,664 rows dated 2026-07-04, e.g.
EIFFL/GOODLUCK/BHAGYANGR at mb_probability ≈0.9999, "10x" tier) to seed
the first data point ahead of the next scheduled Sunday run. A change-log
needs at least 2 runs of history to say anything meaningful, so this item
itself is still not implementable yet — but the blocker (no real data at
all) is now cleared.

**Real production bug found while doing this**: `_execute_multibagger_scoring_job`
(the Sunday 09:30 IST scheduled job wired in #14) invokes
`python -m systems.ml_signal_engine.inference.score_multibagger` with no
`--limit` — i.e. the exact full-universe (~2,300 tickers), no-cached-model
path that was live-verified to exhaust host memory (see #27's note). This
was a real risk to the production job, not just a manual-run inconvenience.
Fixed in `score_multibagger.py`'s `main()`: new `--batch-size` flag
(default 300) trains the model once, then scores the universe in bounded
chunks instead of materializing the full ticker list's OHLCV panel at
once.

### #27 — Multibagger survival-curve labeling fix — IMPLEMENTED 2026-07-04
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
training set the #27 fix produces, each leaf storing its own survival
curve. Fixed by scaling `min_samples_leaf` with the actual training row
count (`max(5, len(X_imputed) // 1000)`) instead of a fixed small
constant. Verified live: the same full-universe run that previously grew
unboundedly to ~7GB and was killed by an external memory monitor completed
training with peak RSS staying under 2GB throughout. First real full-
universe scoring run (with both the batching fix from #26's note and this
fix) kicked off 2026-07-04 to seed real `ml_multibagger` data under the
corrected labeling.

### #28 — RuleBasedExitPolicy: volatility-scaled target/stop
Currently flat +15%/-7.5% (intentional bootstrap stand-in — `ExitSignalModel`
needs ≥200 real closed positions, only 2 trading days of paper-trading
history exist). Make the interim policy itself ATR/volatility-scaled, and
add a hit/miss/timeout metric per closed trade for future retraining
evaluation.

### #29 — Backdated Entry relocation
Feature itself is wanted (documented Gate 7 trade-off), just feels out of
place on the main Paper Trading screen — move under a "Tools"/"Historical
Review" section.

### #76 — `retrain_phase2.py` ticker-chunking fix needs an end-to-end verification run

2026-07-07 session: a manual re-run of `retrain_phase2.py` (started
05:14:20, the 3rd manual re-run attempt that morning) was kernel-OOM-killed
at 09:06 after ~3h52m, peaking at 9.4GB RSS on a ~15GB box. Root cause:
`compute_technical_features()` was called on the whole ~2317-ticker
universe at once (~6-7GB float64 feature matrix), once per horizon
(5d/21d/63d) in `HORIZON_CONFIGS`, with nothing freed between iterations.
Fixed same session: added `DEFAULT_TICKER_CHUNK_SIZE=400` ticker-batched
processing to `_compute_phase2_panel` and a new
`_build_training_dataset_chunked`, float64→float32 downcasting
(`_downcast_floats`), explicit `del`+`gc.collect()` between horizon
iterations, and a `--chunk-size` CLI flag. Verified: module imports
cleanly, helper functions behave correctly on toy inputs, and
`CORE_TECHNICAL_FEATURES`/`PHASE2_FEATURES` were confirmed to have no
cross-sectional (cross-ticker) features (those live in
`features/multibagger.py`, unused by this script) — so ticker-batching
doesn't change any computed value, only peak memory.

**Not done**: no actual end-to-end re-run of the full retrain (a
multi-hour job) to confirm peak RSS actually stays bounded on the real
~2317-ticker universe. Recommend a `--chunk-size 200 --quick` (or similar
reduced) smoke run with RSS monitored (e.g. `ps`/`/proc/<pid>/status`
polling) before trusting this on the next full retrain, and consider
lowering `DEFAULT_TICKER_CHUNK_SIZE` further if the smoke run still shows
high peak memory per batch.

---

## Backtest

### #30 — Unified backtest strategy — benchmark data unblocked, benchmark curve + restructuring both still unbuilt
Scope was explicitly split 2026-07-05/06 into two independent pieces:

**(a) Real Nifty benchmark curve for backtests** — data-source blocker
cleared (same `index_ohlcv` table as #25, live in production since
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

---

## AlphaLens.Fundamental

Gaps surfaced during the 2026-07-05 truthful-mode walkthrough of the 6
Fundamental dashboard screens (dashboard/peers/sector/screener/thesis/
management) against `features/fundamental*.py`, `systems/fundamental_analysis/`,
`ingestion/fundamentals/`, and `datastore/api/routers/fundamentals.py`.

### #40/#41 — Hardcoded empty-state sub-panels (Sector, Management)
`sector.js:16-19`'s "Sector-Unique Metrics" panel and `management.js:20-23`'s
"Related-Party Transactions" panel both call `renderEmptyState(...)`
unconditionally, before any network request — not a loading/error state,
a permanent stub. Matches `alphalens_docs/CLAUDE.md:492`'s documented "one
empty-stated sub-panel each" claim exactly; confirmed accurate, not stale.

### #42 — `systems/fundamental_analysis/*` are dead stub packages
All six subpackages (`growth`, `management`, `peers`, `quality`, `sector`,
`thesis`) are 8-line docstrings with no functions, and nothing imports them
(`grep -rn "import systems.fundamental_analysis"` returns zero hits). Every
real composite score, peer-selection, and quality/growth calc that was
"meant" to live there was instead built directly in
`features/fundamental_composites.py` (which says as much in its own
docstring). Decide: delete the empty directories, or actually backfill them
and move the composites logic in for real module boundaries.

### #43 — Thesis Builder has no PDF export
Zero matches for "PDF" or `print(` in `thesis.html`/`thesis.js`. The "Build"
button only renders templated Strengths/Risks text from real z-score
threshold crossings — no export/download path of any kind exists today.

### #44 — `ingest_external_fundamentals.py` doesn't actually write
The script's write branch only calls `logger.info("Writing: ...")` and
increments a counter; its own comment (~line 124) admits
`DataStoreClient.write_fundamentals` was never implemented. Anyone treating
this script as a working ingestion path is being misled by its log output.

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

## AlphaLens.Forensic

Gaps surfaced during the 2026-07-05 truthful-mode walkthrough of the 7
Forensic dashboard screens (dashboard/redflag/benford/cashflow/heatmap/
report/universe) against `systems/ml_signal_engine/models/forensic/classical_scores.py`,
`features/forensic_classical.py`, and `datastore/api/routers/forensic.py`.
`alphalens_docs/CLAUDE.md:495` labels this app flatly "Real" — verified
that overstates it; closer to Fundamental's "4 Real, 2 Partial" framing.

### #48 — Altman Z-Score structurally NaN in production
Formula itself is correct (`classical_scores.py:207-213`, verified against
published 1.2/1.4/3.3/0.6/1.0 weights), but `mktcap` (book equity proxy),
`re` (book equity proxy), and `ebit` (EBITDA proxy) are all substitutes for
real inputs (`forensic_classical.py:505-512`), and `current_assets`/
`current_liabilities` are never populated by the live scraper — any NaN
term zeroes the whole score (`classical_scores.py:202`). Needs real market
cap, retained earnings, EBIT, and current assets/liabilities ingested.

### #49 — Dechow F-Score always NaN
`forensic_classical.py:541` calls `dechow_f_score({})` — an empty dict,
unconditionally, every time. The formula (`classical_scores.py:365-380`) is
correct but has never received a real input in production. Needs
employee-count, share-issuance, and book-to-market data — no existing
source ingests these today.

### #50 — Beneish M-Score's AQI term permanently NaN
`current_assets`/PPE columns exist in the schema but the live scraper
doesn't populate them (documented gap, `forensic_classical.py:37-49`), so
AQI is always NaN (`:176-184`) and drags into the overall M-Score.

### #51 — Forensic "Group C" fields hardcoded NaN
`unbilled_revenue_ratio`, `cash_revenue_ratio`, `revenue_vs_gst_proxy`, and
`revenue_concentration` are hardcoded `np.nan` (`forensic_classical.py:356-359`),
never computed. Needs a data-source decision (GST filing data, revenue-
concentration inputs) before this can even be scoped.

### #52 — Benford's Law screen exposes far less than it computes
`benford_analysis()` (`classical_scores.py:441-502`) does real math —
`scipy.stats.chisquare` (`:489`) plus a genuine per-digit MAD (`:491`) — but
only `revenue` is ever passed in (`forensic_classical.py:545-546`; no
expense/other line items), and only the single aggregate `benford_mad`
float reaches the API (`datastore/api/routers/forensic.py:56`) and UI
(`benford.js:52` — the frontend's own comment admits the chi-square result
and per-digit histogram are computed internally but never persisted or
exposed). Needs schema + API extended to expose the full distribution, and
more financial series wired into `series_dict`.

### #53 — Investigation Report has no PDF/report-builder backend
`report.js` fetches one ticker's forensic row and fills an HTML template;
"export" is literally `window.print()` (`report.js:38`). No server-side PDF
generation or guided-report endpoint exists in
`datastore/api/routers/forensic.py`. Same shape as #43's Thesis Builder gap
— needs a PDF lib (reportlab/weasyprint) + export endpoint if this is
actually wanted as a real deliverable.

### #54 — Universe Scan has no on-demand trigger
`universe.js` only reads the last offline batch's rows via `/summary`
(`:9`) and `/flagged` (`:31`) — there's no "run scan now" button or
endpoint. The real full-universe iteration exists only as the standalone
CLI `score_forensic.py`, which does correctly load the full, non-hardcoded
`config/nifty500_universe.csv` via `config/universe.py:get_tickers()` and
loop per-ticker — it's just unreachable from the dashboard/API. If
`score_forensic.py` has never been run, `/summary` returns
`available: False` and the whole screen is an empty state. Needs an
endpoint wrapping that script's loop (with the same batching/memory
discipline as #26's multibagger fix) plus a UI trigger.

## AlphaLens.Technical

### #55 — Docstring says "76 core" indicators, code computes 70
`datastore/api/routers/technical.py` docstring claims "76 core" technical
indicators; `CORE_TECHNICAL_FEATURES` in `features/technical.py` (asserted
`== 70`) is the actual, verified count. Needs the docstring corrected to
match code (or vice versa, if 6 indicators were genuinely intended but
never added).

### #56 — Phantom equity trading data on real holidays: root cause dig deeper?
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

### #57 — No charting library on the Technical > Chart screen
`dashboard/static/technical/js/chart.js` itself documents this: no
candlestick/time-series charting library exists in this zero-build-tooling
app, so `chart.html` only shows a snapshot panel (latest price + curated
indicator/pattern list), not an actual chart. A real chart would need a
vendored charting library (the app avoids external CDN dependencies) or a
lightweight custom canvas/SVG renderer.

### #58 — Watchlist screen wiring status unresolved
`dashboard/static/technical/watchlist.html` was flagged in the original
screen-by-screen walkthrough as the newest of the six Technical screens;
its backing endpoint/wiring status was not conclusively verified before
the session moved on to the indicator-persistence and data-integrity
investigation. Needs a follow-up truthful-mode pass to confirm whether it
is fully wired to a real backend endpoint or still partial/stubbed.

---

## Data Quality / Ops / Platform (2026-07-08 proposals)

Four new items scoped 2026-07-08 in response to a direct ask: two new
scheduled quality-gate jobs (#59, #60), remote/mobile dashboard access
(#61), and job-benchmark-driven schedule optimization (#62), plus a UI
responsiveness item (#63) that #61 depends on to be actually usable from a
phone/tablet.

### #59 — Data Integrity Checker
New scheduled job, run **before** the daily Feature Engineering and Model
Run steps (per user decision), so a bad ingest never propagates into a
day's features/signals. Four checks, one job:

**a. Corporate-action cross-check.** For every `corporate_actions` row
actioned in the trailing 7 days (dividend/rights/split/bonus), re-pull the
same ticker/date window from Fyers and diff. Reuses the same comparison
method already proven out in #32's triage
(`scripts/detect_missing_split_reconstruction.py`) and the existing
`corporate_actions_validation` proposed schema (see the "Corporate-action
validation tracking" note above) — this item is effectively that proposal,
turned into a recurring job instead of an ad-hoc script.

**b. Null/NaN sweep.** Scan `features/*` output and the ingested source
tables (`ohlcv_adjusted`, `fundamentals`, `macro_indicators`) for
unexpected null/NaN rates per column, against a per-column baseline
(flag columns that are *structurally* always-NaN, e.g. #48/#49/#50/#51's
already-known forensic gaps, so the checker doesn't re-alert on known,
accepted gaps every run).

**c. Holiday/non-trading-day leakage check.** Cross-reference
`config/nse_holidays.py` (rebuilt authoritative 2005-2026 calendar per
#56) against `ohlcv_adjusted` and any written Parquet feature-store
partitions — any row or file dated on a real NSE holiday is a signal of
the same failure mode #56 already found and fixed at the scraper layer;
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
cross-check and dividend-convention-gap logic already built for #32/#34 —
and (3) proposes a concrete fix (e.g. "insert corporate_actions row:
ticker=X, ex_date=Y, ratio=Z, confirmed via NSE API"). The fix is queued
for manual approval, never auto-applied — matches this project's existing
no-silent-write discipline (AF-5/#12, the null-flagging-not-fixing
pattern).

**Open question:** where RCA+fix-proposal output should live — a new
`data_integrity_findings` table (approve/reject workflow via Ops
dashboard, mirroring #9's `sanity_check_passed` surfacing) is the natural
fit given #12/#59's existing "flag, don't silently write" pattern, but
this needs a short design pass before implementation, not just a table
guess.

### #60 — Pipeline Health Checker
New scheduled job, also run **before** Feature Engineering/Model Run
(same ordering decision as #59) so a missed upstream job is caught before
downstream steps run on stale/incomplete data. Reads the existing
heartbeat store (same one #5, #9's `sanity_check_passed`, and #62's
benchmark data all key off) to confirm every job that was supposed to run
in the trailing 7 days actually recorded a `success` heartbeat —
including the weekend jobs (#5: `weekend_feature_backfill`,
`weekend_fundamentals`, `mf_holdings_ingestion`) that currently have no
visible completeness tracking at all.

On any gap: highlight the specific job + missed date(s), and propose a
catch-up plan (which backfill script covers that job, in what order,
respecting dependencies — e.g. don't queue a Feature Engineering catch-up
before its upstream ingestion catch-up has actually completed). Surface
on the Ops page as a "missed jobs" panel with an approve-to-run
catch-up action, consistent with #59's approve-before-apply pattern.

### #61 — Remote/mobile access to dashboard
**Design recommendation: Tailscale.** The dashboard currently only binds
to localhost/LAN with no auth layer (per item 3's original ask). Rather
than exposing a port to the public internet (which then requires real
hardening — TLS cert management, brute-force protection, a proper
auth/session system — to be safe), put the laptop on a private
[Tailscale](https://tailscale.com) tailnet and install the Tailscale app
on the phone/iPad. This gives:
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
no third-party coordination service, more setup effort). Needs your
sign-off on Tailscale specifically before this is scoped further.

### #62 — Job run-time/memory benchmark history + schedule optimization
Extend the existing heartbeat store (used by #5/#9/#35/#36/#60) with
per-run `duration_seconds` and `peak_rss_mb` fields, written by the same
job-runner wrapper that already records success/failure — no new
storage system, just wider rows on what's already there. Accumulate a
few weeks of real data, then use it to:
1. Identify jobs whose actual runtime/memory footprint no longer matches
   their scheduled slot (e.g. a job that's grown to overlap the next
   scheduled job's start time — the same class of bug #36/#37 already
   fixed reactively for `daily_pipeline`, this makes it visible
   proactively).
2. Rebalance weekday vs. weekend job placement — e.g. move memory-heavy
   jobs (multibagger full-universe scoring, per #26's ~2GB peak) to
   weekend slots with more idle headroom, based on measured data rather
   than guesswork.
Depends on #60's heartbeat read path already existing; the optimization
pass itself is explicitly gated on having enough real weeks of history to
act on (not implementable meaningfully on day one).

### #63 — UI refactor for responsive layout
All 5 dashboard apps currently render fixed desktop-width layouts. Needed
specifically to make #61's remote/mobile access actually usable — SSH'ing
in via Tailscale to a phone browser that renders a desktop-width table is
not a real solution to the "check the dashboard from my phone" ask. Scope
TBD (breakpoint strategy, whether tables collapse to cards on narrow
widths, touch target sizing) — flagged here as a dependency of #61 rather
than designed in detail yet.

### #64 — Write-audit-publish architecture for DuckDB ingestion
**Problem:** scrapers write straight into the same DuckDB tables that
Feature Engineering/Model Run read — a bad parse, a NaN, or a stale/
duplicate source response (the #56 class of bug) becomes production data
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
   sanity checks (AF-5/#12's range-validation gate, #59's null/NaN and
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

**Dependency:** this is the foundation #59 (Data Integrity Checker) should
be built on top of — #59's checks belong at the validation-gate stage
(step 2 above) rather than as a separate after-the-fact scan, so build
#64's landing/staging/publish skeleton first, then wire #59's checks into
the gate rather than building #59 standalone and retrofitting later.

### #65 — Expand sanity_check exemption list + finish backfilling 2026-07-03/06/07

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
`buyback_acceptance_estimated`) are still genuinely blocked — see #66-#70
below for the confirmed-still-blocked list and the newer real leads
(RPT/governance endpoints found but not yet wired) that could close a
few more of these.

### #66 — Real-economy macro: 8 of 10 series remain genuinely blocked

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

### #67 — Real NSE endpoints found but not yet built into a pipeline

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

### #68 — `contingent_liability_ratio`/`subsidiary_count`/`loans_to_related`/`capex_to_assets`/`intangibles_growth`/`off_balance_sheet_proxy`/`noncash_assets_ratio` remain unavailable even from NSE XBRL

The 2026-07-08 NSE XBRL Integrated Filing pipeline (BuildLog.md) resolved
`goodwill_ratio`/`cwip_ratio`/many other balance-sheet fields from the
same real regulatory filing, but live-verified that "Disclosure of notes
on assets and liabilities" — where contingent liabilities, subsidiary
counts, and related-party loan amounts would appear — renders as
freeform "Textual Information" in NSE's own template, not a structured
numeric field. Same gap as Screener/Trendlyne, not resolved by switching
sources. Would need actual NLP/text extraction against unversioned
freeform disclosure text to close, a materially different (and much more
fragile) effort than the rest of this session's JSON/structured-HTML work.

### #69 — `altman_z` still NaN for a real subset of tickers

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

### #70 — Corporate Announcements: "insider" category is an approximation

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

### #71 — 18 "advanced" TA features computed but unused by any ML training pipeline

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

### #72 — `shareholding`/`governance` GET endpoints 500 on NULL numeric fields (FIXED 2026-07-08)

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
500` floods in scheduler logs) and was the likely direct cause of the
2026-07-07 `sanity_check` failure's "58 all-NaN columns" (many are
shareholding/corp-action-derived features). See BuildLog.md 2026-07-08
entry for verification.

### #73 — Multi-day pipeline gaps: signals now backfilled, but UI doesn't flag "backfilled" vs "live"

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

Not yet done: the Ops dashboard and Daily Insights / ML signal screens
don't currently distinguish "this signal was computed same-day" from
"this signal was backfilled N days after its own trading day" — a user
glancing at a backfilled row has no visual cue that it was never
eligible for (and won't get) an actual paper trade. Worth a `backfilled:
bool` or `computed_at`-vs-`date` delta surfaced in the API response and
UI once this has been in use for a while and it's clear it matters in
practice.

### #74 — 2026-07-06 still missing `run_models`/`write_signals`/`sanity_check`/`paper_trade`

During the 2026-07-07 session's OOM investigation (BuildLog.md), a stale
`daily_pipeline` process (PID 1966732, orphaned since the previous
evening's failed run) was found holding the DuckDB file lock and was
killed; a second, legitimately-running backfill process then caught
`2026-07-03` up fully and got `2026-07-06` through `check_ta_alerts`, but
`2026-07-06`'s `run_models`/`write_signals`/`sanity_check`/`paper_trade`
are still sitting at `skipped` in `pipeline_checkpoints` (SQLite) from the
original failed run. These first three ARE backfillable under the current
`checkpoint.py::STEPS` (see #73's fix), so a `force_run_step` (or another
`run_steps_for_date(date(2026,7,6))` call once nothing else holds the DB
lock) should complete them — `paper_trade` should correctly stay skipped
for that date (non-backfillable by design). Not done in that session;
needs a follow-up force-run.

### #75 — `download_index_ohlcv` failing repeatedly on backfill (BSE/NSE 404s)

Observed failing for both `2026-07-03` and `2026-07-06` during the same
backfill (`pipeline_checkpoints` shows `status=failed` for both dates).
Non-critical — downstream steps don't depend on it and proceeded — but
it's now failed on 2+ consecutive backfilled days, suggesting either a
stale index-list/URL assumption in the scraper or an upstream NSE/BSE
response-format change (echoes the existing "BSE BULK/BLOCK... Expecting
value: line 3 column 1" pattern seen for `large_deals` around the same
time in `logs/daily_pipeline.log`). Not investigated further this
session — worth a dedicated look if index-level OHLCV features start
showing gaps.

### #76 — `signal_63d` still stale (2026-06-23) despite the other 7 registry models retraining on 2026-07-06

`datastore/models/registry.json` shows `hmm_market`, `pnd_detector`,
`signal_5d`, `signal_21d`, `meta_labeler`, `conformal_signal5d`, and
`multibagger` all with `last_trained_date: 2026-07-06`, but `signal_63d`
is still dated `2026-06-23` (its original Phase 2 training run). Its
mapped trainer, `retrain_phase2.py`, only overwrites a horizon's registry
entry if the new run's Sharpe is `>=` the existing one (`improved_or_
neutral` check) — so this may be a legitimate "didn't improve, correctly
left alone" outcome, or `retrain_phase2.py` may not have run at all this
cycle. Not investigated this session — worth checking `retrain_phase2.py`
logs/output the next time it runs to confirm which case this is.

### #77 — Confirm which feature-recompute pass actually fed the 2026-07-06 model retrain

This session was tracking an in-flight Stage 2 feature recompute (PID
`1478361`) that had previously failed once (`stage2 exit 1`) and was
manually relaunched. By the next check, that process was gone and
`datastore/logs/emergency_recompute_progress.json` recorded a further
`"error": "timeout after 8h"` with `stage2_done: false` — yet
`registry.json` shows 7 of 8 models successfully retrained the same day
(`2026-07-06`). It's unclear whether those retrains ran against the fully
corrected post-price-adjuster-fix feature set or against a partially
stale/interrupted recompute. Needs verification: compare a spot-check of
`datastore/features/daily/` for a few of the corporate-action-affected
tickers (DRREDDY, ZEEL, BLUEDART, NTPC, BRITANNIA, TVSMOTOR, TVSMOTOR's
144x anomaly ticker set) against the corrected `ohlcv_adjusted` values
before trusting the 2026-07-06 model artifacts in production.

### #78 — `scripts/model_training_status.py` not yet run end-to-end

New CLI status script (`scripts/model_training_status.py`, added this
session per user request) reports per-model training status against
`datastore/models/registry.json` and `_MODEL_TRAINING_SCRIPT_MAP`, plus
the `model_training` job's `scheduler_heartbeats` row and next-scheduled
run time. It was written and reviewed but not executed to completion
inside this session (interrupted before the verification run). Run
`python scripts/model_training_status.py` once and confirm the table
renders correctly and the heartbeat/next-run section reads real values
before relying on it.

### #79 — Regression test for the `model_training` overdue-check union fix

`_execute_model_training_job`'s overdue-check loop (`ingestion/scheduler/
pipeline_scheduler.py`) was changed this session to iterate the union of
`registry.json` keys and `_MODEL_TRAINING_SCRIPT_MAP`'s non-`None` keys,
so a mapped-but-never-registered model (multibagger's original bug) is
always caught as "never trained" rather than silently skipped. No unit
test was added for this in `tests/unit/test_scheduler.py` this session —
worth a test that seeds a registry missing one mapped model and asserts
it appears in `overdue_models`.

### #80 — Daily NSE/BSE bulk/block-deal history is still only 1 real day deep for non-superstar participants

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

### #81 — Non-equity Trendlyne deals (InvITs, REITs, etc.) are silently dropped from the bulk-deal backfill

`TrendlyneScraper.export_bulk_deals_history` drops any deal row whose
`company_name` doesn't match a `stock_master` ticker (same per-holding
isolation as the existing holdings export). For Rakesh Jhunjhunwala and
Associates specifically, only 73 of 131 scraped deals matched (the rest
were instruments like NDR InvIT Trust that fall outside the equity
universe this DB tracks). This is correct behavior for an equities
dashboard, but if InvIT/REIT-level big-investor activity ever becomes
in-scope, `stock_master`/the ticker-resolution map would need extending
first — not attempted this session.

### #82 — Trendlyne bulk-block-deals page pagination not verified across all 62 investors

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

### #83 — No automated test coverage for this session's Big Investor Activity changes

Nothing in `tests/` was added or updated this session for: `_position_and_
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

### #84 — `holding_pct_of_company` / shares-outstanding estimate is a market-cap/price back-derivation, not a real share count

`_position_row_to_dict` (`datastore/api/routers/big_investors.py`)
computes `shares_outstanding_est = market_cap_cr * 1e7 / cmp` rather than
reading `fundamentals.shares_outstanding` directly, since that field is
PIT-gated per fiscal quarter and only ~9% populated project-wide (per
that column's existing documentation elsewhere in the codebase). This is
a reasonable approximation but assumes `stock_master.market_cap_cr` is
itself freshly derived from a recent price × real share count — not
verified this session. Worth cross-checking against real
`fundamentals.shares_outstanding` for a sample of tickers where both
exist, to quantify how far the estimate can drift.

### #85 — "unmapped:" family ↔ Trendlyne holder-name matching is a string-normalization heuristic, not a real identity match

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
