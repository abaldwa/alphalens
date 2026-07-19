# AlphaLens Feature Backlog — Implemented Archive

This file is the completed-items archive split out of `FeatureBacklog.md` on 2026-07-11. It contains every backlog item marked ✅ (done) as of that date, moved here verbatim — status-matrix table rows plus their detailed writeups. `FeatureBacklog.md` now tracks only the still-open (⏳/🔧/🚫) items. IDs are shared across both files, so a cross-reference to an ID not found here is in the other file.

---

## Status Matrix

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
| A25 | Write-audit-publish architecture for DuckDB ingestion (raw landing → validate → atomic publish, N=7 rollback) | Data Layer / Ingestion / Scheduler | ✅ | 2026-07-09: pilot + full rollout both landed and dry-run verified — see writeup below |
| A27 | Real-economy macro: 8 of 10 series remain genuinely blocked | Data Layer / Ingestion | ✅ 2026-07-10 | No free automated source exists (unchanged finding — PMI licensed, GST/rail-freight freeform PIB text, others bot-blocked; IIP unblockable via `data.gov.in` API key signup). Per explicit operator decision, built a manual-entry path instead of continuing to chase an automated one: new `datastore/api/routers/macro.py` (`GET`/`POST /api/v1/macro/indicators`) writes into the SAME `macro_real_economy.parquet` long-format schema (`feature_name, reference_month_end, value, availability_date`) the 2 already-automated series (cement/power) use — a manual entry is indistinguishable to `features/real_economy_macro.py`'s PIT-filtered reader from an automated one. Monthly cadence, upsert-on-(feature_name, month). Explicitly rejects writes to `cement_dispatches_growth`/`power_consumption_growth` (those already have a real scraper — a manual entry must not silently override it). New dashboard screen `dashboard/static/ops/macro.html` + `js/macro.js` (added as a screen under the existing Ops app, not a new top-level app) — one numeric input per blocked indicator + a month picker, shows the last 12 months entered per indicator below the form. 7 new unit tests in `tests/unit/test_macro_router.py` (write/read round-trip, upsert-not-duplicate, rejection of both an automated-source feature and an unknown feature name) — all pass |
| A29 | `shareholding`/`governance` GET endpoints 500 on NULL numeric fields | API / Data Layer | ✅ | Fixed 2026-07-08 |
| A30 | Multi-day-missed pipeline runs: backfill works but UI doesn't flag "backfilled" vs "live" | Scheduler | ✅ | Implemented 2026-07-09 — see writeup below |
| A31 | `download_index_ohlcv` failing repeatedly on backfill (BSE/NSE 404s) | Ingestion | ✅ | Fixed 2026-07-09 — real causes were not scraper/URL related, see writeup below |
| A32 | `scripts/model_training_status.py` not yet run end-to-end | Ops / Tooling | ✅ | Implemented 2026-07-09 — see writeup below |
| A33 | Regression test for the `model_training` overdue-check union fix | Tests | ✅ | Implemented 2026-07-09 — see writeup below |
| A34 | `step_download_fno` may share A31's unwrapped-DB-write gap | Scheduler | ✅ | Fixed 2026-07-09 — same fix pattern as A31, see writeup below |
| A35 | screener source can't join A25 staged publish without an architecture change | Data Layer / Ingestion | ✅ | Fixed 2026-07-09 (client-side batching, per operator decision) — see writeup below |
| A36 | `fundamentals` table has 4 writers with inconsistent upsert-conflict precedence | Data Layer / Ingestion | ✅ | Fixed 2026-07-09 (NSE XBRL > Trendlyne > Screener > Kaggle priority, per operator decision) — see writeup below |
| A37 | `retrain_all_when_free.sh` logged false `exit=0` for crashed stages | Scheduler / ML Signal Engine / Tests | ✅ | Fixed 2026-07-09 — root cause of A28(f)/(g) confusion, see writeup below |
| A39 | `ExitSignalModel` will crash the entire daily inference pipeline the first time paper trading opens a position | ML Signal Engine / Ops | ✅ Fixed 2026-07-09 | `_step_exit` now falls back to `RuleBasedExitPolicy` when no trained model exists, matching `run_daily_paper_trading.py`'s pattern; real trainer still doesn't exist (needs closed-trade data) — see writeup below |
| A41 | Orphaned pre-A38 TFT/BiLSTM `.pt` checkpoints (2026-06-24/06-30/07-01) sit unregistered outside the current save-path convention | ML Signal Engine / Data Layer | ✅ 2026-07-11 | Group 2 backlog sweep: the flat `datastore/models/*.pt` layout is actually the current save convention (not a migration gap as originally assumed) — verified both `tft`/`bilstm` v20260701 checkpoints still load with current code, backfilled `registry.json`, archived stale earlier rounds |
| A43 | Daily Insights / ML signal screens don't surface A30's per-signal backfilled-vs-live flag | Dashboard / API | ✅ 2026-07-10 | Implemented as a Python-side join — `ml_signals` (DuckDB) and `pipeline_checkpoints` (SQLite) are different databases with no foreign key. `ingestion/scheduler/checkpoint.py::CheckpointManager.get_step_is_backfill(date, step_name)` reads the `is_backfill` column A30 already writes for the `write_signals` step; `datastore/api/schemas.py::MLSignalRow` gained an `is_backfill: Optional[bool]` field (None when no checkpoint row exists yet, e.g. very old rows); `datastore/api/routers/signals.py` gained a `_attach_is_backfill()` helper (caches one lookup per distinct date so a multi-row response like `top_buys`/`history` doesn't re-query per row) called from all three GET endpoints (`/ml/{ticker}/{date}`, `/ml/top_buys/{date}`, `/ml/history/{ticker}`). Note: the Ops "Recent Runs" table already surfaced a coarser run-level `is_backfill` badge since A30 — this is the finer per-signal-row flag on the actual Daily Insights/signal-consuming endpoints the original finding was about. Tests: `tests/unit/test_scheduler.py::TestCheckpointManager::test_get_step_is_backfill_returns_recorded_flag`/`test_get_step_is_backfill_returns_none_when_no_checkpoint_row`, and a new `tests/unit/test_signals_is_backfill.py` (4 tests, real on-disk DuckDB fixture + in-memory SQLite `CheckpointManager`, no mocks) exercising all three endpoints end-to-end through the real FastAPI app |
| A47 | Adaptive chunk/batch sizing under memory pressure (self-heal): shrink `SCREENER_BATCH_EXPORT_CHUNK_SIZE`/feature-matrix batch size dynamically instead of OOMing | Scheduler / Ops | ✅ 2026-07-10 | `ingestion/scheduler/resource_guard.py` built earlier + wired into `screener.py::batch_export`. **2026-07-10 follow-up — feature-matrix build now chunked**: confirmed 3 panels (`compute_fundamental_features_panel`'s sector z-score, `compute_mf_holdings_features_panel`'s tier-rank, `compute_multibagger_features`'s universe/sector rank) do real cross-ticker aggregation and must NOT be chunked — left on the full ticker universe, unchanged. The other 6 categories (technical, intraday, hmm, pnd, advanced_technical, patterns) are confirmed per-ticker-independent — new `features/matrix_builder.py::_compute_chunked_ticker_independent_panels` computes these in `resource_guard.adaptive_chunk_size`-sized ticker chunks instead of one full-universe pass, freeing each chunk's 6 derived DataFrames (`gc.collect()`) before the next — bounds peak memory to one chunk's derived-computation footprint instead of holding 6 full-universe-sized derived frames simultaneously alongside the raw OHLCV panel. The raw OHLCV panel itself (`universe_panel`) stays fully loaded regardless (needed whole by `compute_multibagger_features` afterward; already a single efficient bulk fetch, not the OOM source here) — this specifically targets the *derived*-computation memory growth, which is what A47 was actually scoped for. Critical regression test in `tests/unit/test_matrix_builder.py::TestChunkedComputationMatchesUnchunked`: asserts `build_feature_matrix` output is byte-for-byte identical (`pd.testing.assert_frame_equal`) between a single full-universe pass and forced chunk sizes of 2 and 1 (most extreme case, every ticker computed alone) — proves chunking never leaks a chunk boundary into a per-ticker computation. 83 tests across matrix_builder/hmm/pnd/phase3/multibagger/fundamental suites all green |
| A49 | Adopt `psutil` (replacing deliberately-hand-rolled `/proc/meminfo` parsing in `monitor_scheduler_resources.py`) for accurate per-process RSS | Ops | ✅ 2026-07-10 | `psutil==6.1.1` added to `requirements/phase0.txt`, installed, and used by `resource_guard.py` (with a `/proc` fallback if ever absent) — pulled forward from Phase 4 since Phase 2 needed it. `monitor_scheduler_resources.py` itself still uses its own `/proc/meminfo` parsing (unchanged, out of this session's scope — see A48) |
| A50 | DB-lock hold-time reduction + a lock-status monitor (current holder, held-since, wait queue) for `PIPELINE_RUN_LOCK_PATH`/`PUBLISH_RUN_LOCK_PATH` | Scheduler / Ops | ✅ 2026-07-10 | `ingestion/scheduler/lock_monitor.py` + `GET /api/v1/ops/lock-status` built earlier (locked/free + last-activity mtime for both locks); both lock context managers confirmed to already release in `finally` on every path, no hold-time bug there. **2026-07-10 follow-up — the actual fno_data fix**: root cause was `publish_table`'s `CREATE OR REPLACE TABLE fno_data AS SELECT * FROM staging.fno_data`, physically rewriting all ~121M rows in place on every publish. A DELETE+INSERT alternative was considered and rejected — this codebase's own history (`datastore/staging/publish.py`'s docstring) shows that exact pattern was already tried and deliberately replaced by the current atomic swap specifically to eliminate its non-atomic partial-update window; reintroducing it would trade away a safety property already fixed once. Instead: `fno_data` now lives in its own DuckDB file (`config.settings.FNO_DATA_DB_PATH`, derived per-connection via `datastore/api/db.py::fno_db_path_for` — not a single hardcoded path, so isolated test DBs each get their own companion file), ATTACHed transparently via `ATTACH ... AS fno_db` + `SET search_path = 'main,fno_db'` so every existing unqualified `fno_data` reference across all 14 touch points (API router, `features/fno_features.py`, backfill scripts, etc.) continues to work completely unchanged. New `datastore/staging/publish.py::publish_fno_data` builds the new version in a throwaway file and swaps it in via a near-instant `os.replace()` instead of an in-place rewrite — the DuckDB write lock on the production file is now held for a rename, not a 121M-row copy. Two real bugs found and fixed during implementation (both confirmed live, not theoretical): (1) a fresh connection opened after the swap could see stale/empty data unless the new file is explicitly `CHECKPOINT`ed before the swap; (2) the publish function was accidentally reading the wrong (hardcoded, unrelated) file path via `config.settings.FNO_DATA_DB_PATH` instead of introspecting the connection's actual attached path via `PRAGMA database_list` — fixed to always use the real attached path. **Live migration completed and verified**: all 120,686,722 production rows copied into `alphalens_fno_data.duckdb`, row count and data (sample rows + aggregate SUM(oi)/SUM(volume)/distinct-date checksums) confirmed identical, old in-main-file table dropped, `/api/v1/fno/RELIANCE` endpoint verified returning real data (4,983 rows) against the migrated file. 8 new tests in `tests/unit/test_fno_data_file_split.py`, plus a fix to `tests/unit/test_fno_api.py`'s bare `duckdb.connect()` call — full regression sweep (96 tests across staging/fno/schema/paper_trading suites) green. Also caught and fixed a real regression the first implementation attempt introduced (attaching for literally any real DB path broke read-only connections to `SIGNALS_DUCKDB_PATH`, which never has a companion fno file) before landing |
| A51 | Flip A25 staging's default `--publish-mode` from `direct` to `staged` for the remaining writers (kaggle/trendlyne/nse_xbrl fundamentals, amfi holdings, corporate actions, screener `write_batch`) | Data Layer / Ingestion | ✅ 2026-07-10 | trendlyne/nse_xbrl backfill CLI flags + `amfi_holdings.sync_duckdb_table` flipped to default `staged`, pinned by a new fitness test — see writeup below. `load_kaggle_fundamentals.py` portion is now N/A (removed entirely, see A62). **2026-07-10 follow-up investigation — the remaining two items are correctly closed, not gaps**: `corporate_actions.py`'s daily-step call site intentionally stays `direct` per its own docstring — `upsert_corporate_actions_staged`'s full-table `CREATE OR REPLACE` swap is designed for `backfill_corporate_actions.py`'s large accumulated batches, and would be wasteful/add lock contention if applied daily to a handful of same-day rows; flipping it would work against A25's own design, not complete it. Screener's `write_batch` already achieves A25's actual goal (one write-lock acquisition per chunk instead of per-ticker) via `write_fundamentals_batch`'s batched `executemany` in `datastore/api/routers/fundamentals.py` — a live/incremental API endpoint has no bulk-backfill CLI to add a `--publish-mode` flag to in the first place; the staging/publish full-table-swap mechanism doesn't fit this call pattern. Marked ✅ — both design intents already correctly served by different, better-fitting mechanisms |
| A52 | Model training schedule: spread `_MODEL_TRAINING_SCRIPT_MAP`'s scripts across nightly 11pm-6am windows through the week instead of one big Saturday job | Scheduler / ML Signal Engine | ✅ 2026-07-10 | `_MODEL_TRAINING_GROUPS` + `schedule_model_training_nightly` land Mon-Thu 23:00 IST per-group jobs, wired into `daily_pipeline.py::main()` (takes effect on next scheduler restart); old `schedule_model_training` kept intact, unused — see writeup below |
| A53 | "Trained but unused" detector: diff `registry.json` model entries against what `daily_inference.py` actually calls, alerting on a gap (the class of bug behind A38/A40/T5) | Ops / ML Signal Engine | ✅ 2026-07-10 | `ingestion/scheduler/model_usage_audit.py` built (curated `CONSUMERS` map + `find_trained_but_unused_models`), tft/bilstm premarked unused so it has real positives once A38 trains them. 2026-07-10 (Group 1 backlog sweep): verified this row's "not yet wired into an Ops dashboard panel" note was stale — `GET /api/v1/ops/unused-models` (`datastore/api/routers/ops.py`) already calls `find_trained_but_unused_models` and the "Trained-But-Unused Models" panel already renders it in `dashboard/static/ops/index.html`/`js/index.js` (`unused-models-table` / `loadUnusedModels`), landed in the same 2026-07-10 session as this audit module under the A45 writeup. No further code needed; corrected the status here to match reality |
| A57 | Live Ops Monitor showed 880 pending `null_sweep` findings, none reviewed — root cause turned out to be a real, unresolved fundamentals-ratio null collapse since 2026-07-03, not just alert-fatigue noise | Data Layer / Ingestion / Ops | ✅ 2026-07-10 | Root cause: `features/fundamental.py::compute_fundamental_features_panel` and `features/forensic_classical.py::compute_forensic_classical_features_panel` swallowed `httpx.RequestError` in a blanket `except Exception`, silently writing all-NaN fundamentals/forensic-classical features for every ticker whenever the DataStore API had a transient outage (e.g. the 2026-07-03 manual-migration restart) — the NaN was then permanent since nothing re-ran those dates. Fixed: both panels now catch `httpx.RequestError` separately and `raise` (fail loud), matching A44's precedent on the OHLCV path; `step_compute_features` in `ingestion/scheduler/daily_pipeline.py` now calls `_wait_for_datastore_api()` before building either matrix. **2026-07-10 follow-up**: bulk-rejected the 4 pending findings that matched the (now-corrected) allowlist (`benford_mad` — the only allowlisted column that actually had pending findings; a full cross-check confirmed zero other allowlisted columns remain in the pending backlog). The other 876 pending findings are legitimately left open — 220 distinct columns × 4 dates, none matching `_SANITY_KNOWN_SPARSE_COLUMNS` — until the regression itself is verified fixed. Force-regenerating the corrupted 2026-07-03→07-08 daily feature files explicitly deferred per user instruction ("keeping this for a later time") — until that runs, most of the 876 remain pending on purpose, not stale |
| A58 | ~40 of the 880 findings' columns looked like already-documented structurally-sparse forensic gaps (FeatureBacklog FO8/A26), but only about half actually are | Data Layer / Ingestion | ✅ 2026-07-10 | Column-by-column investigation split them three ways. (1) Real bugs, now fixed: `intangibles_growth` in `features/deep_forensic.py` read the wrong dict key (`"intangibles"` instead of the real schema/NSE-XBRL column `"intangible_assets"`, populated on 5,760/36,346 rows) — fixed the lookup key only, left the schema column and the existing YoY-diff calculation untouched per explicit instruction. `audit_qualification_flag`/`goodwill_ratio`/`capex_to_assets`/`noncash_assets_ratio` were already correctly wired to real, populated NSE XBRL columns — the FO8-era "unavailable" docstring claim was stale (predated `ingestion/scrapers/nse_xbrl_financials.py`'s structured parser); docstring corrected, all 5 columns removed from `daily_pipeline._SANITY_KNOWN_SPARSE_COLUMNS` so a future regression in their computation is still caught by `step_sanity_check`. (2) Real-but-unscheduled scrapers, now scheduled: `scripts/backfill_promoter_pledge_nse.py` and `scripts/backfill_balance_sheet_from_screener.py` were both live-verified 2026-07-07 but never invoked by any scheduler — added `schedule_promoter_pledge_backfill`/`schedule_balance_sheet_backfill` (Saturday 11:00/11:30 IST, after `weekend_fundamentals`) to `ingestion/scheduler/pipeline_scheduler.py`, wired into `daily_pipeline.py::main()`. (3) Genuinely unfixable today (no schema column, or freeform-text-only NSE disclosures — `contingent_liability_ratio`, `subsidiary_count`, `loans_to_related`, `off_balance_sheet_proxy`, and Group E's remaining 7 governance columns): left in the allowlist. `benford_mad` added to the allowlist after fixing forensic_classical.py's matching A52-class bug (see A57) — its residual nulls are legitimate new-listing warmup, not breakage |
| A62 | Kaggle confirmed dead code — `scripts/load_kaggle_fundamentals.py` never invoked by any scheduler/job registry/pipeline; operator decision: remove entirely | Data Layer / Ingestion | ✅ 2026-07-10 | Deleted `scripts/load_kaggle_fundamentals.py` (no test file existed for it). Removed `"kaggle": 1` from `SOURCE_PRIORITY` in `features/fundamental_source_priority.py`; real precedence is now NSE XBRL (4) > Trendlyne (3) > Screener (2) — confirmed correct via existing `build_priority_update_clause` logic, no change needed there beyond dropping the dict entry. Updated docstrings/comments in `datastore/schema/create_normalised.py` and `features/fundamental_quality_gate.py` that referenced the removed script. Updated `tests/unit/test_fundamental_source_priority.py`'s 6 tests to use `screener` instead of `kaggle` as the lowest-ranked source in the priority-ordering assertions — all pass. Remaining `kaggle` mentions in the codebase are historical-incident-narrative comments (BuildLog-style "what was true when this was fixed" notes), left as-is per this project's existing convention for such comments |
| A70 | Menu app-switcher labels currently carry an `"AlphaLens."` prefix (`shell.js` `app.name`), forcing the app-tab bar into horizontal scroll; drop the prefix (keep on the logo only) | Dashboard (shell) | ✅ 2026-07-11 | Added a `short` field (e.g. `"ML"`, `"Technical"`, `"BigInvestors"`) to each entry in `APPS` in `dashboard/static/js/shell.js`; the app-switcher tabs (`.app-tabs`, `renderAppShell`) now render `a.short \|\| a.name`, while the top-left logo still renders the full `app.name` (`"AlphaLens.ML"` etc.) unchanged. Only shortens the `.app-tabs` scroll bar as scoped — the per-app `.sub-tabs` bar (e.g. ML's 10 screens) still scrolls on screen-heavy apps; that's tracked separately if the user still wants it addressed after seeing this land |
| A24 | UI refactor for responsive layout (mobile/tablet) | Dashboard (all) | ✅ | 2026-07-10 (Group 1 backlog sweep): landed for AlphaLens.Ops only (`dashboard/static/ops/css/responsive.css`). **2026-07-13: extended framework-wide** — new shared `dashboard/static/css/responsive.css` (same `.card:has(> table)`/`.kv-row`/table-font-shrink/app-bar-collapse rules as Ops' own copy), linked after `shell.css` in every HTML file under `technical/`, `fundamental/`, `forensic/`, `valuation/`, plus `ml/backtest.html` — all 5 apps now have the same responsive pass. |
| A26 | Expand `_SANITY_KNOWN_SPARSE_COLUMNS` with remaining confirmed-unsourceable columns; finish 2026-07-03/06/07 recompute+re-run | Scheduler / Data Layer | ✅ 2026-07-13 | 2026-07-09: audit found 13 of the "remaining ~12" list were already exempted; only `capex_to_assets`/`noncash_assets_ratio` were actually missing — added, with tests. 2026-07-03/06/07 `step_compute_features` recompute + `sanity_check`/`paper_trade` re-run still outstanding (needs an explicit Ops force-run, not run this session). 2026-07-13: confirmed the mechanism (`POST /api/v1/ops/steps/{step_name}/force`, backed by `ingestion/scheduler/force_run.py::force_run_date_sync`) — user gave explicit go-ahead. No live API server was running to hit over HTTP, so wired a direct call (`force_run_date_sync("compute_features", [2026-07-03, 07-06, 07-07], cascade=True)`) into `/tmp/run_production_retrains.py` as its 3rd step, queued behind ML31/A28(g) in the same DB-lock-avoidance chain (waits for the in-flight MultiBagger job + Phase B to finish first). Confirmed `pipeline_checkpoints` already shows `compute_features`/`sanity_check` as `status='success'` for all 3 dates from before the corporate-action data fix — `force_run_date_sync` re-runs regardless of prior success (only checks lower-index prerequisites), so this correctly forces a genuine recompute rather than a no-op. `paper_trade` will correctly stay un-run for these past dates per SPEC-SCHED-006 (enforced by `force_run_date_sync` itself, not skipped by omission). First execution attempt (same day) surfaced 2 new failures: 2026-07-03 `run_models` timed out, and 2026-07-06/07 both failed `sanity_check` on 16 all-NaN F&O columns — root-caused to a real Data Layer bug (see ML34 below), fixed, retry re-launched. Retry #2 confirmed the fix: **2026-07-07 completed fully clean end-to-end (compute_features → sanity_check, `sanity_check: passed ... signal_rows=2317, top_buys=2313, regime=bullish`)**. 2026-07-03/06 hit a *different*, unrelated issue on that attempt — transient DuckDB lock-conflict 500s from the concurrently-running always-on scheduler (`alphalens-scheduler.service`, PID 2123) contending for the same DB files (`run_models`'s NIFTYBEES OHLCV fetch for 07-03, `write_signals`'s `signals.duckdb` write for 07-06) — not a code bug, just momentary contention on a shared box. Retry #3 (07-03/06 only, 07-07 already done): 07-03 completed fully clean (`sanity_check: passed, signal_rows=2317, top_buys=2314`); 07-06 hit one more transient `run_models` timeout (server load, not a bug) — retry #4 (07-06 alone, no other jobs competing) finally succeeded clean (`sanity_check: passed, signal_rows=2317, top_buys=2315`). **All 3 dates now fully recomputed and passing sanity_check — A26 DONE.** `paper_trade` correctly stays un-run for all 3 (past dates, SPEC-SCHED-006). See BuildLog.md 2026-07-13 "fno_data Shadow-Table Bug" |
| A63 | `tests/quality/test_no_stub_or_synthetic_data.py::test_no_unallowlisted_stub_keywords` fails on 3 pre-existing, benign "placeholder" comments | Data Layer / Tests | ✅ | Found 2026-07-10/11 during the FeatureBacklog full sweep (Groups 1-9). **2026-07-13: fixed** on branch `feature/backlog-burn-a42-a63-a64-a67-a72-ml22-ml26-ml28-ml29-ml30-t9` — added narrow `KEYWORD_ALLOWLIST` entries for the (by then 7, not 3 — 4 more had drifted in from the ML gainer system) confirmed-benign matches: `config/nse_holidays.py`, `datastore/schema/create_normalised.py`, `scripts/align_remaining_to_fyers.py` prose, plus `sklearn.dummy.DummyClassifier` imports in 3 multibagger/signal_ranker model files. `tests/quality/test_no_stub_or_synthetic_data.py` (4 tests) passes. |
| A64 | `tests/unit/test_schema.py::TestCreateSignalsSchema::test_duckdb_table_columns_match_architecture_doc[ml_forensic]` fails — schema/doc drift | Data Layer / Tests | ✅ | Found 2026-07-11 during Group 7's schema-addition work. **2026-07-13: fixed** on branch `feature/backlog-burn-a42-a63-a64-a67-a72-ml22-ml26-ml28-ml29-ml30-t9` — the real DuckDB `ml_forensic` table has 2 columns (`benford_detail_json` from FO5, `forensic_flag_label` from P2.6's flag taxonomy) that the architecture doc (`alphalens_docs/12_platform_architecture.md`) and the test's expected-columns constant hadn't caught up to; both are genuine, already-shipped columns per `create_signals.py`'s own comments, so updated the doc/test to match reality. `ml_forensic` param of `TestCreateSignalsSchema` now passes; `ml_multibagger`/`ml_signals` params still fail with similar drift — out of this item's scope (see new backlog note below, `A64-followup`). |
| A64-followup | `tests/unit/test_schema.py::TestCreateSignalsSchema` still fails for `ml_multibagger` and `ml_signals` (same class of schema/doc drift as A64, different tables) | Data Layer / Tests | ✅ | Found 2026-07-13 while fixing A64. **2026-07-13, re-verified same day**: an intervening session had already reconciled both tables — `datastore/schema/create_signals.py`'s DDL, `alphalens_docs/12_platform_architecture.md`, and `tests/unit/test_schema.py::TestCreateSignalsSchema`'s expected-columns sets for `ml_signals`/`ml_multibagger` now match exactly (confirmed via a direct column-set diff against the DDL, both come out identical). `tests/unit/test_schema.py -k TestCreateSignalsSchema` — 6/6 pass. No code change needed this pass, just re-confirmation + status update. |
| A66 | Framework-wide sortable-columns audit — apply existing `sortRows`/`sortableHeader` helper to every dashboard table, not just the ones that already use it | Dashboard (all) | ✅ 2026-07-13 | Implemented on branch `feature/backlog-burn-a66-a68-a69-a73-t6-t10-ml23-ml25-ml32` (PR: see BuildLog.md 2026-07-13 entry). Rather than a per-file audit/edit of ~40 table-rendering JS files, added a generic DOM-level table enhancer to `js/shell.js` (runs on every screen via `renderAppShell`, re-triggered by a `MutationObserver` after async re-renders) that makes every `<th>` clickable-sortable by re-ordering the rendered `<tr>`s by that column's text, skipping headers a screen already made sortable itself via the existing `sortableHeader` helper (detected by its inline `cursor:pointer`) so there's no double-handling |
| A67 | Sparkline column support — no sparkline rendering exists anywhere in the dashboard; needed for price/RS trend columns across tables (Sector Rotation, Signal Deep Dive, etc.) | Dashboard (all) | ✅ | 2026-07-13: added `sparklineSvg()` (`dashboard/static/js/api.js`), wired into Sector Rotation (ML28). **2026-07-13 (2nd pass):** extended to Signal Deep Dive's Raw Signal Log (`ml/signal.js`) — a new "Trend" column shows the since-recommendation price sparkline for each historical call, reusing the same OHLCV closes already fetched for the recommended-price lookup (no extra API call). Two real consumers now — convention proven framework-wide. |
| A68 | Column-alignment convention — amount fields right-aligned, percentage/range fields center-aligned, across all tables | Dashboard (all) | ✅ 2026-07-13 | Same `js/shell.js` enhancer as A66: sniffs each column's rendered cell text (>=60% of non-empty cells matching a percent/range or numeric-amount pattern) and applies `text-align:center`/`right` accordingly — framework-wide, no per-table edits needed. See BuildLog.md 2026-07-13 |
| A69 | Ticker-hyperlink-to-chart convention (every ticker cell links to `technical/chart.html?ticker=...` in a new tab) + a "Signal Deep Dive" icon column that opens `ml/signal.html?ticker=...` in a new tab, applied uniformly | Dashboard (all) | ✅ 2026-07-13 (partial audit) | Added a shared `tickerCell(ticker)` helper to `js/api.js` (chart.html link + 🔎 Signal Deep Dive icon, both new-tab). Applied to every table that previously rendered a bare, unlinked ticker cell (`forensic/heatmap.js`, `big_investors/index.js` x3, `big_investors/announcements.js`, `big_investors/mf_holdings.js`, `ml/hub.js` x2, `ml/exit_urgency.js`, `ml/multibagger.js`, `ml/universe.js`). Tables that already had their own in-app ticker drill-down link (e.g. `valuation/accuracy.js`/`batch.js` → `dcf.html`, `forensic/universe.js` → `dashboard.html`) were left as-is this pass — not re-audited to also add the chart.html+icon pair, since that's an additive-only follow-up, not a fix for a broken/missing convention. See BuildLog.md 2026-07-13 |
| A71 | Shared 1-year price/technical rollup table — a dedicated per-ticker table storing ~1yr of OHLCV + technical datapoints, so charts/sparklines stop reading from the main OHLCV/indicator tables directly | Data Layer | ✅ | 2026-07-13: ran a real in-process load measurement (`TestClient(app)` against the live, real `alphalens.duckdb` — read-only, no synthetic writes) of `GET /api/v1/ohlcv/{ticker}` across 50 real tickers with >=200 rows. Full-history range (2000-01-01..2026-07-13): mean 133.9ms/median 119.7ms/p95 229.5ms/max 313.9ms. Realistic 1-year range (chart.html's actual use case): mean 57.2ms/median 57.6ms/p95 77.6ms/max 94.1ms — well within an interactive single-chart-load budget, confirming the 2026-07-11 exploration's "no apparent perf problem" note with real numbers rather than a hunch. Also checked whether any screen fetches raw per-ticker OHLCV in a loop for multi-ticker sparklines (the scenario that would most plausibly need a rollup table): grepped all dashboard callers of `sparklineSvg()` — only `dashboard/static/ml/js/sector_rotation.js` uses it today, and it renders from `features/sector_rotation.py`'s already-precomputed `sparkline` field, not a live per-ticker OHLCV fetch, so there is no current N-ticker-sequential-fetch code path in production. A synthetic worst-case (50 sequential 1yr fetches, simulating a hypothetical future 50-row sparkline table) measured ~2.95s total — noted here as the number a future session should re-check against if/when such a screen is actually built, since that would cross into "needs a materialized table" territory. Given today's single-chart real numbers are fast and no multi-ticker live-fetch pattern exists yet, building the new table now would be speculative work with no current perf problem to solve — closing as "no new table needed," per the row's own gating condition. Benchmark script: not committed (scratch, per session policy), rerunnable via `TestClient(app)` + `/api/v1/ohlcv/_meta/tickers` |
| A72 | New cross-cutting Events table (corporate actions, bulk/block deals, 5d/21d/63d & MultiBagger recommendation triggers, forensic-flag dates) + chart overlay showing these as markers on `chart.html` | Data Layer / Dashboard (Technical) | ✅ (3/4 event types) | 2026-07-13 (earlier pass): skipped as multi-part/bigger-than-safe. **2026-07-13 (this pass): implemented 3 of 4 event types + the chart overlay.** New `GET /api/v1/events/{ticker}` (`datastore/api/routers/events.py`) merges: `corporate_action` (read-only reuse of `corporate_actions`), `bulk_deal` (read-only reuse of `bulk_deal_positions`), and `recommendation_trigger` (new query, no new table — detects a ticker crossing INTO a signal_5d "buy" call from real `ml_signals` history, i.e. `signal_direction == 'buy'` where the immediately preceding row wasn't). `chart.html`/`chart.js` gained a real marker overlay: a second Chart.js dataset (`eventOverlayDataset`) plotting a colored triangle just above that day's high at each event's matching candle index, with the real event description in the tooltip — no vendored annotation plugin exists under `dashboard/static/vendor/`, so this is a plain second dataset, not a plugin-based annotation. `forensic-flag date` (the 4th type) still NOT implemented — `ml_forensic` only records composite scores as of each (infrequent, quarterly) scoring date, not a discrete "flag raised on this date" event; defining "the date a flag first appeared" would need its own dedup/definition pass, left as a follow-up. Tests: `tests/unit/test_events_router.py` (4 tests, real seeded DuckDB via TestClient). |
| A73 | Resizable/expandable table columns — user-draggable column width (drag handle on column border), framework-wide convention alongside A66/A68/A69 | Dashboard (all) | ✅ 2026-07-13 | Same `js/shell.js` enhancer: adds a `.col-resize-handle` drag handle to every `<th>`, width persisted per `(page path, header label)` in `localStorage` so it survives reloads — framework-wide, no per-table edits. See BuildLog.md 2026-07-13 |

### Technical

| ID | Item | Area | Status | Blocked On |
|---|---|---|---|---|
| T1 | Docstring says "76 core" indicators, code computes 70 | Dashboard (Technical) | ✅ | — |
| T2 | Phantom equity trading data on real holidays: root cause dig deeper? | Ingestion | ✅ 2026-07-10 | Scraper-layer fix (bhavcopy date validation + `config/nse_holidays.py` full calendar) already landed — but a live DB query found the DATA that fix was meant to prevent was still sitting in the DB from before it landed: 4 real holiday dates (2021-11-04, 2022-10-24, 2024-11-01, 2025-10-21) with phantom rows in `ohlcv_adjusted` (7,135), `fno_data` (199,545), and `ohlcv_ca_audit` (1,786, a companion audit table for the same phantom OHLCV rows) — 208,466 rows total. **Deleted** (transaction-wrapped, confirmed with the user first): all 3 tables now show 0 rows on these dates; verified adjacent real trading days (e.g. 2021-11-03) untouched. `macro_indicators` also had 12 rows on these dates (`USD_INR`/`GOLD`/`CRUDE_OIL`) — correctly left alone, forex/commodities trade globally on Indian holidays, not phantom. Re-ran `check_holiday_leakage` with a 10-year lookback post-delete: 0 findings. No stale `holiday_leakage` Ops Monitor findings existed to clean up. Whether other scrapers share the underlying NSE-archive-serves-stale-file-on-holidays quirk remains unconfirmed — out of this session's scope, tracked separately if it resurfaces |
| T3 | No charting library on Technical > Chart screen | Dashboard (Technical) | ✅ 2026-07-11 | — |
| T4 | Watchlist screen wiring status unresolved | Dashboard (Technical) | ✅ | — |
| T5 | 18 "advanced" TA features computed but unused by any ML training pipeline | ML Signal Engine / Data Layer | ✅ 2026-07-11 | Group 2 backlog sweep: A38 already wired all 297 feature columns (including the 18 advanced TA features) into TFT/BiLSTM's registry; A42's audit this session confirmed 297/297 architecturally reach the input tensor. Closed — "wire into training" was the chosen path, not "stop computing" |
| T6 | Make Daily WatchList the AlphaLens.Technical landing page; add a "Technical Deep Dive" page (5/21/63 DMA, 52wk hi/lo, support/resistance, delivery volumes/%) mirroring Signal Deep Dive, opened via a per-row icon in a new tab | Dashboard (Technical) | ✅ 2026-07-13 | `technical/index.html` now redirects to `watchlist.html` (was `screener.html`). New `technical/deep_dive.html`/`js/deep_dive.js` built against the existing `/api/v1/ta/{ticker}/indicators` (SMA 20/50/100/200 ratios — the real feature set has no literal 5/21/63-day SMA columns, `sma_20/50/100/200_ratio` are the closest real equivalents, used as-is rather than inventing new features) and `/patterns` endpoints, plus reusing the Daily WatchList's already-computed support/resistance levels (`/api/v1/ta/watchlist/daily`, matched client-side by ticker since that endpoint has no per-ticker filter). Delivery volume/% surfaced from `delivery_pct`/`delivery_pct_zscore_21d`/`delivery_price_corr_21d`. Opened via a new 🔎 icon column on `watchlist.html`'s table, new tab. Branch `feature/backlog-burn-a66-a68-a69-a73-t6-t10-ml23-ml25-ml32`, see BuildLog.md 2026-07-13 |
| T7 | "Charts currently do not work" — live-repro and fix `technical/chart.html` | Dashboard (Technical) | ✅ 2026-07-13 | Root-caused via a live Playwright browser session (Chromium): API/wiring was fine (2026-07-11 note below), but `chartjs-chart-financial`'s bar-derived candlestick/OHLC controller miscomputes element pixel positions against a continuous `"time"` x-scale once real trading-day gaps (weekends/holidays) are involved — every candle/line element's `x` resolved to `NaN`, so the chart silently rendered blank (axis + legend only, no console error). Confirmed via `chart.getDatasetMeta(0).data[i].x` before/after. Fix: switched both the candlestick and volume charts in `dashboard/static/technical/js/chart.js` to an index-based `"category"` x-scale with date-formatted tick labels (also removes weekend gaps from the plot). Verified rendering on RELIANCE/TCS/IRFC with zero console/page errors. 2026-07-11: confirmed `chart.html`/`chart.js` is fully wired — `GET /api/v1/ohlcv/{ticker}`, `/api/v1/ta/{ticker}/indicators`, `/api/v1/ta/{ticker}/patterns` all verified returning real data via curl; script load order in `chart.html` is correct. |
| T9 | Technical screener appears not to list the full universe — looks like it's only picking up tickers in alphabetical-order order | Dashboard (Technical) / Data Layer | ✅ | **2026-07-13: confirmed merged into branch `feature/backlog-burn-a42-a63-a64-a67-a72-ml22-ml26-ml28-ml29-ml30-t9`** (already present on the base branch this session started from — `fix/ta-screener-tiebreak-t9-engine`'s commit `543be46` was already an ancestor; no new changes needed, cherry-pick came back empty). 2026-07-11: root cause found in `systems/technical_analysis/screener/engine.py::_screen_df` (`:326-331`) — root cause found in `systems/technical_analysis/screener/engine.py::_screen_df` (`:326-331`) — results are sorted `_score desc, _vol desc` where `_vol` is `volume_ratio_21d`; when that column is absent from a given day's feature set, the tiebreak silently drops and ties fall back to the source Parquet's original row order, which is ticker-alphabetical — producing exactly the "alphabetical-only" symptom for any template/day where `volume_ratio_21d` isn't populated. Fix not yet applied: needs a deterministic secondary sort (e.g. always include `_score desc` then market-cap or ADTV desc, never silently falling through to file order) — small, scoped change to `_screen_df`, next session. **2026-07-13: fix implemented, in PR pending merge (not yet ✅).** No `market_cap`/ADTV column is actually present in the daily feature Parquet, so the fix instead falls through a priority list of available volume/liquidity proxy columns (`volume_ratio_21d`, `volume_ratio_5d`, `volume_zscore_10d`, `vol_spike_vs_60d_avg`, `breakout_volume_ratio`, `turnover_acceleration`) and, as a final always-present tiebreak, sorts by a deterministic hash of `ticker` — so ties can never silently degrade to alphabetical Parquet row order again, on any day regardless of which proxy columns are populated. Added regression tests in `tests/unit/test_ta_screener.py::TestScreenDfTiebreakOrdering` (missing-volume-columns case reproduces the original bug and asserts non-alphabetical order; determinism-across-calls case; and a case confirming `volume_ratio_21d` still takes priority when present, i.e. no behavior change for the common path). Full `tests/unit/test_ta_screener.py` (34 tests) and the broader `tests/unit -k "technical or screener or ta_"` sweep (125 tests) pass. Branch `fix/ta-screener-tiebreak-t9-engine` pushed to origin (named `-engine` suffix because the repo had concurrent multi-agent sessions running in the same working tree during this fix and `fix/ta-screener-tiebreak-t9` had already been claimed by an unrelated tenacity-retry-migration branch by the time of push — see PR: https://github.com/abaldwa/alphalens/pull/new/fix/ta-screener-tiebreak-t9-engine). |
| T10 | Persist every technical recommendation with strategy name + date to DB (verify/extend existing `ta_signals`) | Data Layer | ✅ 2026-07-13 (verified, no gap) | Confirmed via code review of `systems/technical_analysis/alerts/daily_alert_checker.py`'s `_CREATE_TA_SIGNALS_SQL`/`_INSERT_SQL`: `ta_signals`' primary key is `(date, ticker, template_name)` — every full template match (score=1.0) is persisted with its exact strategy name and date already, upserted (not overwritten) so history genuinely accumulates day over day rather than only keeping the latest. `score`/`matched_conditions`/`total_conditions`/`key_values` (JSON) are captured alongside. No code change needed — this was already fully built (SPEC-TA-006/008), just unverified before this pass. See BuildLog.md 2026-07-13 |
| T11 | Multi-strategy consensus: when the same stock is recommended by multiple strategies, list all of them and surface the stock with the most concurrent strategy-recommendations first | Dashboard (Technical) / Features | ✅ 2026-07-13 | Implemented `GET /api/v1/ta/consensus/daily` (`datastore/api/routers/technical.py`) — groups `ta_signals` by (date, ticker), counts distinct `template_name`s firing that day, orders by strategy_count desc then avg_score desc, returns template names/categories/avg score per ticker. Verified end-to-end against the live signals DuckDB (2026-07-10, real multi-template consensus rows e.g. UJJIVANSFB/NORTHARC with 25 concurrent template fires) and via TestClient; added regression tests in `tests/unit/test_technical_router.py::TestConsensusDaily` (27/27 router tests pass). |
| T12 | Sell-recommendation section for stocks previously Buy-recommended by AlphaLens.ML | Dashboard (ML) / ML Signal Engine | ✅ 2026-07-13 | Implemented `GET /api/v1/signals/ml/downgrades/{date}` (`datastore/api/routers/signals.py`, read-only query only — no ML training/inference files touched). Uses signal_5d's already-computed `signal_direction` field (literal "buy"/"sell"/"hold" from `CLASS_NAMES` in `systems/ml_signal_engine/models/signal/base_signal_model.py`, read for context only) rather than inventing a new probability threshold: flags any ticker whose most recent row is "sell" but which had an earlier "buy" row within a configurable `lookback_days` window (default 200, matching this backlog's other trailing-window conventions). Verified end-to-end against the live signals DuckDB (2026-07-10, real buy-to-sell transitions e.g. 3IINFOLTD, AARVI, AETHER) and via TestClient; added regression tests in `tests/unit/test_signals_downgrades.py` (6/6 pass, covering buy-then-sell flagged, always-sell/buy-then-hold not flagged, lookback-window exclusion, carry-forward date resolution). |

### Fundamental

| ID | Item | Area | Status | Blocked On |
|---|---|---|---|---|
| F3 | `systems/fundamental_analysis/{growth,management,peers,quality,sector,thesis}/` — all 6 subpackages are dead stubs | Architecture / Fundamental | ✅ 2026-07-10 | Deleted the empty stub directories; real logic already lived in `features/fundamental_composites.py` |
| F4 | Thesis Builder has no PDF/export feature | Dashboard (Fundamental) | ✅ 2026-07-11 | — |
| F5 | `scripts/ingest_external_fundamentals.py`'s "write" path never persists | Ingestion / Fundamentals | ✅ 2026-07-10 | `DataStoreClient.write_fundamentals`/`write_fundamentals_batch` were already real (confirmed in `datastore/client.py`) — the actual bug was entirely in this script, which logged "would write" and never called anything. Real fix, not just wiring: the CSV source is long/EAV-shaped (`ticker,metric,as_of_date,value`) but `FundamentalsWrite` needs a wide per-quarter row — added `_pivot_to_fundamentals_rows` to group metrics by inferred `(ticker, fiscal_year, quarter)` (quarter_end_date inferred as the most recent standard fiscal quarter-end strictly before each metric's `as_of_date`, satisfying SPEC-PIPE-003's `announcement_date > quarter_end_date`), with a metric-name whitelist so an unrecognized CSV column is dropped+logged, never silently mapped onto the wrong field. Writes directly to DuckDB (SPEC-DS-002 exception, same precedent as the other bulk backfill scripts) rather than through the API's `/write_batch`, since that endpoint hardcodes `fundamentals_source="screener"` server-side and would have mislabeled every row. Added a new lowest-priority `"external_csv": 1` entry to `SOURCE_PRIORITY` (`features/fundamental_source_priority.py`) so this never outranks a real scraped source. 13 new unit tests in `tests/unit/test_ingest_external_fundamentals.py` (pivot logic + real write path, including a priority-safety test confirming a higher-priority existing row is never overwritten) — all pass |
| F6 | Valuation Accuracy screen has zero backend/frontend | Dashboard (Valuation) / API | ✅ | — |

### Big Investors

| ID | Item | Area | Status | Blocked On |
|---|---|---|---|---|
| BI1 | Daily NSE/BSE bulk/block-deal history is still only 1 real day deep for non-superstar participants | Ingestion / Data Layer | ✅ 2026-07-10 | Backfilled to the extent a real source exists (no alternate historical source for non-superstar participants — NSE/BSE's live endpoints don't offer historical date ranges, confirmed unblockable). Ran `scripts/backfill_bulk_deals_trendlyne.py` for real: 0 new rows inserted (all 693 scraped rows already present in `large_deals`, min date already 2010-01-14) — this backfill had already run in a prior session; today's run re-confirmed the anti-join dedup is idempotent. Non-superstar history depth remains a hard external-data-availability limit, not a code gap |
| BI2 | Non-equity Trendlyne deals (InvITs, REITs, etc.) are silently dropped from the bulk-deal backfill | Ingestion | ✅ 2026-07-10 | Confirmed by-design, not a gap (explicit operator decision): `_build_company_name_to_ticker_map` (`ingestion/scrapers/trendlyne.py:1084`) only resolves against `stock_master`, which is equity-only — InvIT/REIT company names simply never match, so they're dropped as an intentional side-effect of ticker resolution, not a missing filter. Would need `stock_master`/ticker-map extension if InvIT/REIT coverage ever becomes in-scope. **Accepted residual risk, documented not fixed**: no explicit "is this an equity" check exists, so a future InvIT/REIT name collision with a real equity ticker would NOT be filtered — low-probability, not acted on this session |
| BI3 | Trendlyne bulk-block-deals page pagination not verified across all 62 investors | Ingestion | ✅ 2026-07-10 | Live-verified all 62/62 investors: every fetch succeeded with zero errors, deal counts ranged 0-201 with no artificial cap pattern (confirms `_parse_bulk_block_deals_table`'s docstring claim that the deals table is fully server-rendered in one page load — `JS_autoDataTables` is client-side sort/search only, not AJAX pagination, so there is no pagination mechanism to handle). Earliest-deal-date per investor spans 2007-2026, tracking naturally with when each investor became notable — not a truncation artifact. Only 2 investors (Sangeetha S, Jayesh Patel) have zero disclosed deals — confirmed genuinely empty (fetch succeeded, page just has no rows), not a fetch failure |
| BI4 | No automated test coverage for Big Investor Activity changes | Tests | ✅ 2026-07-11 | Added `tests/unit/test_big_investors.py` (26 tests) against a real seeded DuckDB fixture: `_position_and_wac_asof`'s trade/checkpoint replay (buy/sell WAC math, undisclosed-sale true-down, undisclosed-purchase true-up at nearest close), `_parse_bulk_block_deals_table` (real row shape, dash-price-to-None, missing-table/short-row edge cases), `backfill_bulk_deals_history`'s NOT EXISTS dedup anti-join (new row inserted, exact duplicate skipped, same-day-different-client both kept), and MF Holdings movers' `scheme_count_change` (increasing + new-entry cases) via `TestClient` |
| BI5 | `holding_pct_of_company` / shares-outstanding estimate is a back-derivation, not a real share count | API / Data Layer | ✅ 2026-07-11 | Cross-checked the `market_cap_cr * 1e7 / cmp` estimate against real `fundamentals.shares_outstanding` for 1,559 tickers with both fields populated (latest quarter each). Median absolute drift 3.3%, 69% of tickers within 5%, 93% within 15% — the estimate is sound for the bulk of the universe. A real tail diverges badly (IDEA: estimate implies ~113B shares vs. `shares_outstanding`=1,083,430 — a ~10,000,000% drift), traced to implausible/misscaled values already sitting in `fundamentals.shares_outstanding` itself (a data-quality issue in the source field, not in the back-derivation formula) — worth a follow-up plausibility sweep of `fundamentals.shares_outstanding` outliers, not attempted here (out of BI5's scope, which was to quantify the estimate's drift) |
| BI6 | "unmapped:" family ↔ Trendlyne holder-name matching is a string-normalization heuristic | Data Layer | ✅ 2026-07-11 | Added `_fuzzy_match_unmapped_family` (`datastore/api/routers/big_investors.py`), wired as a fallback in `_position_and_wac_asof` when the existing exact `normalize_client_name` re-match misses. Two independent, deliberately conservative heuristics (either accepts a match): token-Jaccard over stopword-filtered word sets (catches a missing/extra "AND ASSOCIATES" suffix or reordered tokens) and a same-token-count positional prefix check (catches an abbreviated middle name/initial, e.g. "HITESH R JAVERI" vs "HITESH RAMJI JAVERI"). Deliberately did NOT use raw edit-distance ratio — verified it scores "ASHISH KACHOLIA" vs "ASHOK KACHOLIA" (two different real superstar investors) at 0.80 similarity, uncomfortably close to true-positive matches, so the safer structural check was used instead. Ambiguous multi-candidate matches resolve to no-match, not a guess. Covered by 15 new tests in `tests/unit/test_big_investors.py` (direct heuristic unit tests + DB-replay integration tests proving both a real near-miss match and a real false-positive rejection) |

### Damodaran

| ID | Item | Area | Status | Blocked On |
|---|---|---|---|---|
| D1 | 3 failing `test_damodaran.py` sector-alias tests | ML / Valuation Tests | ✅ 2026-07-10 | Already fixed in working tree (found pre-updated, not this session's code change) — tests now assert the real NSE sector string `"Financial Services"` (no separate Banking/NBFC/Insurance tag exists in NSE's taxonomy) instead of the stale per-subsector expectations; `test_financial_services_{banking,nbfc,insurance}` all pass |
| D2 | No router-level tests for `datastore/api/routers/valuation.py` | Valuation / Tests | ✅ 2026-07-11 | New `tests/unit/test_valuation_router.py` (18 tests) covers `/{ticker}`, `/batch/ranked`, `/{ticker}/sensitivity`, `/{ticker}/history`, `/{ticker}/relative` — the endpoints `test_valuation_accuracy.py` (Group 3, F6) didn't touch |

### Forensic

| ID | Item | Area | Status | Blocked On |
|---|---|---|---|---|
| FO5 | Benford's Law screen only surfaces one aggregate MAD float | Dashboard (Forensic) / API | ✅ | — |
| FO6 | Investigation Report has no PDF/report-builder backend | Dashboard (Forensic) | ✅ 2026-07-11 | — |
| FO7 | Universe Scan has no on-demand trigger | Dashboard (Forensic) / API | ✅ | — |
| FO9 | `altman_z` still NaN for a real subset of tickers | ML Signal Engine / Data Layer | ✅ 2026-07-13 | Fixable portion (ebit column, real retained_earnings, PIT market-cap join) merged into `feature/backlog-burn-t7-t8-t11-t12-fo9` — see FO1. Remaining NaN gap for `current_assets`/`current_liabilities`/`total_debt`/`revenue` co-availability is a genuine NSE FY2023-24+ XBRL filing-regime data-coverage floor, not fixable in code — tracked as the known residual, not a further open action item. 2026-07-13 (prior investigation): measured against latest-quarter fundamentals for 2643 tickers (PR `fix/forensic-altman-pit-wiring`, pending merge, same fixes as FO1). Field-level coverage on this snapshot: `current_assets`&`current_liabilities` (working capital term) 62.8%, `total_debt` 36.8%, `revenue` 40.5%, `retained_earnings` (real column, no proxy) 26.0% (proxy-or-real together 27.0%), `ebit`-or-`ebitda` 40.5%, market-cap term via real close-price x shares_outstanding 89.3% (up from ~25% via the old book-equity-only proxy — this is the fix's real, verified effect). The overall Altman-computable count (109/2643, 4.1%) is capped by the `current_assets`/`current_liabilities`/`total_debt`/`revenue` intersection, all ~37–41%, not by `retained_earnings`/`ebit`/market-cap (all now meaningfully higher or unchanged-but-correct) — so the residual NaN gap is dominated by the current_assets/current_liabilities/total_debt NSE-filing-regime floor (FY2023-24+ XBRL Integrated Filing start date), the same unclosable constraint documented for FO1/FO3, not by a wiring bug. Not marking ✅ until merged. |

### Corporate Announcements

| ID | Item | Area | Status | Blocked On |
|---|---|---|---|---|
| CA1 | Triage 174 likely-missing-split tickers vs Fyers, backfill `corporate_actions` | Data Layer / Ingestion | ✅ | 70/174 fixed, rest reclassified — see writeup below |
| CA2 | KANSAINER/AJOONI non-monotonic price-ratio investigation | Data Layer / Ingestion | ✅ 2026-07-11 | Group 7 backlog sweep: real NSE-confirmed data-quality bugs found and fixed — KANSAINER had two mislabeled `corporate_actions` rows (SPLIT should've been BONUS 1:1; a SPLIT ratio was 30 instead of 10) plus a missing 2023-07-04 Bonus 1:2; AJOONI's SPLIT ratio was 7.5 instead of 5.0 (FV Rs10→Rs2), plus two missing RIGHTS actions inserted for tracking (no OHLCV rescale — `price_adjuster.py` has no RIGHTS formula, same documented limitation as CA1). Post-fix mismatch dropped from wildly non-monotonic to a flat ~1.17-1.18%, matching CA3's known residual dividend-adjustment gap — confirms the fix is complete |
| CA3 | Assess 152 higher-cv Fyers-mismatch tickers | Data Layer / Ingestion | ✅ 2026-07-11 | Group 7 backlog sweep: spot-checked 20/152 tickers (ITC, HEROMOTOCO, POWERGRID, HCLTECH, NTPC, SAIL, COALINDIA, ONGC, GAIL, NHPC, BPCL, IOC, PFC, RECLTD, NATIONALUM, HINDZINC, NMDC, CESC, COLPAL, MANAPPURAM) — all show smooth monotonic decay to 0% by the latest date, not a step-jump, confirming the dividend-adjustment-convention gap hypothesis (`PRICE_ADJUSTMENT_ENABLED=False` for dividends is a deliberate existing setting). No code change — closing this gap is a feature decision (enable dividend adjustment), not a bugfix |
| CA4 | Corporate-action validation tracking (`corporate_actions_validation`) | Data Layer / Ingestion | ✅ | 967/967 rows processed. 2026-07-11 (Group 7 backlog sweep): reconciled the 70 (down from 77) `needs_retrain=TRUE` tickers against CA1/CA2/CA3's lists — 16 CA1-collision, 6 CA1-no-match, 1 CA1-reclassified, 21 overlap CA3's dividend gap, leaving 26 genuinely new tickers flagged for a future triage pass (see writeup below). `corporate_actions_validation`'s DDL added to `datastore/schema/create_normalised.py` (was previously live-DB-only, would've been silently lost on a rebuild) |
| CA6 | Real NSE filing endpoints found but not yet built into a pipeline (BRSR, QIP, shareholding, RPT, governance) | Ingestion | ✅ 2026-07-10 | Built what's actually buildable: BRSR (`api/corporate-bussiness-sustainabilitiy`) and QIP (`api/corporate-further-issues-qip`) both live-verified against real tickers (RELIANCE / IDFCFIRSTB+ZOMATO) and fully implemented — new `ingestion/scrapers/nse_brsr_qip.py` + `scripts/backfill_nse_brsr_qip.py` + 2 new schema tables (`qip_details`, `brsr_filings`). Full-universe backfill run completed against all 2,643 tickers: 186 real qip_details rows, 1,268 real brsr_filings rows now live in the DB. BRSR scope deliberately limited to the filing INDEX (submission date + XBRL file URL), not deep-parsing the linked XBRL for individual ESG metrics — a much larger, separately-scoped effort. RPT (`api/related-party-transactions-details`) and governance (`api/corporate-governance`) remain explicitly blocked — both need a secondary lookup param (`seqNum`/`recId`) from an undiscovered master-list endpoint; not guessed at, same "verify before build" discipline as every other scraper here. `mf_pct`/`shareholding` via `api/shareholding-patterns-sdd` also remains blocked — the breakdown is embedded in an iXBRL HTML document requiring an XBRL parser, out of this session's scope |

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
| ML12 | Daily sector rotation report | Features / ML Signal Engine | ✅ 2026-07-11 | Steps 4-6 built: `config/sector_index_map.py`, `features/sector_rotation.py`, `GET /api/v1/sector_rotation/report`, AlphaLens.ML "Sector Rotation" screen. Historical backfill (2023-07-01 → 2026-07-08) run in background this session. |
| ML14 | Multibagger survival-curve labeling fix (flat at 100%) | ML Signal Engine | ✅ | — |
| ML15 | RuleBasedExitPolicy: volatility-scaled target/stop | ML Signal Engine | ✅ | — |
| ML16 | Backdated Entry — relocate off main Paper Trading page | Dashboard (Paper Trading) | ✅ | — |
| ML18 | `ExitSignalModel` training fails on real data: CoxPH ConvergenceError + predict() drops rows | ML Signal Engine / Tests | ✅ 2026-07-11 | Group 2 backlog sweep: fixed two real bugs in `exit_signal.py::train_full()` — a collinearity bug (`days_held` duplicated as both a covariate and the Cox duration column, plus an always-NaN `days_to_next_earnings` column feeding the fit) causing the `ConvergenceError`, and the `predict()` row-count reconciliation bug. Reproduced via an in-memory synthetic fixture (never touching the real DB), confirmed fixed. `tests/unit/test_exit_signal.py`: 12 passed, 14 correctly skipped (only 3 real closed trades exist vs. the 200-trade floor) |
| ML19 | `test_multibagger.py`/`test_paper_trading_router.py` fail only when run inside the full suite, pass standalone | Tests | ✅ 2026-07-11 | Not reproducible after extensive re-bisection — see writeup below |
| ML20 | `test_score_multibagger.py`/`test_rule_based_exit_policy.py` real-data cases require a live DataStore API server, not gated/skipped without one | Tests | ✅ 2026-07-11 | `test_score_multibagger.py` already DB-direct (no live-server dependency); `test_rule_based_exit_policy.py`'s ATR real-OHLCV cases now `pytest.skip` on `httpx.RequestError` instead of hard-failing — see writeup below |
| ML21 | SMOTETomek unbounded oversampling causes repeated OOM in signal_63d retrain | ML Signal Engine / Tests | ✅ 2026-07-10 | Subprocess isolation per horizon + fewer Optuna trials for signal_63d adopted; SMOTETomek sampling-strategy cap built but left opt-in (default unchanged) pending a real before/after Sharpe comparison |
| ML17 | Unified backtest strategy (per-horizon, Nifty benchmark) | Backtest | ✅ | (a) Real Nifty 500 benchmark curve ✅ 2026-07-11 — `backtest/engine.py` now computes `benchmark_cagr`/`benchmark_sharpe`/`excess_return` per fold. (b) **2026-07-13: per-horizon reporting restructured.** New `backtest/report_utils.py::write_per_horizon_reports()` (pure function over already-computed `BacktestResults.to_dict()` dicts — no engine/training changes) writes one standalone JSON report per horizon variant, in addition to each script's existing combined comparison report; wired into both `run_phase2_backtest.py` (writes `phase2_signal_5d_*.json`/`phase2_signal_63d_watchlist_*.json` alongside the combined `phase2_*.json`) and `run_phase3_backtest.py` (`phase3_signal_5d_p2baseline_*.json`/`phase3_signal_21d_p3variant_*.json`). Each horizon's own fold-level results + real-benchmark comparison now stand on their own, independent of whichever other variant that script ran alongside it. Not run as a real (multi-hour) backtest this session — verified via `tests/unit/test_backtest_report_utils.py` (3 tests, injected results dicts) plus a real module-import smoke check of both scripts. |
| ML22 | Merge Daily Insights and Daily WatchList screens — significant column/purpose overlap | Dashboard (ML) | ✅ 2026-07-13 | Merged on branch `feature/backlog-burn-ml22-ml29-ml33dev`: `dashboard/static/ml/index.html`/`js/hub.js` now also render the full Daily WatchList tables (5d/21d/63d horizon + MultiBagger + low-liquidity, ported from the old `watchlist.js`) below the existing regime/alerts/top-buys/positions sections. The hub's own truncated "watchlist-mini" (MB top-3) and "horizon-mini" (21d/63d top-3) sections were dropped as duplicates of the same `/api/v1/watchlist/daily` data with fewer columns — no columns were lost, only the redundant shorter view. `watchlist.html` now redirects to `index.html` for old links; `js/shell.js`'s ML sub-tab nav collapsed to one "Daily Insights & WatchList" entry. |
| ML23 | Surface SHAP-derived descriptive "Basis" text in table rows, not only on the Signal Deep Dive detail view | Dashboard (ML) / ML Signal Engine | ✅ 2026-07-13 | Added `shap_top5_json` to `SignalUniverseRow`/`GET /api/v1/signals/ml/universe/{date}` (datastore/api layer only — no model logic touched) so the row already carries it. New `shapBasisText()` in `ml/js/universe.js` parses it client-side and renders the top-2-by-magnitude SHAP features as a short "Basis" column (e.g. `sma_50_ratio (+0.12), rs_vs_nifty500_21d (+0.08)`) on the Full Universe table (moved there by ML25); full 5-feature bar breakdown remains on the ticker detail view. See BuildLog.md 2026-07-13 |
| ML25 | Split "Full Universe" out of Signal Deep Dive (`ml/signal.html`) into its own page; Signal Deep Dive keeps only the per-ticker detail section | Dashboard (ML) | ✅ 2026-07-13 | New `ml/universe.html`/`js/universe.js` (added to the ML app's sub-tab nav as "Full Universe"). `ml/signal.html`/`signal.js` had the full-universe table/`loadUniverse()`/sort-state code removed, keeping only Ticker Detail + History + Model Scores + SHAP + Price + Regime History. Double-clicking a row on the new Full Universe page opens `signal.html?ticker=...` in a new tab (was: scroll-to-section on the same page) — consistent with A69's new-tab convention. See BuildLog.md 2026-07-13 |
| ML26 | Signal Deep Dive layout redesign: Forensic Score, MultiBagger Score, 52wk hi/lo up top; Recommendation History as paired Buy-date/Sell-date/Buy-price/Sell-price/CMP/rationale rows (collapsing a Buy that persists across N days into 1 row); per-horizon (5d/21d/63d) meta-label probabilities + range + Q50 return; SHAP explanation; all raw model scores moved to the bottom | Dashboard (ML) | ✅ (pairing logic) | 2026-07-13 (earlier pass): skipped — buy/sell-pairing needed its own focused pass. **2026-07-13 (this pass): implemented.** New `pairBuySellHistory()` (`dashboard/static/ml/js/signal.js`) walks the already-fetched signal_5d call history ascending-by-date and collapses a Buy signal that persists across N consecutive days into one paired row (Buy-date/Buy-price/Sell-date/Sell-price/CMP/rationale). Edge cases handled conservatively: an unmatched Buy (no Sell call yet) reports CMP instead of a Sell-date/price; a Buy→Sell→Buy sequence (re-entry) produces two separate paired rows, never merged; extra consecutive Sell calls after a position already closed are ignored (nothing to pair them with) rather than fabricating a match; "hold" calls neither open nor close a position. Rendered as a new "Recommendation History & Sell Rationale" section (paired) above the pre-existing raw per-call "Raw Signal Log" table (kept, relabeled) on `signal.html`. Verified via a real Node invocation of the extracted pairing function against a constructed buy/persist/sell/re-entry sequence (no JS test runner exists in this repo) — confirmed 3-day Buy persistence collapses to one row, first Sell closes it, a subsequent extra Sell is ignored, and the re-entry Buy gets its own unmatched-open row. The broader layout redesign (Forensic/MultiBagger/52wk-hi-lo reordering, per-horizon meta-label panel, raw scores moved to bottom) is NOT done this pass — scoped narrowly to the pairing logic this item's own note flagged as the missing piece. |
| ML29 | Sector accumulation detection: (sum of each stock's delivery % × volume) / sector's total outstanding shares, tracked daily, to surface sectors under constant accumulation; drill-down by clicking a sector's %age | Features / Dashboard (ML) | ✅ 2026-07-13 | Implemented on branch `feature/backlog-burn-ml22-ml29-ml33dev`: new `features/sector_accumulation.py` (`compute_sector_accumulation`/`sector_accumulation_drilldown`) joins `ohlcv_adjusted` (volume, delivery_pct) with `fundamentals` (shares_outstanding, PIT-gated on `announcement_date` via `pd.merge_asof`, never `quarter_end_date`) per `config.universe.load_universe()` sector membership; sector total outstanding shares = simple sum of each constituent's own `shares_outstanding` (user decision). New `GET /api/v1/sector_accumulation/daily` + `/drilldown` endpoints (`datastore/api/routers/sector_accumulation.py`), and a new "Sector Accumulation" table on the existing Sector Rotation dashboard page (`ml/sector_rotation.html`/`js/sector_rotation.js`) with a click-to-drill-down per-stock breakdown. 9/9 new tests pass (`tests/unit/test_sector_accumulation.py`), including a PIT-correctness regression test and a no-guess-on-missing-data test. |
| ML34 | `fno_data` shadow-table bug: a stray, empty, pre-A50-migration `fno_data` table left in `alphalens.duckdb`'s own `main` schema silently shadowed the real 120.7M-row companion file (`fno_db.fno_data`) for every unqualified query | Data Layer | ✅ 2026-07-13 | Found while retrying A26's force-run: 2026-07-06/07 both failed `sanity_check` on 16 all-NaN F&O/options columns (`pcr_oi`, `iv_call`/`iv_put`, `max_pain_level`, etc). Root cause: A50 (2026-07-10) moved `fno_data` into its own file (`alphalens_fno_data.duckdb`, ATTACHed as `fno_db`) with `search_path='main,fno_db.main'` so every existing unqualified `fno_data` reference keeps working transparently — but a leftover local `fno_data` table from before that migration was still sitting in the main file's `main` schema (confirmed via `information_schema.tables`: 0 rows, correct schema, zero code references anywhere in the repo), and `search_path` checks `main` first, so it silently won every unqualified resolution instead of the real, correctly-populated companion file. Verified 0 rows twice (including inside the same write transaction immediately before the drop) and confirmed no code path explicitly targets `alphalens.main.fno_data`, then dropped it (with explicit user sign-off — Auto Mode's safety classifier correctly gated an unprompted `DROP TABLE` on production). Verified post-fix: `information_schema.tables` shows exactly one `fno_data` (in `fno_db`), and unqualified `SELECT COUNT(*) FROM fno_data` now correctly returns 120,723,287. A26's retry re-launched to confirm the sanity_check failures are actually resolved — see A26's row above and BuildLog.md 2026-07-13 |

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

### A34 — `step_download_fno` may share A31's unwrapped-DB-write gap — FIXED 2026-07-09

Found while fixing A31: `step_download_fno` (`ingestion/scheduler/
daily_pipeline.py`) followed the same shape as `step_download_index_ohlcv`
did before the A31 fix — the scraper fetch (`fno.download_fno_bhavcopy`)
was wrapped in a `try/except` that catches and logs, but the subsequent
`DELETE FROM fno_data` + `executemany` DB write happened outside that
`try/except`. If that write ever hit a DuckDB lock conflict (same
`SPEC-SCHED-013` cross-process race that caused A31's `2026-07-06`
failure), it would fail the whole step despite the docstring documenting
it as always-non-critical ("Returns None — Always... failures are caught
and logged, never raised"). Not confirmed to have failed live — no
matching error had been seen in `logs/daily_pipeline.log` for
`download_fno` — but fixed proactively rather than waiting for a live
occurrence, since the fix is a mechanical widen-the-try/except identical
to A31's.

**Fix**: moved the row-building + `DELETE FROM fno_data` +
`conn.executemany(...)` block inside the same `try` that already wraps
`fno.download_fno_bhavcopy(date_str)`, matching
`step_download_index_ohlcv`'s A31 fix exactly — any exception during the
fetch *or* the write now logs a warning and returns `None`, never raises.

**Tests**: `tests/unit/test_daily_pipeline.py::TestStepDownloadFno::
test_db_write_failure_is_caught_and_non_fatal` — monkeypatches
`get_duckdb_connection` to raise a DuckDB lock-conflict `RuntimeError`
(mirroring A31's `test_db_write_failure_is_caught_and_non_fatal` for
`step_download_index_ohlcv`) and asserts the step doesn't raise. Full
`TestStepDownloadFno` class (4 tests) and the whole
`test_daily_pipeline.py` suite (26 tests) pass with no regressions.

### A35 — screener source can't join A25 staged publish without an architecture change — FIXED 2026-07-09

Found while doing A25's full rollout (2026-07-09) to the remaining raw
sources. Every other source that gained `--publish-mode staged`
(trendlyne, nse_xbrl, amfi, corporate_actions) is a batch script: it
already accumulates many rows in memory before writing, so switching the
final write from many small SQL statements to one merge + one atomic
`CREATE OR REPLACE TABLE` swap is a mechanical change with the same
net effect.

`screener` is architecturally different: `ingestion/scrapers/
screener.py::batch_export()` wrote **one ticker at a time**, over HTTP,
via `datastore/client.py`'s `write_fundamentals()` → the DataStore API's
`POST /fundamentals/write` (`datastore/api/routers/fundamentals.py`),
which does a single-row `INSERT ... ON CONFLICT DO UPDATE` per call. This
was a live, request-per-row design, not a script that owns a DuckDB
connection for a whole batch. Two options were scoped (see prior
writeup); **per operator decision (2026-07-09), option 1 (client-side
batching) was chosen.**

**Fix — client-side batching:**
1. New `POST /api/v1/fundamentals/write_batch` endpoint
   (`datastore/api/routers/fundamentals.py`) — accepts
   `FundamentalsWriteBatch{records: List[FundamentalsWrite]}`, runs each
   row through the same SPEC-PIPE-003 check + A36's range-validation gate
   + priority stamping as `/write` (both now share
   `_build_fundamentals_row`/`_validate_and_check_pit` helpers so the two
   endpoints can't drift), then does ONE `conn.executemany(...)` inside a
   single `get_duckdb_connection(...)` acquisition for the whole batch. A
   bad row (e.g. an announcement_date/quarter_end_date violation) is
   isolated and counted in the response's `failed` field, never aborting
   the rest of the batch. Returns `FundamentalsWriteBatchResult{written,
   failed}`.
2. `datastore/client.py::write_fundamentals_batch(records)` — thin POST
   wrapper, same shape as the existing `write_fundamentals`.
3. `ingestion/scrapers/screener.py::batch_export()` — fundamentals
   records are now accumulated in memory and flushed via ONE
   `write_fundamentals_batch()` call every
   `config.settings.SCREENER_BATCH_EXPORT_CHUNK_SIZE` (default 50)
   tickers, instead of one `write_fundamentals()` POST per ticker. This
   is a deliberate **partial-checkpoint compromise**, not a single
   end-of-run flush: a crash mid-run loses at most one chunk's worth of
   already-fetched-but-unflushed tickers (worst case 49 tickers with
   chunk=50), not the old design's "every ticker lands the instant it's
   fetched" durability, but also not "lose an entire multi-hour run" the
   way one single end-of-run flush would. Shareholding is **not**
   batched — still written per-ticker via `write_shareholding()`, exactly
   as before; A35 is specifically about the `fundamentals` table's A25
   staged-publish gap, not a general screener redesign.

**Not done (explicitly out of scope for this fix):** wiring
`write_batch`'s DuckDB write itself through A25's `datastore/staging`
module (`stage_dataframe`/`publish_table`) — it currently does a direct
`executemany` upsert (same transaction-per-batch benefit A25 was after —
one write-lock acquisition per chunk instead of per ticker — without
taking on `merge.py`'s `coalesce_merge` semantics, which don't have a
priority-aware mode yet). If staged-publish parity with the other 4
sources becomes a real requirement later (e.g. for N=7 rollback coverage
specifically on screener-sourced rows), that's a follow-up, not
re-opening A35.

**Tests**: `tests/unit/test_fundamentals_write_batch.py` (3 tests, real
FastAPI app + isolated on-disk DuckDB, same pattern as
`test_pit_alignment.py`) — many-rows-in-one-request, one-bad-row
isolation, and A36's priority logic still applying when writes arrive via
the batch endpoint. `tests/unit/test_screener.py::TestBatchExport`
updated for the new batching behavior + a new
`test_fundamentals_flush_after_chunk_size_reached` case. **Real bug
caught during test-writing**: the first draft of `_flush()` passed the
live `pending_fundamentals` list reference into
`write_fundamentals_batch(...)`, then called `.clear()` on that same list
right after — silently mutating whatever the caller (a test's `Mock`, or
in production nothing since the HTTP layer copies at serialization time,
but not guaranteed) had just captured. Fixed by passing
`list(pending_fundamentals)` (a copy) before clearing.

### A36 — `fundamentals` table has 4 writers with inconsistent upsert-conflict precedence — FIXED 2026-07-09

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
`(ticker, fiscal_year, quarter)` depended on which pair of sources was
involved and which ran more recently, not a single documented priority
order. **Per operator decision (2026-07-09), the real priority order is:
NSE XBRL > Trendlyne > Screener > Kaggle** — NSE XBRL is the regulatory
filing itself (already established as the preferred source for the
fields it uniquely covers, per the 2026-07-07 `_CREATE_FUNDAMENTALS`
comment); Trendlyne and Screener are both third-party renderings of the
same underlying filings (Trendlyne ranked above Screener, matching
Trendlyne's original existing-wins COALESCE being this project's
most-audited precedent); Kaggle is a one-time historical seed load,
correctly lowest since it predates every live scraper.

**Fix:**
1. **`features/fundamental_source_priority.py`** (new) — single source of
   truth for `SOURCE_PRIORITY = {"nse_xbrl": 4, "trendlyne": 3,
   "screener": 2, "kaggle": 1}` and `build_priority_update_clause(columns)`,
   a shared SQL builder for the `ON CONFLICT ... DO UPDATE SET` clause
   every writer now uses — replacing 4 independently hand-written
   COALESCE directions (the bug itself) with one. Per-column semantics: a
   NULL incoming value never blanks an existing one (unchanged "additive
   write" contract); on a REAL conflict (both sides non-NULL), the higher
   `fundamentals_source_priority` wins; a row with no recorded priority
   (written before this fix) is treated as priority 0, so any known
   source can win against it.
2. **Schema**: `fundamentals` gained two nullable columns —
   `fundamentals_source VARCHAR`, `fundamentals_source_priority INTEGER`
   (row-level provenance, not per-field — matches this table's existing
   granularity; a much larger per-field-provenance migration was judged
   out of scope for this fix). Added via both the `CREATE TABLE`
   DDL and an idempotent `ALTER TABLE ... ADD COLUMN IF NOT EXISTS`
   migration (`datastore/schema/create_normalised.py`), same
   self-healing pattern as every other AS-BUILT column addition to this
   table.
3. **All 4 writers updated** to use the shared clause and stamp their own
   priority on every write:
   - `scripts/backfill_fundamentals_trendlyne.py` — `_UPSERT_SQL` rebuilt
     from `build_priority_update_clause`; writes `("trendlyne",
     SOURCE_PRIORITY["trendlyne"])`.
   - `scripts/backfill_fundamentals_nse_xbrl.py` — direct-mode
     (`--publish-mode direct`, the default) update clause rebuilt the same
     way; writes `("nse_xbrl", SOURCE_PRIORITY["nse_xbrl"])`. **Also
     wired into `validate_and_annotate` for the first time** (the other
     A36 finding) — this writer's `_TARGET_COLUMNS` (`current_assets`,
     `current_liabilities`, etc.) overlap `RATIO_RANGES`'s leverage
     checks, so this closes a real range-validation gap, not a no-op.
     (Staged mode's `coalesce_merge(new_wins=True)` path is unchanged —
     out of scope, see A35's writeup for why staged-mode semantics
     weren't touched this session.)
   - `scripts/load_kaggle_fundamentals.py` — kept `ON CONFLICT DO
     NOTHING` (already correct for kaggle's lowest priority — a one-time
     seed load should never overwrite any live scraper's row), but now
     also stamps `("kaggle", SOURCE_PRIORITY["kaggle"])` on the INSERT
     branch so a kaggle-seeded row has real recorded provenance for later
     conflict resolution, instead of being treated as unranked (priority
     0) forever.
   - `datastore/api/routers/fundamentals.py`'s `POST /fundamentals/write`
     (screener) — rebuilt on the shared clause; **also wired into
     `validate_and_annotate` for the first time** (the other A36
     finding — confirmed via the endpoint's own code, it had never called
     this gate). New `quality_flag`/`quality_flag_reason`/
     `fundamentals_source`/`fundamentals_source_priority` columns added to
     `_COLUMNS`/`_SELECT_COLS`. Row-building logic extracted into a
     shared `_build_fundamentals_row` helper, reused by A35's new
     `/write_batch` endpoint so the two can't drift from each other the
     same way the original 4 writers drifted.

**Tests**: `tests/unit/test_fundamental_source_priority.py` (6 tests,
real in-memory DuckDB `fundamentals` table) — priority-order constant
check, higher-priority-wins/lower-priority-can't-overwrite on a real
conflict, the full nse_xbrl→trendlyne→screener→kaggle precedence chain,
NULL-never-blanks regardless of priority, and an unranked legacy row
losing to any known source. `tests/unit/test_schema.py`'s
`NORMALISED_TABLE_COLUMNS["fundamentals"]` fitness function updated for
the two new columns. Full regression run (`test_daily_pipeline.py`,
`test_schema.py`, `test_trendlyne.py`, `test_nse_xbrl_financials.py`,
`test_fundamental_quality_gate.py`, `test_pit_alignment.py`,
`test_screener.py`, `test_tijori.py`, `test_fundamentals_write_batch.py`,
`test_datastore_client.py` — 160 tests) and
`tests/quality/test_duckdb_connection_discipline.py` pass with no
regressions from this session's changes (the one pre-existing
`tests/quality/test_no_stub_or_synthetic_data.py` failure was confirmed
via `git stash` to predate this session, unrelated files).

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

**Fixed 2026-07-10.** Checked `train_deep_models.py`/`tft_model.py`'s
actual save path (`schedule_overnight_training`'s `output_dir /
f"{model}_signal_{horizon}d_v{version}_fold{N}"`, default `output_dir=
"datastore/models"`) — the flat layout these orphaned files use **is**
the current convention; the "should be under `datastore/models/tft/`"
premise in this item's original framing was wrong (there is no such
subdirectory wiring anywhere in the deep-model code). So this was a
registry-only gap, not a file-layout migration:
- Loaded `tft_signal_21d_v20260701_fold0.pt` and
  `bilstm_signal_21d_v20260701_fold0.pt` with the current
  `TFTSignalModel`/`BiLSTMSignalModel.load()` (using each `.json`
  sidecar's `hyperparams.n_features=297` to reconstruct the model) —
  both load cleanly, confirming they're still real/valid, not stale-
  architecture artifacts.
- Backfilled `datastore/models/registry.json` with `tft`/`bilstm` entries
  (`last_trained_date: 2026-07-01`, `folds_trained: 3`, `horizon_days:
  21`, `backfilled_2026_07_10: true` for auditability) pointing at the
  `*_v20260701_fold{0,1,2}.pt` set, matching `_update_registry()`'s exact
  schema so `_execute_model_training_job`'s overdue-check now sees a real
  prior training date instead of "never."
- Archived the superseded older rounds (`tft_signal_21d_v20260624_fold0`,
  `tft_signal_21d_v20260630_fold0`, `bilstm_signal_21d_v20260630_fold0`,
  `.pt`+`.json` each) to `datastore/models/_archive_pre_a38/` — not
  deleted, in case anyone wants to diff/compare across the v20260624 →
  v20260701 progression later.

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

---

### A24 — UI refactor for responsive layout
All 5 dashboard apps currently render fixed desktop-width layouts. Needed
specifically to make A22's remote/mobile access actually usable — SSH'ing
in via Tailscale to a phone browser that renders a desktop-width table is
not a real solution to the "check the dashboard from my phone" ask. Scope
TBD (breakpoint strategy, whether tables collapse to cards on narrow
widths, touch target sizing) — flagged here as a dependency of A22 rather
than designed in detail yet.

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

### A66-A72 — "Create additional Features" framework/UI/data-layer asks (logged 2026-07-11)

**Update 2026-07-11:** A70 implemented and moved to `FeatureBacklogImplemented.md` — see there for details. A66-A69, A71, A72 remain open, scoped per the phased implementation plan in this session's BuildLog entry.

Sourced from a user-provided requirements dump (`Create additional Features.txt`).
Cross-referenced against the 2026-07-11 exploration pass before logging: the
generic sortable-table helper (`sortRows`/`sortableHeader` in `js/api.js`)
already exists and is reused by several screens (A66 is an audit-and-apply
gap, not new infra); no sparkline implementation exists anywhere (A67); no
column-alignment convention is currently enforced (A68); ticker-hyperlink
and Signal-Deep-Dive-icon conventions are inconsistently applied today
(A69); the app-switcher's `"AlphaLens."` prefix is the one half of the
"2 sliders" complaint that's fixable in isolation — the sub-tab bar will
still scroll on screen-heavy apps (A70); `chart.html` already reads
directly from `ohlcv_adjusted`/the OHLCV API without an apparent
performance problem, so A71 (a dedicated 1yr rollup table) is left as a
"measure first" item rather than pre-built speculatively; `corporate_actions`
and `bulk_deal_positions` already exist and cover half of A72's proposed
Events table — only the recommendation-trigger/forensic-flag event types
and the chart overlay itself are net-new.

**Update 2026-07-13:** a fresh point-by-point cross-check of the full
`Create additional Features.txt` doc against this file (dispatched via the
product-owner review agent) confirmed 27 of 28 items are already covered
by A66-A72/T6-T12/ML22-ML32 or elsewhere (A36 for the Trendlyne-priority
policy question, A70 for the menu-slider fix). One genuine gap found and
logged as A73: resizable/expandable table columns (item 1c of the
source doc), distinct from A66/A67/A68/A69, not covered by any existing
entry or infra.

---

---

## Technical

### T1 — Docstring says "76 core" indicators, code computes 70 — ✅ 2026-07-11
`CORE_TECHNICAL_FEATURES` in `features/technical.py` (`assert len(...) == 70`)
is the real, verified count — the code, not the docstring, was correct.
Fixed both stale "76" mentions in `datastore/api/routers/technical.py`
(module docstring + the `/{ticker}/{date}/all` endpoint's own docstring) to
say 70. The module docstring's "94 real, daily-computed columns" total
(70 core + 18 advanced + 6 pattern-scores) was already internally
consistent with 70 (76+18=94 was the old, wrong arithmetic) — left as-is,
no change needed there.

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

### T3 — No charting library on the Technical > Chart screen — ✅ 2026-07-11
Vendored Chart.js 4.4.4 + chartjs-adapter-date-fns 3.0.0 + chartjs-chart-
financial 0.2.1 as plain minified `<script>` files under
`dashboard/static/vendor/` (same zero-CDN, zero-build-step convention as
every other file under `dashboard/static/js/` — no npm/webpack step
added). `chart.html`/`chart.js` now render a real candlestick chart
(`GET /api/v1/ohlcv/{ticker}?from=&to=`, ~400 real trading days) with a
real volume bar chart beneath it, plus toggleable SMA50/SMA200/EMA21
overlay lines computed client-side from the same real close-price series
(standard deterministic formulas over real data, not synthetic — same
data the existing curated indicator panel already reads from the feature
store, just recomputed for the chart line since no time-range indicator
API exists yet). The existing indicator/pattern snapshot panels are
unchanged.

Verified: all 3 vendored bundles pass `node --check` (real syntax, not
truncated downloads) and self-register
(`Chart.register(CandlestickController, ...)` confirmed present in the
financial plugin bundle); `dashboard/static/technical/js/chart.js` passes
`node --check`; live-served all 3 vendor files + `chart.html` at 200 from
the running dev server; `GET /api/v1/ohlcv/20MICRONS?from=2026-01-01&to=
2026-07-10` against the real running API returned real OHLCV rows the
chart consumes. Could not do a full in-browser click-through screenshot
in this environment — Playwright is installed but its Chromium build is
unsupported on this host's Ubuntu 26.04 (`ERROR: Playwright does not
support chromium on ubuntu26.04-x64`); verification relied on endpoint-
level + static-syntax checks instead.

### T4 — Watchlist screen wiring status unresolved — ✅ VERIFIED REAL 2026-07-11
Confirmed by direct code read (`dashboard/static/technical/js/watchlist.js`,
`datastore/api/routers/technical.py::get_ta_daily_watchlist`,
`/watchlist/daily`): fully wired, not a stub. It calls a real endpoint that
queries the real `ta_signals` DuckDB table for the best-scoring screener
template match per ticker on the latest scored date, with a real
plain-English rationale and real resistance/support levels computed from
OHLCV (SPEC-TA-004/SPEC-TA-006). By design this is a system-generated
ranked list (best match per stock, server-computed), not a user-curated
persisted personal watchlist — there is no per-user "state" to persist
here, so nothing further to wire.

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

**Closed out 2026-07-10 — see A41/A42.** The "never trained" premise
above is now also stale: A41 found and registered real, loadable
`tft`/`bilstm` checkpoints (`v20260701`, 3 folds each), and A42 confirmed
by direct inference-time inspection (not just reading the code) that all
297 `ALL_FEATURE_COLUMNS` — including all 18 `advanced_technical.py`
features — are actually present in the tensor TFT consumes, no allowlist
anywhere in the path. So: the 18 features are neither unwired nor
unreachable for TFT/BiLSTM. Whether the models' *learned weights*
actually draw signal from them (vs. carrying them as dead input) is
still open — A42's `get_shap_values()` run to measure that didn't finish
in this session (time budget, not a resource/safety limit; see A42's own
note) — but that's now purely an A42 question, not a T5 one. No further
T5-specific action; this item is closed, its remaining open thread lives
under A42.

---

---

## Fundamental

### F3 — `systems/fundamental_analysis/*` are dead stub packages — ✅ 2026-07-10
All six subpackages (`growth`, `management`, `peers`, `quality`, `sector`,
`thesis`) were 8-line docstrings with no functions, and nothing imported them
(`grep -rn "import systems.fundamental_analysis"` returned zero hits). Every
real composite score, peer-selection, and quality/growth calc that was
"meant" to live there was instead built directly in
`features/fundamental_composites.py` (which says as much in its own
docstring). Deleted the six empty stub directories (`systems/
fundamental_analysis/{growth,management,peers,quality,sector,thesis}/`)
entirely — re-verified zero import hits before deleting. `alphalens_docs/
CLAUDE.md`'s architecture diagram already documented System 4 as "a dead
stub package, deleted 2026-07-10" from a prior attempt; the deletion itself
had already happened (dir was gone on disk) but the backlog row here was
never flipped — this pass just confirms the deletion and closes the row.
No code/doc references remained to clean up beyond the already-updated
CLAUDE.md note; `tests/quality/test_no_stub_or_synthetic_data.py` has one
unrelated pre-existing failure (stray "placeholder" comments in
`config/nse_holidays.py`, `datastore/schema/create_normalised.py`,
`scripts/align_remaining_to_fyers.py`) untouched by this change; no test
imports `systems.fundamental_analysis`.

### F4 — Thesis Builder has no PDF export — ✅ 2026-07-11
Added `GET /api/v1/fundamentals/{ticker}/thesis/pdf`
(`datastore/api/routers/fundamentals.py::get_fundamental_thesis_pdf`) —
reads the same real sector-relative z-scored ratios (`RATIO_FEATURES` from
the daily feature Parquet) and quality/growth composite scores thesis.js
already renders, applies the identical `_THESIS_RATIO_LABELS`/
`_THESIS_LOWER_IS_BETTER` +/-0.5 threshold logic (kept byte-for-byte in
sync with `thesis.js`'s `RATIO_LABELS`/`LOWER_IS_BETTER`, same real data,
no generative text), and renders it as a real PDF document via a new
shared helper `datastore/api/utils/pdf.py::build_pdf_response`
(reportlab `SimpleDocTemplate`, per the user's pure-Python-no-headless-
browser library decision — `reportlab==4.2.5` pinned into
`requirements/phase1.txt`). `thesis.html`/`thesis.js` got a "Download PDF"
button that navigates to the endpoint directly (Content-Disposition:
attachment, no fetch+blob needed).

New `tests/unit/test_thesis_pdf.py` (5 tests, `TestClient(app)`,
monkeypatched `read_feature_row`/`resolve_date`): 404s for
no-feature-day/no-ticker-row, and real `%PDF-`-header/`%%EOF`-trailer/
>1KB PDF bytes returned for both a strengths-triggering and a
lower-is-better-flips-to-risk ratio shape — checks actual PDF content
markers, not just a 200 status. **Live-verified against real production
data**: `GET /api/v1/fundamentals/20MICRONS/thesis/pdf` against the real
running API returned a genuine 1-page PDF (`file` confirms
"PDF document, version 1.4").

### F5 — `ingest_external_fundamentals.py` doesn't actually write
The script's write branch only calls `logger.info("Writing: ...")` and
increments a counter; its own comment (~line 124) admits
`DataStoreClient.write_fundamentals` was never implemented. Anyone treating
this script as a working ingestion path is being misled by its log output.

### F6 — Valuation Accuracy screen has zero backend/frontend — ✅ 2026-07-11
Built `GET /api/v1/valuation/accuracy/backtest` (`datastore/api/routers/
valuation.py`): for every `valuation_signals` row old enough that
`horizon_days` (default 5, query param, 1-252) has actually elapsed, joins
the real entry close price (`ohlcv_adjusted`, on/before signal date) and
the real realized close price strictly after the signal date (on/before
signal date + horizon), computes realized return, and checks whether
`margin_of_safety`'s sign (undervalued/overvalued call) matched the
realized return's sign. Rows with no real forward-priced bar yet are
excluded from `scored`/`hit_rate`, never fabricated — verified this by a
real bug a new test caught (see below). Rebuilt `dashboard/static/
valuation/accuracy.html` + new `js/accuracy.js`: horizon input, run
button, summary cards (hit rate, avg return by undervalued/overvalued
bucket), full per-ticker results table.

**Live-verified against real production data** (not a fixture): `1,563`
`valuation_signals` rows scored down to `507` with a real forward OHLCV
bar, hit_rate `0.4951`. New `tests/unit/test_valuation_accuracy.py` (4
tests, seeded DuckDB + `TestClient(app)`) — one test
(`test_no_forward_price_excludes_row_from_scoring`) initially failed and
caught a real off-by-one bug in the first implementation: the forward-price
query used `date <= target_date` with no lower bound, so it could match the
*entry* row itself (dated one day before the signal) as a fake "forward"
price when no real future bar existed yet. Fixed to `date > sig_date AND
date <= target_date` (strictly after the signal date). All 4 tests pass
after the fix.

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

---

## Big Investors

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

### BI2 — Non-equity Trendlyne deals (InvITs, REITs, etc.) are silently dropped from the bulk-deal backfill — ✅ confirmed still correct 2026-07-11

`TrendlyneScraper.export_bulk_deals_history` drops any deal row whose
`company_name` doesn't match a `stock_master` ticker (same per-holding
isolation as the existing holdings export). For Rakesh Jhunjhunwala and
Associates specifically, only 73 of 131 scraped deals matched (the rest
were instruments like NDR InvIT Trust that fall outside the equity
universe this DB tracks). This is correct behavior for an equities
dashboard, but if InvIT/REIT-level big-investor activity ever becomes
in-scope, `stock_master`/the ticker-resolution map would need extending
first. Re-reviewed this session: no new information changes the decision
— `stock_master`'s schema (`datastore/schema/create_normalised.py`) is
still equity-only (`nse_series`/`is_fno_eligible`/`is_nifty500`, no
instrument-type column), and `_build_company_name_to_ticker_map` still
only resolves against it, so the drop is still the correct, intentional
side-effect of ticker resolution, not a bug. Confirmed still correct,
no code change made.

### BI3 — Trendlyne bulk-block-deals page pagination not verified across all 62 investors — ✅ live-verified 2026-07-11

`_parse_bulk_block_deals_table` assumes the entire deal history is
server-rendered in one page load with no pagination/AJAX — previously
verified true for only 1 of 62 investors (Rakesh Jhunjhunwala and
Associates: 131 rows, 2010-02-02 through 2026-05-14, one fetch). This
session: fetched and parsed all 62/62 investors' real bulk-block-deals
pages live (1 request/sec, no login required — this page is public).
Every fetch returned HTTP 200 and parsed cleanly; row counts ranged 0
(Sangeetha S, Jayesh Patel — confirmed genuinely empty via a follow-up
fetch: `table#bbdealTable` exists with zero `<tr>` rows, not a fetch
failure) to 201 (Sharad Kanayalal Shah and Associates), with no exact
100/200/other round-number cap anywhere in the distribution — the
specific suspicious pattern the original BI3 writeup called out to watch
for. No `pagination`/`dataTables_paginate` markup was found on any of the
62 pages either. Earliest-deal-date per investor ranges from 2007 to
2020, naturally tracking when each investor's activity became notable
rather than clustering at a truncation boundary. This confirms
`_parse_bulk_block_deals_table`'s docstring claim (`JS_autoDataTables` is
client-side DataTables.js sort/search over an already-fully-rendered
table, not AJAX pagination) holds for the whole cohort, not just the one
investor it was originally checked against.

### BI4 — No automated test coverage for Big Investor Activity changes — ✅ 2026-07-11

Added `tests/unit/test_big_investors.py` (26 tests), a real seeded DuckDB
fixture per this repo's no-stub/synthetic-data testing policy (no mocks
of the DB layer) covering everything the original gap called out:
- `_position_and_wac_asof`'s merged bulk-deal/Trendlyne replay: BUY/SELL
  qty+WAC math (a sale doesn't move the cost basis of what's left), a
  Trendlyne checkpoint truing DOWN an undisclosed sale, a checkpoint
  truing UP an undisclosed purchase priced at the nearest OHLCV close,
  and both the exact-normalization and (BI6, see below) fuzzy
  `unmapped:` family matches.
- `_parse_bulk_block_deals_table`: real row shape parsing, the `-`
  dash-price-to-`None` convention, a missing `#bbdealTable` returning `[]`
  rather than raising, and short/malformed rows being skipped.
- `backfill_bulk_deals_history`'s `NOT EXISTS` dedup anti-join, exercised
  directly against a real seeded `large_deals` table (new row inserted;
  an exact duplicate — differing only in `remarks`, which is correctly
  NOT part of the dedup tuple — skipped; a same-day different-client row
  kept).
- MF Holdings movers' `scheme_count_change` (`_mf_movers_rows`), via
  `TestClient` against a real seeded `mf_holdings` table: both the
  increasing-scheme-count and new-entry cases.

### BI5 — `holding_pct_of_company` / shares-outstanding estimate is a market-cap/price back-derivation, not a real share count — ✅ 2026-07-11

`_position_row_to_dict` (`datastore/api/routers/big_investors.py`)
computes `shares_outstanding_est = market_cap_cr * 1e7 / cmp` rather than
reading `fundamentals.shares_outstanding` directly, since that field is
PIT-gated per fiscal quarter and only ~9% populated project-wide (10,695
of 36,346 rows have it non-NULL). Cross-checked the estimate against
real `fundamentals.shares_outstanding` for every ticker where both a
recent `stock_master.market_cap_cr`/`ohlcv_adjusted` close and a real
`shares_outstanding` value exist (1,559 tickers, latest quarter each):
- Median absolute drift: **3.3%**; 540/1,559 (35%) within 2%, 1,072/1,559
  (69%) within 5%, 1,454/1,559 (93%) within 15% — the estimate is a
  reasonable approximation for the large majority of the universe, as the
  original writeup expected.
- A real tail diverges catastrophically: worst case IDEA (Vodafone Idea),
  `market_cap_cr`=158,614 / `cmp`=14.04 implies ~112.97 billion shares,
  but `fundamentals.shares_outstanding`=1,083,430 for the same
  2026-03-31 quarter — a ~10,000,000% drift. Traced this to
  `fundamentals.shares_outstanding` itself carrying an implausible/
  misscaled value (real Vodafone Idea has ~5.6 lakh crore, i.e. tens of
  billions, of shares outstanding — 1,083,430 is off by many orders of
  magnitude), not a flaw in the market-cap/price back-derivation formula.
  Several of the other worst-15 outliers (PNCINFRA, TANLA, GARFIBRES)
  show a similar pattern: round, suspiciously small `shares_outstanding`
  values (exactly 100,000/100,394/100,000) that look like a parsing/unit
  artifact in the underlying source filing, not real share counts.

**Conclusion**: the back-derivation is sound as a general-purpose
estimate and not the bottleneck; the actual data-quality risk is in
`fundamentals.shares_outstanding` itself for a real subset of tickers. A
follow-up plausibility sweep of `fundamentals.shares_outstanding`
outliers (e.g. flag `shares_outstanding` values whose implied price
diverges from the real market_cap_cr/cmp by more than some threshold) is
worth doing but is out of BI5's original scope (quantify the drift, not
fix the source field) — not attempted here.

### BI6 — "unmapped:" family ↔ Trendlyne holder-name matching is a string-normalization heuristic, not a real identity match — ✅ 2026-07-11

`_position_and_wac_asof` matched a `bulk_deal_positions.family_id` of the
form `"unmapped:<normalized name>"` to Trendlyne `public_shareholders.
holder_name` rows by re-normalizing the holder name with the same
`normalize_client_name` used to build the `unmapped:` id — exact-string
matching only, no fuzzy/alias handling. Added
`_fuzzy_match_unmapped_family` (`datastore/api/routers/big_investors.py`)
as a fallback when that exact re-normalization misses, restricted to
`unmapped:` families already known for the SAME ticker (a coincidental
cross-ticker name collision can never produce a false match). Either of
two independent, deliberately conservative signals is accepted:
1. Token-Jaccard overlap (stopwords "AND"/"ASSOCIATES"/"FAMILY"/etc.
   excluded) ≥0.8 — catches a missing/extra "AND ASSOCIATES" suffix or
   reordered tokens.
2. `_is_positional_abbreviation_match` — same token count, order
   preserved, every token identical except one that's a same-prefix
   abbreviation (e.g. "HITESH R JAVERI" vs "HITESH RAMJI JAVERI").

Deliberately did NOT use a raw Levenshtein edit-distance ratio as the
sole/primary signal — verified it scores "ASHISH KACHOLIA" vs "ASHOK
KACHOLIA" (two different real superstar investors already in
`SUPERSTAR_INVESTORS`) at 0.80 similarity, uncomfortably close to the
0.79 a real true-positive case ("HITESH R JAVERI" vs "HITESH RAMJI
JAVERI") scores — not a safe single threshold. The structural
token/positional checks above don't have this problem (verified both
reject the Kacholia pair while accepting the Javeri pair). Ambiguous
matches (more than one candidate clears a check) resolve to no match
rather than guessing, matching this project's "fail loud / don't
fabricate" discipline. This is a heuristic upgrade, not a full identity
resolution — `bulk_deal_reconciliation.py`'s existing note that a
corrected gap lining up with an "unmapped:" client's trades should
eventually grow the `investor_family` seed automatically is still not
implemented (still manual); BI6 only closes the "any drift at all loses
the Trendlyne cross-check" gap, not the underlying manual-seeding
process. Covered by 15 new tests in `tests/unit/test_big_investors.py`
(`TestFuzzyMatchUnmappedFamily` — direct heuristic unit tests including
the Kacholia false-positive guard; two `TestPositionAndWacAsof` cases
proving the fuzzy match changes real replay output end-to-end, and one
proving a genuinely different investor is NOT merged).

---

---

## Damodaran

### D1 — 3 failing `test_damodaran.py` sector-alias tests — ✅ 2026-07-10
Resolved by updating the tests, not the classifier (the decision this item
was blocked on): NSE's real sector taxonomy has no separate "Banking"/
"NBFC"/"Insurance" string — all three tag as `"Financial Services"` in
`config/nifty500_universe.csv`, matching `classifier.py`'s
`_FINANCIAL_SERVICES_SECTORS` comment. `test_financial_services_
{banking,nbfc,insurance}` now classify against the real string and pass —
aliasing the classifier itself would have been solving for sector strings
that don't exist in production data. `pytest tests/unit/test_damodaran.py
-k financial_services`: 3 passed.

### D2 — No router-level tests for `datastore/api/routers/valuation.py`
Only the underlying `systems/damodaran_valuation/` library is tested via
`test_damodaran.py`. Endpoint wiring (param validation, error responses,
peer-group edge cases) is currently unverified by tests.

---

---

## Forensic

### FO5 — Benford's Law screen exposes far less than it computes — ✅ 2026-07-11
`benford_analysis()` (`classical_scores.py`) now returns per-series
`chi2`/`p_value`/`mad`/`digit_distribution` (real 1-9 observed-frequency
list)/`n_obs` plus `benford_expected_distribution`, not just the aggregate
MAD float. `features/forensic_classical.py::compute_forensic_classical_scores`
wires 6 real quarterly series into `series_dict` (was just `revenue`):
`revenue`, `ebitda`, `pat`, `trade_receivables_current`, `current_assets`,
`capex` — each only included if it has >=5 real non-null quarters. Result
JSON-encoded (NaN sanitized to `null` for strict-JSON compliance) into a
new `benford_detail_json` column (`ml_forensic` table, migrated via the
existing `_MIGRATE_ADDED_COLUMNS` idempotent-ALTER pattern), written by
`score_forensic.py`, exposed via the existing `ForensicRow`/`ForensicWrite`
schema and `/api/v1/signals/ml/forensic/{ticker}`. Rebuilt `benford.js` to
render a real per-digit bar chart (observed vs Benford-expected marker) and
chi²/p-value/MAD/n per series instead of the old permanent
`renderEmptyState("... not persisted or exposed")` panel.

**Live-verified against real production data**: ran the full pipeline
(compute -> write -> read) for 50 real tickers via `POST /api/v1/signals/
ml/forensic/scan/run` against the real signals DuckDB (after migrating it
in-place with the new column — the running dev-server process needed a
restart to pick up the new column/route, done as part of this
verification). Confirmed real, non-fabricated multi-series distributions
land in the DB and round-trip through the API (e.g. 20MICRONS:
`benford_revenue_n_obs=15`, `benford_capex_n_obs=5`, distinct real chi²/MAD
per series). Found and left as-is (pre-existing, not FO5-caused): the
`/{ticker}` endpoint's default `as_of=datetime.utcnow()` can read one day
stale right after IST midnight (UTC still on the previous calendar day) —
same behavior for every field on that endpoint, not benford-specific;
not fixed here (out of FO5's scope).

### FO6 — Investigation Report has no PDF/report-builder backend — ✅ 2026-07-11
Added `GET /api/v1/signals/ml/forensic/{ticker}/report/pdf`
(`datastore/api/routers/forensic.py::get_forensic_report_pdf`) — reads the
same real `ml_forensic` row `report.js` templates (Beneish M, Altman Z,
Piotroski F, Sloan accrual, Benford MAD, ML fraud probability, historical
pattern match, blocked/not-blocked recommendation) and renders it via the
same shared `datastore/api/utils/pdf.py::build_pdf_response` helper F4
uses (reportlab, real PDF document, not a screenshot). `report.js`'s
"export" is no longer just `window.print()` — a real "Download PDF"
button was added alongside the existing Print button.

New `tests/unit/test_forensic_report_pdf.py` (4 tests, real seeded DuckDB
`ml_forensic` table via `TestClient(app)`, no mocks): 404 for no row,
real `%PDF-`/`%%EOF`/>1KB PDF bytes for both a clean and a red-flagged
(BLOCKED recommendation) row, and ticker-case-insensitivity. **Live-
verified against real production data**: `GET /api/v1/signals/ml/forensic/
20MICRONS/report/pdf` against the real running API and real `ml_forensic`
row returned a genuine PDF (2,092 bytes, real `%PDF-1.4` header and
`%%EOF` trailer).

### FO7 — Universe Scan has no on-demand trigger — ✅ 2026-07-11
Added `POST /api/v1/signals/ml/forensic/scan/run?limit=&tier=`
(`datastore/api/routers/forensic.py`) — wraps the real
`score_forensic.py::score_universe` per-ticker scoring loop (real
fundamentals, real classical M-09/M-10 models, real writes to
`ml_forensic`), bounded to `limit` tickers per call (default 300, capped
2,500 — never the full ~2,300-ticker universe materialized/loaded at once,
same discipline as A28(c)'s chunking fix), optionally restricted to a
universe tier. Runs via `asyncio.to_thread` so the event loop stays
responsive. `universe.html`/`universe.js` got a real "Run Scan Now" button
(+ tickers-per-run input) that posts to this endpoint and refreshes the
summary/table on completion.

**Live-verified against real production data**: `POST .../scan/run?limit=50`
against the real signals DuckDB scored 50/50 real tickers successfully
(confirmed via direct DB read — real per-ticker `forensic_composite`/flag
rows landed with today's date). Note: `score_universe` retrains its
`ForensicMLModel` from `clean_tickers=tickers` on every call, so a very
small `limit` (e.g. 3) undershoots the model's own real minimum-training-
sample floor (`RuntimeError: ... need at least 30`, from
`forensic_ml.py::load_forensic_training_data_from_db` — a real, correct
no-synthetic-fallback guard, not a bug); the dashboard's default `limit`
(300) comfortably clears that floor. New router-level tests in
`tests/unit/test_phase2_endpoints.py::TestForensicUniverseScan` (stub
`score_universe` itself to test the router's own wiring/bounding — the
underlying scoring pipeline already has its own coverage in
`test_score_forensic.py`).

---

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

### CA2 — KANSAINER/AJOONI non-monotonic price-ratio investigation — ✅ RESOLVED 2026-07-11
Both tickers' `corporate_actions` rows were wrong, not missing/ambiguous.
Fetched each ticker's real, full NSE corporate-actions history live
(`api/corporates-corporateActions?index=equities&symbol=<T>&from_date=
01-01-2005&to_date=31-12-2026` — a wide explicit date range was required;
the endpoint silently truncates to a recent-years window without one) and
diffed against what was in the DB:

**KANSAINER** (Kansai Nerolac Paints): the two existing rows were an
earlier session's "Inferred SPLIT from price-discontinuity scan
(ambiguous-tier)" entries, never actually NSE-cross-validated —
`2010-06-23 SPLIT ratio=30.0` and `2015-03-26 SPLIT ratio=15.0`. NSE's real
history shows `2010-06-23 Bonus 1:1` (BONUS ratio=1.0) and `2015-03-26 Face
Value Split Rs 10→Re 1` (SPLIT ratio=10.0, not 15.0) — plus a **third
action entirely missing from the DB**, `2023-07-04 Bonus 1:2` (BONUS
ratio=0.5). Fixed all three (deleted the bad SPLIT row, inserted the
correct BONUS row for 2010-06-23; corrected the 2015-03-26 ratio 15→10;
inserted the missing 2023-07-04 BONUS row). Re-adjusted via
`adjust_for_corporate_actions()` and re-diffed against the real Fyers
closes already captured in `full_day_comparison_20260705.csv`: the
mismatch went from wildly non-monotonic (93.4% / 1.18% / 48.2% / 1.18%
across the 12 dates) to a **flat ~1.17-1.18% across all 12 dates,
2007-2026** — that flat residual is the known CA3 dividend-adjustment-
convention gap, not a corporate-action error. Root cause: `ratio` field
values 30 and 15 look like plausible SPLIT ratios in isolation, which is
presumably why the earlier "ambiguous-tier" inference picked SPLIT over
BONUS and got the ratio wrong on both — the actual events are one BONUS
and one SPLIT with a different ratio, plus a wholly missed third event.

**AJOONI**: `2022-10-07 SPLIT ratio=7.5` was also wrong — NSE confirms a
Face Value Split Rs 10→Rs 2, which by this table's own documented ratio
semantics (`new shares per old share`) is ratio=5.0, not 7.5. Fixed
(7.5→5.0). NSE's history also has **two RIGHTS issues missing from the
DB entirely**: `2022-11-25 Rights 29:30 @ premium Rs 4` (RIGHTS
ratio=0.9667) and `2024-05-07 Rights 1:1 @ premium Rs 3` (RIGHTS
ratio=1.0) — inserted for tracking/audit, **but no OHLCV rescale applied**:
`ingestion/adjust/price_adjuster.py` has no price-adjustment formula for
RIGHTS at all (documented limitation — a rights issue's price impact
depends on subscription price and take-up rate, not just the entitlement
ratio), the same gap CA1 hit for 9 other tickers and patched with a one-off
empirical `ratio_post/ratio_pre` rescale. AJOONI's one remaining mismatch
in `full_day_comparison_20260705.csv` (2022-11-07, 46.365%, the date
between the split and the first rights issue) is fully explained by this
gap and was **not** patched this session (no live Fyers session available
to compute the empirical rescale factor — see below).

All fixes independently re-verified via a direct `curl` fetch against
NSE's live API in this session (not just relayed from a sub-agent) —
byte-identical to the sub-agent's earlier report. `corporate_actions_
validation` rows for the changed keys were reset to `unchecked` (deleted/
re-inserted rows) rather than hand-marked `confirmed`; a re-run of
`scripts/validate_corporate_actions_fyers.py` this session hit `FYERS_
ACCESS_TOKEN` not configured (interactive OAuth prompt, no TTY) and left
them `error` — an honest "not yet Fyers-revalidated" state. Needs a
follow-up run once a valid Fyers token is available.

### CA3 — Assess 152 higher-cv Fyers-mismatch tickers — ✅ VERIFIED 2026-07-11 (confirmed, no code fix)
Spot-checked 20 tickers spanning the full cv range (0.15-0.55) from
`followup_missing_splits_20260705.csv`'s `likely_missing_split=False` set —
ITC, HEROMOTOCO, POWERGRID, HCLTECH, NTPC, SAIL, COALINDIA, ONGC, GAIL,
NHPC, BPCL, IOC, PFC, RECLTD, NATIONALUM, HINDZINC, NMDC, CESC, COLPAL,
MANAPPURAM — against the real Fyers closes already captured per-date in
`full_day_comparison_20260705.csv`. **20/20 show the same fingerprint**:
`our_close` is always below `fyers_close` (never above), and the gap
**decays smoothly and monotonically to ~0% by the most recent comparison
date** (e.g. ITC: 35.3% in 2007 → 33.4% → 30.8% → ... → 2.7% by 2026-02-20;
no ticker shows a step-jump at any single date). This is the opposite
signature of a missing split (which produces a sharp step, not a smooth
decay) and is exactly what accumulated, un-applied dividend
back-adjustment produces over time for large dividend-paying names —
consistent with `PRICE_ADJUSTMENT_ENABLED=False` for dividends
(`ingestion/scrapers/corporate_actions.py`'s documented, deliberate design
choice: DIVIDEND rows are stored for yield calculations but never fed into
`adj_factor`). **Conclusion confirmed, no code change made** — this is a
known, intentional gap, not a data-quality bug; closing it would mean
building real total-return dividend-adjustment logic, which is a
deliberate future feature decision, not a bugfix.

### CA4 — Corporate-action validation tracking — ✅ IMPLEMENTED 2026-07-05, follow-up ✅ DONE 2026-07-11
Built as `corporate_actions_validation` (keyed on `ticker, ex_date,
action_type`; columns `validation_status`, `needs_retrain`, `pct_diff`,
`fyers_validated_at`) plus `scripts/validate_corporate_actions_fyers.py`,
which checks ratio-consistency (`our_close/fyers_close` before vs after
`ex_date`, since Fyers' `history` endpoint returns already-adjusted prices —
a raw jump is not a valid signal there). Resumable and budget-capped.
As of 2026-07-08 all 967 rows are processed: 859 confirmed, 77
`needs_retrain=TRUE` (mismatch), 29 insufficient_window, 2 no_fyers_data —
see BuildLog.md's 2026-07-05/08 entry for the full needs_retrain ticker
list.

**Follow-up done 2026-07-11**: as of this session the live `needs_retrain=
TRUE` count is 70 (down from 77 — some of the original 77 tickers were
already fixed/reclassified by CA1 since 2026-07-08, dropping their
validation rows out of `needs_retrain` at that time). Cross-referenced all
70 against CA1's own resolution lists and CA3's 152-ticker cv≥0.15 set:
- **16 tickers** overlap CA1's "same-date collision" list (BANCOINDIA,
  JAYBARMARU, HERITGFOOD, GPTINFRA, GULPOLY, IMPAL, FILATEX, INDIANHUME,
  JINDWORLD, JAMNAAUTO, KABRAEXTRU, NESCO, LGBBROSLTD, MUNJALAU,
  PLASTIBLEN, AMRUTANJAN) — already flagged as needing manual
  reconciliation (conflicting existing vs. NSE-implied ratio), not
  actioned this session.
- **6 tickers** overlap CA1's "no NSE match at candidate date" list (IIFL,
  NCC, NDL, SURYAROSNI, TCI, TIMETECHNO) — the Fyers jump is real but
  NSE's API shows nothing on the candidate date; needs a wider date search.
- **1 ticker** (SURANAT&P) overlaps CA1's "reclassified, not a missing
  split" list (scheme of arrangement).
- **21 tickers** overlap CA3's 152-ticker dividend-convention-gap set
  (APCOTEXIND, ASHAPURMIN, AVANTIFEED, AXITA, BLKASHYAP, EASEMYTRIP,
  JTLIND, KPIGREEN, KRITIKA, MAANALU, MMFL, PRECWIRE, RADIOCITY, RELAXO,
  RENUKA, ROTO, RPPL, SALASAR, SANGHVIMOV, SHILPAMED, VIVIDHA) — likely
  the same dividend-adjustment-convention gap CA3 confirmed, not a
  corporate-action defect; not independently spot-checked against Fyers
  individually this session but consistent with the CA3 pattern.
- **26 tickers genuinely unaccounted for** by any prior CA1/CA2/CA3 pass —
  the reconciled retrain scope: AGIIL, ALKYLAMINE, EIHOTEL, FCL, GAEL,
  JAYAGROGN, JYOTISTRUC, KAMOPAINTS, KELLTONTEC, MAHSEAMLES, MANINFRA,
  MKPL, NIITLTD, NRBBEARING, ONEPOINT, PANAMAPET, PCJEWELLER, RAMRAT,
  RATNAMANI, SERVOTECH, SHARDAMOTR, SOUTHBANK, SUVEN, SWELECTES, TTML,
  WABAG. These are genuinely new leads for a future CA1-style NSE-API
  triage pass — not investigated individually this session (out of CA4's
  reconciliation scope; CA2 already used up this session's ticker-level
  investigation budget on KANSAINER/AJOONI).
- KANSAINER and AJOONI themselves are now resolved by CA2 (their
  `corporate_actions_validation` rows are `error` pending a live Fyers
  re-check, not `needs_retrain`).

**Schema migration done**: `corporate_actions_validation`'s DDL added to
`datastore/schema/create_normalised.py` (`_CREATE_CORPORATE_ACTIONS_
VALIDATION`, registered in `_ALL_TABLES`) so a rebuild-from-scratch no
longer silently loses this table — column set matches the live DB exactly
(verified via `describe corporate_actions_validation` against both).
`tests/unit/test_schema.py`'s `NORMALISED_TABLE_COLUMNS` got a matching
entry so the existing parametrized column-check test covers it too.

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

---

## Machine Learning

### ML1 — URGENT: wire multibagger/forensic/21d/63d/conformal into daily scheduler
`score_multibagger.py`/`score_forensic.py` are operator-CLI only, never
scheduled. `daily_inference.py` only scores `signal_5d` daily — 21d/63d/
conformal are trained but never invoked. Add scheduled jobs (multibagger
weekly per its own docstring, forensic likewise), add 21d/63d scoring calls
to the per-ticker loop, add conformal scoring after signal_5d, add an
"as of {date}" staleness indicator matching the existing top_buys pattern.

### ML2 — Daily Insights row fusion ✅ 2026-07-10
Already implemented (found already-done, in `27ea6fc`/same-day accumulated
session work, before this Group 2 pass started — confirmed by inspecting
`datastore/api/routers/signals.py`): `top_buys` now does a real read-time
LEFT JOIN across `meta_labeler`/`pnd_detector`/`hmm_market` rows onto the
base `signal_5d` row (`ON meta.date = s.date AND meta.ticker = s.ticker AND
meta.model_name = 'meta_labeler'`, same pattern for pnd/hmm), keyed by
`(date, ticker, model_name)` as originally scoped. Verified via
`tests/unit/test_signals_is_backfill.py` (28 passed/skipped, 2026-07-10).
No further action needed this session.

### ML3 — SHAP explainability ✅ 2026-07-10
Already implemented (same `27ea6fc` accumulated session work, confirmed
before this Group 2 pass touched anything): `daily_inference.py` now has
`_compute_shap_top5()` using `shap.TreeExplainer(signal_model._lgbm)`,
wired into `_step_signals_and_meta`'s `signal_5d` loop, writing
`shap_top5_json` per ticker/date (falls back to null with a logged warning
if SHAP computation fails, never a hard pipeline failure). Verified via
`tests/unit/test_daily_inference_exit_fallback.py` (all passing). No
further action needed this session.

### ML4 — 5-day recommendation history + Sell rationale — ✅ ALREADY IMPLEMENTED (confirmed 2026-07-11)
Verified real, not a claim taken on faith: `dashboard/static/ml/js/
signal.js`'s `loadHistory()`/`renderSellRationale()` (marked `#17` in-code)
render a real rolling scorecard from `GET /api/v1/signals/ml/history/
{ticker}` (real `datastore/api/routers/signals.py::get_signal_history`
endpoint, last 10 `signal_5d` calls) — recommended date/price (joined
against real `ohlcv_adjusted` closes), expected return (`q50_return`), CMP,
current return, direction — plus an explicit Sell Recommendation card
(`EXIT_TYPE_TEXT` maps all 6 real `RuleBasedExitPolicy` `exit_type` values
— `thesis_broken`/`momentum_exhaustion`/`risk_management`/
`target_achieved`/`opportunity_cost`/`pnd_exit` — to plain-English
rationale) gated on `exit_urgency >= 50`. No further action needed.

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

### ML9 — `fmtInt` numeric audit — ✅ ALREADY IMPLEMENTED (confirmed 2026-07-11)
Verified real: `fmtInt()` exists in `dashboard/static/js/api.js:54` (en-IN
grouping) and is already in real use across multiple apps (`ml/js/
positions.js`, `ml/js/holdings.js`, `technical/js/screener.js`,
`technical/js/chart.js`, `valuation/js/batch.js`, `forensic/js/
universe.js`, `fundamental/js/sector.js`). Project-wide grep for raw
numeric field displays that bypass any `fmt*` helper (patterns like
`[r.count]`/`[t.shares]`/`[h.volume]`/`[row.quantity]` etc., across all 5
apps' `*/js/*.js`) found zero remaining unformatted numeric leaks — every
`<td>` rendering a bare object field renders a string field (ticker, date,
name, badge label), not a raw number. No further action needed.

### ML10 — Dedicated Exit Urgency page — ✅ ALREADY IMPLEMENTED (confirmed 2026-07-11)
Verified real: `dashboard/static/ml/exit_urgency.html` + `js/
exit_urgency.js` (marked `#23` in-code) — a dedicated, sortable table of
all open positions from real `GET /api/v1/paper_trading/exit_urgency`
(`datastore/api/routers/paper_trading.py::get_exit_urgency`), ranked by
`exit_urgency` by default, with `exit_type` shown as the stated reason
badge. No further action needed.

### ML11 — Upload-current-portfolio page — ✅ ALREADY IMPLEMENTED (confirmed 2026-07-11)
Verified real: `dashboard/static/ml/holdings.html` + `js/holdings.js`
(marked `#24` in-code) — CSV upload (ticker,quantity), stored only in
browser `localStorage` (`HOLDINGS_KEY`, explicitly never written to any
server table per its own in-code comment), joined client-side against real
per-ticker `GET /api/v1/signals/ml/{ticker}/{today}` (`signal_5d` model)
for direction/buy-prob/exit-urgency/exit-type/P&D — genuinely excluded
from training/backtest data since nothing here reaches the server. No
further action needed.

### ML12 — Daily sector rotation report ✅ 2026-07-11
Steps 1-3 (data source) were already live since 2026-07-05 — see prior
entries below. This session built the remaining steps 4-6:
4. **Done** — `config/sector_index_map.py`: `SECTOR_INDEX_MAP` maps 8
   distinct semantic sectors (10 raw taxonomy strings, since two sectors —
   Oil & Gas and Media — each have a punctuation-variant duplicate in the
   real `nifty500_universe.csv` sector column) to a real tracked NSE
   index: Financial Services, Information Technology, FMCG, Healthcare,
   Automobile and Auto Components, Metals & Mining, Realty, Oil Gas &
   Consumable Fuels. `EXPLICITLY_EXCLUDED_SECTORS` lists the remaining 12
   real taxonomy values with no matching index (Capital Goods, Chemicals,
   Services, Consumer Services, Consumer Durables, Construction, Textiles,
   Construction Materials, Telecommunication, Utilities, Forest Materials,
   Diversified) plus a documented deliberate non-mapping: "Power" is
   explicitly NOT pointed at "Nifty Energy" even though that's the
   closest-named index, because Nifty Energy is a mixed oil-and-gas +
   power-utility basket, not a pure power-sector index — mapping it would
   misrepresent the sector's real relative strength. A test
   (`test_real_universe_sectors_all_accounted_for`) asserts every real
   sector value in the CSV is either mapped or explicitly excluded, so a
   future new taxonomy value can't silently fall through.
5. **Done** — `features/sector_rotation.py`:
   `compute_index_relative_strength()` reads real `index_ohlcv` closes,
   computes trailing-21-trading-day returns per mapped sector index and
   Nifty 500, ranks by relative strength (sector return minus Nifty 500
   return); sectors with < 22 real trading days of index history are
   excluded from the ranking outright, never guessed. `top_stocks_for_sector()`
   joins the sector's real `config.universe.load_universe()` tickers
   against the latest real `ml_signals`/`ml_multibagger` rows, ranked by
   `buy_prob`/`mb_probability`. `compute_sector_rotation_report()`
   combines both into the full report.
6. **Done** — `GET /api/v1/sector_rotation/report?as_of_date=&top_n_stocks=`
   (`datastore/api/routers/sector_rotation.py`, registered in `main.py`)
   and a new "Sector Rotation" screen in AlphaLens.ML
   (`dashboard/static/ml/sector_rotation.html` + `js/sector_rotation.js`,
   added to `shell.js`'s ML app nav after Multibagger): ranked sector
   table with trailing-21d return / Nifty 500 return / relative strength
   columns and an inline top-stocks-per-sector column (drill-down without
   a separate page, given the small top-N size).

Also ran `scripts/backfill_index_ohlcv.py --from-date 2023-07-01
--to-date 2026-07-08` in the background this session (day-by-day, NSE's
archive has no range/batch endpoint — see that script's docstring) to
backfill real history before the 2026-07-05 daily-job start date; ~3
years / 747 trading days, zero failures through the portion observed
before this session's time budget ended. See BuildLog.md for the exact
row count landed.

New tests: `tests/unit/test_sector_rotation.py` (13 tests — config map
coverage, `compute_index_relative_strength`/`top_stocks_for_sector`/
`compute_sector_rotation_report` against seeded DuckDB fixtures, and the
router endpoint via `TestClient`).

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

### ML15 — RuleBasedExitPolicy: volatility-scaled target/stop ✅ 2026-07-10
Already implemented (same `27ea6fc` accumulated session work, confirmed
before this Group 2 pass touched anything — see FutureDevelopment.md #28
referenced directly in `rule_based_exit_policy.py`'s module docstring):
`predict_full()` now uses per-row ATR-scaled target/stop
(`ATR_PROFIT_MULTIPLIER`/`ATR_STOP_MULTIPLIER` x `atr_pct`, same 2:1
profit:stop ratio as `TripleBarrierLabeler`) when the caller's exit-context
panel carries `atr_pct`, falling back to the flat `TARGET_PCT`/`STOP_PCT`
bootstrap numbers only when it doesn't. `scripts/paper_trading_tracker.py`
has a `classify_target_outcome()` hit/miss/timeout mapping
(`target_achieved`→hit, `opportunity_cost`→timeout, everything else→miss)
written per closed trade as `target_outcome`, ready for future
`ExitSignalModel` retraining evaluation. Verified via
`tests/unit/test_rule_based_exit_policy.py` (all passing, 2026-07-10). No
further action needed this session.

### ML16 — Backdated Entry relocation — ✅ ALREADY IMPLEMENTED (confirmed 2026-07-11)
Verified real: `dashboard/static/ml/tools.html` + `js/tools.js` (marked
`#29` in-code) — "Backdated Entry" now lives on a dedicated `Tools` page
(date picker -> `GET` that day's recommendations -> `POST /api/v1/
paper_trading/backdated_buy`), no longer on the main Paper Trading screen,
with the Gate 7 trade-off note (SPEC-PT-003) carried over verbatim. No
further action needed.

### ML18 — `ExitSignalModel` training fails on real data: CoxPH ConvergenceError + predict() drops rows ✅ 2026-07-10

Fixed. Two independent real bugs found and fixed:

1. **Test-fixture default was masking the real skip condition.**
   `tests/unit/test_exit_signal.py`'s `_load_real_exit_data()` defaulted
   `min_closed_positions=1`, overriding `exit_signal.py`'s own
   `MIN_CLOSED_POSITIONS=200` floor ("below this, urgency/type/CoxPH fits
   are too noisy to trust"). That let the loader hand back as few as the
   3 real closed paper-trading positions that exist today (confirmed:
   `paper_trading/executions/*.csv` → 3 rows with `exit_price` set) —
   mathematically too few for CoxPH to converge, and `X.head(5)` on a
   3-row `X` naturally only returning 3 rows (not a shape-reconciliation
   bug — there were only 3 rows to return). Fixed by restoring the real
   `MIN_CLOSED_POSITIONS` default so these tests correctly `pytest.skip`
   until 200+ real closed trades accumulate, instead of erroring loud on
   a known-too-small sample (this fix pre-existed uncommitted in the
   working tree at the start of this session; verified and kept).
2. **Real design-matrix bug in `train_full()` (production code, not just
   the test fixture)**, found via a synthetic reproduction (in-memory
   only, never touching the real DB): `load_exit_training_data_from_db()`
   sets `duration = days_held` exactly, and `days_held` is also a
   covariate in `X` — a covariate perfectly collinear with the CoxPH
   duration column is singular for the partial-likelihood Hessian
   *regardless of sample size*, matching the "high sample correlation
   with duration" symptom already noted below. Separately,
   `days_to_next_earnings` is always `NaN` at the source (joined at
   scoring time, not backfillable from historical logs), so after
   `SimpleImputer(keep_empty_features=True)` it becomes a constant
   (zero-variance) column — also singular. Fixed in
   `systems/ml_signal_engine/models/exit/exit_signal.py::train_full()`:
   before fitting `CoxPHFitter`, drop any covariate that is zero-variance
   or has `|corr| > 0.98` with the duration column from the Cox design
   matrix specifically (logged when it happens) — the urgency/type
   LightGBM models still see the full feature set, since neither has a
   collinearity requirement. `predict_survival()`/`predict_full()` drop
   the same columns before calling into the fitted `cph` object; the
   dropped-column list round-trips through `save()`/`load()`.
   Reproduced-and-confirmed-fixed with a 250-row synthetic in-memory
   dataset engineered to have the exact same collinearity (never written
   to any DuckDB/CSV) — `train_full()` now converges cleanly and
   `predict_full()` returns the correct row count.

Verified: `tests/unit/test_exit_signal.py` — 12 passed, 14 skipped (the
14 correctly skip on "only 3 real closed positions, need 200").

Found running the full `tests/unit/` suite (2026-07-09, while doing a
post-A34/A35/A36 regression pass — not caused by that session's changes,
confirmed unrelated: no touched file appears in the traceback). 2 real
failures + 9 errors, all in `tests/unit/test_exit_signal.py`:

- `test_train_full_returns_diagnostics` and every `TestExitTypesAndUrgency`
  case (9 errors) fail at fixture/setup time with
  `lifelines.exceptions.ConvergenceError: delta contains nan value(s).
  Convergence halted` — the CoxPH survival-model fit
  (`systems/ml_signal_engine/models/exit_signal.py`, per A39's
  `RuleBasedExitPolicy` fallback writeup) is being handed a design matrix
  with NaN values, not caught/imputed before `lifelines.CoxPHFitter.fit()`
  is called. Warnings alongside the error point at likely contributing
  columns: `days_to_next_earnings` (near-zero variance),
  `entry_price`/`days_held`/`days_to_next_earnings` (high sample
  correlation with the duration column — a possible
  complete-separation/leakage issue, not just a NaN-handling gap).
- `test_simple_train_fits_urgency_regressor_only` fails on a genuine
  shape bug: `model.predict(X.head(5))` returns only 3 rows
  (`len(preds) == 3`, not 5) — the urgency regressor's predict path is
  silently dropping rows (likely the same NaN rows the CoxPH fit above
  chokes on, being filtered internally with the row count never
  reconciled back against the input).

Not fixed here — this is systems/ml_signal_engine/models/exit_signal.py's
real training data pipeline, not the test file; needs someone to trace
where the NaN-producing rows enter `_load_real_exit_data()`'s feature
matrix and decide impute-vs-drop-and-realign, which is a real design
decision (same class as A39's "no real trainer exists yet" gap), not a
one-line fix.

### ML19 — `test_multibagger.py`/`test_paper_trading_router.py` fail only inside the full suite, pass standalone ✅ 2026-07-11 (not reproducible)

Re-investigated per the original writeup's own suggested method: bisection
via progressively wider slices. Findings, in order:

1. `tests/conftest.py` already has two `autouse=True` fixtures
   (`cleanup_connections`, `reset_feature_registry`) that reset the two
   most obvious cross-test leak vectors (DuckDB connection pool, feature
   registry) after every test — no gap found there.
2. Ran the first 62 of 113 `tests/unit/*.py` files together (everything
   alphabetically at/before `test_multibagger.py`, including it) — 667
   passed, 0 failures.
3. Ran `tests/integration/` + `tests/quality/` + that same batch together
   (pytest's real default collection order is directory-alphabetical:
   `integration` < `quality` < `unit`, so this reproduces the actual
   ordering a bare `pytest tests/` would use, not just an alphabetical
   file list within `tests/unit/`) — only the two known pre-existing
   failures (stub-keyword allowlist gap; an unrelated `test_daily_
   pipeline.py::TestPnDBlockExcludedFromTopBuys` case), no multibagger/
   paper-trading failures.
4. Ran the full `tests/unit/` suite unbatched once (1,293 passed, 1
   pre-existing `test_schema.py[ml_forensic]` failure, ~1.9GB peak RSS,
   no OOM) — clean.
5. Ran `test_exit_signal.py` + `test_score_multibagger.py` +
   `test_rule_based_exit_policy.py` + `test_multibagger.py` +
   `test_paper_trading_router.py` together 3x in a row (the ML18/ML19/ML20
   neighborhood from the original diagnostic run) — 65 passed, 14 skipped,
   1 xpassed, identical result every time.

No leaking fixture/test found because there wasn't one to find in this
checkout today — every recombination that originally triggered the
failure now passes clean, repeatedly. Two explanations, both plausible
and neither disprovable in hindsight: (a) the original failures were
genuinely transient/order-dependent (e.g. `PYTHONHASHSEED`-influenced
CoxPH/RandomSurvivalForest solver convergence — sklearn's L-BFGS-B and
lifelines' CoxPH both have known seed/ordering-sensitive convergence
paths, and `TestSurvivalCurveMonotonicity`/`TestMultibaggerModelTraining`
are exactly the tests that would surface that), or (b) one of Groups
1-7's changes earlier in this session incidentally removed the leak
(e.g. ML18's `exit_signal.py::train_full()` collinearity fix touches
the same CoxPH/model-training code path). Given (4) is a full, unbatched,
repeat-free pass of the entire directory — the strongest evidence
available — closing this as verified-fixed/non-reproducible rather than
leaving it open on a diagnosis I can't substantiate with a real
reproduction.

### ML20 — Real-data test cases require a live DataStore API server, not gated/skipped without one ✅ 2026-07-11

Re-examined both files named in the original writeup:

- **`test_score_multibagger.py`** no longer touches `DataStoreClient`/HTTP
  at all — its `trained_model` fixture and `TestScoreUniverse` cases call
  `load_multibagger_training_data_from_db()` (direct DuckDB read,
  rewritten under backlog #27 on 2026-07-04) and use a `MagicMock` for
  `score_universe()`'s injected `client` parameter. The 3 "ERROR" cases
  and 2 `ConnectError` failures the original writeup found no longer
  exist as written — false alarm from a stale version of this file,
  already superseded before this session. No code change needed;
  confirmed via `grep -n "DataStoreClient" tests/unit/test_score_
  multibagger.py` (no hits) and a clean standalone run (10/10 pass).
- **`test_rule_based_exit_policy.py::TestAtrScaledBarriers::
  test_atr_scaling_against_real_historical_ohlcv[RELIANCE|TCS]`** does
  genuinely instantiate a real `DataStoreClient()` and call
  `.get_ohlcv()` over HTTP — this one is real. `DataStoreClient` is a
  thin `httpx` wrapper with no dependency-injection seam for an ASGI
  transport (unlike FastAPI's own `TestClient(app)`), so rewriting onto
  the in-process pattern isn't viable without changing production code
  (out of scope this session — tests/unit/** only). Took option 2 from
  the original writeup instead: wrapped the `client.get_ohlcv()` call in
  `try/except httpx.RequestError: pytest.skip(...)` so an unreachable
  DataStore API now skips cleanly with a clear message instead of a hard
  `ConnectError` failure indistinguishable from a real regression. A live
  server happens to be running in this checkout right now (verified via
  `curl localhost:8000/docs` → 200, confirmed with `ss -ltnp`), so the
  test currently exercises the real path and passes — the skip only
  triggers when no server is up (verified separately by pointing a scratch
  `DataStoreClient(base_url="http://localhost:1/")` at a dead port and
  confirming `httpx.ConnectError` is the exception raised/caught).

### ML21 — SMOTETomek unbounded oversampling causes repeated OOM in signal_63d retrain ✅ 2026-07-10 (options 1+3 adopted; option 2 built but held opt-in)

Implemented the recommended near-term path:

1. **Subprocess isolation per horizon** — `retrain_phase2.py` gained
   `only_horizon`/`--horizon` (run just one of signal_5d/21d/63d
   in-process) and `--subprocess-per-horizon` (spawn 3 separate
   `python -m ... --horizon N` child processes instead of one Python loop
   over `HORIZON_CONFIGS`). `pipeline_scheduler.py::_trigger_model_retrain`
   now passes `--subprocess-per-horizon` whenever it invokes
   `retrain_phase2` — the scheduler's unattended weekly run (the exact
   path that OOM-killed the box twice on 2026-07-09) now gets full OS-
   level memory reclamation between horizons.
2. **Fewer Optuna trials for signal_63d** — new
   `OPTUNA_TRIALS_BY_HORIZON = {5: 5, 21: 5, 63: 3}`, used whenever
   `retrain_phase2()`'s `optuna_trials` argument is left at its new
   default (`None`); passing an explicit int still forces that count for
   every horizon (back-compat with existing callers/tests).
3. **`SMOTETomek` sampling-strategy cap — built, not yet adopted as
   default.** `BaseSignalModel.__init__`/`_resample()` gained
   `max_sampling_ratio: Optional[float]` — when set, caps each minority
   class's post-resample count at `max_sampling_ratio * majority_count`
   (via an explicit per-class `sampling_strategy` dict) instead of
   imblearn's `'auto'` 1:1 parity. Verified directly: on a synthetic
   600-row/5%-minority fixture, `'auto'` drives `min/max` count ratio to
   >0.85 (near-parity) while `max_sampling_ratio=0.3` keeps it <0.6 (see
   `tests/unit/test_signal_models.py::TestResampleMaxSamplingRatio`, 3
   new tests, all passing). **Default left at `None`** (unbounded
   `'auto'`, unchanged behavior) per this item's own requirement — a real
   before/after Sharpe comparison against the full training pipeline is
   needed before capping the ratio becomes the default, and that
   comparison needs a real (multi-hour, OOM-risk) `retrain_phase2` run
   this session deliberately did not launch unattended. Flipping the
   default is a one-line follow-up (`max_sampling_ratio=<value>` in
   `retrain_phase2.py`'s model construction) once that comparison exists.
4. **Tomek-links removal (option 4)**: not done — lowest priority of the
   4 options per the original writeup, superseded by #1/#2 already
   bounding the incident.

Verified: `tests/unit/test_signal_models.py` (29 passed, including the 3
new `TestResampleMaxSamplingRatio` cases) and
`tests/unit/test_retrain_all_when_free_script.py` +
`tests/unit/test_scheduler.py` (43 passed — confirms the
`--subprocess-per-horizon` arg addition didn't break the training-module
dedup/dispatch map).

`retrain_phase2.py` (trains signal_5d/21d/63d) OOM-killed the scheduler box
twice on 2026-07-09 even after capping LightGBM/CatBoost/XGBoost `n_jobs`
and reducing `DEFAULT_TICKER_CHUNK_SIZE` (400→150). Root cause:
`BaseSignalModel._resample()` (`base_signal_model.py:420-428`) calls
`SMOTETomek(random_state=...).fit_resample(X, y)` with the default
`sampling_strategy='auto'`, which oversamples every minority class up to
the majority (`hold`) class's count with no cap. For a 63-day-horizon
label, `hold` heavily dominates `buy`/`sell` (diagnostics from tonight's
run: `class_ratio_before` 49.5% buy / 42.2% hold / 8.3% sell), so
resampling can multiply the training matrix several-fold before it even
reaches Optuna HPO (5 trials) or the 3-model stacking ensemble refit —
none of which log per-step progress, so the RSS climb is silent for
30–60+ minutes before it's visible.

Immediate mitigation applied same day (not a real fix): `n_jobs=2`/
`thread_count=2` caps on the 3 base learners, `ticker_chunk_size` 400→150,
and — the one that actually worked — capping `max_tickers` to 800
(`DEFAULT_MAX_TICKERS` in `retrain_phase2.py`) instead of the full
~2300-ticker universe. This is a workaround (smaller universe → smaller
pre-resample matrix → smaller post-resample matrix), not a fix for the
unbounded-oversampling behavior itself, and reduces training universe
coverage.

Options discussed, not yet decided/implemented:
1. **Subprocess isolation per horizon** (run signal_5d/21d/63d as 3
   separate OS processes instead of one Python loop) — guarantees OS-level
   memory reclamation between horizons regardless of any lingering Python
   references. Zero model-behavior change, pure infra, lowest risk.
2. **Cap `SMOTETomek`'s `sampling_strategy`** (e.g. a fixed ratio instead
   of `'auto'`, or pair with `RandomUnderSampler` on the majority class) —
   directly bounds the data-volume blowup at its source. Changes what the
   model trains on (likely reduces buy/sell recall) — needs a before/
   after Sharpe comparison (the same phase1-vs-phase2 comparison this
   script already logs) before becoming the default, not a silent change.
3. **Fewer Optuna trials for signal_63d specifically** — cheap, low-risk,
   only shaves the repeated-fit multiplier, doesn't address the underlying
   data-size issue.
4. **Drop Tomek-links cleanup** (plain `SMOTE`/`RandomUnderSampler` instead
   of `SMOTETomek`) — removes the pairwise-distance-computation cost on
   top of the oversampling ratio question.

Recommended path (not yet actioned): combine #1 + #3 as a safe near-term
fix, evaluate #2 with a Sharpe comparison before adopting it as the
default oversampling behavior. Revisit before the `max_tickers=800`
workaround becomes stale (i.e. before wanting full-universe coverage
back) — see 2026-07-09 comment above `DEFAULT_MAX_TICKERS` in
`retrain_phase2.py`.
### ML17 — Unified backtest strategy — (a) real Nifty benchmark curve ✅ 2026-07-11, (b) restructuring still unbuilt
Scope was explicitly split 2026-07-05/06 into two independent pieces:

**(a) Real Nifty benchmark curve for backtests — ✅ 2026-07-11.**
`backtest/engine.py`: `BacktestEngine` gained a `benchmark_index`
constructor param (real Nifty 500 `index_ohlcv` closes — distinct from
the pre-existing `benchmark` param, which stays the NIFTYBEES/etc
ETF-price proxy Category 7 relative-strength features already depend on;
these are two different real data sources for two different purposes,
not a duplicate). New `_build_benchmark_curve(test_fold)` builds a
buy-and-hold equity curve normalised to `initial_capital` at the first
date the fold's test window and the real index history overlap, sliced
to that fold's own test dates — returns `None` (no synthetic fallback)
when there's no real overlap. `compute_fold_metrics()` gained an optional
`benchmark_equity_curve` param and now returns `benchmark_cagr`/
`benchmark_sharpe`/`excess_return` (`cagr - benchmark_cagr`) alongside
the existing strategy metrics, all `None` when no benchmark curve was
available. `FoldResult` and `run_full_backtest`'s `aggregate` dict
(`excess_return_mean`, `benchmark_cagr_mean`, averaged only over folds
that actually had real coverage) extended to match; `to_dict()` updated.

`backtest/run_phase1_backtest.py`: new `_fetch_real_benchmark_index()`
fetches real Nifty 500 `index_ohlcv` via a new
`DataStoreClient.get_index_ohlcv(index_name, from_date, to_date)` method
and a new `GET /api/v1/ohlcv/index/{index_name}` endpoint
(`datastore/api/routers/ohlcv.py`) — index names containing spaces/`&`
(e.g. "Nifty Oil & Gas") are percent-encoded client-side since they're
path segments, not query params. Wired into `run_phase1_backtest()`'s
`engine_kwargs`; the per-fold print block now shows
`Benchmark CAGR=... Sharpe=... Excess=...` or an explicit "n/a" when no
real coverage exists for that fold, rather than a silently-missing field.
`run_phase{2,3}_backtest.py` were NOT touched (out of this item's scope —
they don't share `_fetch_real_benchmark_index`/`engine_kwargs` wiring
with phase1; extending them is folded into (b) below).

New tests: `tests/unit/test_backtest_benchmark.py` (8 tests —
`compute_fold_metrics` benchmark-curve math including the "benchmark
curve is normalised off its own first value, not `initial_capital`"
scale-mismatch case, and `_build_benchmark_curve` slicing/overlap logic),
`tests/unit/test_ohlcv_index_endpoint.py` (5 tests, seeded
`index_ohlcv` via `TestClient`), 2 new cases in
`tests/unit/test_datastore_client.py` for `get_index_ohlcv`.

**(b) "One backtest per horizon model, unified cadence" restructuring**
of the 3 existing `run_phase{1,2,3}_backtest.py` scripts — still
unscoped, independent of (a), explicitly out of scope for this session
per the task brief ("ML17b ... is explicitly OUT of scope"), not
attempted.

