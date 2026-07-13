
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
by order within a section. This file now tracks only the still-open (⏳ pending / 🔧 in progress / 🚫 blocked) items — completed (✅) items were split out to `FeatureBacklogImplemented.md` on 2026-07-11. IDs are shared across both files.

## Status Matrix

### Architectural

| ID | Item | Area | Status | Blocked On |
|---|---|---|---|---|
| A22 | Remote/mobile access to dashboard (password-protected) | Ops / Dashboard | 🚫 | Needs the user to install and approve Tailscale (or an equivalent remote-access tool) on their own devices before this can proceed — a design proposal exists (see writeup below) but requires explicit user action/sign-off to unblock |
| A23 | Job run-time/memory benchmark history + weekday/weekend schedule optimization | Ops / Scheduler | 🔧 | 2026-07-09: `job_run_log` now records `duration_seconds`/`peak_rss_mb` for every job (all 13 scheduled job wrappers instrumented) — see writeup below. Schedule-rebalancing pass itself still blocked on weeks of accumulated real data, as originally scoped. |
| A24 | UI refactor for responsive layout (mobile/tablet) | Dashboard (all) | 🔧 | 2026-07-10 (Group 1 backlog sweep): landed for AlphaLens.Ops only (`dashboard/static/ops/`) per this session's explicit scope restriction against touching dashboard files outside that directory — new `dashboard/static/ops/css/responsive.css` (linked from `index.html`/`macro.html`, loaded after `shell.css` so it can page-scope without editing the shared stylesheet other apps use): every `.card` wrapping a `<table>` gets its own horizontal-scroll region instead of overflowing the page (`:has(> table)`), `.kv-row`s stack label-over-value under 900px instead of truncating badge text, table font/padding shrink under 900px/480px, and the app-bar's brand text/build-info clock hide under 480px to leave room for the tab strip. The other 4 apps (Trading Terminal, Fundamental, Forensic, Backtest) still need the same pass — ties into A22 for the mobile-access use case |
| A26 | Expand `_SANITY_KNOWN_SPARSE_COLUMNS` with remaining confirmed-unsourceable columns; finish 2026-07-03/06/07 recompute+re-run | Scheduler / Data Layer | 🔧 | 2026-07-09: audit found 13 of the "remaining ~12" list were already exempted; only `capex_to_assets`/`noncash_assets_ratio` were actually missing — added, with tests. 2026-07-03/06/07 `step_compute_features` recompute + `sanity_check`/`paper_trade` re-run still outstanding (needs an explicit Ops force-run, not run this session) |
| A28 | Emergency feature recompute + 8-model retrain (post corporate-action fix) — consolidated | Data Layer / ML Signal Engine / Scheduler | 🔧 | 2026-07-09: (f)/(g) resolved by log/code audit — see A37; 7/8 models confirmed correctly trained on corrected data, `signal_63d` needs one real `retrain_phase2.py` run (blocker was A37's masked crash, now fixed); Stage 2 parquet recompute itself turned out not to be a retrain dependency, still separately unfinished for `datastore/features/daily/` consumers |
| A38 | T5's "18 advanced TA features unused" is only half right — TFT/BiLSTM already consume them, but neither has ever been trained | ML Signal Engine / Data Layer / Scheduler | 🔧 | 2026-07-09: registry.json write-through + scheduler wiring landed and tested; first-ever real training run (smoke test, then full) still pending — see writeup below |
| A40 | `StackingEnsemble` is fully dormant and its one real training attempt died silently mid-run | ML Signal Engine | 🔧 2026-07-11 | Group 2 backlog sweep: root-caused via `journalctl -k` evidence of `systemd-oomd` SIGKILL behavior on this host (circumstantial — original incident's logs had rotated out, but consistent). Added a bounded `--max-tickers` default and a STARTED/COMPLETED/FAILED status marker to `train_stacking.py`. Deliberately NOT re-run and NOT wired into `daily_inference.py` yet — needs the same subprocess isolation as ML21 first, so still open |
| A42 | Verify which of the 16 `ALL_FEATURE_COLUMNS` categories TFT/BiLSTM actually learn from, and decide a path for categories no serving model uses | ML Signal Engine / Features | ⏳ | Group 2 backlog sweep 2026-07-11: confirmed 297/297 feature columns architecturally reach TFT's input tensor (closes T5), but the actual per-category learned-importance measurement (`get_shap_values()`) didn't finish within the session's time budget — the sequence-building step ran 8+ minutes of CPU with no OOM risk. Follow-up noted: the sequence-building code processes every ticker regardless of any `max_sampling` cap, a real inefficiency worth its own fix before re-attempting |
| A44 | 2026-07-10 laptop-restart OOM: `daily_pipeline` ran unbounded per-ticker fallback against a not-yet-up DataStore API | Scheduler / Ops | 🔧 | 2026-07-10: root cause fixed same session (`_wait_for_datastore_api` health-gate in `daily_pipeline.main()`, fail-fast on `httpx.RequestError` in `matrix_builder._fetch_ohlcv_panel`'s per-ticker fallback) — item kept open for the systemd ordering dependency (A45-adjacent) and a regression test, see writeup below. 2026-07-10 (Pipeline & Monitoring Remediation): the related "run silently looked completed after this class of crash" symptom is now fixed — `pipeline_runs` gets a `status='running'` row the moment a run starts (not only at the end), so a mid-run kill leaves a diagnosable stale row (`GET /api/v1/ops/runs`'s new `is_stale` flag) instead of silently showing a prior day's success as "most recent" — see BuildLog.md 2026-07-10. systemd ordering + a dedicated cold-start-race regression test remain open. 2026-07-10 (Group 1 backlog sweep): the regression test landed — `tests/unit/test_daily_pipeline.py::TestWaitForDatastoreApi` (3 tests: returns immediately when up, retries across simulated cold-start failures then succeeds, gives up after `max_wait_seconds` without raising — `httpx.get`/`time.sleep`/`time.monotonic` monkeypatched so nothing actually blocks or hits a real network/process). The systemd ordering dependency itself (an `After=`/`Wants=` edit to the live `~/.config/systemd/user/alphalens-scheduler.service` unit, plus creating a DataStore API unit — one doesn't exist yet, confirmed) is a live-system change outside any repo file this session's scope list covers, and per A45's same-session precedent is deliberately not made without explicit operator go-ahead — left open. |
| A45 | AlphaLens_Ops "Jobs & Models" monitor screen: schedule/last-next-run rollup, live system-memory polling, and one-click corrective actions | Ops / Dashboard | 🔧 | 2026-07-10: 3 new panels shipped (DB lock status, trained-but-unused models, exception catalog) reusing the existing `ops.py`/`dashboard/static/ops/` frontend — see writeup below. Verified via `TestClient`, not a live browser session (this machine's already-running DataStore API process, pre-dating this session, needs a restart to serve the new routes — not done without explicit go-ahead). Live psutil-based real-time resource polling (vs. today's 30-min `scheduler-resources` card) still open, see A48. 2026-07-10 (Group 1 backlog sweep): A48's live-resources panel landed (see A48) — the one remaining piece of A45's original scope, "one-click corrective actions", was never scoped further than the force-run-step control that already existed pre-A45; kept 🔧 only for the same not-yet-restarted-API verification caveat as the rest of this row, not for missing functionality |
| A46 | Split `daily_pipeline.py`/`pipeline_scheduler.py` monoliths (1869/2488 lines) into per-concern modules | Scheduler | 🚫 | Deferred out of the 2026-07-10 Pipeline & Monitoring Remediation session's Phase 0 — high blast-radius pure refactor, deprioritized vs. the same session's Phase 1 fix; plan (module boundaries) already written, see BuildLog.md 2026-07-10 |
| A48 | Near-real-time (10-30s) resource monitoring during an active pipeline run, replacing `monitor_scheduler_resources.py`'s 30-min poll; uniform memory-limit config across DuckDB PRAGMA/resource-guard/monitor threshold; clean memory release (DuckDB conn close, gc.collect) on step completion | Ops / Scheduler | 🔧 | 2026-07-10: `PIPELINE_MEMORY_CEILING_MB` (uniform config) landed and is used by `resource_guard.py`; `gc.collect()` added after screener's chunk flush. The near-real-time monitor loop itself is still open — `monitor_scheduler_resources.py` runs under a systemd timer this session couldn't safely reconfigure/verify. 2026-07-10 (Group 1 backlog sweep): the near-real-time piece landed via a different, lower-risk mechanism than reconfiguring the systemd timer — rather than shortening `monitor_scheduler_resources.py`'s own 30-min timer interval (a live-system change), added a new on-demand endpoint (`GET /api/v1/ops/live-resources`, `ingestion/scheduler/resource_guard.py::poll_process_resources(pid)`) that reads `alphalens-scheduler.service`'s MainPID via psutil fresh on every call, no caching. The Ops dashboard's new "Live Resource Monitor" card (`dashboard/static/ops/index.html`/`js/index.js`) polls it every 15s automatically **only while `GET /api/v1/ops/runs` shows a `status='running'` row** (`_updateLiveResourcesPolling`, driven off `loadRuns()`), stopping once the run finishes — genuinely near-real-time during an active run without polling uselessly the other 23.5 hours/day. Kept 🔧, not ✅: `monitor_scheduler_resources.py`'s own 30-min timer/log file is unchanged (still the source for the separate `/scheduler-resources` card) and the "clean memory release on step completion" sub-item beyond the screener chunk flush is unverified beyond that one call site |
| A59 | `intangibles_growth`/`contingent_liability_ratio`-style forensic gaps: verify none of Trendlyne/Groww/Tijori already source them, and confirm actual field-level impact before spending more effort | ML Signal Engine / Data Layer | 🚫 | 2026-07-10 investigation (no code change, informational): grepped `ingestion/scrapers/trendlyne.py`/`groww_mf_holdings.py`/`tijori.py` for goodwill/intangible/contingent/governance keywords — zero hits; trendlyne.py's "governance" endpoint is shareholding-pattern data only, unrelated. Direct inspection of all 19,223 cached raw NSE XBRL filings found only 235 (1.2%) even mention "contingent," always as unstructured prose inside a freeform "Textual Information" note — no consistent regex-extractable phrasing, so `contingent_liability_ratio = contingent_liability / total_liability` (as requested) cannot be computed without real NLP extraction, which is out of scope for this session. Impact: these feed `forensic_ml.py`'s Group D (Balance Sheet Quality)/Group E (Governance) ensemble features, `compute_governance_score()` (degrades gracefully to NaN with zero governance signal, doesn't crash), `/forensic/flagged` API, and the Forensic Dashboard (`dashboard/static/forensic/dashboard.html`) — their absence lowers forensic-score confidence for affected tickers but does not break the pipeline. NLP-based contingent-liability extraction and MCA21-based Group E enrichment tracked here for a future phase, not attempted this session |
| A60 | NPA / Gross NPA % feature for Financial Services sector tickers, phased | ML Signal Engine / Data Layer | 🚫 | 2026-07-10: credentials now exist in `.env`, attempted step (1) (verify login) for real — found a deeper blocker than the module's own docstring anticipated. `TijoriAuthError: Could not find csrfmiddlewaretoken` confirmed live: `login()`'s assumed URL (`/accounts/login/`) 500s on Tijori's own backend; the real login page (found via the homepage's `<a href>`, actually `/account/signin`) is a **React SPA** (`/static/react/account/main.js`), not a server-rendered Django form — there is no `csrfmiddlewaretoken` hidden input anywhere on the page (the CSRF token instead lives in `window.django.csrf`/a `body[csrf_token]` attribute, for a client-side JS API call whose endpoint isn't discoverable from the static HTML). Fixing this properly needs either reverse-engineering the minified JS bundle or real headless-browser automation (e.g. Playwright, a new dependency) to capture the actual login network request — materially bigger than "fix the CSRF regex" as originally scoped. **Explicit decision (2026-07-10): defer as its own properly-scoped follow-up**, do not attempt browser automation blind. (2)/(3) remain correctly un-attempted per this item's own gating — do not schedule an unverified scraper |
| A61 | `fundamentals_source`/`fundamentals_source_priority` (A36) appeared unpopulated on all 36,346 rows per a stale mid-session investigation note | Data Layer / Ingestion | 🔧 2026-07-10 | Code review of `datastore/api/routers/fundamentals.py`/`scripts/backfill_fundamentals_nse_xbrl.py` confirmed both writer paths correctly set these columns on every INSERT — the 100%-NULL state was real, just pre-A36 legacy rows that no writer had touched since the fix landed (2026-07-09), not a writer bug. **Backfilled 2026-07-10** (with user sign-off, daemon scheduler paused first): 6,603/36,346 rows tagged `fundamentals_source='nse_xbrl'`, `fundamentals_source_priority=4` — identified via a high-confidence heuristic (at least one NSE-XBRL-exclusive column populated: `goodwill`, `audit_qualified_flag`, `intangible_assets`, `total_liabilities`, and 18 others only `backfill_fundamentals_nse_xbrl.py` ever writes). The remaining 29,743 rows have no reliable retroactive signal to distinguish screener vs. trendlyne and were deliberately left NULL — safe, since `build_priority_update_clause`'s `COALESCE(...,0)` already treats an unranked row as priority 0, so any future real write still resolves correctly against them |
| A63 | `tests/quality/test_no_stub_or_synthetic_data.py::test_no_unallowlisted_stub_keywords` fails on 3 pre-existing, benign "placeholder" comments | Data Layer / Tests | ⏳ | Found 2026-07-10/11 during the FeatureBacklog full sweep (Groups 1-9) — every group independently hit and confirmed this same failure, pre-dating the whole session (`git stash` reproduces it against the untouched baseline). The 3 flagged lines (`config/nse_holidays.py:41,386`, `datastore/schema/create_normalised.py:196`, `scripts/align_remaining_to_fyers.py:8`) are real prose comments describing past fixes/known-limitation notes, not fabricated-data stand-ins — needs a narrow `KEYWORD_ALLOWLIST` entry each (per the test's own stated fix path), not a code change. Left unfixed this session since no group's scope covered `tests/quality/*.py` itself |
| A64 | `tests/unit/test_schema.py::TestCreateSignalsSchema::test_duckdb_table_columns_match_architecture_doc[ml_forensic]` fails — schema/doc drift | Data Layer / Tests | ⏳ | Found 2026-07-11 during Group 7's schema-addition work (confirmed pre-existing via `git diff` — not introduced by any of this session's `create_normalised.py` edits, which only added `corporate_actions_validation`'s DDL). The `ml_forensic` table's real DuckDB columns no longer match `alphalens_docs`' architecture doc; needs a side-by-side diff and a decision on which side is stale before fixing |
| A66 | Framework-wide sortable-columns audit — apply existing `sortRows`/`sortableHeader` helper to every dashboard table, not just the ones that already use it | Dashboard (all) | ⏳ | None — reuses existing `js/api.js` helper, needs a per-table audit + wiring pass |
| A67 | Sparkline column support — no sparkline rendering exists anywhere in the dashboard; needed for price/RS trend columns across tables (Sector Rotation, Signal Deep Dive, etc.) | Dashboard (all) | ⏳ | Depends on A71's rollup table existing for cheap per-ticker history fetches, or can hit OHLCV API directly for a first pass |
| A68 | Column-alignment convention — amount fields right-aligned, percentage/range fields center-aligned, across all tables | Dashboard (all) | ⏳ | None — CSS/class convention + per-table audit |
| A69 | Ticker-hyperlink-to-chart convention (every ticker cell links to `technical/chart.html?ticker=...` in a new tab) + a "Signal Deep Dive" icon column that opens `ml/signal.html?ticker=...` in a new tab, applied uniformly | Dashboard (all) | ⏳ | None — convention + per-table audit |
| A71 | Shared 1-year price/technical rollup table — a dedicated per-ticker table storing ~1yr of OHLCV + technical datapoints, so charts/sparklines stop reading from the main OHLCV/indicator tables directly | Data Layer | ⏳ | Design question: existing `ohlcv_adjusted` + `/api/v1/ohlcv/{ticker}` already serve `chart.html` without an apparent perf problem (per 2026-07-11 exploration) — needs a real load-measurement before committing to a new materialized table, not just building one speculatively |
| A72 | New cross-cutting Events table (corporate actions, bulk/block deals, 5d/21d/63d & MultiBagger recommendation triggers, forensic-flag dates) + chart overlay showing these as markers on `chart.html` | Data Layer / Dashboard (Technical) | ⏳ | `corporate_actions` and `bulk_deal_positions` tables already exist and can be reused for 2 of the 4 event types; recommendation-trigger and forensic-flag event rows are net-new; chart overlay itself does not exist at all today |
| A65 | Real test coverage measurement + improvement toward 90% overall | Tests | ⏳ | 2026-07-11: added `.coveragerc` (scopes `datastore/`, `ingestion/`, `features/`, `systems/`, `backtest/`, `config/`; omits `tests/`, `scripts/` (one-off CLI tools), `dashboard/static/vendor/`, `__init__.py`, migrations) — no coverage config existed before. Baseline measured by running the full `tests/unit/`+`tests/integration/` suite in memory-safe batches (`--cov-append`, heavy ML-training files one at a time per `feedback_coverage` convention): **67.93%** (18,695 stmts / 5,995 missed). Added 3 new real-logic test files closing genuine 0%/low-coverage gaps: `tests/unit/test_build_universe_recompute.py` (6 tests, real seeded DuckDB — `config/build_universe.py`'s `compute_adtv_from_ohlcv`/`compute_market_cap_from_fundamentals`, previously 0%), `tests/unit/test_nse_ipo.py` (5 tests, mocked-HTTP-transport-only pattern matching `test_nse_pledge.py` — `ingestion/scrapers/nse_ipo.py`'s real parse/dedup/retry logic, previously 0%), `tests/unit/test_feature_store_utils.py` (12 tests, real Parquet files on `tmp_path` — `datastore/api/utils/feature_store.py`, previously ~31%). **Final: 68.49%** (5,890 missed) — a genuine but small improvement; reaching 90% overall was not achievable in this session's scope (would require ~40+ new test files across dozens of FastAPI routers, scraper modules, and scheduler steps — realistically multiple further sessions, not a single pass). Per-package breakdown at session end: `features` 80.17%, `config` 76.94%, `datastore` 70.98%, `systems` 66.15%, `ingestion` 63.31%, `backtest` 50.31%. Weakest individual modules (0% or near-0%, still open): `backtest/run_phase1_backtest.py` (21.60%), `backtest/run_phase2_backtest.py`/`run_phase3_backtest.py` (0%, network/live-dependent), `config/build_universe.py`'s `build_universe_csv`/`build_full_nse_universe_from_db` (network-dependent, not covered by this session's DB-driven-function tests), `features/hybrid_compute.py` (0%, 285 stmts, unexamined this session), `datastore/api/routers/technical.py` (19.76%, 253 stmts — large FastAPI router, no existing test file), `datastore/api/routers/ops.py` (33.89%, 298 stmts), `ingestion/scrapers/large_deals.py` (19.30%), `ingestion/scheduler/pipeline_scheduler.py` (41.40%, 744 stmts — large scheduler monolith, see A46). Ran the full quality gate battery (`tests/quality/test_no_stub_or_synthetic_data.py`, `tests/quality/test_duckdb_connection_discipline.py`) plus all new/touched tests: only the 2 known pre-existing failures (A63, A64) reproduced, nothing new introduced. Also independently reproduced `tests/integration/test_daily_pipeline.py::TestPnDBlockExcludedFromTopBuys::test_pnd_blocked_ticker_excluded_from_top_buys` failing (`duckdb.duckdb.ConnectionException: Can't open a connection to same database file with a different configuration`) — a DuckDB cross-process connection-config conflict, environmental (concurrent agent DB access in this shared checkout), not a coverage gap and not introduced by this session's changes; not logged as a new backlog row since a near-identical class of bug (cross-process DuckDB lock races) is already tracked/fixed elsewhere per BuildLog.md. Left open (⏳) for a follow-up session to continue closing router/scraper/scheduler gaps toward 90% |

### Technical

| ID | Item | Area | Status | Blocked On |
|---|---|---|---|---|
| T6 | Make Daily WatchList the AlphaLens.Technical landing page; add a "Technical Deep Dive" page (5/21/63 DMA, 52wk hi/lo, support/resistance, delivery volumes/%) mirroring Signal Deep Dive, opened via a per-row icon in a new tab | Dashboard (Technical) | ⏳ | None — `watchlist.html`/`indicators`/`patterns` APIs already exist to build from |
| T7 | "Charts currently do not work" — live-repro and fix `technical/chart.html` | Dashboard (Technical) | ⏳ | 2026-07-11: confirmed `chart.html`/`chart.js` is fully wired — `GET /api/v1/ohlcv/{ticker}`, `/api/v1/ta/{ticker}/indicators`, `/api/v1/ta/{ticker}/patterns` all verified returning real data via curl; `chartjs-chart-financial` 0.2.1 self-registers against the global `Chart` object exposed by the bundled Chart.js v4.4.4, script load order in `chart.html` is correct. Could not reproduce any failure at the API/wiring level — the reported break is most likely a browser-only runtime issue (console error, CSS/layout glitch, or a stale ticker/date) that requires an actual browser session to catch. Next step: open `chart.html?ticker=<X>` in a real browser and capture the console error the next time it's reported |
| T8 | Backtested Confidence Factor per technical recommendation — hit-rate of hitting resistance before support (or vice versa) over the trailing 200 trading days | ML Signal Engine / Backtest | ⏳ | Confirmed genuinely net-new (2026-07-11 exploration) — no existing table/computation; needs a new aggregation job joining `ta_signals` template fires against subsequent `ohlcv_adjusted` returns |
| T9 | Technical screener appears not to list the full universe — looks like it's only picking up tickers in alphabetical-order order | Dashboard (Technical) / Data Layer | 🔧 | 2026-07-11: root cause found in `systems/technical_analysis/screener/engine.py::_screen_df` (`:326-331`) — results are sorted `_score desc, _vol desc` where `_vol` is `volume_ratio_21d`; when that column is absent from a given day's feature set, the tiebreak silently drops and ties fall back to the source Parquet's original row order, which is ticker-alphabetical — producing exactly the "alphabetical-only" symptom for any template/day where `volume_ratio_21d` isn't populated. Fix not yet applied: needs a deterministic secondary sort (e.g. always include `_score desc` then market-cap or ADTV desc, never silently falling through to file order) — small, scoped change to `_screen_df`, next session. **2026-07-13: fix implemented, in PR pending merge (not yet ✅).** No `market_cap`/ADTV column is actually present in the daily feature Parquet, so the fix instead falls through a priority list of available volume/liquidity proxy columns (`volume_ratio_21d`, `volume_ratio_5d`, `volume_zscore_10d`, `vol_spike_vs_60d_avg`, `breakout_volume_ratio`, `turnover_acceleration`) and, as a final always-present tiebreak, sorts by a deterministic hash of `ticker` — so ties can never silently degrade to alphabetical Parquet row order again, on any day regardless of which proxy columns are populated. Added regression tests in `tests/unit/test_ta_screener.py::TestScreenDfTiebreakOrdering` (missing-volume-columns case reproduces the original bug and asserts non-alphabetical order; determinism-across-calls case; and a case confirming `volume_ratio_21d` still takes priority when present, i.e. no behavior change for the common path). Full `tests/unit/test_ta_screener.py` (34 tests) and the broader `tests/unit -k "technical or screener or ta_"` sweep (125 tests) pass. Branch `fix/ta-screener-tiebreak-t9-engine` pushed to origin (named `-engine` suffix because the repo had concurrent multi-agent sessions running in the same working tree during this fix and `fix/ta-screener-tiebreak-t9` had already been claimed by an unrelated tenacity-retry-migration branch by the time of push — see PR: https://github.com/abaldwa/alphalens/pull/new/fix/ta-screener-tiebreak-t9-engine). |
| T10 | Persist every technical recommendation with strategy name + date to DB (verify/extend existing `ta_signals`) | Data Layer | ⏳ | `ta_signals` already stores `score`/`matched_conditions`/`key_values` per template fire — needs confirming strategy+date are fully captured, not a new table |
| T11 | Multi-strategy consensus: when the same stock is recommended by multiple strategies, list all of them and surface the stock with the most concurrent strategy-recommendations first | Dashboard (Technical) / Features | ⏳ | Depends on T10's persisted recommendation history |
| T12 | Sell-recommendation section for stocks previously Buy-recommended by AlphaLens.ML | Dashboard (ML) / ML Signal Engine | ⏳ | Related to ML26's recommendation-history redesign — may be the same underlying data, different presentation |

### Fundamental

| ID | Item | Area | Status | Blocked On |
|---|---|---|---|---|
| F1 | Sector screen: "Sector-Unique Metrics" sub-panel is a hardcoded empty state | Dashboard (Fundamental) / Features | ⏳ | Needs per-sector metric design (bank GNPA, pharma ANDA approvals, etc.) — no existing data source |
| F2 | Management screen: "Related-Party Transactions" sub-panel is a hardcoded empty state | Dashboard (Fundamental) / Features | 🚫 | Blocked on the same undiscovered-API-param issue as CA6's RPT leg — NSE's `api/related-party-transactions-details` needs a secondary lookup param (`seqNum`/`recId`) from a master-list endpoint that hasn't been found; unblocks once that lookup endpoint is discovered or an alternate RPT data source is identified |

### Big Investors

| ID | Item | Area | Status | Blocked On |
|---|---|---|---|---|

### Damodaran

| ID | Item | Area | Status | Blocked On |
|---|---|---|---|---|

### Forensic

| ID | Item | Area | Status | Blocked On |
|---|---|---|---|---|
| FO1 | Altman Z-Score structurally NaN in production | ML Signal Engine / Forensic / Data Layer | ⏳ | Needs real market cap, retained earnings, EBIT, current assets/liabilities ingested |
| FO2 | Dechow F-Score always called with `{}` — permanently NaN | ML Signal Engine / Forensic | ⏳ | Needs employee-count, share-issuance, book-to-market data — no existing source |
| FO3 | Beneish M-Score's AQI term permanently NaN | ML Signal Engine / Forensic / Data Layer | ⏳ | Needs `current_assets`/PPE columns backfilled from a live scraper |
| FO4 | Forensic Group C fields hardcoded `np.nan` | ML Signal Engine / Forensic | 🚫 | Needs a data-source decision only the user/product owner can make (GST filings vs. an alternate revenue-concentration input) before this can even be scoped — unblocks once that source decision is made |
| FO8 | Several forensic/governance columns unavailable even from NSE XBRL (`contingent_liability_ratio`, etc.) | Data Layer / Ingestion | 🚫 | Only present as freeform "Textual Information" in NSE's template — needs NLP/text extraction |
| FO9 | `altman_z` still NaN for a real subset of tickers | ML Signal Engine / Data Layer | ⏳ | Depends on `shares_outstanding` availability (pre-FY2023-24 filings, implausible-value rejections); full-universe gap size not yet measured |

### Corporate Announcements

| ID | Item | Area | Status | Blocked On |
|---|---|---|---|---|
| CA5 | Corporate Announcements "insider" category is an approximation | Ingestion / Data Layer | 🚫 | No dedicated NSE insider-trading-disclosure endpoint exists (confirmed via investigation) — this is a genuine external-data-availability gap, not a code gap; unblocks only if NSE publishes a dedicated structured endpoint or a paid third-party source is adopted |

### Machine Learning

| ID | Item | Area | Status | Blocked On |
|---|---|---|---|---|
| ML13 | Multibagger tier change-log / "first appeared" date | ML Signal Engine | ⏳ | ML1's scheduled job just landed — needs a few real weekly runs of history to accumulate |
| ML17 | Unified backtest strategy (per-horizon, Nifty benchmark) | Backtest | 🔧 | (a) Real Nifty 500 benchmark curve ✅ 2026-07-11 — `backtest/engine.py` now computes `benchmark_cagr`/`benchmark_sharpe`/`excess_return` per fold. (b) Per-horizon restructuring still unscoped/out of scope. |
| ML22 | Merge Daily Insights and Daily WatchList screens — significant column/purpose overlap | Dashboard (ML) | ⏳ | Needs a design decision on which columns survive the merge (see ML-column-glossary note below) |
| ML23 | Surface SHAP-derived descriptive "Basis" text in table rows, not only on the Signal Deep Dive detail view | Dashboard (ML) / ML Signal Engine | ⏳ | `shap_top5_json` already persisted on `ml_signals`/`ml_multibagger`/`ml_forensic` (ML3 ✅) — this is a rendering/summarization gap, not new plumbing |
| ML24 | Buy Probability / Target / Range inconsistency — e.g. LGINDIA shown with a Buy probability under the 63-day horizon despite a -1.8% target | ML Signal Engine | ⏳ | 2026-07-11: could not reproduce on ticker `LGINDIA` specifically — `GET /api/v1/signals/ml/LGINDIA/{date}` returns `[]` (no rows) across every recent date checked (2026-07-01/05/08/06-22), suggesting either a ticker-symbol typo in the original report or a delisted/uncovered ticker. However, the *general mechanism* the user is describing was independently confirmed on AARTIIND while investigating ML27 (below): `signal_5d.buy_prob` (short-horizon directional) and `q50_return`/`mb_probability` (different-horizon models) can legitimately diverge because they're separate models scored independently, not a single consistent "confidence." Needs: (a) the user to re-confirm the exact ticker/date, (b) a UI fix regardless — label these as distinct model outputs rather than implying one unified probability |
| ML25 | Split "Full Universe" out of Signal Deep Dive (`ml/signal.html`) into its own page; Signal Deep Dive keeps only the per-ticker detail section | Dashboard (ML) | ⏳ | Pure frontend split — both sections already exist on the same page (ML8 ✅) |
| ML26 | Signal Deep Dive layout redesign: Forensic Score, MultiBagger Score, 52wk hi/lo up top; Recommendation History as paired Buy-date/Sell-date/Buy-price/Sell-price/CMP/rationale rows (collapsing a Buy that persists across N days into 1 row); per-horizon (5d/21d/63d) meta-label probabilities + range + Q50 return; SHAP explanation; all raw model scores moved to the bottom | Dashboard (ML) | ⏳ | Builds on ML8's existing "Recommendation History" section — needs the buy/sell-pairing aggregation logic, which doesn't exist yet |
| ML27 | Investigate why MadisonLTD and Aartiind appear as top MultiBagger picks despite apparently negative underlying signals | ML Signal Engine | 🔧 | 2026-07-11: confirmed on AARTIIND — `mb_probability = 0.99995` (`mb_tier: "10x"`, archetype `post_crash_recovery`) on 2026-07-05, while the same-period `signal_5d` model reads `hold` with `buy_prob = 0.094` and `meta_labeler = no_act`. This is a genuine **model-disagreement**, not a wiring bug: MultiBagger scores a long-horizon (multi-year), archetype-based pattern independent of the short-horizon directional models. Root cause is confirmed; remaining work is a UI/labeling fix (surface both signals side-by-side with horizon labels instead of implying they should agree) — not a backend fix. `MADISONLTD` could not be found under that exact ticker symbol (404 on `/api/v1/signals/ml/multibagger/MADISONLTD`) — needs the user to confirm exact symbol |
| ML28 | Extend Sector Rotation (ML12) to 1d/5d/21d/63d relative-strength horizons with trend sparklines, ordered by market cap, with tickers as hyperlinks and %ages explained inline | Dashboard (ML) / Features | ⏳ | Extends `features/sector_rotation.py`, which currently only computes trailing-21-day RS (ML12 ✅) |
| ML29 | Sector accumulation detection: (sum of each stock's delivery % × volume) / sector's total outstanding shares, tracked daily, to surface sectors under constant accumulation; drill-down by clicking a sector's %age | Features / Dashboard (ML) | ⏳ | Needs delivery-percentage and outstanding-shares data already ingested (bhavcopy delivery data) — net-new aggregation, no existing computation |
| ML30 | MyHoldings: move off browser localStorage into a DB-backed table (ticker, purchase date, qty, sale date, sell price, purchase rationale, sell rationale, journal entry) with both manual entry and CSV upload | Data Layer / Dashboard (ML) | ⏳ | `holdings.html`/`js/holdings.js` already exist client-side-only (2026-07-11 exploration) — needs new schema + API routes, then swap the frontend's storage layer |
| ML31 | Investigate why Paper Trading shows no Buy recommendations | ML Signal Engine / Dashboard (ML) | 🔧 | 2026-07-11: root cause confirmed via live query — on 2026-07-08, `/api/v1/signals/ml/top_buys/2026-07-08` returned 20 legitimate `signal_direction: "buy"` candidates (buy_prob 0.56-0.71), but `scripts/run_daily_paper_trading.py::_fetch_buy_candidates` additionally requires `meta_labeler.meta_label == "act"` — every single one of those 20 candidates had `meta_label: "no_act"` with `meta_prob` clustered tightly around 0.44-0.54 (i.e. right at the decision boundary), so the meta-labeler gate vetoes essentially the entire buy list. This explains both `paper_trading/pending` being empty and `gate_status.gate_cleared: false` (only 4/90 days). Not a quick code fix — the meta_labeler model itself appears mis-calibrated (near-random around its threshold rather than confidently separating act/no_act) and needs a retraining/recalibration pass in `systems/ml_signal_engine/models/signal/meta_labeler.py`, which is out of scope for a documentation/UI session |
| ML32 | Documentation deliverable (not code): a column glossary for all ML screens (Q50 Return, Meta Label Prob, P&D Score, MB Prob, etc.) and a list of tickers missing company name/sector | Docs | ⏳ | Flagged as a documentation-only ask so it isn't over-scoped into a feature build; can be produced directly from existing schema + a DB query, no new code |

---

## Architectural

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

**Diagnosed 2026-07-10 (root cause found, not re-run).** The 2026-07-02
window itself has rotated out of `/var/log`/`journalctl -k`, so there's
no smoking-gun log line — but `journalctl -k` on this same host shows
`systemd-oomd` actively SIGKILL-ing AlphaLens processes on sight of
memory pressure (e.g. `alphalens-scheduler.service` killed 2026-07-10
"due to memory pressure ... 88.00% > 50.00% for > 20s with reclaim
activity"). A SIGKILL gives the killed process zero chance to log a
traceback or run an atexit handler — which explains the log's silent
stop after loading 3 TFT checkpoints exactly. This is the same failure
class as the two *dated, confirmed* OOM incidents already documented in
`retrain_phase2.py`'s module docstring (2026-07-07, 2026-07-09):
scoring 5 base models (3 heavy `BacktestEngine` OOF passes + 2 deep
forward passes over 297-feature sequences) in one unbounded process is
the same "everything in one process, no memory ceiling" shape.

Not re-run this session — deliberately, per this task's explicit
instruction to avoid risky full/unattended training runs. Two real
changes made instead:
1. `scripts/train_stacking.py`: `--max-tickers` now defaults to 800
   (was unbounded `None`, matching `retrain_phase2.py`'s
   `DEFAULT_MAX_TICKERS`), and `main()` writes a
   `datastore/models/train_stacking.status.json` STARTED/COMPLETED/
   FAILED marker around the run, specifically so a future silent death
   can be told apart from "never started" or "completed" without
   re-deriving it from log timestamps (a SIGKILL still can't write the
   FAILED marker, but the presence of STARTED-with-no-COMPLETED/FAILED
   is now itself diagnostic).
2. **Decision: not wired into the daily/overnight pipeline this
   session.** Per A42's findings below, TFT/BiLSTM's real per-category
   feature usage is now empirically checked (not just "the code takes
   every column"), but `StackingEnsemble` combining 5 base models is
   still only as trustworthy as its input training runs, and this
   script needs the same per-model subprocess-isolation treatment ML21
   gave `retrain_phase2.py` before it's safe to run unattended. Left as
   an explicit backlog item (not a silent drop) — the fix pattern is
   now proven elsewhere in this codebase, it just hasn't been applied
   here yet.

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

**Attempted 2026-07-10, partially confirmed, not completed.** A38's
training pipeline precondition is now met (A41: real, loadable
`tft_signal_21d_v20260701_fold{0,1,2}.pt`/`bilstm_...` checkpoints
exist, registered in `registry.json`), and `TFTSignalModel` already has
a real, working interpretability method —
`get_shap_values(X)` (`tft_model.py`'s `IExplainableModel` impl) returns
mean Variable-Selection-Network weight per feature across the 63-day
lookback, the TFT's own native per-feature importance signal (not a
proxy/estimate). Confirmed by inspection and by a bounded, inference-only
dry run (no training): loaded `tft_signal_21d_v20260701_fold0.pt`
against a 66-real-parquet-file slice of `datastore/features/daily/` (66
= `SEQ_LEN` 63 + a few extra for sequence construction) —
`sample.columns` intersected with `ALL_FEATURE_COLUMNS` confirmed
**297/297** feature columns are architecturally present at inference
time (i.e. every one of the 16 categories genuinely reaches the model's
input tensor, not just "the code has no allowlist" as a claim).

**Not completed**: the sequence-building step
(`_stream_sequences_from_files`, real per-ticker groupby across the full
~2,300-ticker universe x 66 files) ran for 8+ minutes of CPU time without
finishing on this box even with `max_samples=80`/`300` capped — RSS
stayed safely bounded (~500-650MB, no OOM risk) but it did not converge
within this session's time budget, so `get_shap_values()`'s actual
per-category importance numbers were never produced. This is a
time-budget gap, not a resource/safety one — the script (kept at
`/tmp/.../scratchpad/a42_feature_audit.py`, not committed — a genuine
re-run candidate) would need either a smaller `files` slice restricted
to a small ticker subset (the current `_stream_sequences_from_files`
processes every ticker in every file regardless of `max_samples`, so
capping `max_samples` alone doesn't bound the groupby cost — a real
follow-up finding worth its own fix) or simply more wall-clock time than
this session had.

**Decision, given the above**: A42's "build a dedicated model / build an
independent AlphaLens_Technical screen" fork is **not scoped this
session** — it would be premature to fork new work off a
"category X is dead weight" claim that hasn't actually been measured
yet. What *is* now established: TFT/BiLSTM are provably not
allowlist-restricted (297/297 confirmed reaching the model), so the
"18 advanced TA features are pure overhead" framing T5 originally raised
is resolved for TFT/BiLSTM specifically (they're structurally exposed to
every category); whether the models *learn* from all of them remains
the open, unmeasured question this item's next attempt should resume
directly from (re-run `get_shap_values()` on a small, explicit ticker
subset rather than the full universe).

### A44 — 2026-07-10 laptop-restart OOM: `daily_pipeline` ran unbounded per-ticker fallback against a not-yet-up DataStore API

On a laptop restart, `daily_pipeline.main()` fired its startup catch-up
before the DataStore API (uvicorn, started as a separate process) was up.
`build_feature_matrix`'s bulk OHLCV fetch failed, and its per-ticker
fallback (`_fetch_ohlcv_panel`) then looped through the ~2,300-ticker
universe against a server that wasn't listening, driving observed RSS
from 180MB → 5+GB in under 3 minutes and triggering the OS's low-memory
warning (VS Code + everything else on the box got starved).

**Fixed same session:**
- `ingestion/scheduler/daily_pipeline.py`: new `_wait_for_datastore_api()`,
  called from `main()` before `run_daily_pipeline_once()`. Polls `GET
  /health` (already existed, `datastore/api/routers/system.py`) every 5s
  for up to 120s; logs and proceeds anyway if the API still isn't up
  (steps that need it fail cleanly and retry on the next scheduled/
  catch-up run — same as any other outage, per SPEC-PIPE-006).
- `features/matrix_builder.py::_fetch_ohlcv_panel`: the per-ticker
  fallback now catches `httpx.RequestError` specifically and `break`s
  after the first one instead of `continue`-ing through the rest of the
  universe — a connection error means the whole API is down, not that
  one ticker lacks data, so there's nothing to gain by retrying it 2,300
  more times the same way.
- Verified live: killed the runaway process, started the API, restarted
  `daily_pipeline` — RSS held flat around 290MB through the same 5-day
  backfill that previously ballooned.
- `tests/unit/test_daily_pipeline.py` (26 tests) still green.

**Left open (why this is 🔧 not ✅):**
1. No systemd `Wants=`/`After=` ordering between the DataStore API and
   `daily_pipeline` — both are still started independently, so the
   health-gate is a mitigation, not a guarantee the race is impossible
   (e.g. a much slower API cold-start than 120s would still hit the
   fallback). Should eventually get its own unit + `After=` dependency,
   possibly folded into A45's monitor screen so the ordering is enforced
   and visible in one place.
2. No regression test for the new health-gate or the fail-fast fallback
   path (needs a fake DataStoreClient that raises `httpx.RequestError`
   to assert the loop breaks after ticker 1, not ticker 2,300).
3. The exact reason RSS grew as fast as it did (vs. failing in ~30s the
   way the equivalent `compute_features` RuntimeError path did on
   2026-07-09) was not root-caused with certainty — the fix targets the
   two concrete unbounded-loop mechanisms found, but a live `py-spy`
   profile of the actual runaway process wasn't captured (blocked on
   `sudo` needing interactive auth in this environment). If this recurs
   with the health-gate in place, that's the next thing to capture.

### A55 — 2026-07-11 `alphalens-scheduler.service` OOM during 6-day catch-up backfill: `run_daily_inference` scored the full ~2,317-ticker universe unchunked

Real production incident, live on this machine: `alphalens-scheduler.service`
was killed by `systemd-oomd` at 07:54 IST while running a 6-day catch-up
backfill. `journalctl` confirmed a pressure-based kill (not a hard
`MemoryMax` breach): "Current Memory Usage: 5G", user-slice memory
pressure Avg10=85.36% > its 50% threshold for >20s, "Killed
.../alphalens-scheduler.service ... due to memory pressure". The
scheduler's own `job_run_log` table has no `run_models` row for that
run — it was killed before it could record one — but a prior
`morning_catchup` run on 2026-07-10 recorded `peak_rss_mb=15804.7`,
consistent with the same unchunked full-universe path spiking far past
the cgroup ceiling on a different occasion.

Root cause: `ingestion/scheduler/daily_pipeline.py::step_run_models`
loads the full-universe `feature_matrix`/`pnd_feature_matrix` Parquets
(~2,317 tickers) and calls
`systems/ml_signal_engine/inference/daily_inference.py::run_daily_inference`
on the whole cross-section in one unchunked pass — unlike A47's
`features/matrix_builder.py::_compute_chunked_ticker_independent_panels`,
which already chunks the equivalent full-universe feature-computation
step by ticker using `ingestion/scheduler/resource_guard.adaptive_chunk_size`.
Inside `run_daily_inference`, the heaviest step by far is
`_step_signals_and_meta`: 5 models (signal_5d, meta_labeler, signal_21d,
signal_63d, conformal) each scoring the full eligible cross-section at
once, plus a SHAP `TreeExplainer` pass whose `shap_values` output is a
dense `(n_tickers, n_features, n_classes)` float64 array — for ~150
features x 3 classes x 2,317 tickers, a large array held in memory all
at once before a single row is written. `_step_pnd_filter` has the same
shape of problem at smaller scale.

**Fixed same session:**
- `systems/ml_signal_engine/inference/daily_inference.py`:
  `_step_signals_and_meta` and `_step_pnd_filter` now score/write in
  ticker CHUNKS via `resource_guard.adaptive_chunk_size` (same pattern
  as A47), instead of one full-universe pass. Models are loaded ONCE
  outside the chunk loop (constant-size regardless of ticker count —
  reloading them per chunk would add I/O cost for no memory benefit).
  Each per-chunk `proba.join(meta_out)` result is concatenated at the
  end so `run_daily_inference`'s `tickers_scored` count is unaffected.
- Deliberately did NOT chunk `_step_psi_check` (documented in its own
  docstring) — PSI is a genuinely cross-sectional statistic comparing
  today's full per-feature distribution against a baseline; chunking it
  would silently compare each chunk's non-representative sub-distribution
  instead, corrupting the drift numbers, the same "real cross-ticker
  aggregation" reason A47 excluded fundamental/mf_holdings/multibagger
  from its own chunking. It's also small (tens of MB) and not the
  memory-pressure source. Did NOT chunk the market-wide HMM step either
  — it is inherently a single, non-per-ticker computation. Did NOT
  chunk `_step_exit` — it only ever scores currently-held positions
  (a real portfolio's size, not the full universe).
- `~/.config/systemd/user/alphalens-scheduler.service`: lowered
  `MemoryMax=6G`→`5G`, `MemoryHigh=5G`→`4G`, grounded in `job_run_log`
  showing the routine (heavier, unrelated) nightly `model_training` job
  peaks around 3.9GB, and in `journalctl -k`/`systemd-oomd` logs showing
  the OOM was pressure-based (whole user-slice, not just this unit's own
  ceiling) — on this 14GB host with 3-4GB routinely used by other
  processes, 6G/5G left too little slack for the pressure-based kill to
  avoid firing. **NOT restarted** — the unit file was edited but the
  service was left stopped for a human to review before restarting (per
  explicit instruction — this is a live-system change).
- New `tests/unit/test_daily_inference_chunking.py` (8 tests): proves
  `_step_signals_and_meta`/`_step_pnd_filter`'s chunked output is
  equivalent to a single full-batch pass at multiple forced chunk sizes
  (full-batch, 5, 1 — using real trained Signal5DModel/MetaLabeler/
  PnDDetector instances, not mocks), tolerant of the small (~1e-14
  relative) floating-point noise LightGBM/SHAP's batch-size-dependent
  internal summation order genuinely introduces (verified directly —
  not a chunking correctness bug; see the test module's docstring for
  the investigation). `tests/unit/test_daily_inference_exit_fallback.py`
  and `tests/integration/test_daily_pipeline.py` (pre-existing
  `TestPnDBlockExcludedFromTopBuys` failure confirmed pre-existing via
  `git stash`, unrelated to this change) re-run clean otherwise.

**Left open (🔧 not ✅ — needs human sign-off):**
1. The systemd unit's new `MemoryMax=5G`/`MemoryHigh=4G` values are
   evidence-grounded but not battle-tested against a real multi-day
   catch-up backfill post-chunking — recommend the operator watch
   `job_run_log.peak_rss_mb` / `poll_process_resources` on the next
   real catch-up run before trusting the new ceiling fully.
2. A dedicated `tracemalloc`/`resource.getrusage`-based test proving the
   chunked path uses proportionally less peak memory than the unchunked
   path was not added — a reliable, host-independent memory measurement
   at unit-test scale (tens of tickers, fast) wouldn't reflect the real
   ~2,317-ticker peak-memory delta the fix is meant to address, and
   would either be too noisy to be a real gate or too slow (full
   universe) to run in the normal suite. The correctness-equivalence
   test above is the primary proof; the memory-reduction claim rests on
   the same structural argument A47's chunking PR already relied on
   (bounded working set per chunk vs. one full-universe-sized pass) plus
   this incident's own `job_run_log`/`journalctl` numbers.
3. `_step_pnd_filter`/`_step_signals_and_meta` were chunked; `daily_
   pipeline.py::step_run_models`'s own Parquet reads (`pd.read_parquet`
   of the full feature matrices) were NOT chunked — those are one-shot
   full-DataFrame reads handed to `run_daily_inference` as a single
   argument by design (its own docstring), and chunking that read would
   require restructuring `run_daily_inference`'s public signature, out
   of scope for this incident fix. Feature-matrix Parquet reads are
   also far smaller than the per-ticker model-scoring arrays that
   actually caused this incident.

### A45 — AlphaLens_Ops "Jobs & Models" monitor screen

Prompted directly by A44: even with that fix, the operator (running this
on a single laptop, no ops team) had no single place to see "is something
about to eat all my RAM" or "when did/will each job/model actually run"
without SSHing in and running `ps`/`free` by hand. Needs a real screen,
not another log file nobody tails.

**Scope, in two halves:**

1. **Jobs & Models rollup table.** One screen on `AlphaLens_Ops` listing:
   - Every scheduled job (the 13 wrappers A23 already instruments:
     `daily_pipeline`, `morning_catchup`, `mf_holdings_ingestion`,
     `model_training`, `weekend_feature_backfill`,
     `weekend_fundamentals`, `daily_backup`, `job_health_check`,
     `multibagger_scoring`, `forensic_scoring`, `nse_xbrl_fundamentals`,
     `backfill_catchup`, `emergency_recompute`) — last run time, last
     status, next scheduled fire time (APScheduler already exposes this;
     `/health`'s `scheduler` block above is most of the data source),
     last `duration_seconds`/`peak_rss_mb` (A23's columns).
   - Every model in `datastore/models/registry.json` (all 8+ phase-1/2
     models, TFT/BiLSTM, multibagger, `signal_63d`, `ExitSignalModel`,
     the still-dormant `StackingEnsemble` from A40) — last trained date,
     which script trains it, whether it's actually wired into
     `daily_inference.py` (surfaces A38/A40/A41/A42's "is this model
     real or orphaned" questions in one place instead of a fresh model
     audit each time).
   Mostly plumbing: a new API endpoint joining `job_run_log` +
   APScheduler's job store + `registry.json`, and a new dashboard route
   under the existing Ops app (same pattern as A4's Console/A5's weekend
   schedule panel).

2. **Live, broader memory polling + alerting + one-click actions.**
   A16's existing 30-min resource monitor
   (`datastore/logs/scheduler_resource_monitor.log`, feeding the Ops
   Monitor panel) only tracks `mem_available`/load average, on a 30-min
   cadence, and its only "action" is throttling `hmm_workers`/
   `preload_workers` for its own process. A44 showed that's not enough:
   it can't see per-process RSS (couldn't tell you *which* process is
   the problem), doesn't poll continuously, and has no operator-facing
   alert or manual controls. Needs:
   - Continuous (seconds-to-low-minutes, not 30-min) polling of: system
     available/used/swap (what `free` shows), per-process RSS for every
     AlphaLens process (what `ps`/`psutil` shows — this is what would
     have let A44 be spotted and diagnosed in seconds instead of several
     `ps`/`free` round-trips), and DuckDB file-lock contention (ties into
     the `check_ta_alerts` lock race fixed earlier this project).
   - A visible alert state on the Ops screen (not just a log line) when
     any threshold is crossed, so it's seen without tailing a file.
   - Manual corrective controls exposed in the UI: adjust
     `FEATURE_CACHE_PRELOAD_WORKERS`/`HMM_FEATURE_WORKERS`/chunk sizes
     live (or trigger the restart A16 already does programmatically),
     kill/restart a specific runaway job, without dropping to a shell.
   - **Build-vs-adopt decision, explicitly left open:** rolling this by
     hand (a `psutil`-based poller feeding a new DuckDB table + a
     dashboard page) keeps everything inside one stack the operator
     already knows, but reinvents a lot of what a real monitoring tool
     does well. Worth evaluating self-hosted open-source options that
     can run *outside* the AlphaLens process entirely (operator has said
     they're open to this) before building bespoke:
     - **Netdata**: single-binary, per-second granularity, built-in
       anomaly alerts, has a "systemd services" collector that would
       show each AlphaLens job's cgroup memory directly — probably the
       best fit for "just tell me when a process is about to eat all my
       RAM," but is a separate always-on service on the laptop.
     - **Glances**: lighter, Python-native (fits this stack), has a
       web UI + REST API that AlphaLens_Ops could poll/embed instead of
       re-implementing process listing.
     - **psutil + a thin custom exporter** feeding Prometheus/Grafana:
       most flexible, most work; probably overkill for a single laptop.
     Whichever is chosen (or hand-rolled), the AlphaLens_Ops screen's job
     is to surface it — either by embedding/linking the external tool's
     UI, or by polling its API into the same Jobs & Models screen from
     (1) above, so the operator has one place to look, not two.
   - Depends on A23 (job-level duration/RSS history) and A16 (existing
     resource-monitor plumbing/throttling logic) as the in-repo starting
     points; genuinely new work is the always-on poller, the alerting
     surface, and the live-control wiring.

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

---

---

## Technical

### T6-T12 — "Create additional Features" AlphaLens.Technical asks (logged 2026-07-11)

Sourced from the same requirements dump as A66-A72/ML22-ML32. 2026-07-11
exploration found `technical/chart.html`/`chart.js` fully implemented
against real OHLCV/indicator/pattern APIs (candlesticks, SMA/EMA overlays,
volume, curated indicators panel) — so T7 ("charts don't work") is logged
as a bug-repro item, not a rebuild. T8 (backtested Confidence Factor) is
confirmed genuinely net-new: no existing table or computation backs a
per-recommendation historical hit-rate today. T10 likely mostly exists
already via `ta_signals` (score/matched_conditions/key_values per template
fire) and needs verification rather than a new table.

---

## Fundamental

### F1/F2 — Hardcoded empty-state sub-panels (Sector, Management)
`sector.js:16-19`'s "Sector-Unique Metrics" panel and `management.js:20-23`'s
"Related-Party Transactions" panel both call `renderEmptyState(...)`
unconditionally, before any network request — not a loading/error state,
a permanent stub. Matches `alphalens_docs/CLAUDE.md:492`'s documented "one
empty-stated sub-panel each" claim exactly; confirmed accurate, not stale.

### F3 — Trendlyne `current_assets`/`current_liabilities` fallback: 405-cascade fixed, never actually populated the DB at scale (2026-07-13)

**Background.** `features/fundamental_source_priority.py` already correctly
ranks `nse_xbrl=4 > trendlyne=3 > screener=2` and
`scripts/backfill_fundamentals_trendlyne.py` is wired to that priority
system, but zero rows in the real DB are tagged
`fundamentals_source='trendlyne'` — the two live runs on record both
failed:
  - `logs/trendlyne_backfill.log` (2026-06-25): 0/~1148 sampled tickers
    matched (mass HTTP 405s starting a few minutes into the run).
  - `logs/trendlyne_backfill_full2644_20260630.log` (2026-06-30): only
    138/2644 tickers matched (2506 "not-on-Trendlyne").

**Root cause (confirmed, not the ticker-matching bug it looked like).**
A live re-check on 2026-07-13 shows the exact same tickers that failed in
both runs — including large caps like ADANIPORTS, and the very first
failure in the 06-25 log (ACLGATI) — resolve fine today with the
*identical* login/URL/dash-slug-fallback logic already in the code (that
fallback, referenced at `BuildLog.md:6790`, was already correctly in
place by 06-30 and is not the fix). Live validation runs today:
10/10, then 65/65 (before hitting a live block), then 20/20 sampled
universe tickers all succeeded, with real `current_assets`/
`current_liabilities` values pulled (e.g. INFY CA=103489.0 CL=52322.0,
HDFCBANK CA=3611332.96 CL=252977.53, RELIANCE CA=594249.0 CL=541254.0,
ADANIPORTS CA=21974.83 CL=15760.35 — all cross-checked as plausible
real INR-Cr balance-sheet figures for FY2026).

The real bug: Trendlyne intermittently returns HTTP 405 as a WAF/
rate-limit signal (confirmed live 2026-07-13 — even the *login* endpoint
itself returned 405 during one of this session's own rapid-succession
validation attempts, recovering after a ~90s pause). The old code in
`_fetch_ticker_data` treated 405 exactly like a genuine 404 "ticker not
on Trendlyne" and applied the fast `0.3x` notfound retry delay before the
next request — which fed the block instead of backing off from it,
turning a transient rate-limit trip into a cascading near-100%-failure
tail for the rest of the run (this exactly explains both historical
runs' shape: mostly-clean start, then a wall of 405s once the WAF
tripped).

**Fix implemented** (branch `fix/trendlyne-405-waf-circuit-breaker`,
commit 66fca7f, PR: https://github.com/abaldwa/alphalens/pull/new/fix/trendlyne-405-waf-circuit-breaker):
`_fetch_ticker_data` now returns `(body, reason)` with `reason` in
`{"ok", "404", "405", "error"}`, keeping the existing dash-slug fallback
(a real, if rare, case) but classifying a still-failing 405/403 as a
possible block rather than folding it into "not on Trendlyne". `main()`'s
loop applies the full-length sleep (not the fast 404 skip) on 405, and a
circuit breaker: after 5 consecutive 405s, back off 60s and re-login
before continuing — observed live during this session's own validation
(triggered once at ticker 70/80, recovered on the next attempt).
Added `tests/unit/test_backfill_fundamentals_trendlyne.py` (6 tests,
mocked `requests.Session`, no live network) covering the 404-vs-405-vs-ok
classification, including the "405 + failing dash-slug fallback stays
405, not 404" case that was the actual gap.

**Full 2644-ticker backfill: recommended as a safe next step, NOT run in
this task.** With the fix, sampled runs show ~100% match rate on tickers
genuinely on Trendlyne (vs. 0% and 5.2% before) modulo the WAF's own
transient trips, which the circuit breaker now survives automatically
(costs ~60s per trip, self-recovers) instead of silently degrading the
whole run. Recommend running via the script's existing
`--publish-mode staged` (default, A25 rollback point) exactly as already
built — `nohup .venv/bin/python3 scripts/backfill_fundamentals_trendlyne.py
--universe-only > logs/trendlyne_backfill_YYYYMMDD.log 2>&1 &`, expect
several circuit-breaker trips (~60s each) over the ~1hr run, watch the
log for the final `ROE/ROCE completeness` summary and confirm a
non-trivial `fundamentals_source='trendlyne'` row count afterward. Not
run here per this task's explicit scope (live full-backfill go-ahead is a
separate action, same precedent as A26's Ops force-run).

---

## Big Investors

_(No detailed writeups — all items in this area are either fully captured by the table row above or completed; see FeatureBacklogImplemented.md.)_

---

## Damodaran

_(No detailed writeups — all items in this area are either fully captured by the table row above or completed; see FeatureBacklogImplemented.md.)_

---

## Forensic

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

---

## Corporate Announcements

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

---

## Machine Learning

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

### ML22-ML32 — "Create additional Features" AlphaLens.ML asks (logged 2026-07-11)

Sourced from the same requirements dump as A66-A72/T6-T12. Cross-referenced
against 2026-07-11 exploration to avoid duplicating already-shipped work:
Signal Deep Dive already has a Full-Universe section and a Ticker-Detail
section with Recommendation History, All Model Scores, and a "SHAP — Why
This Signal" section (ML8 ✅, ML3 ✅) — ML23/ML25/ML26 are layout/logic
redesigns of that existing page, not new plumbing. `shap_top5_json` is
already persisted on `ml_signals`/`ml_multibagger`/`ml_forensic` and
returned by the signals API. Sector Rotation already computes trailing-21-
day relative strength (ML12 ✅) — ML28 is an extension (more horizons +
sparklines + market-cap ordering), not a rebuild. Paper Trading
(`positions.html`) and MyHoldings (`holdings.html`) both already exist;
MyHoldings is currently browser-localStorage-only (explicitly not server-
persisted, not used in training) — ML30 is the DB-persistence gap.
ML24/ML27/ML31 are bug reports (Buy-Prob/Target inconsistency, MultiBagger
picks with apparently negative signals, Paper Trading showing no Buy
recommendations) that need live investigation/repro before any fix is
scoped — logged as investigation items, not assumed bugs.

Separately, the user asked (in the same document) why NSE XBRL is the
default fundamentals source with Trendlyne used only as a backup for spot-
checking data integrity — this is **already the case**, not a gap:
`features/fundamental_source_priority.py` ranks `nse_xbrl=4 > trendlyne=3 >
screener=2 > external_csv=1` (A36 ✅), `datastore/schema/
create_normalised.py` documents NSE XBRL as the preferred/primary source
with Trendlyne/Screener as fallback, and A59/A61 track the known remaining
gaps in that sourcing. No new backlog item logged for this — it's a
clarification, answered by pointing at existing entries.
