# AlphaLens — Claude Code Prompt Guide
## All Phases · All Prompts · Spec-Referenced · No Ambiguity

**How to use this file:**
- Copy each prompt verbatim into Claude Code
- Run the ✅ TEST step before every 🔀 COMMIT
- Never skip a gate check — it blocks real downstream work
- If a prompt fails, add: "The error is: [paste error]. Fix it and retry."
- CLAUDE.md is read automatically at session start. No need to re-explain architecture.

---

## Legend
```
📋 PROMPT    — Paste this into Claude Code
✅ TEST      — Run this before committing
🔀 COMMIT    — Git commit with this message
🔒 GATE      — All items must pass before next phase
⚠️  MANUAL   — You must do this yourself (not Claude Code)
```

---

# PHASE 0 — Infrastructure & Data Foundation (Weeks 1–4)s

---

## P0.1 — Project Skeleton

📋 **PROMPT:**
```
Read alphalens_docs/CLAUDE.md, alphalens_docs/12_platform_architecture.md, and alphalens_docs/specs/08_specifications.md sections SPEC-SYS-001 through SPEC-DS-007.

Create the full project skeleton:
1. All directories from the platform architecture (datastore/, ingestion/, features/, systems/, backtest/, config/, tests/, requirements/)
2. config/settings.py with every constant from SPEC-SYS-001 through SPEC-SYS-005 — universe size, paths, thresholds, cost model
3. config/nse_holidays.py with all NSE trading holidays for 2025 and 2026 (used by SPEC-SCHED-008)
4. config/universe.py that loads Nifty 500 tickers from a CSV file
5. requirements/phase0.txt: pandas, numpy, pyarrow, duckdb, sqlalchemy, requests, beautifulsoup4, APScheduler, python3-dotenv, pytest, pytest-cov
6. requirements/phase1.txt: all phase0 + lightgbm, catboost, xgboost, hmmlearn, scikit-learn, mapie, optuna, imbalanced-learn, shap, ta-lib, lifelines, scikit-survival, ruptures, hdbscan, river, fastapi, uvicorn, pydantic, httpx. Do not add mlfinlab; triple-barrier labeling is implemented natively.
7. .env.example with placeholder keys: FYERS_APP_ID, FYERS_SECRET_ID, FYERS_ACCESS_TOKEN
8. .gitignore: .env, *.db, *.duckdb, datastore/raw/, datastore/normalised/, datastore/features/, datastore/models/, __pycache__, *.pyc
9. README.md: one-paragraph purpose, setup instructions (conda env + pip install), how to run pipeline

Every file must have a module-level alphalens_docstring referencing its SPEC-ID per SPEC-TRACE-002. No hardcoded values anywhere — all constants in config/settings.py.
```

✅ **TEST:**
```bash
python3 -c "from config.settings import *; print('Settings OK')"
python3 -c "from config.universe import load_universe; print(f'Universe loader OK')"
pytest tests/ -v  # should show 0 tests, 0 failures
```

🔀 **COMMIT:** `feat(SPEC-SYS-001): project skeleton, settings, universe config, requirements`

---

## P0.2 — DataStore Schema & API Shell

📋 **PROMPT:**
```
Read alphalens_docs/12_platform_architecture.md (Six Stores section) and alphalens_docs/specs/08_specifications.md sections SPEC-DS-001 through SPEC-DS-007, SPEC-PIPE-003.

Build the DataStore foundation:
1. datastore/schema/create_normalised.py — creates all DuckDB tables:
   - ohlcv_adjusted(date, ticker, open, high, low, close, volume, delivery_qty, delivery_pct, adj_factor)
   - corporate_actions(ticker, ex_date, action_type, ratio, announcement_date, record_date)
   - fundamentals(ticker, fiscal_year, quarter, quarter_end_date, announcement_date, revenue, ebitda, pat, eps, operating_margin, ebitda_margin, net_margin, roe, roce, debt_to_equity, interest_coverage, fcf, asset_turnover, inventory_days, receivable_days, payable_days, book_value_per_share, shares_outstanding)
   - shareholding(ticker, quarter_end_date, filing_date, promoter_pct, promoter_pledge, fii_pct, dii_pct, mf_pct, retail_pct)
   - macro_indicators(date, indicator, value)
   - stock_master(ticker, company_name, sector, industry, nse_series, listing_date, market_cap_cr, adtv_cr, current_tier, is_fno_eligible, is_nifty500)
2. datastore/schema/create_signals.py — creates SQLite signals.db:
   - pipeline_runs(run_id, date, started_at, completed_at, status, stocks_processed, error_message)
   - ml_signals table (all columns from architecture doc)
   - ml_multibagger table
   - ml_forensic table
3. datastore/client.py — DataStoreClient class per SPEC-SOLID-005:
   - get_ohlcv(ticker, from_date, to_date), get_fundamentals_pit(ticker, as_of), get_signals(ticker, date)
   - All methods make httpx calls to localhost:8000 — no direct DB access
4. datastore/api/main.py — FastAPI shell:
   - GET /health — returns pipeline status, last run, stocks processed
   - GET /api/v1/ohlcv/{ticker} — stub, returns empty list
   - GET /api/v1/fundamentals/{ticker} — stub, returns null
   - GET /api/v1/signals/ml/{ticker}/{date} — stub, returns null
5. Unit test tests/unit/test_schema.py:
   - Creates schemas in an in-memory DuckDB
   - Verifies all tables exist with correct columns
   - Tests that ohlcv query with as_of date only returns rows where date <= as_of (PIT rule)

All PIT rules from SPEC-PIPE-003 must be enforced by schema constraints or documented as API-layer enforcement.
```

✅ **TEST:**
```bash
# Run schema scripts with -m, from the project root — never as a bare file
# path. `python3 datastore/schema/create_normalised.py` puts only
# datastore/schema/ on sys.path, not the project root, so the script's own
# `from datastore.api.db import ...` import fails with
# `ModuleNotFoundError: No module named 'datastore'`. `-m` puts the project
# root on sys.path instead, which resolves it correctly.
python3 -m datastore.schema.create_normalised
python3 -m datastore.schema.create_signals
python3 -c "import duckdb; conn = duckdb.connect('datastore/normalised/alphalens.duckdb'); print(conn.execute('SHOW TABLES').fetchall())"
uvicorn datastore.api.main:app --host 0.0.0.0 --port 8000 &
curl http://localhost:8000/health
pytest tests/unit/test_schema.py -v
```

🔀 **COMMIT:** `feat(SPEC-DS-007): DataStore DuckDB schema, signals SQLite schema, API shell, DataStoreClient`

---

## P0.3 — Scheduler & Checkpoint Engine

📋 **PROMPT:**
```
Read alphalens_docs/13_scheduler_resilience.md and alphalens_docs/specs/08_specifications.md sections SPEC-SCHED-001 through SPEC-SCHED-011.

Build the scheduler and checkpoint system:
0. Do not show interim steps or explain every thought on the screen. Run the necessary terminal commands, implement the changes, and record the major steps, errors, and resolutions as structured markdown in a file named BuildLog.md in the current directory.
1. If changes are required, update this file for all subsequent prompts.
2. ingestion/scheduler/pipeline_scheduler.py — APScheduler with SQLAlchemyJobStore:
   - Three modes: linear (default), timestamp, manual (SPEC-SCHED-001)
   - On startup: query pipeline_runs table, find all trading dates since last successful run
   - Backfill mode: process missing dates chronologically, oldest first (SPEC-SCHED-004)
   - No ML inference during backfill (SPEC-SCHED-006)
   - NSE holiday awareness from config/nse_holidays.py (SPEC-SCHED-008)
   - misfire_grace_time=86400
3. ingestion/scheduler/checkpoint.py — CheckpointManager class:
   - save_checkpoint(date, step_name, status) — writes to pipeline_runs SQLite
   - load_checkpoint(date) — returns last completed step for a date
   - Steps: ['download_bhavcopy', 'download_fno', 'adjust_prices', 'compute_features', 'run_models', 'write_signals']
   - On failure: record error_message, status='failed'; next startup resumes from failed step (SPEC-SCHED-002)
   - Atomic writes only (SPEC-SCHED-010)
4. ingestion/scheduler/gap_detector.py:
   - detect_gaps() — returns list of missed trading dates between last run and today
   - Uses NSE holiday calendar to skip non-trading days
5. tests/unit/test_scheduler.py:
   - Test gap detection finds 3 missed dates when last run was 5 days ago with 2 holidays
   - Test checkpoint save and resume: simulate failure at 'compute_features', verify next run starts from that step
   - Test backfill processes dates oldest-first
6. tests/integration/test_scheduler_resume.py:
   - Full integration: run pipeline, simulate crash at step 3, restart, verify it resumes not restarts

alphalens_docstrings in all files must reference SPEC-SCHED-001 through SPEC-SCHED-011 as applicable.
```

✅ **TEST:**
```bash
pytest tests/unit/test_scheduler.py -v
pytest tests/integration/test_scheduler_resume.py -v
```

🔀 **COMMIT:** `feat(SPEC-SCHED-001): APScheduler pipeline, checkpoint-resume, gap detection, holiday awareness`

---

## P0.4 — NSE Bhavcopy + FNO Scrapers

📋 **PROMPT:**
```
Read alphalens_docs/specs/08_specifications.md SPEC-PIPE-001, SPEC-PIPE-005, SPEC-PIPE-006 and alphalens_docs/specs/API_SPEC.md.

Build the data ingestion scrapers:
0. Do not show interim steps or explain every thought on the screen. Run the necessary terminal commands, implement the changes, and record the major steps, errors, and resolutions as structured markdown in a file named BuildLog.md in the current directory.
1. If changes are required, update this file for all subsequent prompts.
2. ingestion/scrapers/bhavcopy.py:
   - download_bhavcopy(date: str) → pd.DataFrame
   - Downloads NSE equity bhavcopy from archives.nseindia.com
   - Columns: ticker, open, high, low, close, volume, traded_qty, delivery_qty, series
   - Filter to EQ series only; skip BE, BL, SM, ST series
   - Raises ConnectionError after 3 retries; raises ValueError if < 450 stocks found
   - Validates: no ticker appears twice, delivery_pct in [0, 100], prices > 0
3. ingestion/scrapers/fno.py:
   - download_fno_bhavcopy(date: str) → pd.DataFrame
   - Downloads NSE F&O bhavcopy
   - Stores OI, volume, settle_price by ticker/expiry/strike/option_type
4. ingestion/scrapers/macro.py:
   - download_vix(date: str) → float — from NSE VIX page
   - download_fiidii(date: str) → dict — FII/DII buy/sell from NSE
   - download_fx(date: str) → dict — USD/INR from RBI or Yahoo Finance
   - All have retry=3, fallback to previous day value if unavailable (SPEC-PIPE-006)
5. ingestion/adjust/price_adjuster.py:
   - adjust_for_corporate_actions(conn, ticker: str) → None
   - Idempotent (SPEC-PIPE-002): checks adj_factor before applying
   - SPLIT: multiply all pre-ex prices by 1/ratio; BONUS: multiply by 1/(1+ratio)
   - Post-check: price continuity at ex-date < 1% gap after adjustment
6. tests/unit/test_bhavcopy.py:
   - Test download returns DataFrame with required columns
   - Test raises ValueError when < 450 stocks returned (mock the HTTP response)
   - Test delivery_pct validation catches out-of-range values
7. tests/unit/test_price_adjuster.py:
   - Test split adjustment is idempotent (calling twice gives same result)
   - Test bonus adjustment: price × 1/(1+ratio)
   - Test continuity check passes for valid adjustment

All functions: alphalens_docstrings with SPEC-ID references per SPEC-TRACE-002.
```

✅ **TEST:**
```bash
pytest tests/unit/test_bhavcopy.py tests/unit/test_price_adjuster.py -v
# Manual spot check:
python3 -c "from ingestion.scrapers.bhavcopy import download_bhavcopy; df = download_bhavcopy('2025-01-15'); print(df.shape, df.columns.tolist())"
```

🔀 **COMMIT:** `feat(SPEC-PIPE-001): bhavcopy scraper, FNO scraper, macro scraper, corporate action adjuster`

---

## P0.5 — FYERS Historical Backfill

⚠️ **MANUAL FIRST:** Open FYERS account at fyers.in. Get App ID + Secret from myapi.fyers.in. Add to `.env`.

📋 **PROMPT:**
```
Read alphalens_docs/03_data_pipeline.md sections on historical backfill and alphalens_docs/specs/08_specifications.md SPEC-PIPE-001, SPEC-PIPE-002.

Build the FYERS historical backfill pipeline:
0. Do not show interim steps or explain every thought on the screen. Run the necessary terminal commands, implement the changes, and record the major steps, errors, and resolutions as structured markdown in a file named BuildLog.md in the current directory.
1. If changes are required, update this file for all subsequent prompts.
2. ingestion/scrapers/fyers_backfill.py:
   - FYERSBackfill class using fyers-apiv3 (pip install fyers-apiv3)
   - get_access_token() — OAuth2 flow using FYERS_APP_ID and FYERS_SECRET_ID from .env
   - download_history(ticker, from_date, to_date, timeframe='D') → pd.DataFrame
   - Rate limiting: max 1000 API calls/day; built-in throttle with 0.5s sleep
   - batch_download(tickers: List[str], from_date, to_date) — downloads all with progress bar (tqdm)
   - Saves each batch to datastore/raw/fyers/TICKER_YYYY-MM-DD_YYYY-MM-DD.parquet
3. ingestion/backfill_runner.py:
   - Loads Nifty 500 ticker list from config/universe.py
   - Calls batch_download for 5 years of daily data
   - After each ticker: write to DuckDB ohlcv_adjusted table via DataStore API
   - Tracks progress: skip tickers already in DuckDB with sufficient history
   - Estimated runtime displayed: "Estimated 3.5 hours based on rate limit"
4. ingestion/scrapers/nse_delivery_loader.py:
   - Parses NSE historical bhavcopy archives for delivery data (5 years)
   - Merges delivery_qty and delivery_pct into existing ohlcv_adjusted rows
5. tests/unit/test_fyers_backfill.py:
   - Mock FYERS API response; test batch_download processes all tickers
   - Test rate limiting: verify 0.5s sleep between calls
   - Test resumes from last completed ticker (checkpoint)

Include a progress checkpoint: save last completed ticker to a resume file so backfill can restart after interruption.
```

✅ **TEST:**
```bash
pytest tests/unit/test_fyers_backfill.py -v
# Get a FYERS access token. Use the non-interactive two-step CLI below, not
# FYERSBackfill's built-in input()-based flow directly -- a blocking
# input() hangs forever in any terminal/IDE pane without a connected
# stdin (confirmed in practice during P0.5 development; see BuildLog.md
# "Post-handoff bug #3"):
python3 -m ingestion.scrapers.fyers_backfill login
# -> open the printed URL in a browser, log in, then copy the FULL
#    redirected URL from the browser's address bar. It will show a
#    connection-refused page at https://127.0.0.1/?auth_code=...&state=...
#    -- that's expected; nothing needs to be listening there.
python3 -m ingestion.scrapers.fyers_backfill exchange "<paste the redirected URL here>"
# -> exchanges the code and caches a real access token to disk for the rest of the day
```

⚠️ **MANUAL — Run the full backfill** (3–4 hours, run overnight):
```bash
python3 -m ingestion.backfill_runner --from 2020-01-01 --to 2025-12-31
# -- NOT `python3 ingestion/backfill_runner.py ...` (direct script path):
# this module uses absolute imports (config.settings, datastore.api.db)
# that only resolve when run with -m, which puts the project root on
# sys.path; a direct path invocation fails with ModuleNotFoundError.
# Verify:
python3 -c "import duckdb; conn = duckdb.connect('datastore/normalised/alphalens.duckdb'); print(conn.execute('SELECT COUNT(*) FROM ohlcv_adjusted').fetchone())"
# Must return >= 600,000 rows (assumes the official full Nifty 500 list has
# replaced the starter config/nifty500_universe.csv sample -- see that
# file's own docstring)
```

🔀 **COMMIT:** `feat(SPEC-PIPE-001): FYERS backfill pipeline, NSE delivery loader, resume checkpoint`

---

## P0.6 — Laptop-Only Daily Pipeline Scheduler (Oracle Cloud deferred)

✅ **ALREADY COMPLETE** — see BuildLog.md "P0.6 — Laptop-Only Pivot + Daily Pipeline
Scheduler Job" for the full record. Oracle Cloud Free Tier provisioning was attempted
and abandoned: `ap-mumbai-1` had zero free `VM.Standard.A1.Flex` capacity at any size,
and the account's Free Trial status blocks subscribing to an alternate region without
an irreversible upgrade to Pay-As-You-Go. SPEC-SCHED-009 (formerly "Oracle Cloud
Independence") already specified an Oracle-first/NSE-archive-fallback design — the
fallback path is now simply the only path; no ingestion code ever had a hard Oracle
dependency. See alphalens_docs/06_deployment.md "Oracle Cloud (deferred)" if Oracle
capacity becomes worth revisiting later (Phase 2+, when always-on intraday capture
actually matters).

What was actually built instead, for any future session re-reading this file:
- `ingestion/scheduler/daily_pipeline.py`: concrete `step_runner` wiring real
  ingestion functions (bhavcopy, macro, price adjustment) into the
  `ingestion/scheduler/pipeline_scheduler.py` engine built in Phase 0.3 — registered
  as a persistent APScheduler job (`schedule_daily_pipeline()`, 18:00 IST Mon-Fri),
  **not** OS-level crontab. Run via `python -m ingestion.scheduler.daily_pipeline`
  (foreground or `nohup ... &`) and leave it running; it self-catches-up on startup.
- `download_fno` and the live option-chain scraper remain deferred to Phase 2 — F&O
  features aren't needed for Phase 1, and NSE's F&O bhavcopy archive endpoint is
  currently broken (serves a PDF, not a CSV) regardless of Oracle/laptop hosting.
- A real pre-existing bug was found and fixed in this phase: nothing had ever written
  to the `pipeline_runs` table, so the startup gap-detection/catch-up mechanism from
  Phase 0.3 would have silently never triggered. Fixed in `pipeline_scheduler.py`.

If a future session needs to revisit Oracle Cloud deployment, do **not** reuse the
prompt that originally lived here — it assumed `ingestion/oracle_scrapers/` as a
parallel scraper tree, which would now duplicate `ingestion/scheduler/daily_pipeline.py`'s
step dispatch. Read BuildLog.md's P0.6 entry first and design Oracle as an additional
*execution environment* for the existing step functions, not a separate codebath.

---

## P0.7 — Data Quality + Observability + PSI Baseline

📋 **PROMPT:**
```
Read alphalens_docs/specs/08_specifications.md sections SPEC-PIPE-005, SPEC-OBS-001 through SPEC-OBS-005.

Build data quality and observability:
0. Do not show interim steps or explain every thought on the screen. Run the necessary terminal commands, implement the changes, and record the major steps, errors, and resolutions as structured markdown in a file named BuildLog.md in the current directory.
1. If changes are required, update this file for all subsequent prompts.
2. ingestion/quality/validator.py:
   - validate_bhavcopy(df: pd.DataFrame, expected_tickers: List[str]) → dict
   - Returns: {'ok': bool, 'missing': List[str], 'anomalies': List[str], 'stock_count': int}
   - Anomaly: any stock with > 30% single-day price change (without known corp action)
   - Completeness gate: ok=False if stock_count < 450 (SPEC-SYS-003)
3. ingestion/quality/drift_monitor.py:
   - PSIMonitor class: compute_psi(feature_name, current_values, baseline_values) → float
   - Alerts: PSI > 0.10 = warning (reduce position sizing 50%), PSI > 0.25 = halt + retrain
   - compute_baseline(feature_matrix: pd.DataFrame) → saves to datastore/features/baseline/stats_baseline.pkl
   - Daily: run top-50 features through PSI check after feature matrix is built
4. config/observability.py — master observability switch per SPEC-OBS-001:
   - OBSERVABILITY_LEVEL: 'production' | 'development' | 'debug'
   - In production mode: no verbose logging, no intermediate file writes (SPEC-OBS-005)
5. ingestion/quality/structured_logger.py — per SPEC-OBS-003:
   - log_pipeline_step(step, status, stocks, duration_s, error=None)
   - Output format: JSON lines to logs/pipeline_YYYY-MM-DD.jsonl
   - Never logs raw financial data values (security — SPEC-SEC-001)
6. tests/unit/test_validator.py:
   - Test completeness gate blocks at 449 stocks
   - Test anomaly detection flags 35% price change
   - Test PSI calculation: known distribution shift returns expected PSI value
7. Compute PSI baseline:
   - baseline_runner.py: load 2 years of existing data, compute stats_baseline.pkl
   - Must run after backfill is complete

All functions alphalens_docstrings reference SPEC-OBS-001 through SPEC-OBS-005, SPEC-PIPE-005.
```

✅ **TEST:**
```bash
pytest tests/unit/test_validator.py -v
python3 -m ingestion.quality.baseline_runner  # run after backfill
```

🔀 **COMMIT:** `feat(SPEC-PIPE-005): data validation, PSI drift monitor, observability, structured logging, PSI baseline`

---

## 🔒 PHASE 0 GATE CHECK

📋 **PROMPT:**
```
Run the Phase 0 gate check. Read alphalens_docs/14_engineering_standards.md section "Phase Gate Checklists" for Phase 0→1 criteria.

Check all of the following and report PASS or FAIL per item:
1. Run pytest tests/ --cov=. and report coverage percentage
2. Query DuckDB: SELECT COUNT(*) FROM ohlcv_adjusted — must be >= 600,000
3. Query pipeline_runs SQLite: SELECT * FROM pipeline_runs ORDER BY date DESC LIMIT 5
4. Check datastore/features/baseline/stats_baseline.pkl exists and is non-empty
5. Check .env exists with FYERS_APP_ID set (do NOT print the value)
6. Check no credentials appear in any .py file: grep -r "API_KEY\|SECRET\|PASSWORD\|TOKEN" --include="*.py" (excluding .env.example and test mocks)
7. Check all python3 files have module-level alphalens_docstrings with SPEC-ID references
8. Verify checkpoint-resume works: simulate a failed run and restart

Report: list of PASS/FAIL per item. List all blocking items.
```

🔒 **All items must PASS before starting Phase 1.**

---

# PHASE 1 — Core Signal Engine (Weeks 5–14)

---

## P1.1 — 76 Technical Features + Calendar + Macro

✅ **STATUS: IMPLEMENTED** (see `BuildLog.md` "P1.1" and "P1.1 re-audit" for
the full build log, decisions, and bugs found/fixed). The prompt below is
left close to its original wording for history, with **`[AS BUILT]`**
annotations marking every place the shipped implementation differs from
the original ask and why. Treat the annotations, not the original
wording, as current truth — `features/technical.py`'s own
`CORE_TECHNICAL_FEATURES` list is the single source of truth for the
exact feature catalog, not this document.

📋 **PROMPT (historical — see `[AS BUILT]` notes):**
```
Read alphalens_docs/01_features.md Phase 1 features section and alphalens_docs/specs/08_specifications.md SPEC-FEAT-001 through SPEC-FEAT-005.

Build the core feature computation modules. All computation must be fully vectorized — no python3 loops over individual stocks:
0. Do not show interim steps or explain every thought on the screen. Run the necessary terminal commands, implement the changes, and record the major steps, errors, and resolutions as structured markdown in a file named BuildLog.md in the current directory.
1. If changes are required, update this file for all subsequent prompts.
2. features/technical.py — 76 core technical features:
   [AS BUILT] This prompt's own 11 per-category counts (8+8+4+9+8+5+5+5+5+5+8)
   sum to 70, not 76 — implemented exactly the 70 named/countable features
   below (CORE_TECHNICAL_FEATURES in features/technical.py); the 6-feature
   gap to "76" is unresolved because no source names the missing 6 (see
   BuildLog.md "P1.1"). Do not silently re-pad to 76 without an explicit
   list of names.
   - Category 1 (8): pct_rank_5d, pct_rank_21d, pct_rank_63d, dist_from_52w_high, dist_from_52w_low, open_close_range_pct, high_low_range_pct, prev_close_gap_pct
   - Category 2 (8): sma_20_ratio through sma_200_ratio
     [AS BUILT] sma_20_ratio, sma_50_ratio, sma_100_ratio, sma_200_ratio, sma_20_50_ratio, sma_50_100_ratio, sma_50_200_ratio, sma_200_weekly_ratio
   - Category 3 (4): ema_8_ratio, ema_21_ratio, ema_55_ratio, ema_89_ratio
   - Category 4 (9): rsi_14, rsi_2, stoch_k, stoch_d, macd_hist, williams_r, cci_20, mfi_14, roc_10
   - Category 5 (8): adx_14, di_plus, di_minus, supertrend_dir, supertrend_signal, linear_reg_slope_21, linear_reg_r2_21, trend_consistency_21
   - Category 6 (5): atr_14_pct, bb_position, bb_width_pct, keltner_position, hist_vol_21
   - Category 7 (5): rs_vs_nifty50_21d through rs_vs_nifty500_21d, beta_63d, alpha_21d
     [AS BUILT] rs_vs_nifty50_21d, rs_vs_nifty100_21d, rs_vs_nifty500_21d, beta_63d, alpha_21d — benchmarked against NIFTYBEES/NIF100BEES/MONIFTY500 (the Nifty index-tracking ETFs actually present in ohlcv_adjusted; no raw index-level series is ingested as of Phase 1). NOTE: these 3 ETF tickers only have 2 days of real history in the dev DB as of this writing (the 5-year FYERS backfill only covers the 502-stock universe, not benchmark ETFs) — Category 7 features will read all-NaN against the dev DB until that backfill gap is closed (a separate ingestion follow-up, not done in P1.1/P1.2).
   - Category 8 (5): composite_momentum_5d, composite_momentum_21d, composite_momentum_63d, ema_ribbon_alignment, ema_ribbon_spread
   - Category 9 (5): volume_ratio_5d, volume_ratio_21d, delivery_pct, delivery_pct_zscore_21d, delivery_price_corr_21d
   - Category 10 (5): ichimoku_cloud_position, ichimoku_leading_span_a, tenkan_kijun_signal, chikou_span_signal, ichimoku_breakout
   - Category 11 (8): base_breakout_ratio, vol_compression_21d, vol_compression_63d, gap_up_pct, gap_down_pct, intraday_reversal_score, close_position_in_range, body_to_range_ratio
   - Use ta-lib for all standard indicators; numpy for derived features
   - Input: DuckDB ohlcv_adjusted; output: 500 rows × 76 columns DataFrame
     [AS BUILT] features/technical.py's compute_technical_features(ohlcv, benchmark) is a pure function over an already-fetched OHLCV DataFrame — it never touches DuckDB directly. features/matrix_builder.py is the one that sources ohlcv_adjusted (via the DataStore API, per SPEC-SOLID-005, not direct DuckDB — see item 5). Output is 70 columns, not 76 (see gap note above).

3. features/calendar.py — 7 calendar features:
   - month_sin, month_cos (cyclic encoding — SPEC-FEAT-003), day_of_week_sin, day_of_week_cos, is_expiry_week, days_to_expiry, quarter_end_proximity
   [AS BUILT] Implemented exactly as specified, no divergence.

4. features/macro_features.py — 14 macro features:
   - india_vix, vix_5d_change, usd_inr, crude_oil_price, gold_price, nifty_50_return_5d, nifty_50_return_21d, advance_decline_ratio, fii_net_5d, dii_net_5d, market_breadth_21d, yield_10yr, yield_spread_10yr_2yr, rl_regime_label (stub=0 for Phase 1)
   [AS BUILT] All 14 names implemented exactly as specified. All are now backed by real ingestion sources (ingestion/scrapers/macro.py): india_vix/usd_inr/fii_net_5d/dii_net_5d from NSE (P1.1), crude_oil_price/gold_price from Yahoo Finance and yield_10yr/yield_spread_10yr_2yr from FRED (added in P1.2 — see BuildLog.md "P1.2": yield_spread_10yr_2yr's short leg is a documented 3-month-rate proxy, not a literal 2yr G-Sec yield, since no free daily India 2yr source was found). rl_regime_label remains the specified Phase-1 stub (=0).

5. features/matrix_builder.py:
   - build_feature_matrix(date: str, tickers: List[str]) → pd.DataFrame (500 rows × 98 cols)
     [AS BUILT] Signature matches (plus optional client/save/compute_hmm kwargs for testability). Row count matches (one row per requested ticker, even if its OHLCV fetch failed — all-NaN row, never dropped). Column count is 102 (date + ticker + 100 features), not 98: 70 technical + 7 calendar + 14 macro = 91 from P1.1 alone, plus 3 intraday + 6 HMM regime columns merged in by default as of P1.2 (a later, explicit decision — see BuildLog.md "P1.2" and "P1.1 re-audit"). Pass compute_hmm=False to skip the (expensive) HMM fit; there is no flag to drop intraday/HMM columns entirely from the output shape — features.matrix_builder.ALL_FEATURE_COLUMNS is the authoritative column list.
   - Reads OHLCV from DataStore API (SPEC-SOLID-005 — no direct DuckDB access)
   - Saves output to datastore/features/daily/YYYY-MM-DD.parquet (SPEC-DS-005)
   - Validates: < 1% nulls per feature, delivery_pct in [0,100], ratios in [0.1, 10]
     [AS BUILT] The ratio-range [0.1, 10] check is scoped to CORE_TECHNICAL_FEATURES only (sma/ema price ratios) — a bug where it also caught macro's advance_decline_ratio (which can legitimately exceed 10) as a false positive was found and fixed in P1.2; see tests/unit/test_matrix_builder.py's regression test.

6. tests/unit/test_features_technical.py:
   - Test all 76 features return float64, no infinities, rsi_14 in [0,100]
     [AS BUILT] Tests all 70 implemented features (the catalog's own count, asserted via test_catalog_has_70_features).
   - Test vectorization: identical output for 10 stocks vs 500 stocks
   - Test SPEC-FEAT-001: stocks with < 252 days return NaN for lookback features
   - Benchmark: 500 stocks in < 15 minutes (skip benchmark in CI with pytest.mark.slow)
   [AS BUILT] All four implemented and passing; 500 stocks x 300 days completes in seconds, well under budget. Two additional test files were added beyond what this prompt asked for: tests/unit/test_features_intraday.py and tests/unit/test_matrix_builder.py (the latter had zero coverage before P1.2 — see BuildLog.md).
```

✅ **TEST:**
```bash
pytest tests/unit/test_features_technical.py -v   # 10 passed
python3 -c "
from features.matrix_builder import build_feature_matrix
import pandas as pd
df = build_feature_matrix('2025-01-15', ['RELIANCE', 'TCS', 'INFY', 'HDFCBANK', 'ICICIBANK'])
print(df.shape)   # AS BUILT: (5, 102), not (5, 98) — see [AS BUILT] notes on item 5 above
print(df.isnull().sum().sum())   # AS BUILT: null count is date-dependent — 2025-01-15 predates
                                  # the dev DB's benchmark-ETF/macro_indicators history, so the
                                  # ~16 columns that depend on those (rs_vs_*, beta/alpha, india_vix,
                                  # crude/gold/yield, etc.) will be fully NaN for this specific date.
                                  # Requires the DataStore API running first: uvicorn datastore.api.main:app
"
```
Note: the multi-line `python3 -c "..."` form above is prone to terminal
paste truncation — prefer the one-liner or file+`PYTHONPATH=.` forms
documented in `BuildLog.md` if copy-pasting into a terminal.

🔀 **COMMIT:** `feat(SPEC-FEAT-001): 76 technical features, calendar features, macro features, matrix builder`

---

## P1.2 — HMM Regime Detector (M-01)

⚠️ **STATUS: IMPLEMENTED BUT MATERIALLY DIVERGENT** from the prompt below
— built ad hoc off `02_models.md` + a draft template before this literal
prompt was (re-)found in this file; the user reviewed the diff and chose
to **keep the existing implementation rather than rework it** (see
`BuildLog.md` "P1.2 addendum"). If a future session needs the canonical
behavior described below (most importantly: model **persistence** — the
current version refits from scratch on every call, a real cost at
500-ticker/day production scale — and the **market-wide Nifty 50
instance**, which other code such as `BEAR_REGIME_POSITION_SCALE` likely
expects), that is a real rework, not a quick patch. Do not assume the
items below exist without checking `systems/ml_signal_engine/models/hmm/
regime_detector.py` directly first.

| | This prompt (canonical, NOT built) | Actually built |
|---|---|---|
| Observables | `realized_vol_21d`, `volume_ratio_5d`, `atr_pct_14d` | `realized_vol_10d`, `volume_ratio_20d`, `atr_pct` (14d — matches) |
| Outputs | 5: `hmm_state`, `hmm_state_prob`, `hmm_stability_score`, `hmm_days_in_state`, `hmm_transition_flag` | 6: `HMM_REGIME_FEATURES` = `hmm_regime`, `hmm_regime_prob_bullish`, `hmm_regime_prob_bearish`, `hmm_regime_duration`, `hmm_regime_transition`, `hmm_regime_stability` |
| State labeling | 4 qualitative labels via 2-D classification (mean return **and** vol: bullish/bearish/volatile/sideways) | 1-D rank by mean `daily_return` only |
| Scope | Two instances: market-wide (Nifty 50) + per-stock | Per-stock only — no market-wide instance |
| Persistence | `save`/`load` to `datastore/models/hmm/TICKER_hmm_vYYYYMMDD.pkl` | None — refits every call |
| Interface | `BaseModel`-style: `train`, `predict`, `predict_proba`, `save`, `load` | `contracts.interfaces.IRegimeModel`: `fit`, `predict_regime` |
| Tests | COVID March-2020-bearish / 2021-bullish Nifty 50 regression, stability-decreases-on-transition, save/load round-trip, + a separate integration test | `tests/unit/test_hmm.py` only — synthetic two-regime structural-break test, graceful-NaN-on-insufficient-history, no integration test, no COVID assertion |

📋 **PROMPT (canonical — not what was built; see table above):**
```
Read alphalens_docs/02_models.md section on M-01 HMM and alphalens_docs/specs/08_specifications.md SPEC-MODEL-001.

Build the HMM regime detector:
0. Do not show interim steps or explain every thought on the screen. Run the necessary terminal commands, implement the changes, and record the major steps, errors, and resolutions as structured markdown in a file named BuildLog.md in the current directory.
1. If changes are required, update this file for all subsequent prompts.
2. systems/ml_signal_engine/models/hmm/regime_detector.py — HMMRegimeDetector class:
   - 4-state GaussianHMM using hmmlearn
   - 5 observables per stock: daily_return, log_return, realized_vol_21d, volume_ratio_5d, atr_pct_14d
   - Trains on 252+ days of data (SPEC-MODEL-001)
   - Runs TWO separate instances: market-wide on Nifty 50 index, per-stock for all 500
   - Outputs per stock: hmm_state (0-3), hmm_state_prob, hmm_stability_score, hmm_days_in_state, hmm_transition_flag
   - State labeling: after training, label each state by mean return: highest mean = state 'bullish', lowest = 'bearish', near-zero high-vol = 'volatile', near-zero low-vol = 'sideways'
   - Saves trained model to datastore/models/hmm/TICKER_hmm_vYYYYMMDD.pkl per SPEC-MODEL-005
   - Implements BaseModel interface from SPEC-SOLID-003 (train, predict, predict_proba, save, load)

3. tests/unit/test_hmm.py:
   - Test 4 states are produced and labeled correctly (mock 252 days synthetic data)
   - Test state stability score decreases during transitions
   - Test market-wide HMM on Nifty 50 produces plausible regimes (bullish 2021, bearish early 2020)
   - Test save/load round-trip: loaded model produces identical predictions

4. tests/integration/test_hmm_pipeline.py:
   - End-to-end: load real OHLCV from DataStore, train HMM, save, load, predict
   - Verify Nifty 50 shows bearish regime in March 2020 COVID crash period

All alphalens_docstrings reference SPEC-MODEL-001, SPEC-SOLID-003.
```

✅ **TEST (as actually built — not the canonical block above):**
```bash
pytest tests/unit/test_hmm.py -v   # 10 passed; tests/integration/test_hmm_pipeline.py does not exist
```

🔀 **COMMIT:** `feat(SPEC-MODEL-001): M-01 HMM regime detector, 4-state, per-stock and market-wide, BaseModel interface`
[AS BUILT] No commit under this exact message exists — the HMM work landed as part of the broader "P1.2" feature-matrix-expansion work (intraday + HMM + macro sourcing), not as an isolated M-01 commit. See `BuildLog.md` "P1.2" for what actually shipped together.

---

## P1.3 — P&D Features + P&D Detector (M-06) + Known Fraud Regression Tests

📋 **PROMPT:**
```
Read alphalens_docs/01_features.md P&D features section and alphalens_docs/specs/08_specifications.md SPEC-MODEL-006, SPEC-FEAT-004.

Build the P&D detection system — this is the MOST CRITICAL safety component:
0. Do not show interim steps or explain every thought on the screen. Run the necessary terminal commands, implement the changes, and record the major steps, errors, and resolutions as structured markdown in a file named BuildLog.md in the current directory.
1. If changes are required, update this file for all subsequent prompts.
1. features/pnd_features.py — 22 P&D detection features:
   - Volume anomalies (6): vol_spike_ratio_3d, vol_spike_ratio_5d, vol_spike_vs_60d_avg, volume_zscore_10d, cumulative_vol_change_5d, unusual_vol_days_count_10d
   - Price anomalies (5): consecutive_up_days, consecutive_circuit_days, price_acceleration_5d, upper_circuit_proximity, max_single_day_move_5d
   - Delivery collapse (4): delivery_pct_3d_avg, delivery_vs_4w_avg, delivery_collapse_flag, delivery_spike_then_collapse
   - Microstructure (4): bid_ask_spread_proxy, price_impact_ratio, turnover_acceleration, operator_signature_score
   - Cross-feature (3): pnd_momentum_breakout, circuit_filter_proximity_10d, reversal_after_spike_flag

2. systems/ml_signal_engine/models/pnd/pnd_detector.py — PnDDetector class:
   - LightGBM primary + IsolationForest anomaly layer
   - Training data: known P&D cases from NSE circular archive + synthetic negatives
   - SMOTETomek for class imbalance (expected 1–3% positive rate — SPEC-MODEL-004)
   - Output: pnd_score (0–100), pnd_phase ('normal'|'accumulation'|'pump'|'dump'|'aftermath'), pnd_block (bool: score > 60), pnd_flag (bool: score > 40)
   - predict_full(X: pd.DataFrame) → pd.DataFrame with all 4 output columns
   - P&D BLOCK threshold (60) read from config/settings.py PND_BLOCK_THRESHOLD — NEVER hardcoded
   - Implements BaseModel interface (SPEC-SOLID-003)

3. tests/unit/test_pnd_features.py:
   - Test circuit_day detection: 5 consecutive upper circuits returns consecutive_circuit_days=5
   - Test delivery collapse: high volume + low delivery flagged correctly
   - Test all 22 features return float64, no infinities

4. tests/regression/test_known_pnd.py — CRITICAL REGRESSION TEST:
   - Load synthetic data replicating 3 known NSE P&D patterns (create mock data matching patterns)
   - Pattern 1: Volume 10x + price up 40% over 5 days + delivery collapse → must score >= 70
   - Pattern 2: 8 consecutive upper circuits + delivery < 5% → must score >= 80
   - Pattern 3: Normal blue-chip trading (HDFC Bank stable) → must score <= 20
   - These tests must pass on every build — they are the safety net

The hard block logic: pnd_score > 60 → pnd_block=True. This flag is checked BEFORE any buy signal reaches the user (SPEC-MODEL-006). No exceptions permitted.
```

✅ **TEST:**
```bash
pytest tests/unit/test_pnd_features.py -v
pytest tests/regression/test_known_pnd.py -v   # MUST ALL PASS
```

🔀 **COMMIT:** `feat(SPEC-MODEL-006): M-06 P&D detector, 22 features, hard-block logic, known pattern regression tests`

---

## P1.4 — Triple-Barrier Labeling + Walk-Forward Backtester

📋 **PROMPT:**
```
Read alphalens_docs/specs/08_specifications.md SPEC-MODEL-002, SPEC-MODEL-003, SPEC-BT-001 through SPEC-BT-004 and alphalens_docs/04_backtesting.md.

Build the labeling and backtesting infrastructure:
0. Do not show interim steps or explain every thought on the screen. Run the necessary terminal commands, implement the changes, and record the major steps, errors, and resolutions as structured markdown in a file named BuildLog.md in the current directory.
1. If changes are required, update this file for all subsequent prompts.

1. systems/ml_signal_engine/training/labeling.py — TripleBarrierLabeler class:
   - Triple-barrier labels: +1 (profit target hit first), -1 (stop loss hit first), 0 (timeout)
   - Default: profit_multiplier=2.0, stop_multiplier=1.0 (ATR-based barriers), max_holding=21 days
   - Strict PIT: labels only use returns starting AFTER entry date; no look-ahead
   - Native SPEC-MODEL-002 implementation (mlfinlab is not used; unavailable on PyPI)
   - Validates: labels ∈ {-1, 0, 1}; no labels that extend beyond available data
   - Class distribution report: prints % of each class

2. systems/ml_signal_engine/training/walk_forward.py — WalkForwardValidator class:
   - 5 folds: Train[2020-22]→Test[2023], Train[2020-23]→Test[2024], Train[2020-24]→Test[2025], Train[2020-25]→Test[2026-H1], + 1 expanding window
   - HPO: NEVER on test fold — only on last 20% of training fold as validation
   - split_data(df, n_folds=5) → List[Tuple[train_df, test_df]]
   - Integrity checker: run_integrity_checks(results) → validates all 9 rules from SPEC-BT-001

3. backtest/integrity_checker.py — BacktestIntegrityChecker class:
   - check_01_walk_forward(): verifies no random splits used
   - check_02_pit(): verifies no future data in any feature
   - check_03_corp_actions(): verifies adj_factor column present
   - check_04_survivorship(): verifies delisted stocks included
   - check_05_costs(): verifies TOTAL_ROUNDTRIP_COST applied (from settings.py)
   - check_06_liquidity(): verifies MIN_ADT_INR filter applied
   - check_07_no_hpo_on_test(): verifies hyperparams only tuned on train+val
   - check_08_fold_stability(): std(fold_sharpes) < 0.5
   - check_09_benchmarks(): beats at least Nifty 50 buy-hold in 3+ folds
   - check_10_random_feature(): random feature test scores 48-52%
   - run_all_checks() → {check_name: pass/fail} dict; raises if any CRITICAL check fails

4. backtest/costs.py — IndianTransactionCosts class:
   - Full Indian cost model: STT + exchange fees + SEBI charges + stamp duty + brokerage
   - compute_roundtrip_cost(price, quantity) → float
   - Validates against TOTAL_ROUNDTRIP_COST in settings.py

5. tests/unit/test_labeling.py:
   - Test no label extends beyond its max_holding period
   - Test +1 label when price hits profit target before stop
   - Test 0 label when timeout occurs before either barrier
6. tests/unit/test_backtester.py:
   - Test 5 folds are produced with correct date ranges
   - Test no overlap between train and test folds
   - Test integrity checker catches a deliberately introduced data leak
```

✅ **TEST:**
```bash
pytest tests/unit/test_labeling.py tests/unit/test_backtester.py -v
python3 -c "
fro m systems.ml_signal_engine.training.labeling import TripleBarrierLabeler
# Quick sanity check
print('Label classes must be -1, 0, 1 only')
"
```

🔀 **COMMIT:** `feat(SPEC-MODEL-002): triple-barrier labeling, walk-forward validator, integrity checker (9 rules), Indian cost model`

---

## P1.5 — Signal Models M-02/M-03/M-04/M-05

📋 **PROMPT:**
```
Read alphalens_docs/02_models.md sections M-02, M-03, M-04, M-05 and alphalens_docs/specs/08_specifications.md SPEC-MODEL-003, SPEC-MODEL-004, SPEC-MODEL-007.

Build the core signal models:
0. Do not show interim steps or explain every thought on the screen. Run the necessary terminal commands, implement the changes, and record the major steps, errors, and resolutions as structured markdown in a file named BuildLog.md in the current directory.
1. If changes are required, update this file for all subsequent prompts.

1. systems/ml_signal_engine/models/signal/base_signal_model.py — BaseSignalModel (extends BaseModel):
   - predict_signals(X) → DataFrame with: signal_buy_prob, signal_hold_prob, signal_sell_prob, signal_q10, signal_q50, signal_q90
   - All three probabilities must sum to 1.0

2. systems/ml_signal_engine/models/signal/signal_5d.py — Signal5DModel:
   - Ensemble: LightGBM + CatBoost + XGBoost stacking with logistic regression meta-learner
   - Input: Phase 1 features as actually produced by features.matrix_builder.ALL_FEATURE_COLUMNS
     [AS BUILT, P1.1/P1.2] This list (not a hardcoded number) is the source of truth — it has
     drifted twice already (98 in this prompt's original wording -> 102 actual: 70 technical +
     3 intraday + 7 calendar + 6 HMM + 14 macro; see BuildLog.md "P1.1"/"P1.2") and will grow
     again once P1.3 adds 22 P&D features. Read the column count from the live code at execution
     time, do not hardcode it here or anywhere downstream (e.g. fixed-width model input layers).
   - Labels from TripleBarrierLabeler with horizon=5 days
   - Optuna HPO: 100 trials, on train-fold validation split ONLY (SPEC-MODEL-003)
   - SMOTETomek on training data ONLY (SPEC-MODEL-004)
   - Outputs: Q10/Q50/Q90 quantile estimates of 5d forward return
   - Threshold optimisation on validation fold using F1 per class

3. systems/ml_signal_engine/models/signal/signal_21d.py — Signal21DModel:
   - Same architecture as Signal5D with horizon=21 days

4. systems/ml_signal_engine/models/signal/meta_labeler.py — MetaLabeler:
   - Binary classifier: Act (1) / Don't Act (0)
   - Label: 1 if the primary signal (5d or 21d) was profitable AFTER transaction costs
   - Uses same feature set as primary signal model
   - Threshold optimised for precision (reduce false acts)

5. systems/ml_signal_engine/models/uncertainty/conformal.py — ConformalPredictor:
   - Uses MAPIE >= 1.3 with ACI (Adaptive Conformal Inference) variant
   - Target coverage: 90% (SPEC-MODEL-007)
   - Output: (lower_bound, upper_bound) for each prediction
   - Narrow interval heuristic: width < 4 percentage points = narrow (high conviction)

6. Run first walk-forward training:
   - systems/ml_signal_engine/inference/train_all_phase1.py
   - Trains HMM → P&D → Signal5D → Signal21D → MetaLabeler → Conformal
   - Saves each model to datastore/models/ with metadata (SPEC-MODEL-005)
   - Prints integrity check results after training

7. tests/unit/test_signal_models.py:
   - Test buy+hold+sell probabilities sum to 1.0
   - Test conformal interval achieves >= 88% coverage on held-out test data
   - Test meta-labeler precision > 0.55 (if not, report as WARNING not failure)
```

✅ **TEST:**
```bash
pytest tests/unit/test_signal_models.py -v
python3 -m systems.ml_signal_engine.inference.train_all_phase1 --folds 2 --quick  # 2 folds for quick test
```

🔀 **COMMIT:** `feat(SPEC-MODEL-003): M-02 Signal5D, M-03 Signal21D, M-04 MetaLabeler, M-05 ConformalPredictor, stacking ensemble`

---

## P1.6 — Exit Signal (M-07) + First Backtest

📋 **PROMPT:**
```
Read alphalens_docs/02_models.md M-07 section and alphalens_docs/specs/08_specifications.md SPEC-MODEL-002.

Build the exit signal model and run the first full backtest:
0. Do not show interim steps or explain every thought on the screen. Run the necessary terminal commands, implement the changes, and record the major steps, errors, and resolutions as structured markdown in a file named BuildLog.md in the current directory.
1. If changes are required, update this file for all subsequent prompts.

1. systems/ml_signal_engine/models/exit/exit_signal.py — ExitSignalModel:
   - Input: all Phase 1 features + position_specific: entry_price, days_held, unrealised_pnl_pct, days_to_next_earnings
   - Urgency score 0–100: LightGBM regression
   - Exit type classifier: 6 types: thesis_broken, momentum_exhaustion, risk_management, target_achieved, opportunity_cost, pnd_exit
   - CoxPH survival curves (lifelines): probability position is still profitable at 5d/21d/63d
   - Output contract per SPEC-SOLID-003: exit_urgency, exit_type, exit_survival_5d, exit_survival_21d, exit_survival_63d
   - ALWAYS surface exit type to user — bare "sell" without type is a BUILD FAILURE

2. backtest/portfolio.py — PortfolioSimulator class:
   - Tracks positions, applies exit signals, computes P&L per trade
   - Position sizing: equal-weight or ATR-based (configurable)
   - Max position size: MAX_POSITION_PCT from settings (default 10%)
   - Max sector exposure: MAX_SECTOR_PCT (default 40%)
   - Applies full Indian transaction costs per trade

3. backtest/engine.py — BacktestEngine class:
   - run_full_backtest(model_name, from_date, to_date, folds=5) → BacktestResults
   - Runs walk-forward: P&D filter → Signal → MetaLabel → Conformal → Exit
   - Returns per-fold metrics: CAGR, Sharpe, MaxDD, WinRate, profit_factor
   - Calls integrity checker automatically

4. Run the first real backtest:
   - backtest/run_phase1_backtest.py
   - Signal 5d + MetaLabeler + P&D filter + equal-weight sizing
   - Print full integrity check results
   - Print per-fold and aggregate metrics
   - Generate backtest report: backtest/reports/phase1_YYYYMMDD.json

5. tests/unit/test_exit_signal.py:
   - Test 6 exit types are all producible by the model
   - Test exit type 'pnd_exit' fires when pnd_score spikes above 50 mid-position
   - Test urgency=84 maps to 'immediate exit' action in portfolio simulator
```

✅ **TEST:**
```bash
pytest tests/unit/test_exit_signal.py -v
python3 -m backtest.run_phase1_backtest --quick --folds 2
# Check output: "Integrity checks: 9/9 PASSED" required before proceeding
```

🔒 **FIRST BACKTEST GATE — Manual review required:**
- [ ] Fold Sharpe std < 0.5
- [ ] Random feature test: 48–55% accuracy
- [ ] All 9 integrity checks pass
- [ ] Beats Nifty 50 buy-hold in at least 3 of 5 folds
- [ ] P&D block: verify XYZLTD-equivalent stocks are blocked (check known pump stocks in test data)

🔀 **COMMIT:** `feat(SPEC-MODEL-002): M-07 exit signal, portfolio simulator, backtest engine, Phase 1 backtest PASSED`

---

## P1.7 — DataStore API (Full) + Daily Pipeline + Phase 1 Dashboard

📋 **PROMPT:**
```
Read alphalens_docs/12_platform_architecture.md API Groups section and alphalens_docs/specs/08_specifications.md SPEC-DS-002, SPEC-UI-001 through SPEC-UI-005.

Complete the DataStore API and build the daily inference pipeline and dashboard:
0. 

Do not show interim steps or explain every thought on the screen. Run the necessary terminal commands, implement the changes, and record the major steps, errors, and resolutions as structured markdown in a file named BuildLog.md in the current directory.
1. If changes are required, update this file for all subsequent prompts.

1. datastore/api/routers/ — implement all Phase 1 API endpoints:
   - ohlcv.py: GET /api/v1/ohlcv/{ticker}?from=&to=&adjusted=true
   - signals.py: GET /api/v1/signals/ml/{ticker}/{date}, GET /api/v1/signals/ml/top_buys/{date}
   - regime.py: GET /api/v1/macro/regime (market-wide HMM state)
   - watchlist.py: GET /api/v1/watchlist/current (stub for Phase 1)
   - alerts.py: GET /api/v1/alerts/today
   - system.py: GET /health (pipeline status, last run, stock count, drift status)
   - All endpoints enforce PIT with as_of parameter (SPEC-DS-003)

2. systems/ml_signal_engine/inference/daily_inference.py:
   - Runs each day after data collection: HMM → PSI check → P&D filter → Signals → MetaLabel → Conformal → Exit → Write to DataStore
   - P&D filter runs FIRST, before signals (SPEC-MODEL-006)
   - PSI check: if PSI > 0.25, halt and alert (SPEC-PIPE-005)
   - Writes all outputs to signals.db via DataStore API
   - Logs each step timing to structured logger
   - Completes within 90 minutes (SPEC-SYS-002)

3. dashboard/screens/daily_dashboard.py — Phase 1 CLI dashboard:
   - Reads from DataStore API (localhost:8000)
   - Prints: market regime, top 5 buy signals with probabilities + intervals
   - Prints: exit urgency for any held positions
   - Prints: P&D blocks and warnings
   - Prints: pipeline health status
   - No complex UI needed in Phase 1 — clear terminal output is sufficient

4. ingestion/scheduler/daily_pipeline.py — wires everything together:
   - Step 1: download_bhavcopy → validate
   - Step 2: download_macro
   - Step 3: adjust_prices
   - Step 4: build_feature_matrix → save Parquet
   - Step 5: run_daily_inference
   - Step 6: write_signals_to_datastore
   - Each step: checkpoint save before + after; skip if checkpoint shows complete

5. tests/integration/test_daily_pipeline.py:
   - Full end-to-end test on 5 stocks for 3 dates
   - Verify signals written to DataStore are readable via API
   - Verify P&D block prevents signal from appearing in top_buys endpoint
```

✅ **TEST:**
```bash
pytest tests/integration/test_daily_pipeline.py -v
uvicorn datastore.api.main:app --port 8000 &
curl "http://localhost:8000/api/v1/signals/ml/top_buys/2025-01-15" | python3 -m json.tool
python3 -m dashboard.screens.daily_dashboard
```

🔀 **COMMIT:** `feat(SPEC-DS-002): DataStore API complete, daily inference pipeline, dashboard CLI, end-to-end integration`

---

## 🔒 PHASE 1 GATE CHECK

📋 **PROMPT:**
```
Run the Phase 1 gate check. Read alphalens_docs/14_engineering_standards.md Phase 1→2 gate criteria.

Check and report PASS/FAIL:
1. Run pytest tests/ --cov=. — must show >= 80% coverage
2. Run backtest integrity check: python3 -m backtest.run_phase1_backtest --check-only
3. Verify daily pipeline timing: python3 -m ingestion.scheduler.daily_pipeline --dry-run --timing — must complete simulation in < 90 minutes
4. Verify P&D hard block: python3 -c "from systems.ml_signal_engine.models.pnd.pnd_detector import PnDDetector; print('PND block at:', PnDDetector.BLOCK_THRESHOLD)"
5. Verify no hardcoded thresholds: grep -rn "0\.60\|0\.65\|0\.50\|60\b" systems/ --include="*.py" — flag any magic numbers not imported from settings
6. Verify DataStore API is running and healthy: curl localhost:8000/health
7. Check git log has SPEC-ID in every commit: git log --oneline | grep -v "SPEC-" (should return 0 lines)
8. Run pip-audit (pip install pip-audit) for known CVEs in dependencies
9. Verify paper trading log exists: check if you have started tracking paper trades (create paper_trading/log.csv if not)

Report: PASS/FAIL per item. List all blocking items.
```

🔒 **All items must PASS. Start paper trading before Phase 2.**

---

# PHASE 2 — Fundamentals + Multibagger (Weeks 15–26)

⚠️ **MANUAL BEFORE STARTING:**
- Subscribe Screener.in Premium (₹4,999/yr)
- Subscribe Trendlyne StratQ (₹5,900/yr)
- Subscribe Tijori Finance Pro (₹3,500/yr)

---

## P2.1 — Fundamental Data Ingestion + PIT Validation

📋 **PROMPT:**
```
Read alphalens_docs/03_data_pipeline.md fundamentals section, alphalens_docs/specs/08_specifications.md SPEC-PIPE-003 (PIT — CRITICAL), SPEC-FEAT-002.

Build fundamental data ingestion. SPEC-PIPE-003 is the most important constraint: NEVER use quarter_end_date as a join key. Always use announcement_date:
0. Do not show interim steps or explain every thought on the screen. Run the necessary terminal commands, implement the changes, and record the major steps, errors, and resolutions as structured markdown in a file named BuildLog.md in the current directory.
1. If changes are required, update this file for all subsequent prompts.

1. ingestion/scrapers/screener.py — ScreenerScraper class:
   - Login to screener.in using credentials from .env (SCREENER_USERNAME, SCREENER_PASSWORD)
   - export_company_data(ticker: str) → dict with quarterly P&L, balance sheet, cashflow
   - batch_export(tickers: List[str]) — exports all 500 stocks with rate limiting
   - Maps Screener fields to our schema columns exactly
   - Stores announcement_date as 45 days after quarter_end_date if not available (conservative PIT)
   - Saves to fundamentals table in DuckDB via DataStore API write endpoint

2. features/fundamental.py — 28 fundamental features:
   - Growth (6): revenue_growth_yoy, revenue_growth_qoq, pat_growth_yoy, eps_growth_yoy, ebitda_growth_yoy, revenue_cagr_3yr
   - Profitability (6): gross_margin, operating_margin, ebitda_margin, net_margin, roe, roce
   - Capital efficiency (4): asset_turnover, capex_intensity, fcf_conversion, roic
   - Leverage (4): debt_to_equity, interest_coverage, net_debt_to_ebitda, current_ratio
   - Working capital (4): inventory_days, receivable_days, payable_days, cash_conversion_cycle
   - Valuation (3): pe_ratio, pb_ratio, ev_to_ebitda
   - Staleness (3 — MANDATORY): days_since_results, quarter_age_pct, results_pending_flag
   - ALL fundamental features: sector-relative z-score normalisation (SPEC-FEAT-002)

3. features/governance.py — 12 governance features:
   - promoter_pct, promoter_change_qoq, promoter_pledge, promoter_pledge_change_qoq
   - fii_pct, fii_change_qoq, dii_pct, dii_change_qoq, mf_pct, mf_change_qoq
   - promoter_pledge_spiral_flag (pledge > 20% AND price falling), institutional_conviction_flag

4. tests/unit/test_pit_alignment.py — CRITICAL:
   - Test 1: fundamentals joined on announcement_date, NOT quarter_end_date
   - Test 2: no feature uses data with announcement_date > feature computation date
   - Test 3: staleness features correct (days_since_results = compute_date - announcement_date)
   - Test 4: an announcement 30 days away returns results_pending_flag=1
   - These are CRITICAL — a PIT bug silently inflates all backtest results
```

✅ **TEST:**
```bash
pytest tests/unit/test_pit_alignment.py -v   # ALL MUST PASS
python3 -c "
import duckdb; conn = duckdb.connect('datastore/normalised/alphalens.duckdb')
# Verify PIT: announcement dates are AFTER quarter end dates
result = conn.execute('''
  SELECT COUNT(*) FROM fundamentals
  WHERE announcement_date <= quarter_end_date
''').fetchone()[0]
print(f'PIT violations: {result} (must be 0)')
"
```

🔀 **COMMIT:** `feat(SPEC-PIPE-003): fundamental data ingestion, 28 fundamental features, 12 governance features, PIT validation`

---

## P2.2 — AMFI MF Holdings + Corporate Action Features

📋 **PROMPT:**
```
Read alphalens_docs/01_features.md MF holdings and corporate action features sections and alphalens_docs/specs/08_specifications.md SPEC-FEAT-004.

Build MF holdings and corporate action features:
0. Do not show interim steps or explain every thought on the screen. Run the necessary terminal commands, implement the changes, and record the major steps, errors, and resolutions as structured markdown in a file named BuildLog.md in the current directory.
1. If changes are required, update this file for all subsequent prompts.

1. ingestion/scrapers/amfi_holdings.py:
   - Scrapes amfiindia.com monthly portfolio disclosures for all ~44 AMCs
   - Parses all scheme holdings into: scheme_name, isin, ticker, quantity, value_inr, month
   - Saves to datastore/normalised/mf_holdings/YYYY-MM.parquet
   - PIT rule: available from ~5th of following month (stored as availability_date = 5th of month+1)
   - Scheduled via ingestion/scheduler/daily_pipeline.py's APScheduler job store
     (laptop-only, SPEC-SCHED-009), not a separate Oracle/OS-level cron entry —
     register as a monthly job, 5th of each month, 08:00 IST

2. features/mf_holdings.py — 12 MF holding features:
   - mf_scheme_count, mf_scheme_count_change_1m, mf_total_holding_change_1m, mf_smallcap_fund_holding
   - mf_new_entry_count (schemes freshly entering), mf_exit_count, mf_concentration_top5
   - mf_avg_holding_period, mf_sip_inflow_proxy, mf_crowdedness_rank
   - superstar_investor_flag (Dolly Khanna, Vijay Kedia, Ashish Kacholia tracked via Trendlyne)
   - superstar_investor_change (+1 increased, -1 decreased, 0 unchanged)
   - PIT: use availability_date = 5th of month+1 for all features

3. features/corporate_action_features.py — 10 features:
   - days_to_record_date, corp_action_anticipation_return, buyback_price_spread
   - buyback_acceptance_estimated, index_inclusion_days, ipo_lockin_expiry_proximity
   - ipo_listing_age_months, post_earnings_drift_signal, dividend_yield_vs_fd_rate, qip_dilution_impact

4. tests/unit/test_mf_holdings.py:
   - Test PIT: mf features for date=2024-06-01 use only May 2024 data (not June)
   - Test mf_new_entry_count: 3 new schemes in June vs May → returns 3
   - Test superstar_investor_flag triggers when any tracked investor holds stock
```

✅ **TEST:**
```bash
pytest tests/unit/test_mf_holdings.py -v
python3 -c "from features.mf_holdings import compute_mf_features; print('MF features OK')"
```

🔀 **COMMIT:** `feat(SPEC-FEAT-004): AMFI MF holdings, 12 MF features, 10 corporate action features, superstar investor tracking`

---

## P2.3 — F&O Features + Signal 63d + Feature Matrix Expansion

📋 **PROMPT:**
```
Read alphalens_docs/01_features.md F&O features section and alphalens_docs/specs/08_specifications.md SPEC-FEAT-004.

Build F&O features and expand to full Phase 2 feature matrix:
0. Do not show interim steps or explain every thought on the screen. Run the necessary terminal commands, implement the changes, and record the major steps, errors, and resolutions as structured markdown in a file named BuildLog.md in the current directory.
1. If changes are required, update this file for all subsequent prompts.

1. features/fno_features.py — 16 F&O derivative features (F&O eligible stocks only):
   - pcr_oi, pcr_volume, iv_call, iv_put, iv_skew, atm_straddle_premium_pct
   - oi_buildup_flag, oi_unwinding_flag, max_pain_level, max_pain_distance_pct
   - option_chain_support, option_chain_resistance, synthetic_futures_spread
   - rollover_cost, rollover_pcr, futures_basis_pct
   - Returns NaN for non-F&O stocks — LightGBM handles natively (SPEC-FEAT-004)
   - Source: FYERS Option Chain API (real-time), historical from NSE F&O archive

2. Update features/matrix_builder.py to Phase 2 feature set (268 features total):
   - 76 technical + 14 macro + 7 calendar + 1 HMM + 22 P&D + 28 fundamental + 12 governance + 12 MF + 10 corp_action + 16 F&O + 70 multibagger-specific (stub NaN for now) = 268

3. systems/ml_signal_engine/models/signal/signal_63d.py — Signal63DModel:
   - Same stacking architecture as Signal5D/21D
   - Uses Phase 2 full feature set (268 features)
   - Retrain trigger: when new quarterly fundamentals are announced (SPEC-MODEL-008)
   - Quantile outputs: Q10/Q50/Q90 for 63-day forward return

4. Retrain Signal5D and Signal21D with Phase 2 features:
   - systems/ml_signal_engine/inference/retrain_phase2.py
   - Trains all three signal models with expanded feature set
   - Compares Phase 1 vs Phase 2 Sharpe — must show improvement or neutral

5. tests/unit/test_fno_features.py:
   - Test F&O features return NaN for non-F&O stock (e.g., a BSE SME stock)
   - Test pcr_oi range: must be in (0, 10]
   - Test max_pain_level within 5% of ATM strike
```

✅ **TEST:**
```bash
pytest tests/unit/test_fno_features.py -v
python3 -c "
from features.matrix_builder import build_feature_matrix
df = build_feature_matrix('2025-01-15', ['RELIANCE', 'NIFTY_SME_STOCK'])
print('Phase 2 feature count:', df.shape[1], '(should be 268)')
"
python3 -m systems.ml_signal_engine.inference.retrain_phase2 --quick --folds 2
```

🔀 **COMMIT:** `feat(SPEC-FEAT-004): 16 F&O features, Signal63D, Phase 2 feature matrix (268 features), Signal5D/21D retrained`

---

## P2.4 — Multibagger Model (M-08)

📋 **PROMPT:**
```
Read alphalens_docs/02_models.md M-08 section, alphalens_docs/01_features.md multibagger features, and alphalens_docs/specs/08_specifications.md SPEC-MODEL-001.

Build the multibagger detection system:
0. Do not show interim steps or explain every thought on the screen. Run the necessary terminal commands, implement the changes, and record the major steps, errors, and resolutions as structured markdown in a file named BuildLog.md in the current directory.
1. If changes are required, update this file for all subsequent prompts.

1. features/multibagger.py — 33 multibagger-specific features:
   - Base formation (6): base_length_days, base_tightness_pct, base_depth_pct, breakout_volume_ratio, pre_breakout_vol_compression, consolidation_pattern_score
   - Accumulation signals (7): delivery_accumulation_21d, institutional_accumulation_flag, mf_discovery_score, volume_trend_21d, quiet_accumulation_score, smart_money_flow, promoter_buying_flag
   - Relative strength (5): rs_rank_universe, rs_rank_sector, rs_vs_nifty_52w, rs_momentum_acceleration, rs_stability_score
   - Trend quality (5): trend_quality_score, atr_ratio_trend, ema_ribbon_health, higher_highs_lower_lows, weekly_trend_alignment
   - Volatility compression (4): vol_compression_ratio_63d, vol_compression_ratio_126d, iv_compression_flag, range_compression_score
   - Historical analogues (6): base_pattern_similarity, post_base_breakout_score, recovery_from_correction, sector_cycle_position, market_cycle_alignment, analogue_composite_score

2. systems/ml_signal_engine/models/multibagger/multibagger_model.py — MultibaggerModel:
   - LightGBM lambdarank (primary) + Random Survival Forest
   - Input: 109 features: 76 technical + 33 multibagger-specific — NO fundamental features in Phase 2
   - Weekly run schedule (Monday only — SPEC-MODEL-001 weekly cadence)
   - Output contract: mb_probability (0–1), mb_tier (2x|3x|5x|10x|none), mb_archetype (long_base_breakout|post_crash_recovery|quiet_accumulator|sector_rotation_leader), survival curves at 6/12/18/24/36 months
   - Top-20 watchlist generation: sort by mb_probability, take top 20 with mb_probability > 0.30
   - Historical analogue mining: for each watchlist stock, find 3 most similar historical patterns from last 15 years
   - Label construction: binary 1 if stock returned 2x+ within 3 years; 0 otherwise (use confirmed historical data only)
   - Validates: P&D episodes excluded from positive labels (forensic_composite < 30 required)

3. systems/ml_signal_engine/models/multibagger/analogue_miner.py:
   - find_analogues(ticker, n=3) → List[Analogue]
   - Each Analogue: stock_name, entry_year, return, duration_months, similarity_score
   - Uses cosine similarity on the 33 multibagger features at time of entry

4. tests/unit/test_multibagger.py:
   - Test survival curve is monotonically non-increasing
   - Test mb_probability > 0.30 for known historical multibaggers
   - Test weekly cadence: model only scores when is_monday=True
   - Test top-20 list excludes any stock with pnd_score > 40

5. tests/regression/test_multibagger_historical.py — HITL regression:
   - Load pre-computed features for AVANTIFEED (2017 entry), RELAXO (2016), PAGEIND (2019)
   - These are confirmed historical multibaggers — each must score mb_probability > 0.45
   - This test flags model degradation during retraining
```

✅ **TEST:**
```bash
pytest tests/unit/test_multibagger.py -v
pytest tests/regression/test_multibagger_historical.py -v   # known multibaggers must score high
python3 -c "
from systems.ml_signal_engine.models.multibagger.multibagger_model import MultibaggerModel
model = MultibaggerModel()
print('Multibagger model ready:', model is not None)
"
```

.venv/bin/pytest tests/unit/test_multibagger.py -v
.venv/bin/pytest tests/regression/test_multibagger_historical.py -v
.venv/bin/python3 -c "
from systems.ml_signal_engine.models.multibagger.multibagger_model import MultibaggerModel
model = MultibaggerModel()
print('Multibagger model ready:', model is not None)
"


🔀 **COMMIT:** `feat(SPEC-MODEL-001): M-08 multibagger model, 33 multibagger features, survival curves, analogue mining, historical regression tests`

---

## P2.5 — Forensic Scoring (M-09/M-10)

📋 **PROMPT:**
```
Read alphalens_docs/Forensic_Accounting_ML_Specification.md (full document) and alphalens_docs/specs/08_specifications.md SPEC-MODEL-009, SPEC-MODEL-010.

Build the forensic accounting system:
0. Do not show interim steps or explain every thought on the screen. Run the necessary terminal commands, implement the changes, and record the major steps, errors, and resolutions as structured markdown in a file named BuildLog.md in the current directory.
1. If changes are required, update this file for all subsequent prompts.

1. systems/ml_signal_engine/models/forensic/classical_scores.py — M-09:
   - Beneish M-Score: compute all 8 components (DSRI, GMI, AQI, SGI, DEPI, SGAI, TATA, LVGI), composite score using: -4.84 + 0.92×DSRI + 0.528×GMI + 0.404×AQI + 0.892×SGI + 0.115×DEPI - 0.172×SGAI + 4.679×TATA - 0.327×LVGI
   - Altman Z-Score: 5 components, Z < 1.81 = distress
   - Piotroski F-Score: 9 binary components
   - Ohlson O-Score: 9 components
   - Sloan Accrual: (NI - CFO) / avg_total_assets
   - Benford's Law: chi-squared test on first-digit distribution of revenue/expenses/receivables, compute MAD
   - All 7 classical scores combined into forensic_classical_composite (weighted average)

2. systems/ml_signal_engine/models/forensic/forensic_ml.py — M-10:
   - LightGBM + XGBoost ensemble on 84 features (Groups A–I from spec)
   - Training data: known Indian fraud cases + clean companies
   - Fraud cases: create synthetic data matching Satyam, DHFL, Vakrangee, IL&FS, Yes Bank patterns
   - IsolationForest anomaly layer: z-score reconstruction error
   - 4-layer composite: classical 20% + ML fraud 40% + anomaly 20% + governance 20%
   - Flag levels: Green (0–20), Yellow (21–40), Orange (41–60), Red (61–80), Black (81–100)

3. features/forensic_classical.py — 30 features from Groups A–C:
   - Group A (8): all Beneish components
   - Group B (10): cfo_to_net_income, accrual_ratio, accrual_ratio_change, cash_flow_variability, capex_to_cfo_ratio, cfo_net_income_divergence, fcf_to_revenue, interest_income_vs_cash, tax_paid_to_pbt, operating_cash_cycle_change
   - Group C (8): receivable_days_change, unbilled_revenue_ratio, cash_revenue_ratio, revenue_vs_gst_proxy, revenue_concentration, round_number_revenue_flag, channel_stuffing_indicator, quarter_end_revenue_spike

4. tests/regression/test_known_frauds.py — CRITICAL:
   - Satyam 2008 (pre-revelation): must score forensic_composite >= 60
   - Vakrangee 2017 (pre-crash): must score >= 55
   - HDFC Bank 2024 (clean): must score <= 20
   - TCS 2024 (clean): must score <= 25
   - These 4 tests are permanent regression tests that run on every build

5. tests/unit/test_forensic_classical.py:
   - Test Beneish M-Score computes correctly on known inputs (validate against published examples)
   - Test Benford MAD > 0.015 for artificially manipulated revenue series
   - Test all 30 classical features return finite floats
```

✅ **TEST:**
```bash
.venv/bin/pytest tests/regression/test_known_frauds.py -v   # ALL MUST PASS
.venv/bin/pytest tests/unit/test_forensic_classical.py -v
```

🔀 **COMMIT:** `feat(SPEC-MODEL-009): M-09 classical forensic scores, M-10 forensic ML, 7 classical models, known fraud regression tests PASSED`

---

## P2.6 — Phase 2 Integration: Trendlyne + DataStore Expansion + Full Backtest

📋 **PROMPT:**
```
Read alphalens_docs/Fundamental_Data_Sourcing_Guide.md Trendlyne and Tijori sections. Read alphalens_docs/12_platform_architecture.md Data Flow Matrix.

Integrate all Phase 2 data sources and run the full Phase 2 backtest:
0. Do not show interim steps or explain every thought on the screen. Run the necessary terminal commands, implement the changes, and record the major steps, errors, and resolutions as structured markdown in a file named BuildLog.md in the current directory.
1. If changes are required, update this file for all subsequent prompts.

1. ingestion/scrapers/trendlyne.py:
   - ScreenerSync class using Trendlyne StratQ API (TRENDLYNE_API_KEY from .env)
   - Superstar investor portfolios: Dolly Khanna, Vijay Kedia, Ashish Kacholia, Sunil Singhania, Porinju Veliyath
   - Downloads quarterly portfolio changes; maps to tickers in stock_master
   - Writes to governance table (superstar_flag, superstar_change columns)

2. ingestion/scrapers/tijori.py:
   - TijoriScraper class using Tijori Finance API (TIJORI_API_KEY from .env)
   - For each sector: fetch operational metrics (ARPU for telecom, NPA for banking, ANDA for pharma, etc.)
   - Writes to fundamentals table (sector_specific_metric_1 through sector_specific_metric_6)
   - Sector detection from stock_master.sector column

3. Expand DataStore API with all Phase 2 endpoints:
   - GET /api/v1/fundamentals/{ticker}/history?quarters=8
   - GET /api/v1/governance/{ticker}?as_of=
   - GET /api/v1/watchlist/current (multibagger top-20)
   - GET /api/v1/signals/ml/forensic/{ticker}
   - GET /api/v1/signals/ml/multibagger/{ticker}

4. Run full Phase 2 walk-forward backtest:
   - backtest/run_phase2_backtest.py
   - Includes Signal63D + Multibagger watchlist filter
   - Report: compare Phase 1 vs Phase 2 Sharpe, CAGR, MaxDD

5. Update dashboard to show Phase 2 outputs:
   - Add multibagger watchlist (top 5, weekly refresh)
   - Add forensic alert count (red/amber breakdown)
   - Add Signal63D predictions alongside 5d and 21d

6. Update the DataStore Client SDK (datastore/client.py) with Phase 2 methods:
   - get_multibagger_watchlist(), get_forensic_score(ticker), get_governance(ticker, as_of)
```

✅ **TEST:**
```bash
.venv/bin/pytest tests/ --cov=. -v  # full suite
.venv/bin/python3 -m backtest.run_phase2_backtest --quick --folds 2
curl "http://localhost:8000/api/v1/watchlist/current" | python3 -m json.tool
```

🔀 **COMMIT:** `feat(SPEC-DS-005): Trendlyne integration, Tijori integration, Phase 2 API expansion, Phase 2 backtest`

---

## 🔒 PHASE 2 GATE CHECK

📋 **PROMPT:**
```
Run the Phase 2 gate check. Read alphalens_docs/14_engineering_standards.md Phase 2→3 gate criteria.

Check and report PASS/FAIL:
1. pytest tests/ --cov=. — coverage must be >= 80%
2. python3 -m backtest.run_phase2_backtest --report — Mean Sharpe must be > 1.0 across all folds
3. python3 -c "from systems.ml_signal_engine.models.forensic.classical_scores import *; print('Forensic OK')" — must work
4. Verify sector z-scores: SELECT COUNT(*) FROM features WHERE roe_zscore IS NULL AND roe IS NOT NULL — should return 0
5. Verify Screener PIT: SELECT COUNT(*) FROM fundamentals WHERE announcement_date <= quarter_end_date — must return 0
6. Check Trendlyne API key set: python3 -c "import os; assert os.getenv('TRENDLYNE_API_KEY'), 'Missing'"
7. Check paper trading log has >= 3 months of tracked signals
8. Run pip-audit — report any HIGH severity CVEs (must fix before Phase 3)
9. Verify forensic regression tests still pass: pytest tests/regression/test_known_frauds.py -v
```

🔒 **All items must PASS. Mean Sharpe > 1.0 required.**

---

# PHASE 3 — Deep Learning + Consumer Systems (Weeks 27–38)

---

## P3.1 — Phase 3 Advanced Features (62 new features)

📋 **PROMPT:**
```
Read alphalens_docs/01_features.md Phase 3 features and alphalens_docs/specs/08_specifications.md SPEC-FEAT-001 through SPEC-FEAT-005.

Install Phase 3 libraries first:
pip install torch pytorch-forecasting pytorch-tabnet PyWavelets

Build Phase 3 feature modules (62 additional features → 330 total):
0. Do not show interim steps or explain every thought on the screen. Run the necessary terminal commands, implement the changes, and record the major steps, errors, and resolutions as structured markdown in a file named BuildLog.md in the current directory.
1. If changes are required, update this file for all subsequent prompts.

1. features/advanced_technical.py — wavelet + entropy features (18):
   - Wavelet decomposition (4): ruptures + PyWavelets — wavelet_trend, wavelet_noise, wavelet_energy_ratio, wavelet_regime_signal
   - Hurst exponent (2): hurst_exp_21d, hurst_exp_63d (using hurst library or manual R/S)
   - Entropy features (5): approx_entropy_21d, sample_entropy_21d, permutation_entropy_21d, spectral_entropy, fractal_dimension
   - Fractional differentiation (3): fracdiff_d_optimal, fracdiff_price, fracdiff_volume
   - Complexity (4): lyapunov_exponent_proxy, rqa_rec_rate, time_series_complexity, nonlinear_trend_strength

2. features/pattern_scores.py — 6 pattern recognition scores:
   - head_shoulders_score, double_bottom_score, cup_handle_score, flag_pattern_score, wedge_score, base_breakout_score
   - Use ta-lib pattern functions + custom scoring (0–1 probability each pattern is forming)

3. features/real_economy_macro.py — 10 real economy indicators:
   - gst_collection_growth, pmi_manufacturing, pmi_services, iip_growth, auto_monthly_sales_growth
   - cement_dispatches_growth, power_consumption_growth, rail_freight_growth, upi_transaction_growth, bank_credit_growth
   - All monthly frequency; forward-filled to daily; availability date tracked for PIT

4. features/deep_forensic.py — 28 forensic features (Groups D–I):
   - Group D (12): goodwill_ratio, cwip_ratio, contingent_liability_ratio, subsidiary_count, loans_to_related, capex_to_assets, intangibles_growth, off_balance_sheet_proxy, noncash_assets_ratio, asset_quality_score, balance_sheet_manipulation_score, asset_inflation_flag
   - Group E governance (partial, 8): salary_to_pat, rpt_intensity, audit_qualification_flag, auditor_change_flag, cfo_tenure_months, board_independence, director_resignation_count_4q, whistle_blower_policy
   - Groups F-I (8 total): benford_mad, altman_z, interest_coverage_trend, pledge_spiral_risk, gst_revenue_divergence, peer_outlier_score, tax_rate_anomaly, insider_selling_flag

5. Update features/matrix_builder.py to Phase 3 (330 features total):
   - Add all new features; document in alphalens_docs/01_features.md feature catalog update

6. tests/unit/test_phase3_features.py:
   - Test hurst exponent: brownian motion returns ~0.5, trending series returns > 0.6
   - Test pattern scores are in [0,1] range
   - Test real economy features are forward-filled correctly (no lookahead)
```

✅ **TEST:**
```bash
.venv/bin/pytest tests/unit/test_phase3_features.py -v
python3 -c "
from features.matrix_builder import build_feature_matrix
df = build_feature_matrix('2025-01-15', ['RELIANCE', 'TCS'])
print('Phase 3 feature count:', df.shape[1], '(should be 330)')
"
```

🔀 **COMMIT:** `feat(SPEC-FEAT-001): Phase 3 features — wavelet, entropy, fractal, pattern scores, real economy macro, deep forensic (330 total)`

---

## P3.2 — TFT + BiLSTM + Mamba-2 Deep Learning Models (M-11/M-12)

📋 **PROMPT:**
```
Read alphalens_docs/02_models.md sections M-11, M-12 and alphalens_docs/05_ml_algorithms.md deep learning section.

Build deep learning signal models — schedule overnight GPU training runs:
0. Do not show interim steps or explain every thought on the screen. Run the necessary terminal commands, implement the changes, and record the major steps, errors, and resolutions as structured markdown in a file named BuildLog.md in the current directory.
1. If changes are required, update this file for all subsequent prompts.

1. systems/ml_signal_engine/models/deep/tft_model.py — M-11 Temporal Fusion Transformer:
   - Uses pytorch-forecasting TFT implementation
   - Input: 330 Phase 3 features formatted as time series (lookback=63 days)
   - Outputs: Q10/Q50/Q90 quantile forecasts for 5d/21d/63d horizons
   - Implements BaseModel interface (SPEC-SOLID-003): train, predict, predict_proba, save, load
   - Attention map extraction: get_attention_weights() for interpretability
   - Training: batch_size=64, learning_rate=1e-3, epochs=50, early_stopping patience=10
   - Validation: save best epoch by validation loss
   - Estimated training time: 4–6 hours on CPU (Ryzen 5 7535U) — schedule overnight

2. systems/ml_signal_engine/models/deep/bilstm_model.py — M-12:
   - Bidirectional LSTM + optional Mamba-2 layer (mamba-ssm package, Linux only)
   - If mamba-ssm not available: fallback to BiLSTM only with additional attention layer
   - Input: same 330-feature time series as TFT
   - Output: same Q10/Q50/Q90 quantile outputs
   - Training: same schedule, batch_size=128, learning_rate=5e-4

3. Validation of deep learning models:
   - TFT attention maps must show temporal structure (earlier timesteps should have lower weight than recent)
   - BiLSTM validation loss must be lower than a naive baseline (predict median return)
   - Both models must pass integrity checks before inclusion in ensemble

4. tests/unit/test_deep_models.py:
   - Test TFT forward pass produces (batch_size, 3) output for Q10/Q50/Q90
   - Test BiLSTM handles variable sequence lengths gracefully
   - Test save/load preserves model weights exactly
   - Test attention weights sum to 1.0 per sample

Note: Full training is slow. Use --quick flag for CI (2 epochs, 50 samples).
```

✅ **TEST:**
```bash
.venv/bin/pytest tests/unit/test_deep_models.py -v
# Quick training test (2 epochs):
python3 -m systems.ml_signal_engine.models.deep.tft_model --quick --epochs 2
```

⚠️ **MANUAL — Schedule full overnight TFT training:**
```bash
nohup python3 -m systems.ml_signal_engine.inference.train_deep_models \
  --model tft --folds 2 &> logs/tft_training.log &
# Check next morning: tail logs/tft_training.log
```
.venv/bin/python3 -m systems.ml_signal_engine.models.deep.tft_model --quick --epochs 2

nohup .venv/bin/python3 -m systems.ml_signal_engine.inference.train_deep_models \
  --model tft --folds 5 > logs/tft_training.log 2>&1 &

# Monitor progress:
tail -f logs/tft_training.log


Whether we should do the deep learning now or wait for more data to be accumputated

🔀 **COMMIT:** `feat(SPEC-MODEL-001): M-11 TFT model, M-12 BiLSTM model, deep learning training pipeline, overnight schedule`

---

## P3.3 — Stacking Ensemble + TabNet + Phase 3 Backtest (M-13/M-14)

📋 **PROMPT:**
```
Read alphalens_docs/02_models.md M-13, M-14 sections and alphalens_docs/04_backtesting.md Phase 3 section.

Build the stacking ensemble and run Phase 3 backtest:
0. Do not show interim steps or explain every thought on the screen. Run the necessary terminal commands, implement the changes, and record the major steps, errors, and resolutions as structured markdown in a file named BuildLog.md in the current directory.
1. If changes are required, update this file for all subsequent prompts.

1. systems/ml_signal_engine/models/deep/stacking.py — M-13 StackingMetaLearner:
   - Base models: Signal5D (LGB+CatBoost+XGB), Signal21D, Signal63D, TFT, BiLSTM
   - Meta-learner: logistic regression on out-of-fold predictions from each base model
   - All base model weights must be >= 0.1 (if any weight < 0.1, flag WARNING in log)
   - Output: final_buy_prob, final_hold_prob, final_sell_prob with stacking confidence
   - Adaptive weighting: base model weights updated monthly based on recent accuracy

2. systems/ml_signal_engine/models/training/feature_selection.py — M-14 TabNet validator:
   - Run TabNet feature selection ONCE on full dataset
   - Cross-validate with SHAP: prune feature only if BOTH TabNet AND SHAP agree it's unimportant
   - List pruned features in alphalens_docs/01_features.md as "Phase 3 pruned — reason"
   - This is a research tool; do NOT retrain production models until Phase 3 backtest validates pruning

3. Run full Phase 3 backtest:
   - backtest/run_phase3_backtest.py — ensemble of all 5 base models + meta-learner
   - Measure: Sharpe improvement vs Phase 2 LightGBM-only baseline
   - Must show >= 0.1 Sharpe improvement (Phase 3 gate)
   - Full integrity checks, all 9 rules

4. HITL manual tests (you run these manually):
   - HITL-04: Pull 5 stocks through regime transition (bear→bull) — verify attention maps change correctly
   - HITL-05: Review 10 SHAP explanations — verify they make business sense

5. tests/unit/test_stacking.py:
   - Test all base model weights sum to 1.0
   - Test meta-learner predictions are in [0,1]
   - Test adaptive weight update changes weights when one model underperforms
```

✅ **TEST:**
```bash
pytest tests/unit/test_stacking.py -v
python3 -m backtest.run_phase3_backtest --quick --folds 2
# Verify: "Sharpe improvement vs Phase 2: +X.XX" — must be >= 0.1
```

Phase 3 backtest runs end-to-end; Sharpe gate correctly fails on synthetic random-walk data (as designed)
HITL-04 and HITL-05 are documented as PENDING — both require overnight TFT/BiLSTM training before execution. Run python -m backtest.run_phase3_backtest --real --folds 5 after training completes to clear the Phase 3 gate.

🔀 **COMMIT:** `feat(SPEC-MODEL-001): M-13 stacking ensemble, M-14 TabNet feature selection, Phase 3 backtest PASSED`



---

## P3.4 — Technical Analysis System (Consumer System 1)

📋 **PROMPT:**
```
Read alphalens_docs/15_future_applications.md APP-1 section (SPEC-TA-001 through SPEC-TA-008) and alphalens_docs/12_platform_architecture.md Consumer Systems section.

Build AlphaLens.Technical as a standalone consumer system. It reads from DataStore API only — no direct DB access:
0. Do not show interim steps or explain every thought on the screen. Run the necessary terminal commands, implement the changes, and record the major steps, errors, and resolutions as structured markdown in a file named BuildLog.md in the current directory.
1. If changes are required, update this file for all subsequent prompts.

1. systems/technical_analysis/__init__.py — FastAPI app on port 8002
2. systems/technical_analysis/charts/chart_engine.py:
   - get_chart_data(ticker, from_date, to_date, chart_type) → dict ready for frontend
   - chart_type: candlestick|ohlc|heikin_ashi|line
   - Includes: OHLCV + HMM regime colour overlay (reads from DataStore API)
   - Indicator computation: SMA(20/50/100/200), EMA(8/13/21/34/55/89), RSI(14), MACD, Bollinger, ATR, VWAP-proxy
   - All indicators via ta-lib; uses DataStore /api/v1/ohlcv endpoint for data

3. systems/technical_analysis/patterns/pattern_detector.py:
   - detect_patterns(ticker, from_date) → List[PatternResult]
   - Head & Shoulders, Cup & Handle, Double Bottom, Flag, Wedge, Darvas Box
   - Each PatternResult: pattern_name, confidence (0–1), target_price, stop_loss
   - Write results back to DataStore via POST /api/v1/signals/ta (SPEC-DS-004)

4. systems/technical_analysis/screener/ta_screener.py:
   - run_screener(strategy_name: str, universe_tickers: List[str]) → List[ScreenerResult]
   - Pre-built templates from alphalens_docs/15_future_applications.md SPEC-TA-005:
     - 'minervini_trend': all 8 SEPA criteria
     - 'ibd_base': cup-with-handle/double-bottom/flat-base patterns
     - 'turtle_donchian': new 20d high + ADX >= 15 + 2×ATR stop
     - 'stan_weinstein_stage2': weekly close > rising 30wk SMA
     - 'can_slim': all 7 CAN SLIM criteria
   - Custom criteria: user_defined dict of {'feature_name': ('op', threshold)}

5. systems/technical_analysis/api_writer.py:
   - Writes TA signals to DataStore for ML consumption (SPEC-DS-005)
   - ta_signals: date, ticker, pattern_name, pattern_score, support, resistance, trend_direction

6. tests/unit/test_ta_screener.py:
   - Test Minervini template: RELIANCE with all 8 criteria met → included in results
   - Test CAN SLIM: stock missing 'N' (new product) criterion → excluded
   - Test results are sorted by pattern_score descending
```

✅ **TEST:**
```bash
pytest tests/unit/test_ta_screener.py -v
uvicorn systems.technical_analysis:app --port 8002 &
curl "http://localhost:8002/health" | python3 -m json.tool
```

🔀 **COMMIT:** `feat(SPEC-TA-001): AlphaLens.Technical — chart engine, pattern detector, TA screener (42 templates), DataStore write-back`

---

## P3.5 — Damodaran Valuation System (Consumer System 2)

📋 **PROMPT:**
```
Read alphalens_docs/15_future_applications.md APP-3 section (SPEC-VAL-001 through SPEC-VAL-010) and alphalens_docs/Damodaran_Valuation_Module.md (full document).

Build AlphaLens.Valuation as a standalone consumer system:
0. Do not show interim steps or explain every thought on the screen. Run the necessary terminal commands, implement the changes, and record the major steps, errors, and resolutions as structured markdown in a file named BuildLog.md in the current directory.
1. If changes are required, update this file for all subsequent prompts.

1. systems/damodaran_valuation/__init__.py — FastAPI app on port 8004
2. systems/damodaran_valuation/lifecycle/classifier.py:
   - classify_lifecycle(ticker: str, as_of: str) → LifecycleStage
   - 6 stages + FINANCIAL_SERVICES branch (SPEC-VAL-001)
   - Uses fundamentals from DataStore API (via DataStoreClient — SPEC-SOLID-005)

3. systems/damodaran_valuation/dcf/dcf_engine.py:
   - compute_dcf(ticker, as_of, model_type=None) → DCFResult
   - Auto-selects model_type from lifecycle: FCFF_2STAGE, FCFF_3STAGE, FCFE, EXCESS_RETURN, COMMODITY_NORMALIZED, MERTON_OPTION
   - WACC computation (SPEC-VAL-003): risk_free = G-Sec yield - India default spread; ERP = 4.2% + 2.3% CRP; beta = Damodaran industry beta relevered; lambda adjustment for exporters
   - Sensitivity grid: ±1% WACC × ±1% terminal growth — always included in output
   - Monte Carlo: 10,000 simulations using scipy stats distributions

4. systems/damodaran_valuation/dcf/wacc_calculator.py:
   - compute_wacc(ticker, as_of) → WACCResult
   - Downloads India G-Sec yield from RBI (or uses settings.GSEC_YIELD_DEFAULT fallback)
   - Synthetic credit rating for unrated companies using ICR table from alphalens_docs

5. systems/damodaran_valuation/api_writer.py:
   - Writes valuation outputs to DataStore: intrinsic_value, valuation_gap_pct, margin_of_safety, wacc, dcf_model_type, scenario_bull/base/bear, mc_probability_undervalued
   - ML models in Phase 3+ read valuation_gap_pct as a feature

6. Annual Damodaran dataset downloader:
   - ingestion/damodaran/annual_download.py
   - Downloads from pages.stern.nyu.edu/~adamodar: betas, country risk, WACC by industry
   - Runs once per year (January); data stored in datastore/normalised/damodaran/

7. tests/unit/test_dcf.py:
   - Test WACC calculation: known inputs produce expected WACC (verify against Damodaran's India example)
   - Test lifecycle classification: startup stock (revenue CAGR > 30%, negative margin) → YOUNG_GROWTH
   - Test sensitivity grid always produced (never skipped)
   - Test bank/NBFC → FINANCIAL_SERVICES lifecycle, uses EXCESS_RETURN model
```

✅ **TEST:**
```bash
pytest tests/unit/test_dcf.py -v
uvicorn systems.damodaran_valuation:app --port 8004 &
curl "http://localhost:8004/health" | python3 -m json.tool
# Test one valuation:
curl "http://localhost:8004/api/v1/valuation/RELIANCE?as_of=2025-01-15" | python3 -m json.tool
```

🔀 **COMMIT:** `feat(SPEC-VAL-001): AlphaLens.Valuation — lifecycle classifier, DCF engine, WACC India-specific, Monte Carlo, DataStore write-back`

---

## 🔒 PHASE 3 GATE CHECK

📋 **PROMPT:**
```
Run the Phase 3 gate check. Check and report PASS/FAIL:
0. Do not show interim steps or explain every thought on the screen. Run the necessary terminal commands, implement the changes, and record the major steps, errors, and resolutions as structured markdown in a file named BuildLog.md in the current directory.
1. If changes are required, update this file for all subsequent prompts.

1. pytest tests/ --cov=. — coverage >= 80%
2. python3 -m backtest.run_phase3_backtest --report — Sharpe improvement vs Phase 2 must be >= 0.1
3. Verify TA System is running: curl http://localhost:8002/health
4. Verify Valuation System is running: curl http://localhost:8004/health
5. Verify cross-system signal fusion: SELECT COUNT(*) FROM ml_signals WHERE valuation_gap_pct IS NOT NULL — should have values for Phase 3+ feature set
6. Verify DataStore API documents all endpoints: curl http://localhost:8000/alphalens_docs (should load Swagger UI)
7. Run HITL-05 manually: review 10 SHAP explanations for business coherence (document in HITL_RESULTS.md)
8. Check paper trading has continued: 6+ months of tracked signals required
9. Run pip-audit — no HIGH severity CVEs
```

🔒 **Sharpe >= 0.1 improvement and both consumer systems live are mandatory.**

---

# PHASE 4 — Fundamental Analysis System + RL Agent (Weeks 39+)

---

## P4.1 — Fundamental Analysis System (Consumer System 3)

📋 **PROMPT:**
```
Read alphalens_docs/15_future_applications.md APP-2 section (SPEC-FA-001 through SPEC-FA-008) and alphalens_docs/Fundamental_Data_Sourcing_Guide.md.

Build AlphaLens.Fundamental as a standalone consumer system:
0. Do not show interim steps or explain every thought on the screen. Run the necessary terminal commands, implement the changes, and record the major steps, errors, and resolutions as structured markdown in a file named BuildLog.md in the current directory.
1. If changes are required, update this file for all subsequent prompts.

1. systems/fundamental_analysis/__init__.py — FastAPI app on port 8003
2. systems/fundamental_analysis/quality/quality_scorer.py:
   - compute_quality_score(ticker, as_of) → float (0–100)
   - Composite of: profitability consistency (5yr), capital efficiency, balance sheet quality, FCF generation, earnings quality (CFO/NI)
   - Sector-relative ranking: quality_score is percentile rank within sector

3. systems/fundamental_analysis/sector/ — 12 sector-specific modules:
   - bfsi.py: GNPA, NNPA, CASA, NIM, provision_coverage, cost_to_income, CAR — sources: RBI quarterly
   - insurance.py: combined_ratio, solvency_ratio, claim_settlement, persistency, VNB — sources: IRDAI
   - it_services.py: revenue_per_employee, utilisation, attrition, tcv_pipeline — sources: company filings
   - pharma.py: anda_count, usfda_observations, rd_to_revenue — sources: FDA database
   - fmcg.py: volume_growth, distribution_reach, gross_margin_resilience — sources: Tijori
   - auto.py: capacity_utilisation, ev_transition_pct, dealer_inventory_days — sources: Vahan
   - infra.py: order_book_revenue_ratio, execution_rate, debtor_days — sources: company filings
   - metals.py: realisation_per_tonne, cost_per_tonne, ebitda_per_tonne — sources: industry data
   - telecom.py: arpu, subscriber_adds, data_usage — sources: TRAI quarterly
   - power.py: plf, cost_of_generation, renewable_pct — sources: CEA generation reports
   - chemicals.py: specialty_vs_commodity_mix, customer_concentration — sources: Tijori
   - real_estate.py: presales_growth, net_debt_equity, unsold_inventory_months — sources: company filings

4. systems/fundamental_analysis/management/mgmt_scorer.py:
   - compute_mgmt_quality(ticker, as_of) → float (0–100)
   - Inputs: promoter_holding_trend, pledge_level, rpt_intensity, auditor_continuity, board_independence, capital_allocation_track_record, guidance_accuracy, equity_dilution_history

5. systems/fundamental_analysis/peers/peer_comparison.py:
   - get_peers(ticker, n=8) → List[str] — sector + mcap proximity from stock_master
   - compare_peers(ticker, peers, as_of, metrics) → pd.DataFrame

6. systems/fundamental_analysis/api_writer.py:
   - Writes fa_signals to DataStore: quality_score, growth_score, mgmt_quality_score, sector_rank, fa_rating

7. tests/unit/test_fundamental_analysis.py:
   - Test quality_score: HDFC Bank should score > 70 quality_score
   - Test sector routing: ticker with sector='Banking' routes to bfsi.py module
   - Test peer comparison: peers are in same sector, within 3x market cap
```

✅ **TEST:**
```bash
pytest tests/unit/test_fundamental_analysis.py -v
uvicorn systems.fundamental_analysis:app --port 8003 &
curl "http://localhost:8003/health" | python3 -m json.tool
```

🔀 **COMMIT:** `feat(SPEC-FA-001): AlphaLens.Fundamental — quality scorer, 12 sector modules, management quality, peer comparison, DataStore write-back`

---

## P4.2 — AlphaLens.Forensic Consumer System (Consumer System 4)

📋 **PROMPT:**
```
Read alphalens_docs/15_future_applications.md APP-4 section (SPEC-FOREN-001 through SPEC-FOREN-012) and alphalens_docs/Forensic_Accounting_ML_Specification.md.

Build AlphaLens.Forensic as a standalone consumer system:
0. Do not show interim steps or explain every thought on the screen. Run the necessary terminal commands, implement the changes, and record the major steps, errors, and resolutions as structured markdown in a file named BuildLog.md in the current directory.
1. If changes are required, update this file for all subsequent prompts.

1. systems/forensic_accounting/__init__.py — FastAPI app on port 8005
2. systems/forensic_accounting/dashboard/score_api.py:
   - GET /api/v1/forensic/{ticker}/score — returns M-09 + M-10 outputs from DataStore
   - GET /api/v1/forensic/{ticker}/drill_down/{flag_type} — detailed drill-down per SPEC-FOREN-003
   - GET /api/v1/forensic/universe_scan?sector=&tier= — full universe ranked by risk
   - GET /api/v1/forensic/{ticker}/benford — Benford digit distribution data
   - GET /api/v1/forensic/{ticker}/cashflow_quality — CFO/NI trend + accrual analysis

3. systems/forensic_accounting/investigation/report_builder.py:
   - build_report(ticker: str, as_of: str) → ForensicReport
   - Sections: executive summary, flag breakdown, peer comparison, historical pattern match
   - Pattern match: compare to Satyam/Vakrangee/DHFL patterns stored in fraud_case_library.py
   - Saves to datastore/outputs/forensic_reports/TICKER_YYYYMMDD.json

4. systems/forensic_accounting/case_library/fraud_case_library.py:
   - 15+ confirmed Indian fraud cases with pre-fraud financial fingerprints
   - Each case: fraud_type, year_revealed, key_signals (dict of feature: value), pattern_description
   - Cases: Satyam, DHFL, IL&FS, Yes Bank, PC Jeweller, Vakrangee, Manpasand, Bhushan Steel, Kingfisher, ADAG, Gitanjali, Ricoh India, Cox & Kings, CG Power, Karvy
   - match_pattern(forensic_features: dict) → best matching historical case + similarity score

5. systems/forensic_accounting/watchlist/alert_manager.py:
   - maintain_watchlist(tickers: List[str]) — user-maintained watchlist
   - check_alerts() → List[ForensicAlert]
   - Alert types: flag_escalation (green→amber, amber→red), beneish_breach (M > -1.78), pledge_spike (>5%/quarter), auditor_change, cfo_ni_breach (< 0.5)
   - Store false positive feedback (user dismissals) for ML model improvement loop

6. tests/unit/test_forensic_system.py:
   - Test report builder generates all required sections
   - Test pattern match returns Vakrangee for Vakrangee-like inputs
   - Test alert generation triggers on pledge spike from 15% to 22% in one quarter
```

✅ **TEST:**
```bash
pytest tests/unit/test_forensic_system.py -v
uvicorn systems.forensic_accounting:app --port 8005 &
curl "http://localhost:8005/health" | python3 -m json.tool
```

🔀 **COMMIT:** `feat(SPEC-FOREN-001): AlphaLens.Forensic — score API, drill-down, report builder, 15 fraud case library, alert manager`

---

## P4.3 — RL Meta-Agent (M-15) — Only After 3 Months Paper Trading

⚠️ **PREREQUISITE CHECK — Run this first:**

📋 **PROMPT:**
```
Check RL prerequisites. Read alphalens_docs/02_models.md M-15 section and alphalens_docs/11_phase_delivery_plan.md Phase 4 prerequisites.
0. Do not show interim steps or explain every thought on the screen. Run the necessary terminal commands, implement the changes, and record the major steps, errors, and resolutions as structured markdown in a file named BuildLog.md in the current directory.
1. If changes are required, update this file for all subsequent prompts.


Verify ALL of the following before starting any RL code:
1. Count paper_trading/log.csv rows: must have >= 60 trading days of tracked signals (3 months)
2. Count experience tuples: SELECT COUNT(*) FROM rl_experience_buffer — must be >= 500,000
3. Verify all Phase 1–3 models are stable: run full test suite and report any failures
4. Verify backtest Sharpe > 1.0 consistently across last 3 months of paper trading
5. Report: READY or NOT READY with specific blocking items

Do NOT generate any RL code if any prerequisite fails.
```

Only proceed if all prerequisites pass.

📋 **PROMPT (only after prerequisites pass):**
```
Read alphalens_docs/02_models.md M-15 PPO RL section and alphalens_docs/specs/08_specifications.md SPEC-MODEL-001.
0. Do not show interim steps or explain every thought on the screen. Run the necessary terminal commands, implement the changes, and record the major steps, errors, and resolutions as structured markdown in a file named BuildLog.md in the current directory.
1. If changes are required, update this file for all subsequent prompts.


Build the RL meta-agent in 5 bootstrapping stages. Stage 1 only today — do not proceed to Stage 2 without validation:

STAGE 1 — Build the environment only:
1. pip install stable-baselines3 gymnasium
2. systems/ml_signal_engine/models/rl/trading_env.py — AlphaLensTradingEnv(gymnasium.Env):
   - State vector: 30 dimensions — all Phase 3 model probabilities + portfolio state (cash%, positions, drawdown) + HMM regime + drift score
   - Actions: StrongBuy, Buy, Hold, Reduce50pct, FullExit per stock
   - Reward: risk-adjusted return minus transaction costs; negative reward for drawdown exceeding -20%
   - Episode: 1 year of trading days
   - Safety guardrails: MAX_POSITION_PCT (10%), MAX_SECTOR_PCT (40%) enforced as hard constraints — not just rewards

3. systems/ml_signal_engine/models/rl/experience_buffer.py:
   - Builds 500K+ experience tuples from historical supervised model outputs (Stage 1 bootstrap)
   - Each tuple: (state_vector, action_taken, reward, next_state, done)
   - Offline experience from backtest signals (do NOT use actual live data until Stage 4)

4. tests/unit/test_trading_env.py:
   - Test environment reset produces valid state vector (shape: (30,))
   - Test all 5 action types execute without errors
   - Test safety guardrails: attempting > MAX_POSITION_PCT is clipped, not rejected

Stage 2 (offline PPO training on replay buffer) to be done as a separate prompt after Stage 1 tests pass.
```

✅ **TEST (Stage 1 only):**
```bash
pytest tests/unit/test_trading_env.py -v
python3 -c "
from systems.ml_signal_engine.models.rl.trading_env import AlphaLensTradingEnv
env = AlphaLensTradingEnv()
obs, _ = env.reset()
print('State shape:', obs.shape, '— Expected: (30,)')
"
```

🔀 **COMMIT:** `feat(SPEC-MODEL-001): M-15 RL trading environment, state vector, safety guardrails, experience buffer (Stage 1 of 5)`

---

## P4.4 — RL Stages 2–3 (Offline Training + Synthetic Scenarios)

📋 **PROMPT:**
```
Read alphalens_docs/02_models.md M-15 bootstrapping stages. Build Stages 2 and 3 only.
0. Do not show interim steps or explain every thought on the screen. Run the necessary terminal commands, implement the changes, and record the major steps, errors, and resolutions as structured markdown in a file named BuildLog.md in the current directory.
1. If changes are required, update this file for all subsequent prompts.

STAGE 2 — Offline PPO training on replay buffer:
1. systems/ml_signal_engine/models/rl/ppo_trainer.py:
   - Offline PPO training using stable-baselines3 PPO
   - Input: experience_buffer from Stage 1 (500K+ tuples)
   - 5 regime-conditioned sub-policies: Bull, Bear, Sideways, HighVol, Transition
   - HMM regime label activates the corresponding sub-policy
   - Training: 1M timesteps per sub-policy; learning_rate=3e-4; ent_coef=0.01
   - Evaluation: compare cumulative reward vs supervised-only baseline after each 100K steps
   - Save checkpoint every 100K steps to datastore/models/rl/

STAGE 3 — Synthetic scenario augmentation:
2. systems/ml_signal_engine/models/rl/scenario_augmentor.py:
   - generate_crash_scenario() — 40% drawdown over 20 days (COVID March 2020 template)
   - generate_boom_scenario() — sustained 30%+ rally (2021 bull market template)
   - generate_transition_scenario() — bear→bull transition with 30+ day sideways period
   - Each scenario: 500K synthetic tuples added to replay buffer
   - Validate RL agent behaviour in crash: must reduce positions, not add

3. tests/unit/test_ppo_trainer.py:
   - Test sub-policy activation: bear regime → bear sub-policy selected
   - Test crash scenario: RL agent reduces all positions to < 50% within 5 days of crash start
   - Test reward improves vs baseline after 100K training steps

This is CPU-intensive training. Schedule overnight:
  nohup python3 -m systems.ml_signal_engine.models.rl.ppo_trainer --stages 2,3 > logs/rl_training.log &
```

✅ **TEST:**
```bash
pytest tests/unit/test_ppo_trainer.py -v
# Check overnight training log:
tail -20 logs/rl_training.log
```

🔀 **COMMIT:** `feat(SPEC-MODEL-001): M-15 offline PPO training (Stage 2), synthetic crash/boom/transition scenarios (Stage 3)`

---

## P4.5 — PSI+ADWIN Drift Monitor (M-16) + Full Platform Integration

📋 **PROMPT:**
```
Read alphalens_docs/02_models.md M-16 section and alphalens_docs/specs/08_specifications.md SPEC-OBS-001 through SPEC-OBS-005.
0. Do not show interim steps or explain every thought on the screen. Run the necessary terminal commands, implement the changes, and record the major steps, errors, and resolutions as structured markdown in a file named BuildLog.md in the current directory.
1. If changes are required, update this file for all subsequent prompts.

Build drift monitoring and complete the platform integration:
1. systems/ml_signal_engine/models/monitoring/drift_monitor.py — M-16 PSI+ADWIN DriftMonitor:
   - PSI monitor: daily computation for top-50 features vs baseline (SPEC-PIPE-005)
   - ADWIN (river library): detects concept drift in model accuracy stream
   - Actions by severity:
     - PSI 0.10–0.25: log WARNING + reduce position sizing 50%
     - PSI > 0.25: halt new positions + trigger retrain alert
     - ADWIN drift detected: schedule retrain + notify dashboard
   - Returns drift_status dict: {feature_name: {'psi': float, 'adwin': bool}}

2. Complete the full 5-system DataStore integration test:
   - tests/integration/test_full_platform.py:
     - Start all 5 systems: DataStore(8000), ML(8001), TA(8002), FA(8003), Val(8004), Forensic(8005)
     - Run daily inference for 3 dates
     - Verify signals appear in DataStore
     - Verify TA system reads HMM regime from DataStore correctly
     - Verify Valuation system valuation_gap_pct appears in ML feature set
     - Verify Forensic system score appears in ML forensic composite
     - Test full cross-system signal fusion (SPEC-DS-005)

3. deploy/start_platform.sh — starts all 6 services in correct order:
   - Order: DataStore API → ML Signal Engine → Technical → Fundamental → Valuation → Forensic
   - Health check after each start
   - Logs each service to logs/service_NAME.log

4. deploy/stop_platform.sh — graceful shutdown of all services

5. Final documentation update:
   - Update alphalens_docs/CLAUDE.md with Phase 4 status
   - Update alphalens_docs/07_truthful_expectations.md with actual vs expected performance (honest)
   - Generate alphalens_docs/SYSTEM_STATUS.md: all 16 models, 4 consumer systems, current status
```

✅ **TEST:**
```bash
pytest tests/integration/test_full_platform.py -v
bash deploy/start_platform.sh
# Verify all 6 health checks pass:
for port in 8000 8001 8002 8003 8004 8005; do
  curl -s http://localhost:$port/health | python3 -m json.tool
done
```

🔀 **COMMIT:** `feat(SPEC-OBS-001): M-16 PSI+ADWIN drift monitor, full platform integration test, start/stop scripts`

---

## 🔒 FINAL PLATFORM GATE CHECK

📋 **PROMPT:**
```
Run the final platform gate check across all 5 phases:
0. Do not show interim steps or explain every thought on the screen. Run the necessary terminal commands, implement the changes, and record the major steps, errors, and resolutions as structured markdown in a file named BuildLog.md in the current directory.
1. If changes are required, update this file for all subsequent prompts.

1. pytest tests/ --cov=. — coverage >= 80%, report per-module
2. Run all regression tests: pytest tests/regression/ -v — ALL MUST PASS
3. Run HITL tests:
   - HITL-01: "Load DHFL 2018 governance data. Does the system flag escalating pledge spiral?"
   - HITL-02: "Load 5 known P&D stocks from 2022. Does the P&D model block all 5?"
   - HITL-03: "Show current multibagger top-20. Do the archetypes make business sense?"
   - HITL-06: "Run forensic on the Satyam 2007 financial profile. Does it flag red?"
4. Run full backtest: python3 -m backtest.run_phase3_backtest --report — Sharpe > 1.0
5. Verify all 16 models are registered: cat datastore/models/registry.json | python3 -m json.tool
6. Verify all 6 services healthy: bash deploy/start_platform.sh && sleep 5 && bash deploy/check_health.sh
7. Run pip-audit — zero HIGH or CRITICAL CVEs
8. Check paper trading performance: python3 -m paper_trading.performance_report — report actual vs predicted returns honestly
9. Verify API alphalens_docs accessible: curl http://localhost:8000/alphalens_docs -o /dev/null -w "%{http_code}" — must return 200

Report every PASS/FAIL. This is your go/no-go for using live signal outputs.
```

---

# APPENDIX — Cross-Cutting Prompts

---

## Any Time: Fix a Failing Test

📋 **PROMPT:**
```
The test [TEST_FILE_PATH]::[TEST_FUNCTION] is failing with this error:
[PASTE ERROR]

1. Read the failing test and identify what it expects
2. Read the spec it references (look at the alphalens_docstring for SPEC-ID)
3. Fix the source code — do not change the test unless the test itself has a bug
4. Re-run the test and confirm it passes
5. Check that no other tests broke: pytest [TEST_FILE_PATH] -v
```

---

## Any Time: Add a New Feature Not Yet in Spec

📋 **PROMPT:**
```
I want to add [DESCRIBE FEATURE]. Before writing any code:
1. Check alphalens_docs/specs/08_specifications.md — does this already have a SPEC-ID?
2. If not: create a new SPEC-ID following the naming convention (SPEC-[CATEGORY]-[NNN])
3. Add the new spec to alphalens_docs/specs/08_specifications.md in the right section
4. Add the corresponding test ID to alphalens_docs/tests/09_automated_tests.md (SPEC-TRACE-001)
5. Then implement the feature
6. Then write the test referencing the new SPEC-ID

Never implement a feature without a spec. Never merge a spec without a test.
```

---

## Any Time: Quarterly Library Security Audit (SPEC-LIB-003)

📋 **PROMPT:**
```
Run the quarterly library security audit per SPEC-LIB-003:
1. Run: pip-audit --requirement requirements/phase1.txt --format json > audit_results.json
2. Parse audit_results.json and list all CVEs by severity (CRITICAL, HIGH, MEDIUM)
3. For each HIGH or CRITICAL CVE: check the SPEC-LIB-002 upgrade protocol — create a branch, upgrade, run tests, compare backtest metrics
4. Report: list of CVEs found, action taken or planned for each
```

---

## Any Time: Model Retrain (Monthly Cadence — SPEC-MODEL-008)

📋 **PROMPT:**
```
Run the monthly model retrain per SPEC-MODEL-008:
1. Check: has enough new data accumulated? (>= 20 new trading days since last retrain)
2. Run: python3 -m systems.ml_signal_engine.inference.train_all_phase[N] --incremental
3. Compare new model vs current: run backtest on last 3 months, compare Sharpe and accuracy
4. If new model is better: update registry.json with new version, update symlink
5. If new model is worse: log the comparison, keep current model, investigate why
6. Run regression tests: pytest tests/regression/ -v — all must pass with new models
7. Commit: git commit -m "chore(SPEC-MODEL-008): monthly retrain [DATE] — Sharpe [OLD] → [NEW]"
```

---

## Any Time: Debug a Data Pipeline Failure

📋 **PROMPT:**
```
The pipeline run for [DATE] failed. The error from pipeline_runs table is:
[PASTE ERROR MESSAGE]

1. Query SQLite pipeline_runs for this date: what step failed?
2. Read the checkpoint: what step was last completed successfully?
3. Read the source file for the failing step
4. Identify the failure type: data quality issue, scraper failure, model error, or disk space
5. Fix the root cause
6. Test the fix in isolation: python3 -m [failing_module] --date [DATE] --dry-run
7. Resume pipeline from checkpoint: python3 -m ingestion.scheduler.daily_pipeline --date [DATE] --resume
8. Verify the run completes and signals are written to DataStore
```

---

# PHASE X — Explain-Me Walkthrough (Truthful-Mode Brainstorming Series)

**Purpose:** Unlike every prompt above (which builds/implements), this series is
for *understanding what already exists* — walking the finished application
screen-by-screen and system-by-system, capturing the answers as a permanent FAQ,
and capturing any improvement ideas that surface along the way. Do not write
code or change behavior during this series; it is read-only/audit-style.

**Output files (created/updated by these prompts, not by you before running them):**
- `ExplainMe.md` — the FAQ. Every explanation Claude gives ends up here as a final,
  durable Q&A entry (not a chat transcript — a cleaned-up answer).
- `FutureDevelopment.md` — a running backlog of improvement ideas noticed during
  the walkthrough. Not implemented now — just captured.

## Ground Rules — Truthful Mode (embedded in every prompt below)

```
Operate in truthful mode for this entire session:
- Only state things you have verified by reading the actual code, config, or
  running output in this repo. Do not describe intended/spec behavior as if it
  were implemented behavior — if a spec says X but the code does Y (or nothing),
  say so explicitly and flag the gap.
- Cite file:line for every factual claim about how something works.
- If you don't know or can't verify something from this repo, say "not verifiable
  from the codebase" instead of guessing or extrapolating from the docs.
- No sycophancy, no hedging filler ("great question!"), no inflating how complete
  or robust something is. If a screen is an empty-state stub, say that plainly.
- Where the docs (alphalens_docs/) and the actual code disagree, the code wins —
  note the discrepancy so the docs can be corrected later.
```

## Workflow (repeat for every module below)

1. Run the module's prompt.
2. Review Claude's answers in-chat; ask follow-ups if something is unclear or
   you don't believe it — truthful mode means it should hold up to pushback.
3. Once you're satisfied, tell Claude: **"Finalize this module — write it up."**
   Claude should then:
   - Append a new `## <Module Name>` section to `ExplainMe.md` with clean final
     Q&A entries (question as asked or paraphrased, answer as verified, with
     file:line citations).
   - Append any improvement ideas surfaced during discussion to
     `FutureDevelopment.md` under a `## <Module Name>` heading (one bullet per
     idea, plain description, no priority/estimate needed yet).
4. Run `/clear`.
5. Move to the next module's prompt in a fresh session.

Do this in order — later modules assume the data-layer module has already been
covered, so cross-references in `ExplainMe.md` stay coherent.

---

## X.0 — Platform Architecture & Data Layer

📋 **PROMPT:**
```
Operate in truthful mode for this entire session (see ground rules: verify every
claim against actual code/config, cite file:line, flag any place where
alphalens_docs/ describes something that isn't actually implemented, say
"not verifiable" rather than guess).

Walk me through AlphaLens end to end at the architecture level, as if explaining
it to someone who has never seen the repo:
1. What is the central DataStore, what are its 6 sub-stores, and which files
   actually implement reads/writes to each one today (not just what the docs say
   should exist)?
2. What does the daily pipeline (ingestion/scheduler/daily_pipeline.py) actually
   do step by step right now — which steps are real, which are stubbed/
   NotImplementedError, which are skipped?
3. What is config/settings.py responsible for, and what are the load-bearing
   constants a new reader needs to know (universe filters, thresholds, paths)?
4. What is the DataStore API (datastore/api/) — list every router that exists
   today and what each one actually serves, verified from the router files
   themselves, not from a spec doc.
5. Where does real market/fundamental data actually come from today (which
   scrapers are wired into the pipeline vs. which exist but aren't called)?

This is a brainstorming session — answer conversationally, I may push back or
ask you to double check something. When I say "finalize this module", write the
final answers as Q&A entries into ExplainMe.md under a "## Platform Architecture
& Data Layer" heading, and log any improvement ideas that came up into
FutureDevelopment.md under the same heading. Don't touch either file until I say
to finalize.
```

---

## X.1 — AlphaLens.ML (Daily Insights, Signal Deep Dive, Multibagger, Positions, Backtest)

📋 **PROMPT:**
```
Operate in truthful mode (verified claims only, file:line citations, flag
doc-vs-code mismatches, no guessing).

Walk me through the AlphaLens.ML app screen by screen. For each of the 5 screens
(dashboard/static/ml/index.html, signal.html, multibagger.html, positions.html,
backtest.html):
1. What does the screen show a user, concretely — what data, what layout?
2. Which API endpoint(s) does it call (datastore/api/routers/...), and is that
   endpoint backed by real computed data today, or partially/fully empty-state?
3. Which model(s) under systems/ml_signal_engine/ feed the numbers shown, and
   what is each model's actual current state — trained and running daily, or
   only implemented in isolation without a live daily run?
4. For Positions specifically: explain the Pending Actions review/approve flow
   (paper_trading/pending/, datastore/api/routers/paper_trading.py) — what
   triggers a pending action, what happens on approve/reject, where results land.
5. What's genuinely working end-to-end today vs. what looks complete in the UI
   but is fed by a screen that's real-but-thin (e.g. only one date has live
   signals — check whether that's still true).

Conversational session — I'll ask follow-ups. When I say "finalize this module",
append final Q&A to ExplainMe.md under "## AlphaLens.ML" and improvement ideas to
FutureDevelopment.md under the same heading.
```

---

## X.2 — AlphaLens.Technical (Chart, Screener, Compare, Alerts, Market Overview)

📋 **PROMPT:**
```
Operate in truthful mode (verified claims only, file:line citations, flag doc-vs-
code mismatches, say "not verifiable" instead of guessing).

Walk me through AlphaLens.Technical screen by screen (dashboard/static/technical/
chart.html, screener.html, compare.html, alerts.html, overview.html, and the newer
watchlist.html):
1. What does each screen do, and which systems/technical_analysis/ module and
   datastore/api/routers/technical.py endpoint backs it?
2. The Strategy Screener claims "42 pre-built templates" per the design doc —
   verify how many templates actually exist in code today and list them.
3. Alert Manager: walk through the actual alert lifecycle — creation, the
   cross-process DuckDB lock issue that was fixed recently (see BuildLog.md /
   recent commits about check_ta_alerts), how alerts get evaluated, and where
   triggered alerts surface to the user.
4. Is charting using real OHLCV, and is it corporate-action adjusted per the
   platform's own rule (alphalens_docs/CLAUDE.md rule #2)? Verify, don't assume.
5. What's the actual state of the new watchlist screens
   (dashboard/static/technical/watchlist.html + js/watchlist.js) — fully wired
   or in-progress?

Conversational — I'll push back if something feels off. When I say "finalize this
module", append final Q&A to ExplainMe.md under "## AlphaLens.Technical" and ideas
to FutureDevelopment.md under the same heading.
```

---

## X.3 — AlphaLens.Fundamental (Dashboard, Peers, Sector, Screener, Thesis, Management)

📋 **PROMPT:**
```
Operate in truthful mode (verified claims only, file:line citations, flag doc-vs-
code mismatches, no guessing).

Walk me through AlphaLens.Fundamental screen by screen (dashboard/static/
fundamental/dashboard.html, peers.html, sector.html, screener.html, thesis.html,
management.html), backed by systems/fundamental_analysis/*.

1. alphalens_docs/CLAUDE.md's screen table says Sector and Management each have
   "one empty-stated sub-panel" — verify this claim against the actual HTML/JS
   and current API responses. Is it still true, and which specific sub-panel(s)?
2. Financial Dashboard: which ratios/traffic-light thresholds are real
   calculations vs. placeholders? Where does quarterly fundamentals data
   actually come from (Screener.in scrape, Trendlyne, Tijori, Kaggle backfill)
   and is point-in-time alignment (announcement_date, not quarter_end_date)
   actually enforced in the code path this screen uses?
3. Thesis Builder: does the PDF export actually work today, or is it a stub?
4. Peer Comparison: how are peers selected (sector-based, manual, hardcoded)?
5. Confirm me that each and every feature is generate correctly.

Conversational — ask me to clarify or push back as needed. When I say "finalize
this module", append final Q&A to ExplainMe.md under "## AlphaLens.Fundamental"
and ideas to FutureDevelopment.md under the same heading.
```

---

## X.4 — AlphaLens.Valuation (DCF, Relative, Batch, Accuracy)

📋 **PROMPT:**
```
Operate in truthful mode (verified claims only, file:line citations, no guessing).

alphalens_docs/CLAUDE.md's screen table marks all 4 Valuation screens as "Empty
(no backend)" as of the last doc update, but the git status shows dashboard/static/
valuation/*.html and a new dashboard/static/valuation/js/ directory have changed
recently, and systems/damodaran_valuation/ (dcf/models.py, dcf/wacc.py,
lifecycle/classifier.py, valuation_engine.py) has recent edits too. Your job is
to determine the CURRENT truth, not repeat the stale doc claim:

1. For each of dcf.html, relative.html, batch.html, accuracy.html: does it call
   a real datastore/api/routers/valuation.py endpoint today, and does that
   endpoint return real computed DCF/relative-value output, or still an
   empty-state placeholder? Check each one individually — don't assume they've
   all moved together.
2. Walk through systems/damodaran_valuation/ — what's implemented (DCF engine,
   WACC calc, lifecycle classifier, scenarios) and what a full run actually
   produces today when pointed at a real ticker.
3. If some screens are now real and others aren't, tell me exactly which is
   which, and what's the smallest gap left to close for the remaining ones.
4. Correct the record: is alphalens_docs/CLAUDE.md's status table now out of
   date? Say so explicitly if it is.
5. Confirm me that each and every feature is generate correctly.

Conversational — I'll ask follow-ups. When I say "finalize this module", append
final Q&A to ExplainMe.md under "## AlphaLens.Valuation" and ideas to
FutureDevelopment.md under the same heading, and separately flag to me (in
chat, not in the files) that alphalens_docs/CLAUDE.md's screen-status table
should be corrected if you found it stale.
```

---

## X.5 — AlphaLens.Forensic (Dashboard, Red Flags, Benford, Cash Flow, Peer Heatmap, Investigation, Universe Scan)

📋 **PROMPT:**
```
Operate in truthful mode (verified claims only, file:line citations, no guessing).

Walk me through AlphaLens.Forensic screen by screen (dashboard/static/forensic/
dashboard.html, redflag.html, benford.html, cashflow.html, heatmap.html,
report.html, universe.html):

1. What forensic score(s) actually get computed, where (features/forensic_classical.py,
   systems/ml_signal_engine/models/forensic/ if present), and which formulas are
   real classical formulas (e.g. Beneish M-Score, Altman Z) vs. custom scores?
2. Benford's Law screen: verify the digit-distribution/chi-square/MAD computation
   is real math over real financial-statement figures, not a static/demo chart.
3. Investigation Report (report.html): is the "guided report builder → PDF"
   actually functional, or a UI shell?
4. Universe Scan: does this run across the full configured universe, or a
   hardcoded small sample? Check config/universe.py wiring.
5. Cross-check alphalens_docs/CLAUDE.md's claim that this app is "Real" (not
   partial/empty like Valuation) — does that hold up screen by screen?

Conversational — push back is welcome. When I say "finalize this module", append
final Q&A to ExplainMe.md under "## AlphaLens.Forensic" and ideas to
FutureDevelopment.md under the same heading.
```

---

## X.6 — Paper Trading & Execution Tracking

📋 **PROMPT:**
```
Operate in truthful mode (verified claims only, file:line citations, no guessing).

Walk me through the paper trading subsystem:
1. paper_trading/pending/, paper_trading/executions/, paper_trading/
   sim_reports/, paper_trading/portfolio_state.json — what does each actually
   contain, and what writes/reads each one (scripts/run_daily_paper_trading.py,
   scripts/paper_trading_tracker.py, backtest/portfolio_state.py, datastore/api/
   routers/paper_trading.py)?
2. Walk the full lifecycle of one signal: model emits a signal → pending action
   created → user approves/rejects via ML-D Position Monitor → execution
   recorded → portfolio_state updated. Verify each hop against the actual code,
   don't narrate the "intended" flow from docs.
3. Per memory/BuildLog: paper trading has had 0 real trading days tracked as of
   the last status review. Check the current state — how many days of real
   executions exist now (paper_trading/executions/*.csv), and is that number
   still effectively zero or has it started accumulating?
4. What guardrails exist against duplicate/erroneous pending actions (recent
   commits mention an Ops Monitor DuckDB lock race fix — is that the same
   subsystem or a different one)?

Conversational. When I say "finalize this module", append final Q&A to
ExplainMe.md under "## Paper Trading & Execution" and ideas to
FutureDevelopment.md under the same heading.
```

---

## X.7 — Backtesting Engine

📋 **PROMPT:**
```
Operate in truthful mode (verified claims only, file:line citations, no guessing).

Walk me through backtest/ (engine.py, portfolio.py, costs.py, metrics.py,
integrity_checker.py) and its dashboard surface (ML-E Backtest Dashboard,
dashboard/static/ml/backtest.html):

1. Confirm walk-forward validation is actually implemented (no random
   train/test split anywhere in this path) — show me the code that enforces it.
2. What transaction cost model is used (costs.py) — is it India-specific
   (STT, brokerage, slippage) and are the numbers real or placeholder defaults?
3. What does integrity_checker.py actually check, and does the Backtest
   Dashboard surface those checks (C-INTEGRITY-CHECKLIST) with real pass/fail
   state or hardcoded green checks?
4. Are benchmark comparisons (C-BENCHMARK-TABLE) computed against a real index
   series, and which one?

Conversational. When I say "finalize this module", append final Q&A to
ExplainMe.md under "## Backtesting Engine" and ideas to FutureDevelopment.md
under the same heading.
```

---

## X.8 — Ingestion, Scheduler & Ops Monitor

📋 **PROMPT:**
```
Operate in truthful mode (verified claims only, file:line citations, no guessing).

Walk me through the ingestion/scheduling/ops layer:
1. ingestion/scrapers/ — list every scraper file and, for each, whether it's
   actually invoked by ingestion/scheduler/daily_pipeline.py today or dormant/
   standalone (include the new etf_list.py and config/etf_exclusions.py — are
   these wired in yet or still untracked additions?).
2. ingestion/scheduler/daily_pipeline.py + checkpoint.py — walk the real
   checkpoint/resume behavior, verified against the code, not the docs'
   idealized 15-step flow.
3. dashboard/static/ops/index.html (Ops Monitor) — what does it actually poll
   and display, and what was the cross-process DuckDB lock race bug (recent
   commit "Fix check_ta_alerts cross-process DuckDB lock race") — root cause
   and fix, in plain terms.
4. What's the current state of NSE 2026 holiday coverage (config/nse_holidays.py)
   — was NSE_HOLIDAYS_2026_PENDING ever resolved?

Conversational. When I say "finalize this module", append final Q&A to
ExplainMe.md under "## Ingestion, Scheduler & Ops" and ideas to
FutureDevelopment.md under the same heading.
```

---

## X.9 — Test Suite & Quality Gates

📋 **PROMPT:**
```
Operate in truthful mode (verified claims only, file:line citations, no guessing).

Walk me through tests/ and the quality-gate story:
1. tests/unit, tests/integration, tests/regression, tests/hitl — what does each
   directory actually cover today (rough count of test files/cases, not just
   directory names), and which of these actually pass right now if run?
2. tests/quality/ — the no-stub/synthetic-data enforcement mentioned in memory.
   Show me the actual check and confirm it's still active/passing.
3. Is there a real CI setup (GitHub Actions or similar) running these, or is
   test execution entirely manual today?
4. Coverage: what does actual coverage look like for the highest-risk paths
   (PIT joins, walk-forward backtest, P&D pre-filter) vs. thin/untested paths?

Conversational. When I say "finalize this module", append final Q&A to
ExplainMe.md under "## Test Suite & Quality Gates" and ideas to
FutureDevelopment.md under the same heading.
```

---

## X.10 — Cross-Cutting Wrap-Up

📋 **PROMPT:**
```
Operate in truthful mode (verified claims only, file:line citations, no guessing).

This is the closing session. Read the full current ExplainMe.md and
FutureDevelopment.md (built up over the previous modules) plus the current
alphalens_docs/CLAUDE.md screen-status table, and:

1. Give me a single honest one-paragraph-per-app status summary: ML, Technical,
   Fundamental, Valuation, Forensic — real vs. partial vs. empty, as verified
   across all prior modules (not the doc's claims).
2. List every place where alphalens_docs/CLAUDE.md or other alphalens_docs/*.md
   files are now stale relative to what the walkthrough found, so they can be
   fixed later.
3. Pull the single highest-leverage item out of FutureDevelopment.md — the one
   gap that, if closed, would most improve the honesty of the "what's real vs.
   stubbed" story across the app.

When I say "finalize", append this as a final "## Cross-Cutting Summary
(<date>)" section at the top of ExplainMe.md (after the title, before the
Platform Architecture & Data Layer section) and do not add anything further to
FutureDevelopment.md beyond what's already there unless something genuinely new
surfaced.
```

---

### Notes on running the Explain-Me series

- Run X.0 → X.10 in order. X.10 depends on the prior modules having populated
  `ExplainMe.md` / `FutureDevelopment.md`.
- `/clear` between every module — that's the point of finalizing before moving
  on; each module prompt is self-contained and doesn't depend on in-chat memory
  from the previous one, only on the files it wrote.
- If a module prompt turns up something big enough to want fixed immediately
  (not just logged), stop, fix it in a separate normal session, then come back
  and re-run that module's prompt fresh once the fix lands — don't let a
  mid-walkthrough fix contaminate the truthful-mode explanation session.
- This series is read-only/audit-style and should never itself contain
  code-change instructions, unlike every prompt in the phases above.

---

*End of AlphaLens Claude Code Prompt Guide*
*Generated: June 2026 | Covers: 5 phases, 16 models, 4 consumer systems, plus the Explain-Me walkthrough series (added 2026-07-04)*
*Reference: alphalens_docs/CLAUDE.md (master context) | alphalens_docs/specs/08_specifications.md (all spec IDs)*
