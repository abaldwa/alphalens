# AlphaLens — Claude Code Prompt Guide
## All Phases · No Ambiguity · Production-Ready Sequence

**How to use this file:**
- Copy each prompt verbatim into a Claude Code session (`claude` in terminal)
- 🔀 = Git commit point. Do not skip these.
- ✅ = Run tests now. Do not proceed until green.
- 🔧 = Manual action outside Claude Code. Do this yourself.
- Each prompt is self-contained — if your session dies, restart and run from the same prompt.
- Context per session: Claude Code reads CLAUDE.md automatically at start. Prompts that need deeper context say "also read X".

---

## PRE-FLIGHT (Before any prompt)

```bash
# Terminal setup (run once, outside Claude Code)
cd ~/alphalens_platform
conda activate alphalens
claude  # starts Claude Code session
```

---

## PHASE 0 — Infrastructure & Data Foundation
### Weeks 1–4 · Goal: Every scraper live, historical data loaded, scheduler running

---

### P0-01 · Project Skeleton + Config + Contracts

```
Read CLAUDE.md and docs/12_platform_architecture.md in full.
Then create the complete project skeleton:

1. Create every directory from the Repository Structure in CLAUDE.md.
   Include __init__.py in every Python package directory.

2. Create config/settings.py — all constants from SPEC-SYS-001 through SPEC-SYS-005.
   Include: DATA_ROOT, DB_PATH, FEATURES_PATH, MODELS_PATH, LOGS_PATH, MIN_COMPLETE_STOCKS=450,
   SIGNAL_BUY_THRESHOLD=0.65, META_ACT_THRESHOLD=0.50, PND_BLOCK_THRESHOLD=60,
   PND_FLAG_THRESHOLD=40, EXIT_URGENT_THRESHOLD=80, EXIT_REDUCE_THRESHOLD=60,
   MAX_POSITION_PCT=0.10, MAX_SECTOR_PCT=0.40, TOTAL_ROUNDTRIP_COST=0.005.
   Use >= version pins in all requirements files (SPEC-LIB-001).

3. Create config/nse_holidays.py — a set of NSE trading holidays for 2025 and 2026.
   Include is_trading_day(date) function.

4. Create config/logging_config.py — structured logging with pipeline step timing.
   Include a @pipeline_step decorator that logs start time, end time, and any exception.

5. Create contracts/interfaces.py — abstract base classes:
   IModel (train, predict, predict_proba, save, load, get_shap_values)
   IClassificationModel(IModel), IRegimeModel(IModel), ISurvivalModel(IModel)
   IDataStoreReader, IDataStoreWriter. These enforce SPEC-SOLID-003 and SPEC-SOLID-004.

6. Create requirements/datastore.txt: fastapi>=0.115, uvicorn>=0.30, pydantic>=2.9,
   httpx>=0.27, duckdb>=1.2, sqlalchemy>=2.0, python-dotenv>=1.0, pyarrow>=16.0.
   Create requirements/phase1.txt: pandas>=2.2, numpy>=2.0, ta-lib>=0.5,
   lightgbm>=4.3, catboost>=1.2, xgboost>=2.1, scikit-learn>=1.5, hmmlearn>=0.3,
   mapie>=1.3, optuna>=3.6, imbalanced-learn>=0.12, shap>=0.45,
   lifelines>=0.30, scikit-survival>=0.23, ruptures>=1.1.9, hdbscan>=0.8.40,
   river>=0.22, fastapi>=0.115, uvicorn>=0.32, pydantic>=2.9, httpx>=0.27,
   APScheduler>=3.10, pytest>=8.0, pytest-cov>=5.0. Triple-barrier labeling
   is implemented natively; do not add mlfinlab.

7. Create .gitignore: include datastore/raw/, datastore/normalised/, datastore/features/,
   datastore/models/, .env, __pycache__, *.pyc, *.pkl, *.db, *.parquet.

8. Create .env.example with placeholders:
   FYERS_APP_ID=, FYERS_SECRET_ID=, FYERS_ACCESS_TOKEN=, SCREENER_EMAIL=,
   SCREENER_PASSWORD=, ORACLE_DB_URL=, ORACLE_STORAGE_NAMESPACE=.

Every file: module-level docstring stating purpose, SPEC-IDs implemented, phase.
Every function: docstring referencing SPEC-ID per SPEC-TRACE-002.
No file > 500 lines (SPEC-SOLID-001).
```

🔀 `git add . && git commit -m "feat(SPEC-SYS-001,SPEC-LIB-001): project skeleton, config, contracts, requirements"`

---

### P0-02 · DataStore Schema + DuckDB + SQLite Setup

```
Read CLAUDE.md and docs/12_platform_architecture.md (DataStore Architecture section).
Also read docs/03_data_pipeline.md.

Create the DataStore schema layer:

1. Create datastore/normalised/schema.py — DuckDB schema definitions for ALL tables:
   ohlcv_adjusted(date, ticker, open, high, low, close, volume, delivery_qty,
     delivery_pct, adj_factor, data_staleness_flag)
   corporate_actions(ticker, ex_date, action_type, ratio, announcement_date, record_date)
   fundamentals(ticker, fiscal_year, quarter, quarter_end_date, announcement_date,
     revenue, ebitda, pat, eps, operating_margin, ebitda_margin, net_margin,
     roe, roce, debt_to_equity, interest_coverage, fcf, asset_turnover,
     inventory_days, receivable_days, payable_days, book_value_per_share, shares_outstanding)
   shareholding(ticker, quarter_end_date, filing_date, promoter_pct, promoter_pledge,
     fii_pct, dii_pct, mf_pct, retail_pct)
   macro_indicators(date, indicator, value)
   stock_master(ticker, company_name, sector, industry, nse_series, listing_date,
     market_cap_cr, adtv_cr, current_tier, is_fno_eligible, is_nifty500)
   Note: use announcement_date for fundamentals PIT and filing_date for shareholding PIT.
   This enforces SPEC-PIPE-003. Add a CHECK constraint that rejects announcement_date <= quarter_end_date.

2. Create datastore/normalised/db.py — connection manager for DuckDB and SQLite.
   Functions: get_duckdb_conn(), get_sqlite_conn(db_name), close_all().
   Use context managers. Thread-safe for APScheduler.

3. Create datastore/signals/schema.py — SQLite schema for the signals store:
   ml_signals, ml_multibagger, ml_forensic, ta_signals, fa_signals, valuation_signals.
   (See Platform Architecture doc for full column lists.)

4. Create datastore/normalised/init_db.py — run this once to create all tables.
   Function: initialize_datastore() creates all tables if not exist.
   Idempotent: safe to run multiple times.

5. Create tests/unit/test_schema.py — verify:
   - All tables create without error
   - PIT constraint: inserting a fundamentals row where announcement_date < quarter_end_date raises an error
   - DuckDB AsOf join works for PIT queries (SPEC-DS-003)
   Every test docstring references its SPEC-ID (SPEC-TRACE-004).
```

✅ `pytest tests/unit/test_schema.py -v`

🔀 `git commit -m "feat(SPEC-DS-007,SPEC-PIPE-003): datastore schema, DuckDB/SQLite setup, PIT constraints"`

---

### P0-03 · DataStore API (FastAPI) — Phase 0 Endpoints

```
Read CLAUDE.md and docs/specs/API_SPEC.md.

Build the DataStore API (datastore/api/):

1. datastore/api/pit.py — PIT enforcement layer (SPEC-DS-003):
   pit_query(table, ticker, as_of_date) returns latest row where date_col <= as_of_date.
   Raise ValueError if as_of_date is in the future.

2. datastore/api/routers/ohlcv.py — GET /api/v1/ohlcv/{ticker}, GET /api/v1/ohlcv/{ticker}/latest
   GET /api/v1/ohlcv/universe?tier=1&date=YYYY-MM-DD.

3. datastore/api/routers/universe.py — GET /api/v1/universe/stocks, GET /api/v1/universe/tiers.

4. datastore/api/routers/system.py — GET /api/v1/system/health (returns pipeline status,
   last run date, stock count, DuckDB status).

5. datastore/api/schemas.py — Pydantic v2 models for all request/response types.

6. datastore/api/main.py — FastAPI app, mount all routers, startup event initializes DB.

7. datastore/client.py — DataStoreClient SDK that wraps all API calls with httpx.
   Consumer systems use ONLY this client — never raw httpx or direct DB (SPEC-SOLID-005).
   Methods: get_ohlcv(ticker, from_date, to_date), get_ohlcv_latest(ticker),
   get_universe(tier=None), post_ml_signal(data), get_health().

8. tests/integration/test_datastore_api.py — spin up the API with TestClient, verify:
   - /health returns 200
   - PIT query rejects future dates
   - OHLCV endpoint returns correct columns
   Every test: SPEC-DS-002 or SPEC-DS-003 in docstring.
```

✅ `pytest tests/integration/test_datastore_api.py -v`

🔀 `git commit -m "feat(SPEC-DS-002,SPEC-SOLID-005): datastore FastAPI, PIT layer, DataStoreClient SDK"`

---

### P0-04 · Scheduler + Checkpoint + Pipeline Skeleton

```
Read CLAUDE.md and docs/13_scheduler_resilience.md in full.

Build the scheduler and pipeline orchestration:

1. ingestion/scheduler/scheduler.py — APScheduler with SQLAlchemyJobStore (SPEC-SCHED-001).
   - Use SQLiteJobStore for job persistence (survives restarts)
   - misfire_grace_time=86400 (jobs missed during downtime re-fire on restart)
   - Three modes: linear (default), timestamp (specific time), manual (for testing)

2. ingestion/scheduler/checkpoint.py — checkpoint/resume engine (SPEC-SCHED-003):
   - checkpoint_write(step_name, date, status, metadata) saves to SQLite
   - checkpoint_read(date) returns last completed step for that date
   - On pipeline start: check if today's run is partially complete; resume from last checkpoint
   - On startup: detect gap between last successful run and today; enqueue missed dates oldest-first

3. ingestion/scheduler/gap_detector.py — gap detection (SPEC-SCHED-007):
   - get_missing_trading_dates(from_date, to_date) uses nse_holidays.py
   - Returns sorted list of trading dates with no completed pipeline run

4. ingestion/scheduler/daily_pipeline.py — pipeline orchestrator:
   Ordered steps: (1) bhavcopy, (2) option_chain, (3) macro, (4) validate_data_quality,
   (5) compute_features, (6) run_models, (7) generate_signals, (8) update_signals_store.
   Each step: wrapped with @pipeline_step decorator, checkpoint written after success.
   Completeness gate: abort step 5 if < 450 stocks complete (SPEC-SYS-003).

5. tests/unit/test_scheduler.py — verify:
   - Checkpoint write/read roundtrip
   - Gap detector finds correct missing dates for a known holiday period
   - Pipeline aborts if completeness gate fails (mock < 450 stocks)
   All test docstrings reference SPEC-SCHED-001, SPEC-SCHED-003, SPEC-SCHED-007.
```

✅ `pytest tests/unit/test_scheduler.py -v`

🔀 `git commit -m "feat(SPEC-SCHED-001,SPEC-SCHED-003,SPEC-SCHED-007): APScheduler, checkpoint-resume, gap detector, pipeline skeleton"`

---

### P0-05 · OHLCV Scraper + Historical Backfill + Corporate Actions

```
Read CLAUDE.md and docs/03_data_pipeline.md (SPEC-PIPE-001, SPEC-PIPE-002).

Build the data ingestion layer:

1. ingestion/scrapers/bhavcopy.py — NSE bhavcopy daily scraper (SPEC-PIPE-001):
   download_bhavcopy(date) → DataFrame with columns: ticker, open, high, low, close,
   volume, traded_qty, delivery_qty, series.
   Raises ConnectionError after 3 retries. Raises ValueError if < 450 stocks in result.
   Writes to datastore/raw/bhavcopy/YYYY-MM-DD.csv (raw, unmodified).
   Then normalises and upserts into ohlcv_adjusted table via DataStoreClient.

2. ingestion/scrapers/fyers_backfill.py — FYERS API historical OHLCV backfill:
   backfill_ticker(ticker, from_date, to_date) using fyers_apiv3.
   Rate limiting: max 1000 calls/hour with exponential backoff.
   Run for all 500 Nifty 500 tickers with 5-year history.
   Delivery qty: source from NSE bhavcopy archive (FYERS does not provide delivery).

3. ingestion/adjust/price_adjuster.py — corporate action engine (SPEC-PIPE-002):
   apply_corporate_actions(ticker) — retroactively adjusts all historical prices.
   SPLIT: pre-ex prices × ratio. BONUS: pre-ex prices / (1 + ratio).
   Must be idempotent. Post-adjustment: price gap at ex-date < 1%.
   Log: before_price, after_price, ratio, ex_date per adjustment.

4. ingestion/scrapers/macro.py — macro indicators (SPEC-PIPE-006):
   Scrape: India VIX (NSE), USD/INR + Crude + Gold (Yahoo Finance), FII/DII (NSE).
   Retry 3× on failure. Mark unavailable if scrape fails (non-critical — don't abort pipeline).

5. ingestion/quality/validator.py — data quality checks (SPEC-PIPE-005):
   validate_daily_data(date) checks: null rate per feature < 1%, delivery_pct in [0,100],
   price > 0, volume > 0. Returns {ok: bool, issues: list}.
   Completeness count: how many of 500 stocks have complete data.

6. tests/unit/test_bhavcopy.py — mock the NSE download, verify parsing and validation.
   tests/unit/test_price_adjuster.py — verify split/bonus/rights adjustments are correct.
   Include known-value tests: a 2:1 split should halve all pre-ex prices.
   All test docstrings: SPEC-PIPE-001, SPEC-PIPE-002.
```

✅ `pytest tests/unit/test_bhavcopy.py tests/unit/test_price_adjuster.py -v`

🔧 **Manual now:** Run `python ingestion/scrapers/fyers_backfill.py` to load 5yr OHLCV.
Expected runtime: 3–4 hours. Verify with:
`SELECT COUNT(*), MIN(date), MAX(date) FROM ohlcv_adjusted`
Result must be ≥ 600,000 rows.

🔀 `git commit -m "feat(SPEC-PIPE-001,SPEC-PIPE-002,SPEC-PIPE-006): bhavcopy scraper, FYERS backfill, corporate action adjuster, macro scraper, data validator"`

🔧 **Phase 0 Gate — verify manually before starting Phase 1:**
```bash
SELECT COUNT(*) FROM ohlcv_adjusted;           -- must be >= 600,000
SELECT COUNT(DISTINCT ticker) FROM ohlcv_adjusted; -- must be >= 490
SELECT MAX(date) FROM ohlcv_adjusted;           -- must be yesterday's date
python -c "from ingestion.scheduler.daily_pipeline import run_pipeline; run_pipeline('manual')"
# Pipeline must complete without errors
```

---

## PHASE 1 — Core Signal Engine
### Weeks 5–14 · Goal: Daily Buy/Hold/Sell signals, P&D protection, exit alerts

---

### P1-01 · Technical Features (76 core) + HMM Regime

```
Read CLAUDE.md and docs/01_features.md (Phase 1 features section).
Also read docs/02_models.md (M-01 HMM section).

Build the Phase 1 feature engineering layer:

1. features/technical.py — 76 core technical features (SPEC-FEAT-001 through SPEC-FEAT-004):
   CATEGORY 1 Price Position (8): pct_rank_5d, pct_rank_21d, pct_rank_63d,
     dist_from_52w_high, dist_from_52w_low, dist_from_sma200, dist_from_ema21,
     above_sma200_flag.
   CATEGORY 2 SMA Ratios (8): sma5/20, sma20/50, sma50/200, sma20/200,
     sma5/sma50, sma10/sma200, sma50_slope, sma200_slope.
   CATEGORY 3 EMA (4): ema8/21, ema13/34, ema21/55, ema_ribbon_bullish_flag.
   CATEGORY 4 Momentum (9): rsi_14, macd_hist, stoch_k, stoch_d, cci_20,
     williams_r, mfi_14, roc_21, roc_63.
   CATEGORY 5 Trend (8): adx_14, di_plus, di_minus, supertrend_direction,
     psar_position, linear_reg_slope_21, linear_reg_slope_63, trend_strength.
   CATEGORY 6 Volatility (5): atr_pct_14, bb_width_20, bb_position, keltner_position,
     vol_ratio_21_63.
   CATEGORY 7 Relative Strength (5): rs_vs_nifty50_21d, rs_vs_nifty50_63d,
     rs_vs_sector_21d, rs_vs_sector_63d, rs_rank_in_sector.
   CATEGORY 8 Momentum Scores (5): momentum_3m, momentum_6m, momentum_12m,
     momentum_24m, momentum_composite.
   CATEGORY 9 Volume/Delivery (5): vol_ratio_5_21, vol_ratio_1_5, delivery_pct,
     delivery_pct_ma5, delivery_pct_vs_ma20.
   CATEGORY 10 Ichimoku (5): tenkan, kijun, above_cloud, cloud_thickness, chikou_position.
   CATEGORY 11 Engineered (8): base_formation_flag, base_length_days, vol_compression_ratio,
     breakout_strength, gap_up_pct, intraday_reversal_score, close_position_in_range, 
     body_to_range_ratio.

   All features MUST be vectorised (no Python loops per stock — use pandas/numpy group operations).
   Use TA-Lib for indicators where available (SPEC-LIB-004).
   Sector RS features need a sector index DataFrame as input.

2. features/intraday.py — 8 intraday pattern features from daily OHLCV:
   gap_up_pct, gap_down_pct, intraday_reversal_score, upper_shadow_pct,
   lower_shadow_pct, body_to_range_ratio, close_position_in_range, opening_drive_strength.

3. features/calendar.py — 7 calendar features:
   month_sin, month_cos, dow_sin, dow_cos, is_expiry_week, days_to_expiry,
   quarter_start_flag.
   Use sin/cos encoding per SPEC-FEAT-003. No raw integers.

4. features/macro_features.py — 14 macro features from macro_indicators table:
   vix, vix_5d_change, usd_inr, usd_inr_5d_change, crude_oil, crude_5d_change,
   fii_net_5d, dii_net_5d, advance_decline_ratio, nifty50_1d_return,
   nifty50_5d_return, nifty50_21d_return, bond_yield_10yr, fii_net_21d.

5. features/matrix_builder.py — assembles full feature matrix (SPEC-PIPE-004):
   build_feature_matrix(date, tickers, db_path) → DataFrame (len(tickers) × N_features).
   Raises ValueError if complete_stocks < 450.
   Writes output as datastore/features/daily/YYYY-MM-DD.parquet.

6. models/hmm/regime_detector.py — M-01 HMM (SPEC-MODEL-009 not applicable; see 02_models.md):
   GaussianHMM with 4 states on 5 observables: daily_return, log_return, realized_vol,
   volume_ratio, atr_pct.
   Run on Nifty 50 (market-wide regime) AND per stock.
   Label states by mean return: highest=Bullish, lowest=Bearish, stable=Sideways, rest=Volatile.
   save_regimes(date, tickers) writes to ml_signals table via DataStoreClient.

7. tests/unit/test_features_technical.py:
   - All 76 features compute without NaN for a stock with 252+ days history
   - Vectorised: timing test on 500 stocks must complete in < 15 minutes
   - Delivery pct in [0, 100] (SPEC-PIPE-005)
   - sin/cos calendar encoding: month 1 and month 13 give same value
   All test docstrings: SPEC-FEAT-001 through SPEC-FEAT-004, SPEC-PIPE-004.

8. tests/unit/test_hmm.py:
   - 4 states produced
   - States labelled correctly by mean return sign
   - Regime output written to signals table
   Test docstring: M-01 HMM spec.
```

✅ `pytest tests/unit/test_features_technical.py tests/unit/test_hmm.py -v`

🔀 `git commit -m "feat(SPEC-FEAT-001,SPEC-FEAT-003,SPEC-FEAT-004,SPEC-PIPE-004): 76 core technical features, HMM regime detector, feature matrix builder"`

---

### P1-02 · P&D Features + P&D Detector Model

```
Read CLAUDE.md and docs/01_features.md (P&D features section).
Also read docs/02_models.md (M-06 P&D Detector section) and docs/specs/08_specifications.md
(SPEC-MODEL-006 section).

Build the P&D protection layer — CRITICAL: this must be correct before any signal model:

1. features/pnd_features.py — 22 P&D detection features:
   vol_spike_ratio (current volume / 20d average), vol_spike_5d_consecutive,
   price_spike_1d, price_spike_5d, delivery_pct_collapse (sudden drop > 30%),
   delivery_pct_trend_5d, upper_circuit_count_5d, lower_circuit_count_3d,
   bid_ask_pattern (if available), price_vs_52w_high_pct, turnover_spike_ratio,
   retailer_vol_proxy, promoter_pledge_change_1q, price_vol_divergence,
   circuit_proximity_upper, circuit_proximity_lower, abnormal_return_3d,
   abnormal_return_5d, peer_divergence_score, social_spike_proxy,
   institutional_exit_flag, pnd_composite_score_classical.
   All from daily OHLCV + governance data via DataStoreClient.

2. models/pnd/pnd_detector.py — M-06 P&D Detector (SPEC-MODEL-006):
   Class PnDDetector(IClassificationModel).
   Primary: LightGBM classifier on 22 features.
   Anomaly layer: IsolationForest (if LightGBM uncertain, anomaly score elevates final score).
   Class imbalance: SMOTETomek (SPEC-MODEL-004 — training data ONLY).
   predict_full(X) → DataFrame with: pnd_score (0–100), pnd_phase, pnd_block, pnd_flag.
   pnd_block=True when score > 60 (SPEC-MODEL-006 hard constraint).
   Implements save(), load() from IClassificationModel (SPEC-SOLID-003).

3. models/training/imbalance.py — resampling utilities:
   apply_smote_tomek(X_train, y_train) → resampled X, y.
   CRITICAL: Never accepts X_val or X_test — raises ValueError if called with validation data.

4. tests/unit/test_features_pnd.py — verify all 22 features compute for F&O-eligible stocks.
5. tests/regression/test_known_pnd.py — known P&D patterns must score > 60:
   - 8+ consecutive upper circuits → score > 60
   - Delivery collapse (90% → 15% in 5 days) → score > 40
   - SMOTE only applied on training split (assert on fit/transform call pattern)
   Test docstrings: SPEC-MODEL-006.
```

✅ `pytest tests/unit/test_features_pnd.py tests/regression/test_known_pnd.py -v`

🔀 `git commit -m "feat(SPEC-MODEL-006,SPEC-MODEL-004): P&D detection features, PnD detector model, SMOTE utilities"`

---

### P1-03 · Triple-Barrier Labeling + Walk-Forward Engine

```
Read CLAUDE.md and docs/04_backtesting.md in full.
Also read docs/specs/08_specifications.md (SPEC-MODEL-002, SPEC-MODEL-003, SPEC-BT-001 through SPEC-BT-004).

Build the labeling and backtesting infrastructure:

1. models/training/labeling.py — triple-barrier labels (SPEC-MODEL-002):
   compute_triple_barrier_labels(prices_df, horizon_days, atr_multiplier) using the
   native AlphaLens implementation.
   Barriers: 1.5×ATR (5d), 3×ATR (21d), 5×ATR (63d).
   Labels: +1 (upper hit first), -1 (lower hit first), 0 (vertical barrier hit).
   Validate: output only contains {-1, 0, 1}. No lookahead (labels end at horizon date).
   Exclude stocks where pnd_block=True from +1 labels (SPEC-MODEL-006).

2. backtest/costs.py — Indian transaction cost model (SPEC-BT-002):
   compute_round_trip_cost(price, quantity, adtv, stock_tier) → total_cost_pct.
   Components: brokerage (0.03%), STT sell (0.1%), exchange (0.00345%),
   GST (18% of brokerage+exchange), stamp duty (0.015% buy only),
   slippage (0.10% Tier1, 0.20% Tier2, 0.30% Tier3/4 per ADTV < ₹1Cr).
   Round-trip total target: ~0.40–0.50%.

3. backtest/engine.py — walk-forward backtester (SPEC-MODEL-003, SPEC-BT-001):
   WalkForwardBacktester class.
   Expanding training window, 1-year test window.
   Minimum 3 folds (Phase 1 gets 5 folds: Train 2021→Test 2022 ... Train 2021-25→Test 2026).
   Selection metric: walk-forward Sharpe, not accuracy.
   run_fold(fold_num) → FoldResult(cagr, sharpe, max_drawdown, win_rate, n_trades).

4. backtest/integrity_checker.py — all 9 integrity rules (SPEC-BT-001):
   check_all(backtest_config) raises IntegrityError on any violation.
   Rules: walk-forward only, PIT data, corp-action-adjusted, survivorship-unbiased,
   costs included, liquidity constraints, no HPO on test, fold stability (std_sharpe < 0.5),
   beat 4 benchmarks (Nifty, equal-weight-50, 6m-momentum, random-20).

5. tests/unit/test_labeling.py — verify triple-barrier logic with synthetic price series.
   test: labels are only {-1,0,1}. test: barrier hit order correct on known synthetic series.
   SPEC-MODEL-002 in docstrings.
6. tests/unit/test_costs.py — known cost components:
   Tier1, ₹100 stock, 100 shares → total round-trip ~0.45%. SPEC-BT-002 in docstring.
7. tests/integration/test_backtester.py — run backtester on synthetic signals, verify
   all 9 integrity checks run. SPEC-BT-001 in docstring.
```

✅ `pytest tests/unit/test_labeling.py tests/unit/test_costs.py tests/integration/test_backtester.py -v`

🔀 `git commit -m "feat(SPEC-MODEL-002,SPEC-MODEL-003,SPEC-BT-001,SPEC-BT-002): triple-barrier labeling, walk-forward backtester, 9 integrity rules, transaction costs"`

---

### P1-04 · Signal Models (5d + 21d) + Meta-Labeler + Conformal

```
Read CLAUDE.md and docs/02_models.md (M-02, M-03, M-04, M-05 sections).
Also read docs/specs/08_specifications.md (SPEC-MODEL-003, SPEC-MODEL-004, SPEC-MODEL-007).

Build the core signal model stack:

1. models/signal/base_signal.py — BaseSignalModel(IClassificationModel):
   Shared logic for all signal models: feature validation, threshold optimisation,
   SHAP computation, predict_signals() returning DataFrame with
   {signal_buy_prob, signal_hold_prob, signal_sell_prob, signal_q10, signal_q50, signal_q90}.
   Threshold optimised on validation fold — NEVER use 0.5 default (SPEC-MODEL-004).

2. models/signal/signal_5d.py — M-02 Signal 5d:
   LightGBM + CatBoost + XGBoost stacking ensemble.
   Stacking: logistic regression meta-learner on out-of-fold predictions.
   Quantile outputs via LightGBM quantile regression (Q10/Q50/Q90).
   Optuna HPO: 100 trials on validation fold only (SPEC-MODEL-003).

3. models/signal/signal_21d.py — M-03 Signal 21d (same architecture as 5d, different horizon).

4. models/signal/meta_labeler.py — M-04 Meta-Labeler:
   Input: primary signal prediction + features.
   Label: is this signal profitable after round-trip costs?
   Acts as a second layer of confidence — output is Act/Don't-Act probability.
   Only approve signals with meta_prob > 0.50 (META_ACT_THRESHOLD from settings).

5. models/uncertainty/conformal.py — M-05 Conformal Prediction (SPEC-MODEL-007):
   ACI (Adaptive Conformal Inference) variant, NOT standard CQR.
   Reason: ACI handles time-series non-exchangeability.
   Target: 90% coverage (α=0.10). Monthly validation of actual coverage on last 63 days.
   Alert if actual coverage < 85%.

6. models/training/walk_forward.py — wire signal models into WalkForwardBacktester.
   train_and_evaluate_signal(model_class, features, labels, n_folds) →
   List[FoldResult] + aggregated metrics.

7. tests/unit/test_signal_5d.py:
   - Model trains without error on synthetic data
   - Output DataFrame has correct columns (signal_buy_prob + signal_q10/50/90)
   - Threshold is NOT 0.5 after optimisation on a skewed dataset
   - SMOTE applied in training, NOT validation
   SPEC-MODEL-003, SPEC-MODEL-004 in docstrings.
8. tests/unit/test_conformal.py:
   - Coverage on held-out time-series >= 85%
   - Interval width decreases in high-confidence periods
   SPEC-MODEL-007 in docstring.
```

✅ `pytest tests/unit/test_signal_5d.py tests/unit/test_conformal.py -v`

🔀 `git commit -m "feat(SPEC-MODEL-003,SPEC-MODEL-004,SPEC-MODEL-007): signal 5d/21d, meta-labeler, conformal prediction (ACI variant)"`

---

### P1-05 · First Walk-Forward Backtest

```
Read CLAUDE.md and docs/04_backtesting.md (integrity rules section).

Run the first end-to-end walk-forward backtest and verify all 9 integrity rules:

1. Assemble the full Phase 1 feature matrix for 5 years of data.
   Call build_feature_matrix() for each trading date in 2021–2026.
   Expected output: 5 years × 500 stocks × 98 features.

2. Compute triple-barrier labels for all dates (5d horizon).

3. Run WalkForwardBacktester with Signal5d + MetaLabeler + PnDDetector.
   5 folds: 2021→2022, 2021-22→2023, 2021-23→2024, 2021-24→2025, 2021-25→2026.

4. Run IntegrityChecker.check_all() on results.
   ALL 9 rules must pass before proceeding. Fix any failures before continuing.

5. Run random feature test: train signal model with shuffled feature values.
   Shuffled accuracy must be < 55% (proves model learnt real signal, not noise).

6. Print and save backtest summary to datastore/outputs/backtest/signal_5d_phase1.json:
   Per fold: CAGR, Sharpe, MaxDD, WinRate, N_trades.
   Aggregated: mean_sharpe, std_sharpe, mean_cagr.
   4 benchmark comparisons.

If IntegrityChecker raises ANY error, stop and fix the violation before proceeding.
Common failures: (a) future data leaking through PIT misalignment — re-check
announcement_date vs quarter_end_date in the query; (b) std_sharpe > 0.5 — check
for regime-specific features leaking regime state into training.
```

✅ Check backtest output: `cat datastore/outputs/backtest/signal_5d_phase1.json`
✅ Verify: `std_sharpe < 0.5` and `mean_sharpe > 0.8` and all 9 integrity rules PASS.

🔀 `git commit -m "test(SPEC-BT-001,SPEC-MODEL-003): Phase 1 first walk-forward backtest, all 9 integrity rules pass"`

---

### P1-06 · Exit Signal Model + Position Monitor

```
Read CLAUDE.md and docs/02_models.md (M-07 Exit Signal section).
Also read docs/specs/08_specifications.md (SPEC-UI-004 Exit urgency display).

Build the exit signal model:

1. models/exit/exit_signal.py — M-07 Exit Signal:
   Class ExitSignalModel(ISurvivalModel).
   predict_exit(X, positions) → DataFrame with:
     exit_urgency (0–100), exit_type, exit_survival_5d, exit_survival_21d, exit_survival_63d.
   Exit types: thesis_broken, momentum_exhaustion, risk_management, target_achieved,
     opportunity_cost, pnd_exit. NEVER output a bare "Sell" without a type.
   exit_urgency > 80: immediate alert. 60-80: reduce 50%. 40-60: monitor.
   Components: LightGBM urgency regressor + CoxPH survival model (lifelines).
   Position-specific inputs: entry_price, days_held, unrealised_pnl.
   Counterfactual: what features would need to flip to hold instead?

2. models/exit/position_tracker.py:
   Position dataclass: ticker, entry_date, entry_price, qty, current_price, days_held.
   PositionTracker: add_position(), close_position(), get_all_positions().
   Persists to SQLite (survives restarts).

3. Wire ExitSignalModel into daily_pipeline.py step 5 (exit monitoring after signal generation).
   For each position in PositionTracker: compute exit_urgency + exit_type.
   Write results to ml_signals table via DataStoreClient.

4. tests/unit/test_exit_signal.py:
   - All 6 exit types produced in synthetic scenarios
   - exit_urgency > 80 when CoxPH survival_5d < 0.2
   - pnd_exit type triggered when pnd_score > 50 on held position
   SPEC-MODEL annotations in docstrings.
```

✅ `pytest tests/unit/test_exit_signal.py -v`

🔀 `git commit -m "feat(SPEC-MODEL-007 exit): exit signal model, 6 exit types, position tracker, CoxPH survival, exit urgency integration"`

---

### P1-07 · Model Registry + PSI Drift Monitor + Retrain Logic

```
Read CLAUDE.md and docs/specs/08_specifications.md (SPEC-MODEL-005, SPEC-MODEL-008,
SPEC-PIPE-005 PSI section).

Build model lifecycle management:

1. models/registry.py — Model Registry (SPEC-MODEL-005, SPEC-SOLID-002):
   ModelRegistry class. All models registered here — new models added WITHOUT modifying
   existing code (Open-Closed Principle).
   Methods: register(name, model_class), get_current(name), list_all(), promote(name, version).
   Storage: datastore/models/registry.json with keys:
     model_name, version (YYYYMMDD_fold), path, train_date, accuracy, sharpe, shap_top5.
   Previous 3 versions retained. Production model symlinked as {name}_current.pkl.

2. ingestion/quality/drift_monitor.py — PSI drift monitor (SPEC-PIPE-005):
   compute_psi(feature_name, current_dist, baseline_dist) → psi_score.
   run_daily_psi(feature_matrix, baseline_path) → Dict[feature_name, psi_score].
   Alerts: psi > 0.10 → log WARNING + write to alerts table.
   psi > 0.25 → log CRITICAL + write to alerts table + set drift_block=True
     (blocks new positions until resolved, per SPEC-PIPE-005).
   Baseline loaded from datastore/features/baseline/stats_baseline.pkl.

3. models/training/retrain_scheduler.py — retrain logic (SPEC-MODEL-008):
   shadow_test(new_model, current_model, X_test, y_test, positions) →
     {accuracy_win, calibration_win, shap_rank_win}.
   promote_if_wins_2_of_3(results) — promotes new model to production if ≥ 2/3 criteria win.
   Log comparison to registry.json.
   Retrain trigger: monthly schedule OR psi_score > 0.25 on top feature.

4. tests/unit/test_drift_monitor.py:
   - PSI = 0.0 on identical distributions
   - PSI > 0.25 on heavily shifted distributions triggers drift_block=True
   - baseline loaded correctly from .pkl file
   SPEC-PIPE-005 in docstrings.
5. tests/unit/test_model_registry.py:
   - New model registered without modifying existing registry entries (SPEC-SOLID-002)
   - Promote creates correct symlink
   SPEC-MODEL-005, SPEC-SOLID-002 in docstrings.
```

✅ `pytest tests/unit/test_drift_monitor.py tests/unit/test_model_registry.py -v`

🔀 `git commit -m "feat(SPEC-MODEL-005,SPEC-MODEL-008,SPEC-PIPE-005): model registry, PSI drift monitor, retrain shadow-test protocol"`

---

### P1-08 · DataStore API (Phase 1 Full) + AlphaLens.ML Dashboard

```
Read CLAUDE.md and docs/specs/API_SPEC.md. Also read docs/screens/SCREEN_INVENTORY.md.

Complete the Phase 1 DataStore API and wire up the AlphaLens.ML dashboard backend:

1. Add Phase 1 API routers to datastore/api/:
   routers/features.py: GET /api/v1/features/{ticker}/{date}, GET /api/v1/features/matrix/{date}
   routers/signals.py: GET /api/v1/signals/ml/{ticker}/{date},
     GET /api/v1/signals/ml/top_buys/{date},
     POST /api/v1/signals/ml (for ML engine write-back),
     GET /api/v1/signals/forensic/{ticker}, GET /api/v1/watchlist/current
   routers/alerts.py: GET /api/v1/alerts/today, GET /api/v1/alerts/history
   routers/portfolio.py: GET /api/v1/portfolio/positions, POST /api/v1/portfolio/positions

2. Update DataStoreClient SDK to expose all new endpoints.

3. systems/ml_signal_engine/inference/daily_inference.py:
   Reads features from DataStore API. Runs PnD → HMM → Signal5d → Signal21d →
   MetaLabeler → Conformal → ExitSignal in correct order.
   Writes ALL outputs to ml_signals table via DataStoreClient.
   Wired into daily_pipeline.py step 6.

4. datastore/events.py — file-based JSON lines event bus (SPEC-SOLID-002):
   emit(event_type, payload) appends JSON line to datastore/outputs/events/events.jsonl.
   Events: pnd_block_added, exit_urgent, model_retrained.
   subscribe(event_type, callback) for consumer systems to react.

5. Wire AlphaLens.ML backend to DataStore:
   systems/ml_signal_engine/api_writer.py writes all ML outputs via DataStoreClient.
   Read-back: verify written signals can be queried via GET /api/v1/signals/ml/top_buys/today.

6. tests/integration/test_full_pipeline.py — end-to-end:
   Run daily_pipeline for a single test date. Verify signals written to DataStore.
   Verify /api/v1/signals/ml/top_buys/{date} returns ≥ 3 results.
   SPEC-DS-002, SPEC-UI-001 in docstrings.
```

✅ `pytest tests/integration/test_full_pipeline.py -v --cov=. --cov-report=term-missing`
✅ Check coverage: must be ≥ 80% across Phase 1 modules.

🔧 **Start paper trading now:** Record all buy signals with entry price, date, confidence. Track daily.

🔀 `git commit -m "feat(SPEC-DS-002,SPEC-UI-001,SPEC-SOLID-002): Phase 1 DataStore API complete, ML inference pipeline, event bus, daily signals flowing"`

🔧 **Phase 1 Gate — verify before starting Phase 2:**
```bash
pytest tests/ -v --cov=. --cov-fail-under=80
python -c "
from datastore.client import DataStoreClient
c = DataStoreClient('http://localhost:8000')
signals = c.get_top_buys('today')
print(f'Signals today: {len(signals)}')
print(f'Health: {c.get_health()}')
"
# Paper trading: must have ≥ 2 weeks of tracked signals before Phase 2
```

---

## PHASE 2 — Fundamentals + Multibagger
### Weeks 15–26 · Cost: ₹14,400/yr · Goal: 63d signals, multibagger watchlist, forensic protection

🔧 **Manual prerequisites before P2-01:**
- Subscribe Screener.in Premium (₹4,999/yr) at screener.in/plans/
- Subscribe Trendlyne StratQ (₹5,900/yr)
- Subscribe Tijori Finance Pro (₹3,500/yr)
- Credentials in .env: SCREENER_EMAIL, SCREENER_PASSWORD, TRENDLYNE_TOKEN, TIJORI_TOKEN

---

### P2-01 · Fundamental Data + Governance Features + Shareholding Scraper

```
Read CLAUDE.md and docs/01_features.md (Phase 2 fundamental and governance features).
Also read docs/03_data_pipeline.md (SPEC-PIPE-003 PIT section — CRITICAL).

Build the fundamental data layer:

1. ingestion/scrapers/screener.py — Screener.in data scraper:
   scrape_fundamentals(ticker) → dict with 10 years of quarterly P&L, BS, CF data.
   Store with announcement_date column (CRITICAL: NOT quarter_end_date — SPEC-PIPE-003).
   Announce date: if not in filing, estimate as quarter_end + 45 days.
   Bulk export: use Screener Premium Excel export for all 500 stocks.
   Upsert into fundamentals table via DataStoreClient.

2. ingestion/scrapers/bse_shareholding.py — BSE shareholding scraper:
   Quarterly data for all 500 stocks: promoter_pct, promoter_pledge, fii_pct, dii_pct, mf_pct.
   Store with filing_date column (CRITICAL: NOT quarter_end_date — SPEC-PIPE-003).
   Source: BSE corporate filings API or Trendlyne shareholding data.

3. features/fundamental.py — 28 fundamental features (SPEC-FEAT-002 sector z-scores):
   GROWTH (6): revenue_growth_yoy, ebitda_growth_yoy, pat_growth_yoy, eps_growth_yoy,
     revenue_growth_qoq, eps_growth_qoq.
   PROFITABILITY (5): gross_margin, operating_margin, ebitda_margin, net_margin,
     margin_trend_4q (slope of last 4 quarters net margin).
   CAPITAL_EFFICIENCY (4): roe, roce, roa, asset_turnover.
   LEVERAGE (4): debt_to_equity, interest_coverage, debt_to_ebitda, fcf_to_debt.
   WORKING_CAPITAL (3): inventory_days, receivable_days, payable_days.
   VALUATION (6): pe, pb, peg (pe/eps_growth_yoy), ev_to_ebitda, mcap_to_sales,
     dividend_yield.
   All fundamental features: sector-relative z-score (SPEC-FEAT-002).
   Z-scores clipped to [-5, +5]. NaN where insufficient fundamental history.
   Staleness features: days_since_results, quarter_age_pct, results_pending_flag (SPEC-PIPE-003).

4. features/governance.py — 12 governance features:
   promoter_pct, promoter_pct_change_1q, promoter_pct_change_2q,
   promoter_pledge, promoter_pledge_change_1q, pledge_spiral_risk,
   fii_pct, fii_pct_change_1q, dii_pct, dii_pct_change_1q,
   mf_pct, mf_pct_change_1q.
   PIT: use filing_date for all shareholding lookups.

5. tests/unit/test_pit_alignment.py (CRITICAL):
   - fundamentals query with as_of=2024-01-15 returns only rows where announcement_date ≤ 2024-01-15
   - shareholding query with as_of=2024-01-15 returns only rows where filing_date ≤ 2024-01-15
   - Inserting a row with announcement_date < quarter_end_date raises IntegrityError
   - z-scores are in [-5, +5] range after clipping
   ALL of these MUST pass. PIT failure = data leak = corrupted model. SPEC-PIPE-003.
```

✅ `pytest tests/unit/test_pit_alignment.py -v` — MUST ALL PASS before continuing.

🔀 `git commit -m "feat(SPEC-PIPE-003,SPEC-FEAT-002): fundamental data scraper, PIT alignment enforced, 28 fundamental features, 12 governance features, sector z-scores"`

---

### P2-02 · MF Holdings + Corporate Action Features + AMFI Scraper

```
Read CLAUDE.md and docs/01_features.md (MF holdings and corporate action features).

Build the Phase 2 alternative data features:

1. ingestion/scrapers/amfi_holdings.py — monthly AMFI portfolio scraper:
   scrape_amfi_month(year_month) → DataFrame per scheme per stock.
   Availability rule: data available from ~5th of following month (SPEC-PIPE-003).
   Store in datastore/normalised/mf_holdings/YYYY-MM.parquet.
   Schedule: Oracle Cloud cron, 5th of each month at 08:00 IST.

2. features/mf_holdings.py — 12 MF holding features:
   mf_scheme_count (number of MF schemes holding), mf_scheme_count_change_1m,
   mf_total_holding_change_1m, mf_smallcap_fund_holding, mf_new_entry_count,
   mf_exit_count, mf_concentration_top5, mf_avg_holding_period,
   mf_sip_inflow_proxy, superstar_investor_flag, superstar_investor_change,
   mf_crowdedness_rank.
   Source: AMFI monthly + Trendlyne superstar investor data.
   PIT: available from ~5th of following month.

3. features/corporate_action_features.py — 10 corporate action event features:
   days_to_record_date (next record date for split/bonus/dividend),
   corp_action_anticipation_return, buyback_price_spread, buyback_acceptance_estimated,
   index_inclusion_days, ipo_lockin_expiry_proximity, ipo_listing_age_months,
   post_earnings_drift_signal, dividend_yield_vs_fd_rate, qip_dilution_impact.
   Source: NSE corporate actions table + Trendlyne events data.

4. tests/unit/test_mf_holdings.py — verify PIT availability rule (data before 5th is NaN),
   mf_crowdedness_rank is a percentile (0–100), superstar_investor_flag is binary.
   SPEC-PIPE-003 in docstring.
```

✅ `pytest tests/unit/test_mf_holdings.py -v`

🔀 `git commit -m "feat(SPEC-PIPE-003): AMFI monthly scraper, 12 MF holding features, 10 corporate action features"`

---

### P2-03 · F&O Features + Signal 63d + Retrain All Phase 2 Models

```
Read CLAUDE.md and docs/01_features.md (F&O features section) and
docs/02_models.md (Signal 63d section).
Also read docs/specs/08_specifications.md (SPEC-FEAT-004).

Build Phase 2 F&O features and retrain signal models with full feature set:

1. features/fno_features.py — 16 F&O derivative features (SPEC-FEAT-004):
   pcr_oi (put/call ratio by OI), pcr_volume, iv_atm, iv_skew, iv_term_structure,
   max_pain_distance, oi_buildup_call, oi_buildup_put, support_from_oi,
   resistance_from_oi, futures_basis, rollover_pct, fno_ban_flag,
   lot_size_normalised_oi, delivery_vs_fno_ratio, open_interest_change_pct.
   Source: FYERS Option Chain API (3:25 PM snapshot from Oracle Cloud).
   ONLY for ~250 F&O-eligible stocks. NaN for non-F&O stocks.
   LightGBM handles NaN natively (SPEC-FEAT-004 — no imputation needed).

2. Retrain Signal 5d with Phase 2 feature set (now 98 + 28 + 12 + 12 + 10 = 160 features).
   Retrain Signal 21d with same expanded feature set.
   Follow SPEC-MODEL-008 retrain protocol: shadow-test 63 days → promote if wins 2/3.

3. Build M-03b: models/signal/signal_63d.py — Signal 63d (new — needs fundamentals):
   Same ensemble as 5d/21d. Retrain trigger: new quarterly fundamental announcement.
   Horizon: 63 trading days. Barriers: 5× ATR.
   All 160 Phase 2 features + F&O features.

4. Rebuild feature_matrix_builder.py to assemble Phase 2 full matrix:
   98 technical + 28 fundamental + 12 governance + 12 MF + 10 corp_action + 16 F&O = 176 features.
   Note: F&O features are NaN for ~250 non-F&O stocks — this is correct.

5. Run Phase 2 walk-forward backtest on Signal 63d (5 folds, 2021–2026).
   Target: mean_sharpe > 1.0 (Phase 2 has more information → higher expected Sharpe).

6. tests/unit/test_fno_features.py:
   - F&O features are NaN for non-F&O stocks (no error raised)
   - PCR > 0 for F&O stocks on known trading dates
   SPEC-FEAT-004 in docstring.
```

✅ `pytest tests/unit/test_fno_features.py -v`
✅ Verify Signal 63d backtest: `cat datastore/outputs/backtest/signal_63d_phase2.json`

🔀 `git commit -m "feat(SPEC-FEAT-004,SPEC-MODEL-003): 16 F&O features, signal 63d, Phase 2 model retrain with 176-feature matrix"`

---

### P2-04 · Multibagger Model + Watchlist

```
Read CLAUDE.md and docs/02_models.md (M-08 Multibagger Model section).
Also read docs/01_features.md (multibagger-specific features section).

Build the multibagger detection system:

1. features/multibagger.py — 33 multibagger-specific features:
   BASE_FORMATION (6): base_length_days, base_depth_pct, vol_contraction_pct,
     tight_price_range_weeks, base_tightness_score, above_all_emas_flag.
   ACCUMULATION (7): delivery_accumulation_score, vol_pattern_updown_ratio,
     institutional_buying_proxy, price_vs_52w_high_breakout_days, reversal_count_in_base,
     mf_discovery_score, quiet_breakout_flag.
   RS_STRENGTH (5): rs_vs_sector_improving, rs_new_52w_high, outperforming_nifty_4q,
     sector_rotation_leader_flag, rs_rank_improving.
   TREND_QUALITY (5): ema_ribbon_aligned, supertrend_bullish, atr_expansion_on_breakout,
     volume_on_breakout_vs_avg, higher_highs_higher_lows_count.
   VOL_COMPRESSION (4): bb_width_percentile, keltner_squeeze, atr_percentile_126d,
     vol_compression_breakout_imminent.
   FUNDAMENTAL_QUALITY (6): roe_above_sector_median, roce_improving_4q,
     fcf_positive_streak, promoter_buying_flag, debt_reducing_flag, margin_expanding.

2. models/multibagger/multibagger_model.py — M-08:
   LightGBM lambdarank + CatBoost + Random Survival Forest ensemble.
   176 Phase 2 features + 33 multibagger-specific = 209 total.
   Two-tower fusion (simple concatenation for Phase 2).
   Runs WEEKLY (Monday only). Raises error if called on non-Monday.
   Output: mb_probability, mb_tier (2x/3x/5x/10x/none), mb_archetype,
     survival_6m/12m/24m/36m, shap_top5_json, analogues_json.
   Archetypes: long_base_breakout, post_crash_recovery, quiet_accumulator, sector_rotation_leader.
   P&D exclusion: stocks with pnd_block=True NEVER in positive multibagger labels.
   Weekly top-20 watchlist: write to DataStore watchlist table.
   Add new DataStore API endpoint: GET /api/v1/watchlist/current.

3. tests/unit/test_multibagger.py:
   - Survival curve values in [0,1]
   - All 4 archetypes can be assigned
   - P&D-blocked stocks not in top-20 watchlist
   - Raises error if run on non-Monday
   M-08 spec in docstrings.
4. tests/regression/test_multibagger_analogues.py (HITL-03, must run manually):
   Print top-20 watchlist with archetypes and analogues.
   Manually verify that each archetype label matches the stock's price pattern.
   Document result in tests/regression/hitl_results.md.
```

✅ `pytest tests/unit/test_multibagger.py -v`

🔧 **Manual HITL-03:** Run `pytest tests/regression/test_multibagger_analogues.py -v -s`
Inspect printed top-20 list. Each archetype label must make sense.
Document pass/fail in `tests/regression/hitl_results.md`.

🔀 `git commit -m "feat(M-08): multibagger model, 33 multibagger features, weekly watchlist, survival curves, 4 archetypes, HITL-03 documented"`

---

### P2-05 · Classical Forensic Scores + Forensic ML Model

```
Read CLAUDE.md and docs/02_models.md (M-09, M-10 sections).
Also read docs/specs/08_specifications.md (SPEC-MODEL-009, SPEC-MODEL-010).
Also read docs/Forensic_Accounting_ML_Specification.md in the uploaded docs if available.

Build the forensic accounting fraud detection layer:

1. models/forensic/classical_scores.py — M-09 Classical Forensic (SPEC-MODEL-009):
   No training. Pure formula computation from quarterly fundamentals.
   compute_beneish_m(ticker, as_of) → float (threshold: -1.78).
   compute_altman_z(ticker, as_of) → float (distress zone: < 1.81).
   compute_piotroski_f(ticker, as_of) → int 0–9 (strong: ≥ 7).
   compute_ohlson_o(ticker, as_of) → float.
   compute_dechow_f(ticker, as_of) → float.
   compute_sloan_accrual(ticker, as_of) → float.
   compute_benford_mad(ticker, as_of) → float (non-conforming: > 0.015).
   compute_all(ticker, as_of) → dict of all 7 scores + composite flag.

2. models/forensic/forensic_ml.py — M-10 ML Forensic (SPEC-MODEL-010):
   LightGBM + XGBoost + IsolationForest ensemble.
   84 features across 9 groups (Groups A–I from forensic spec).
   12 sector-specific sub-models for: BFSI, IT, Pharma, FMCG, Auto, Infra, Metals,
     Chemicals, Telecom, Power, Real_Estate, General.
   Training data: confirmed Indian fraud cases:
     Satyam, DHFL, IL&FS, Yes Bank, PC Jeweller, Vakrangee, Manpasand,
     Bhushan Steel, Kingfisher Airlines, CG Power, Karvy.
   4-layer composite: classical (20%) + ML fraud (40%) + anomaly (20%) + governance (20%).
   Output: forensic_composite (0–100), forensic_flag (green/amber/orange/red/black).

3. Wire forensic scoring into daily/quarterly pipeline.
   Classical scores: run quarterly (after each quarterly result season).
   ML model: run quarterly with fresh fundamentals.
   Write all forensic outputs to ml_forensic table via DataStoreClient.
   Add DataStore API endpoint: GET /api/v1/signals/forensic/{ticker}.

4. tests/regression/test_known_frauds.py (CRITICAL — SPEC-MODEL-010):
   Use pre-fraud financials from known fraud cases.
   Satyam (2008 Q3 financials) → forensic_flag must be 'amber' or 'red'.
   Beneish M-Score of -0.82 (above -1.78 threshold) → flagged.
   Piotroski F-Score ≤ 3 → flagged.
   Nifty 50 false positive rate: ≤ 2/50 stocks get 'red' flag.

5. tests/unit/test_classical_scores.py:
   - Beneish M-Score formula matches published reference values
   - Benford MAD = 0.0 for perfectly conforming digit distribution
   - Benford MAD > 0.015 for synthetic non-conforming data
   SPEC-MODEL-009 in docstrings.
```

✅ `pytest tests/unit/test_classical_scores.py tests/regression/test_known_frauds.py -v`
✅ Verify Nifty 50 false positive rate: ≤ 2 red flags among Nifty 50 constituents.

🔀 `git commit -m "feat(SPEC-MODEL-009,SPEC-MODEL-010): Beneish/Altman/Piotroski/Benford classical scores, ML forensic ensemble, 12 sector sub-models, known fraud regression tests"`

---

### P2-06 · Trendlyne + Tijori Integration + Phase 2 DataStore Expansion

```
Read CLAUDE.md and docs/12_platform_architecture.md (DataStore API section).

Complete Phase 2 data integration and API expansion:

1. ingestion/scrapers/trendlyne.py:
   scrape_superstar_portfolio(investor_name) — Dolly Khanna, Vijay Kedia, Ashish Kacholia,
   Sunil Singhania, Radhakishan Damani, Nikhil Kamath.
   Store in shareholding table with filing_date.
   Update superstar_investor_flag and superstar_investor_change features.

2. ingestion/scrapers/tijori.py:
   scrape_operational_metrics(ticker, sector) — sector-specific operational KPIs:
   BFSI: GNPA, NNPA, CASA, NIM, CAR. IT: attrition, utilisation, TCV.
   Pharma: ANDA count, USFDA observations. Auto: capacity utilisation.
   Store in fundamentals table as additional columns (nullable for non-relevant sectors).

3. Add Phase 2 DataStore API routers:
   routers/fundamentals.py: GET /api/v1/fundamentals/{ticker}?as_of=,
     GET /api/v1/fundamentals/{ticker}/history?quarters=8,
     GET /api/v1/fundamentals/{ticker}/staleness.
   routers/governance.py: GET /api/v1/governance/{ticker}?as_of=,
     GET /api/v1/governance/{ticker}/pledge_history.
   routers/macro.py: GET /api/v1/macro/{indicator}?from=&to=,
     GET /api/v1/macro/regime.
   All endpoints: PIT enforcement via pit.py (SPEC-DS-003).

4. Update DataStoreClient SDK with all new Phase 2 endpoints.

5. Run Phase 2 complete backtest: all 3 signal models (5d, 21d, 63d) + multibagger.
   Target Phase 2 gate: mean_sharpe > 1.0 for all 3 signal models.

6. tests/integration/test_phase2_pipeline.py:
   End-to-end: run pipeline for one test date. Verify fundamental features use
   announcement_date (not quarter_end_date). Verify forensic scores written.
   Verify multibagger watchlist has ≥ 10 entries.
   SPEC-PIPE-003, SPEC-DS-003 in docstrings.
```

✅ `pytest tests/ -v --cov=. --cov-fail-under=80`
✅ Verify Phase 2 backtest: `cat datastore/outputs/backtest/phase2_summary.json`

🔀 `git commit -m "feat(SPEC-DS-002,SPEC-DS-003,SPEC-PIPE-003): Trendlyne/Tijori integration, Phase 2 DataStore API complete, PIT enforced on all fundamentals endpoints"`

🔧 **Phase 2 Gate — verify before Phase 3:**
```bash
# All tests pass, coverage >= 80%
pytest tests/ -v --cov=. --cov-fail-under=80

# Multibagger watchlist running weekly
python -c "from models.multibagger.multibagger_model import run_weekly_scan; run_weekly_scan()"

# Forensic scores for 500 stocks
python -c "
from datastore.client import DataStoreClient
c = DataStoreClient('http://localhost:8000')
# Check worst forensic score - should not be trivially 0
top5 = c.get_forensic_top_risk(n=5)
print(top5)
"

# Paper trading: must have >= 3 months of tracked signals before Phase 3 deep learning
```

---

## PHASE 3 — Deep Learning Ensemble + TA System + Damodaran Valuation
### Weeks 27–38 · Goal: Deep learning ensemble (≥0.1 Sharpe gain), 2 new consumer systems

🔧 **Install Phase 3 dependencies first:**
```bash
pip install torch>=2.3 pytorch-forecasting>=1.0 pytorch-tabnet>=4.1
pip install mamba-ssm  # Ubuntu/Linux only — may need CUDA
pip install PyWavelets>=1.7 ruptures>=1.4
pip install -r requirements/phase3.txt
```

---

### P3-01 · Phase 3 Advanced Features (62 new features)

```
Read CLAUDE.md and docs/01_features.md (Phase 3 features section).

Build all 62 Phase 3 features:

1. features/wavelet.py — 4 wavelet decomposition features:
   Using PyWavelets (pywt library). Decompose price series into approximation + details.
   wavelet_approx_slope (low-frequency trend), wavelet_detail1_energy,
   wavelet_detail2_energy, wavelet_noise_ratio.

2. features/complexity.py — 7 entropy and Hurst features:
   hurst_exponent (R/S method), approximate_entropy, sample_entropy,
   lempel_ziv_complexity, fractal_dimension, permutation_entropy, spectral_entropy.
   Use ruptures library for change-point detection.

3. features/patterns.py — 6 technical pattern scores (0.0–1.0 confidence):
   head_shoulders_score, inverse_head_shoulders_score, double_bottom_score,
   double_top_score, cup_handle_score, flag_pennant_score.
   Implement using price series segmentation (ruptures) + geometric validation.

4. features/real_economy.py — 10 real economy macro features:
   gst_collection_growth (monthly GST from government portal),
   pmi_manufacturing (S&P Global India PMI),
   pmi_services, iip_growth (Index of Industrial Production from MoSPI),
   auto_monthly_sales_growth (SIAM data), cement_dispatches_growth,
   power_consumption_growth (POSOCO daily data — only daily real-economy feature),
   rail_freight_growth, upi_transaction_growth, bank_credit_growth.
   Note: these are monthly/daily — forward-fill to daily in feature matrix.

5. features/forensic_deep.py — 24 deep forensic features (Groups B–H from forensic spec):
   Group B: cfo_to_net_income, cfo_net_income_divergence, accrual_ratio,
     accrual_ratio_change, cash_flow_variability, capex_to_cfo_ratio.
   Group C: receivable_days_change, unbilled_revenue_ratio, cash_revenue_ratio (8 total).
   Group D: goodwill_ratio, cwip_ratio, contingent_liability_ratio, subsidiary_count (12 total).
   Group E: (from existing governance features — already built) (15 total).
   (Remaining groups build on Group F Benford which exists, G distress which exists.)

6. Update features/matrix_builder.py to include all 62 Phase 3 features.
   New total: 176 + 62 = 238 features (excluding F&O NaN for non-eligible stocks).

7. tests/unit/test_wavelet.py — wavelet features are finite and in reasonable range.
   tests/unit/test_patterns.py — pattern scores in [0.0, 1.0].
   SPEC references in all docstrings.
```

✅ `pytest tests/unit/test_wavelet.py tests/unit/test_patterns.py -v`

🔀 `git commit -m "feat(Phase3-features): 62 Phase 3 features — wavelet, entropy, pattern scores, real economy macro, deep forensic Groups B-H"`

---

### P3-02 · Deep Learning Models (TFT + BiLSTM + Mamba-2)

```
Read CLAUDE.md and docs/02_models.md (M-11, M-12 sections).
Also read docs/05_ml_algorithms.md.

Build the deep learning signal models:

1. models/deep/tft_model.py — M-11 TFT via pytorch-forecasting:
   TemporalFusionTransformer with:
   - Sequence length: 63 trading days
   - Static covariates: sector, tier, is_fno_eligible
   - Time-varying known: calendar features, macro
   - Time-varying unknown: all 238 technical + fundamental features
   - Quantile output: [0.1, 0.5, 0.9]
   Implements IClassificationModel from contracts/interfaces.py.
   Schedule first training run as overnight job (expect 4–8 hours on Ryzen 5).
   Validate: attention maps show interpretable temporal patterns (not uniform).

2. models/deep/bilstm_model.py — M-12 BiLSTM:
   Bidirectional LSTM, 2 layers, 128 hidden units.
   Input: sequence of 63 days × 238 features.
   Output: Buy/Hold/Sell probability.
   Mamba-2 sequence module: replace LSTM with Mamba-2 SSM if mamba-ssm installs cleanly.
   If Mamba-2 fails on hardware, keep BiLSTM only — document the limitation.
   Implements IClassificationModel.

3. models/deep/stacking.py — M-13 Stacking Meta-Learner:
   Logistic regression meta-learner on out-of-fold predictions from:
   Signal5d, Signal21d, TFT, BiLSTM (Phase 3 ensemble).
   validate: all 4 base models must have weight ≥ 0.1 in final ensemble.
   If any model has weight < 0.1, log a warning and investigate underperformance.

4. Run Phase 3 walk-forward backtest with full ensemble.
   Target: Sharpe improvement ≥ 0.1 vs Phase 2 LightGBM alone.
   If improvement < 0.1: deep learning models add complexity without payoff.
   Document result honestly in datastore/outputs/backtest/phase3_dl_comparison.json.

5. tests/unit/test_tft.py — TFT forward pass produces correct output shape.
   tests/unit/test_stacking.py — all 4 models have weight >= 0.1 on synthetic data.
   M-11, M-12, M-13 spec references in docstrings.
```

✅ `pytest tests/unit/test_tft.py tests/unit/test_stacking.py -v`
✅ Verify: `cat datastore/outputs/backtest/phase3_dl_comparison.json` — Sharpe gain ≥ 0.1

🔧 **Note:** TFT training will take several hours. Run overnight:
`nohup python -m models.deep.tft_model --train --folds=5 &`

🔀 `git commit -m "feat(M-11,M-12,M-13): TFT, BiLSTM+Mamba-2, stacking meta-learner, Phase 3 ensemble walk-forward backtest"`

---

### P3-03 · Technical Analysis System (Consumer System 1)

```
Read CLAUDE.md and docs/12_platform_architecture.md (System 2: Technical Analysis section).
Also read docs/15_future_applications.md (APP-1 Technical Analysis section).
Also read docs/screens/SCREEN_INVENTORY.md.

Build AlphaLens.Technical as a standalone consumer system:

1. Create systems/technical_analysis/ directory structure.
   This system reads ONLY from DataStore API via DataStoreClient. No direct DB access.

2. systems/technical_analysis/charts/chart_engine.py:
   build_chart_data(ticker, from_date, to_date) → dict suitable for frontend charting.
   Includes: OHLCV, indicator overlays (SMA, EMA, RSI, MACD, BB from feature store),
   HMM regime background colour (green=bullish, red=bearish from ml_signals).
   Reads via: DataStoreClient.get_ohlcv(), DataStoreClient.get_features(),
   DataStoreClient.get_ml_signal().

3. systems/technical_analysis/patterns/pattern_detector.py:
   detect_patterns(ticker, as_of) → List[PatternResult(name, confidence, target, stop)].
   Uses pattern scores from features/patterns.py (already in feature store).
   Also detects: support_resistance levels from price cluster analysis.
   Writes to ta_signals table via DataStoreClient.post_ta_signal().

4. systems/technical_analysis/screener/ta_screener.py:
   run_screener(strategy_id, universe_tickers) → List[ScreenerResult].
   42 pre-built strategy templates from docs/15_future_applications.md SPEC-TA-005.
   Each template reads criteria from feature store (no re-computation).
   Must complete for 500 stocks in < 5 seconds (features already computed).

5. systems/technical_analysis/api_writer.py:
   Writes ta_signals to DataStore: date, ticker, pattern_name, pattern_score,
   support_level, resistance_level, trend_direction, trend_strength,
   ta_buy_signal, ta_sell_signal.

6. Add DataStore API endpoint: GET /api/v1/signals/ta/{ticker}/{date}.
   Update DataStoreClient.get_ta_signal().

7. Wire ta_signals into ML Signal Engine as Phase 3 features (read via DataStoreClient):
   pattern_score → feature in multibagger model (cup_handle reinforces long_base_breakout).

8. tests/integration/test_ta_system.py:
   - Pattern detector finds ≥ 1 pattern for RELIANCE over 1-year lookback
   - Screener returns results in < 5 seconds for 500 stocks
   - ta_signals written to DataStore and readable via API
   SPEC-TA-001 through SPEC-TA-008 in docstrings.
```

✅ `pytest tests/integration/test_ta_system.py -v`

🔀 `git commit -m "feat(APP-1,SPEC-TA-001-008): AlphaLens.Technical — chart engine, pattern detector, 42-template screener, TA signals to DataStore, cross-system wiring"`

---

### P3-04 · Damodaran Valuation System (Consumer System 2)

```
Read CLAUDE.md and docs/12_platform_architecture.md (System 3: Damodaran section).
Also read docs/15_future_applications.md (APP-3 Damodaran Valuation section).

Build AlphaLens.Valuation as a standalone consumer system:

1. Create systems/damodaran_valuation/ directory structure.
   Reads ONLY from DataStore API via DataStoreClient. No direct DB access.

2. systems/damodaran_valuation/lifecycle/classifier.py:
   classify_lifecycle(ticker, as_of) → stage from:
   YOUNG_GROWTH, HIGH_GROWTH, MATURE_GROWTH, MATURE_STABLE, DECLINING, DISTRESSED,
   FINANCIAL_SERVICES.
   Rules from SPEC-VAL-001 in docs/15_future_applications.md.

3. systems/damodaran_valuation/dcf/dcf_engine.py:
   compute_dcf(ticker, as_of, stage) → DCFResult(intrinsic_value, wacc, terminal_value_pct,
     model_type, scenario_bull, scenario_base, scenario_bear).
   8 models selected by stage (SPEC-VAL-002): FCFF 2-stage, FCFF 3-stage, FCFE,
   Excess Return (banks), Relative/Regression, Option Pricing (Merton), Commodity Normalised,
   Monte Carlo.
   WACC computation (SPEC-VAL-003): risk_free = G-Sec yield minus India default spread.
   India default spread and ERP from Damodaran annual dataset (SPEC-VAL-009).
   Lambda adjustment for company-specific country risk.
   Synthetic rating for unrated companies (ICR → default spread table).
   India-specific adjustments (SPEC-VAL-004): cross-holdings, governance discount,
   conglomerate complexity.

4. systems/damodaran_valuation/api_writer.py:
   Writes valuation_signals to DataStore via DataStoreClient.

5. Add DataStore API endpoint: GET /api/v1/signals/valuation/{ticker}.
   Update DataStoreClient.get_valuation().

6. Wire valuation_gap_pct and margin_of_safety into ML Signal Engine as Phase 3 features.
   Retrain Signal 63d and Multibagger with these 2 new features.

7. ingestion/scrapers/damodaran_dataset.py — annual Damodaran dataset download (SPEC-VAL-009):
   Download 11 datasets from pages.stern.nyu.edu/~adamodar every January.
   Datasets: Betas, country risk premiums, WACC by industry, margins, ROE,
   reinvestment rates, EV/EBITDA, PE ratios, rating spreads, tax rates, div/FCFE.
   Store in datastore/normalised/damodaran/YYYY/ directory.
   Schedule: Oracle Cloud cron, January 15th each year.

8. tests/integration/test_valuation_system.py:
   - WACC is in [0.06, 0.25] range for Indian large-caps
   - Terminal value < 90% of total intrinsic value (SPEC-VAL-010 honest caveat)
   - Sensitivity table generated (±1% WACC × ±1% growth)
   - valuation_gap_pct written to DataStore and readable via API
   SPEC-VAL-001 through SPEC-VAL-010 in docstrings.
```

✅ `pytest tests/integration/test_valuation_system.py -v`

🔀 `git commit -m "feat(APP-3,SPEC-VAL-001-010): AlphaLens.Valuation — DCF engine, 8 Damodaran models, WACC with India adjustments, valuation signals to DataStore, cross-system wiring"`

🔧 **Phase 3 Gate — verify before Phase 4:**
```bash
pytest tests/ -v --cov=. --cov-fail-under=80
pip-audit  # SPEC-LIB-003: quarterly security audit

# Verify Phase 3 ensemble improvement
python -c "
import json
with open('datastore/outputs/backtest/phase3_dl_comparison.json') as f:
    r = json.load(f)
print(f'Phase 2 Sharpe: {r[\"phase2_sharpe\"]}')
print(f'Phase 3 Sharpe: {r[\"phase3_sharpe\"]}')
print(f'Improvement: {r[\"sharpe_delta\"]} (need >= 0.1)')
"

# Paper trading: must have >= 3 months before Phase 4 RL
```

---

## PHASE 4 — Fundamental Analysis System + RL Meta-Agent
### Weeks 39+ · Prerequisite: ≥ 3 months paper trading validated

🔧 **Prerequisite check — do this before P4-01:**
```bash
# Verify paper trading history exists
python -c "
from datastore.client import DataStoreClient
c = DataStoreClient('http://localhost:8000')
portfolio = c.get_portfolio_history()
days = len(set(p['date'] for p in portfolio))
print(f'Paper trading days tracked: {days} (need >= 63)')
assert days >= 63, 'Need 3+ months paper trading before Phase 4'
"
```

---

### P4-01 · Fundamental Analysis System (Consumer System 3)

```
Read CLAUDE.md and docs/12_platform_architecture.md (System 4: FA section).
Also read docs/15_future_applications.md (APP-2 Fundamental Analysis section).

Build AlphaLens.Fundamental as a standalone consumer system:

1. Create systems/fundamental_analysis/ directory structure.
   Reads ONLY from DataStore API via DataStoreClient. No direct DB access.

2. systems/fundamental_analysis/quality/quality_scorer.py:
   compute_quality_score(ticker, as_of) → quality_score (0–100).
   Based on: ROE, ROCE, FCF conversion, debt trend, margin stability, working capital.
   Sector-relative scoring (not absolute).

3. systems/fundamental_analysis/sector/ — 12 sector-specific modules:
   bfsi.py: GNPA, NNPA, CASA, NIM, CAR, cost-to-income, provision coverage.
   it_services.py: utilisation, attrition, TCV, client concentration, digital mix %.
   pharma.py: ANDA count, USFDA observations, R&D/revenue, chronic/acute mix.
   fmcg.py: volume/price split, rural %, distribution reach, ad spend.
   auto.py: capacity utilisation, EV %, dealer inventory days.
   infra.py: order_book/revenue, execution rate, debtor days.
   metals.py: EBITDA/tonne, realisation/tonne, utilisation.
   chemicals.py: specialty %, customer concentration, import substitution score.
   telecom.py: ARPU, churn, data usage/subscriber.
   power.py: PLF, T&D loss %, renewable mix %.
   real_estate.py: pre-sales, collections efficiency, unsold months.
   insurance.py: combined ratio, solvency, VNB margin.

4. systems/fundamental_analysis/management/mgmt_scorer.py:
   compute_management_quality_score(ticker, as_of) → score (0–100).
   Based on: promoter trend, pledge, RPT intensity, auditor continuity, board independence,
   capital allocation history, guidance accuracy.

5. systems/fundamental_analysis/api_writer.py:
   Writes fa_signals: date, ticker, quality_score, growth_score, mgmt_quality_score,
   sector_rank, fa_rating.
   POST to DataStore via DataStoreClient.

6. Wire fa_signals into ML Signal Engine:
   quality_score, mgmt_quality_score → additional features for Multibagger model.

7. tests/integration/test_fa_system.py:
   - Quality score in [0, 100] for Nifty 50 stocks
   - BFSI module computes NIM for known banking stocks
   - fa_signals written to DataStore and readable via API
   SPEC-FA-001 through SPEC-FA-008 in docstrings.
```

✅ `pytest tests/integration/test_fa_system.py -v`

🔀 `git commit -m "feat(APP-2,SPEC-FA-001-008): AlphaLens.Fundamental — 12 sector modules, quality/management scoring, FA signals to DataStore, cross-system wiring"`

---

### P4-02 · RL Meta-Agent (only after paper trading validation)

```
Read CLAUDE.md and docs/02_models.md (M-15 RL Agent section).
PREREQUISITE: Paper trading history >= 63 days must exist (confirmed in prerequisite check).

Build the RL Meta-Agent in 5 bootstrapping stages — do not skip any:

STAGE 1 — Supervised baseline experience:
1. backtest/rl_experience.py:
   Convert Phase 1–3 walk-forward backtest history into experience tuples.
   Each tuple: (state_30d, action, reward, next_state, done).
   state_30d: 30-dimensional vector (all model probabilities + portfolio state + regime + drift).
   action: 0=Strong_Buy, 1=Buy, 2=Hold, 3=Reduce, 4=Exit.
   reward: sharpe-adjusted return, penalised for drawdown and turnover.
   Target: >= 500,000 experience tuples from Phase 1–3 backtest history.

STAGE 2 — Offline PPO training:
2. models/rl/trading_env.py — Gymnasium trading environment:
   ObservationSpace: 30-dim state vector.
   ActionSpace: Discrete(5) — Strong_Buy, Buy, Hold, Reduce, Exit.
   reset() loads random date from experience replay. step() executes action.
   Reward: sharpe_delta - 0.1*turnover - 0.5*max_drawdown_breach.

3. models/rl/ppo_agent.py — PPO agent via stable-baselines3:
   Custom policy network: 2-layer MLP, 256 hidden units.
   5 regime-conditioned sub-policies (Bull, Bear, Sideways, HighVol, Transition).
   Train: 1M steps on offline experience buffer (Stage 1 tuples).

STAGE 3 — Synthetic scenario augmentation:
4. models/rl/scenario_augmentor.py:
   Generate synthetic market scenarios: 2008-style crash, 2020 COVID, 2021 bull.
   Each scenario: 252 trading days of synthetic feature data + realistic shocks.
   Add 100K synthetic tuples to replay buffer.
   Retrain PPO on augmented buffer.

STAGE 4 — Paper trading validation (manual phase):
5. models/rl/paper_trader.py:
   Run RL agent in shadow mode: observe all live signals, propose actions,
   compare vs supervised ensemble decisions.
   Track: RL divergence rate, position sizing recommendations, regime response.
   Run for >= 63 trading days before Stage 5.
   Log all RL decisions to datastore/outputs/rl_paper_trading.jsonl.

STAGE 5 — Live deployment with safety caps:
6. models/rl/safety_guardrails.py:
   MAX_POSITION_PCT = 0.10 (from settings.py — hard cap).
   MAX_SECTOR_PCT = 0.40 (hard cap).
   MIN_ADT_INR = 1_000_000 (liquidity floor).
   Any RL action that violates these → override with safe action + log override.
   Output: final_action (RL or overridden), override_flag, override_reason.

7. tests/unit/test_rl_environment.py:
   - Observation space matches 30-dim state vector
   - Action space has exactly 5 actions
   - Safety guardrails reject MAX_POSITION_PCT breach
   - Reward is negative for large drawdowns
   M-15 spec in docstrings.
```

✅ `pytest tests/unit/test_rl_environment.py -v`

🔧 **Manual Stage 4:** Run `python -m models.rl.paper_trader --shadow-mode --days=63`
Monitor for 63 trading days. Review divergence log weekly.
Document result in `datastore/outputs/rl_shadow_validation_report.md`.

🔀 `git commit -m "feat(M-15): RL PPO agent, 5-stage bootstrapping, Gymnasium trading env, safety guardrails, shadow paper trading started"`

---

### P4-03 · Final Integration — All 4 Consumer Systems on DataStore

```
Read CLAUDE.md and docs/12_platform_architecture.md (Cross-system signal flow section).

Verify and wire the complete platform:

1. Test all 4 consumer systems read from DataStore via DataStoreClient only.
   grep -r "import duckdb\|import sqlite3" systems/ — must return 0 results.
   All systems use DataStoreClient exclusively (SPEC-SOLID-005).

2. Verify full signal flow (wire where not yet connected):
   TA signals → ta_buy_signal, ta_sell_signal → ML feature in Multibagger (Phase 3+)
   FA signals → quality_score, mgmt_quality_score → ML feature in Multibagger (Phase 4)
   Valuation signals → valuation_gap_pct, margin_of_safety → ML feature in Signal 63d
   ML forensic → forensic_composite → FA management quality overlay
   ML signals → hmm_regime → TA chart background colour

3. Run final end-to-end integration test: simulate one complete trading week.
   For each of 5 trading dates: pipeline runs, all 4 systems compute outputs,
   all outputs written to DataStore, all endpoints return correct data.

4. Run pip-audit (SPEC-LIB-003) — fix any critical CVEs before marking Phase 4 complete.

5. tests/integration/test_full_platform.py:
   - All 4 consumer systems start without error
   - No system accesses database directly (only via DataStoreClient)
   - All cross-system signal flows verified
   - DataStore health endpoint returns status=ok
   - Pipeline completes in < 90 minutes (SPEC-SYS-002)
   SPEC-SOLID-005, SPEC-DS-002 in docstrings.
```

✅ `pytest tests/ -v --cov=. --cov-report=html`
✅ `pip-audit` — must show 0 critical CVEs

🔧 **Final RTM check (SPEC-TRACE-001):**
```bash
# Verify all 80 SPEC-IDs have passing tests
python scripts/check_rtm.py  # create this script to grep SPEC-IDs in passing tests
```

🔀 `git commit -m "feat(SPEC-SOLID-005,SPEC-DS-002): full platform integration, all 4 consumer systems on DataStore, cross-system signal flows verified, pip-audit clean"`

---

## MAINTENANCE PROMPTS (ongoing, quarterly)

### MQ-01 · Quarterly Retrain Check

```
Read CLAUDE.md. Run the quarterly model health check:

1. Run PSI check on all features: identify any PSI > 0.10 since last check.
2. For each model with PSI > 0.10 on its top feature: initiate SPEC-MODEL-008 retrain.
3. Run pip-audit (SPEC-LIB-003). Fix any critical CVEs.
4. Check walk-forward Sharpe on last 3 months of paper trading vs backtest expectation.
5. If Sharpe gap > 0.3: add to retrain queue.
6. Run SPEC-MODEL-007 conformal coverage check: is actual 90d coverage >= 85%?
7. Update registry.json with new metrics for all production models.
8. Commit: git commit -m "maint: Q{N} model retrain, PSI check, pip-audit clean"
```

### MQ-02 · Annual Damodaran Dataset Refresh

```
Read CLAUDE.md. Run the annual Damodaran dataset update (January each year):

1. Run ingestion/scrapers/damodaran_dataset.py — download 11 datasets from NYU.
2. Verify new ERP, default spread, and beta tables are loaded.
3. Retrain Damodaran valuation models with new dataset.
4. Verify WACC calculations are still in [0.06, 0.25] range for benchmark stocks.
5. Commit: git commit -m "maint(SPEC-VAL-009): annual Damodaran dataset refresh YYYY"
```

### MQ-03 · Universe Tier Review (quarterly)

```
Read CLAUDE.md and docs/specs/08_specifications.md (SPEC-SYS-001).

1. Run universe tier review: update stock_master tiers based on latest market cap and ADTV.
2. Identify tier promotions (new stocks entering Nifty 500 or crossing mcap thresholds).
3. For promoted stocks: trigger feature backfill for new tier coverage level.
4. For demoted stocks: reduce model coverage to new tier level.
5. Commit: git commit -m "maint(SPEC-SYS-001): Q{N} universe tier review, {N} promotions, {N} demotions"
```

---

## REFERENCE: Git Commit Format

All commits follow SPEC-TRACE-003:
```
feat(SPEC-ID[,SPEC-ID]): short description — for new features
fix(SPEC-ID): short description — for bug fixes
test(SPEC-ID): short description — for test additions
maint: short description — for maintenance (quarterly tasks)
refactor(SPEC-ID): short description — for refactoring without behaviour change
```

## REFERENCE: Test Run Commands

```bash
# Unit tests only (fast, < 2 min)
pytest tests/unit/ -v

# Integration tests (slower, needs DataStore running)
uvicorn datastore.api.main:app --port 8000 &
pytest tests/integration/ -v

# Regression tests (known fraud + known P&D patterns)
pytest tests/regression/ -v

# Full suite with coverage
pytest tests/ -v --cov=. --cov-report=term-missing --cov-fail-under=80

# Single spec check
pytest tests/ -k "SPEC-PIPE-003" -v

# Security audit (quarterly)
pip-audit
```

## REFERENCE: Phase Gate Summary

| Gate | Command | Criteria |
|------|---------|---------|
| Phase 0→1 | `pytest tests/unit/ -v` | All pass. Scrapers live. ≥600K OHLCV rows. |
| Phase 1→2 | `pytest tests/ --cov=. --cov-fail-under=80` | 80% coverage. Backtest integrity 9/9. |
| Phase 2→3 | `pytest tests/ -v` + paper trading check | 3+ months paper trading tracked. Forensic regression pass. |
| Phase 3→4 | `pip-audit` + Sharpe delta check | Deep learning adds ≥0.1 Sharpe. 0 critical CVEs. |
| Phase 4 final | Full RTM check | All 80 SPEC-IDs have passing tests. 4 systems on DataStore. |
