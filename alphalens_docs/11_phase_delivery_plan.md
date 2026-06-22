# AlphaLens Platform — Phase-Wise Delivery Plan
## From zero to full platform · Solo developer · AMD Ryzen 5 7535U · Ubuntu 22.04

**Last updated:** May 2026
**Hardware:** HP 15 (2026), AMD Ryzen 5 7535U 6-core, 16 GB DDR5, 512 GB NVMe SSD
**Total phases:** 5 (Phase 0 through Phase 4)

---

## Phase Summary

| Phase | Name | Weeks | Deliverable | Data Cost |
|-------|------|-------|-------------|-----------|
| 0 | Infrastructure & Data Foundation | 1–4 | Everything running, scrapers live, historical data loaded | ₹0 |
| 1 | Core Signal Engine | 5–14 | Daily Buy/Hold/Sell signals, P&D protection, exit alerts | ₹0 |
| 2 | Fundamentals + Multibagger | 15–26 | Multibagger watchlist, forensic scoring, 63d signals | ₹14,400/yr |
| 3 | Deep Learning Ensemble | 27–38 | TFT/BiLSTM ensemble, Damodaran valuation, TA system | ₹14,400/yr |
| 4 | Advanced & RL | 39+ | RL meta-agent, Fundamental Analysis System, full platform | ₹14,400/yr |

---

## Phase 0 — Infrastructure & Data Foundation
### Weeks 1–4 · Cost: ₹0 · Prerequisite for everything

**Goal:** Every piece of infrastructure is live and collecting data before a single model
is trained. Option chain data is non-recoverable — every missed day is gone permanently.

---

### Week 1 — Hardware, OS, Oracle Cloud

**Actions:**
- [ ] Boot Ubuntu 22.04 LTS on HP 15 laptop
  - Download Ubuntu 22.04.6 LTS ISO from ubuntu.com
  - Create bootable USB (use Rufus on Windows or `dd` on Linux)
  - Install alongside or replacing existing OS
  - Enable AMD Ryzen power settings: `sudo apt install ryzen-smu`
- [ ] Check RAM upgrade path
  - Open laptop back panel
  - Check if second SODIMM slot is empty
  - If yes: purchase 16 GB DDR5-4800 SODIMM (~₹4,500) and install → brings to 32 GB
- [ ] Install Miniconda + Python 3.11 environment
  ```bash
  wget https://repo.anaconda.com/miniconda/Miniconda3-latest-Linux-x86_64.sh
  bash Miniconda3-latest-Linux-x86_64.sh
  conda create -n alphalens python=3.11 -y
  conda activate alphalens
  ```
- [ ] Install Phase 0 libraries
  ```bash
  pip install pandas numpy pyarrow duckdb sqlalchemy requests beautifulsoup4
  pip install APScheduler python-dotenv pytest
  ```
- [ ] Create Oracle Cloud account at cloud.oracle.com
  - Use a personal email (not company)
  - Select home region: ap-mumbai-1 (first choice) or ap-hyderabad-1
  - Verify payment method (required even for free tier; no charges unless you upgrade)
- [ ] Provision Oracle A1 instance
  - Shape: VM.Standard.A1.Flex
  - OCPUs: 4, RAM: 24 GB (Always Free allocation)
  - Image: Ubuntu 22.04
  - If "Out of Host Capacity": retry at 2–4 AM IST or try ap-hyderabad-1
- [ ] Set up Oracle Object Storage bucket: `alphalens-raw-data` (20 GB free)
- [ ] Set up Oracle Autonomous Database (20 GB free) for cloud-accessible normalised data
- [ ] Install keep-alive script on Oracle instance to prevent idle reclamation
- [ ] Set up SSH key access between laptop and Oracle instance

**Gate:** SSH from laptop to Oracle instance works. Ubuntu running on laptop.

---

### Week 2 — Repository, Config, and Brokerage Accounts

**Actions:**
- [ ] Initialise git repository: `git init alphalens`
- [ ] Create project structure as per `CLAUDE.md` directory layout
- [ ] Create `.env` file with placeholder credentials (gitignored)
- [ ] Create `config/settings.py` with all constants
- [ ] Open FYERS demat account at fyers.in
  - Required for historical OHLCV backfill via FYERS API v3
  - Account opening: submit PAN, Aadhaar, bank details
  - Takes 2–3 business days for verification and activation
  - Cost: ₹0 (free account opening; no minimum balance)
- [ ] Create FYERS API app at myapi.fyers.in
  - Get App ID and Secret ID
  - Store in `.env`: `FYERS_APP_ID`, `FYERS_SECRET_ID`
- [ ] Install TA-Lib (official wheel, August 2025 onwards)
  ```bash
  pip install ta-lib
  ```
- [ ] Install Phase 1 ML libraries
  ```bash
  pip install lightgbm catboost xgboost hmmlearn scikit-learn duckdb
  pip install mapie optuna imbalanced-learn shap
  # mlfinlab intentionally omitted: not available on PyPI; triple-barrier
  # labeling is implemented natively per SPEC-MODEL-002
  pip install lifelines scikit-survival ruptures hdbscan river
  ```

**Gate:** FYERS account active. `pip install ta-lib` succeeds. Git repo created.

---

### Week 3 — Scrapers Live on Oracle + Historical Backfill

**Actions (on Oracle Cloud instance — must run every trading day):**
- [ ] Deploy option chain scraper: `pipeline/ingest/option_chain.py`
  - Fires at 15:25 IST every trading day
  - Writes to Oracle Object Storage: `alphalens-raw-data/option_chain/`
  - This is the most time-critical action in the entire project
  - Cron: `25 9 * * 1-5 python3 option_chain.py`  (09:55 UTC = 15:25 IST)
- [ ] Deploy NSE bhavcopy scraper: `pipeline/ingest/bhavcopy.py`
  - Fires at 16:05 IST every trading day
  - Cron: `35 10 * * 1-5 python3 bhavcopy.py`
- [ ] Deploy F&O bhavcopy scraper: `pipeline/ingest/fno.py`
  - Cron: `40 10 * * 1-5 python3 fno.py`
- [ ] Deploy FII/DII macro scraper: `pipeline/ingest/macro.py`
  - Cron: `30 12 * * 1-5 python3 macro.py`
- [ ] Deploy keep-alive: `*/3 * * * * python3 keep_alive.py`

**Actions (on laptop — one-time historical backfill):**
- [ ] Run FYERS backfill script for Nifty 500:
  - 5 years × 500 stocks × 366 days/request = ~7,000 API calls
  - Estimated runtime: 3–4 hours (respect FYERS rate limit: 1 lakh/day)
  - Output: `data/db/ohlcv.db` → `ohlcv_adjusted` table with 625,000+ rows
- [ ] Validate backfill: check price continuity, no phantom gaps at ex-dates
- [ ] Download historical NSE bhavcopy archive (for delivery data):
  - NSE maintains archives back to 1994
  - Download 5 years: `https://archives.nseindia.com/content/historical/EQUITIES/`
  - Parse delivery quantity into `ohlcv_adjusted` table (missing from FYERS)
- [ ] Download historical F&O bhavcopy (5 years) from NSE archives

**Gate:** Scrapers confirmed running on Oracle Cloud (verify day 1 collection).
Historical OHLCV with delivery data for 500 stocks loaded in SQLite.
`SELECT COUNT(*) FROM ohlcv_adjusted` returns ≥ 600,000 rows.

---

### Week 4 — Corporate Actions, Data Quality, Baseline

**Actions:**
- [ ] Download and load corporate actions ledger (splits, bonuses, rights: 5 years)
  - Source: NSE corporate actions archive
  - Apply retroactive adjustments to all historical OHLCV
  - Validation: price at ex-date has continuity < 1% gap (after adjustment)
- [ ] Download BSE shareholding patterns (5 years, quarterly)
  - Store in `data/db/shareholding.db`
  - Verify `filing_date` column populated (NOT `quarter_end_date` — PIT rule)
- [ ] Download macro data (5 years): VIX, USD/INR, Crude, FII/DII, advance/decline
  - Sources: NSE, Yahoo Finance, RBI website
- [ ] Set up daily cron on laptop for post-market pipeline (4:00 PM IST trigger)
  - Use `APScheduler` with `SQLAlchemyJobStore` (NOT `schedule` library)
  - Configure `misfire_grace_time=86400` so missed runs are detected on next startup
  - Build gap detection: on startup, compare last `pipeline_runs.date` vs today
  - Build catch-up mode: process all missed trading dates oldest-first before today's run
  - Test: shut laptop down for 2 days, restart, verify catch-up runs correctly
  - See SPEC-SCHED-001 through SPEC-SCHED-010 for full specification
- [ ] Compute baseline feature statistics for PSI drift detection
  - Run on first 2 years of data; save to `data/features/baseline/stats_baseline.pkl`
- [ ] Run full data quality validation across all loaded data
- [ ] Sync all normalised data to Oracle Autonomous DB (for multi-system access)

**Phase 0 Gate — All of these must be true before Phase 1 begins:**
- [ ] Scrapers: option chain, bhavcopy, F&O, macro all running on Oracle Cloud
- [ ] Historical data: 5yr OHLCV + delivery for 500 stocks in SQLite
- [ ] Corporate actions: applied and validated
- [ ] Oracle Cloud: instance live, storage provisioned, DB accessible
- [ ] Baseline stats: computed for PSI monitoring
- [ ] Daily pipeline skeleton: runs without errors (even if it just downloads data)

---

## Phase 1 — Core Signal Engine
### Weeks 5–14 · Cost: ₹0 · Goal: System usable for paper trading by Week 9

---

### Weeks 5–6 — Technical Features + HMM

**Deliverables:** 76 core technical features computing daily for all 500 stocks

**Actions:**
- [ ] Build `pipeline/features/technical.py`
  - Implement all 76 features using TA-Lib + numpy (vectorised — no stock loops)
  - Unit tests: `tests/unit/test_features_technical.py` — must pass before proceeding
  - Benchmark: 500 stocks in < 15 minutes on Ryzen 5 7535U
- [ ] Build `pipeline/features/intraday.py` — 8 intraday pattern features from OHLCV
- [ ] Build `pipeline/features/calendar.py` — 7 calendar features (pure date math)
- [ ] Build `pipeline/features/macro_features.py` — 14 macro features
- [ ] Build M-01: `models/hmm/regime_detector.py`
  - 4-state GaussianHMM on 5 observables
  - Run on Nifty 50 (market-wide) AND per-stock
  - Validate: 4 states correctly map to Bullish/Bearish/Sideways/Volatile by mean return
- [ ] Build PSI drift detector: `pipeline/quality/drift_monitor.py` — **build alongside first models**

**Check:** Run `pytest tests/unit/test_features_technical.py` — all pass.
Feature matrix for 500 stocks × 98 features generates in < 15 minutes.

---

### Weeks 7–8 — P&D Detector + Triple-Barrier Labeling

**Deliverables:** P&D pre-filter live. Triple-barrier labels computed for signal model training.

**Actions:**
- [ ] Build `pipeline/features/pnd_features.py` — 22 P&D detection features
- [ ] Build M-06: `models/pnd/pnd_detector.py`
  - LightGBM primary + IsolationForest anomaly layer
  - Class imbalance: SMOTETomek (1–3% positive rate)
  - Threshold optimisation on validation fold
  - Unit tests: `tests/unit/test_features_pnd.py`
  - Regression tests: `tests/regression/test_known_pnd.py`
  - Validate: hard block at score > 60 enforced in pipeline
- [ ] Build `models/training/labeling.py`
  - Native triple-barrier label construction per SPEC-MODEL-002
  - Validate: labels only contain {-1, 0, 1}; no labels beyond horizon date
- [ ] Build `models/training/imbalance.py`
  - SMOTE, SMOTETomek, threshold optimisation utilities
- [ ] Build `backtest/costs.py` — full Indian transaction cost model
  - Unit tests: `tests/unit/test_transaction_costs.py`

**Check:** P&D model correctly blocks test P&D patterns (see `test_known_pnd.py`).
Triple-barrier labels have reasonable class distribution (each class ≥ 5%).

---

### Weeks 9–10 — Signal Models + Walk-Forward Backtester

**Deliverables:** First working Buy/Hold/Sell signals. First backtest result.
**This is the first milestone where the system produces usable output.**

**Actions:**
- [ ] Build `models/training/walk_forward.py` — WalkForwardBacktester class
  - 5 folds: Train 2021 → Test 2022 ... Train 2021–25 → Test 2026
  - Integrity checker: all 9 rules validated automatically
  - Unit tests: `tests/integration/test_backtester.py`
- [ ] Build M-02: `models/signal/signal_5d.py`
  - LightGBM + CatBoost + XGBoost stacking
  - Quantile outputs Q10/Q50/Q90
  - Optuna HPO: 100 trials on validation fold only
- [ ] Build M-03: `models/signal/signal_21d.py`
- [ ] Build M-04: `models/signal/meta_labeler.py`
  - Labels: profitability after costs (NOT directional accuracy)
- [ ] Build M-05: `models/uncertainty/conformal.py`
  - MAPIE ≥1.3, ACI variant for time-series non-exchangeability
- [ ] Run first walk-forward backtest: Signal 5d + Meta-Labeler + P&D filter
- [ ] Run backtest integrity checks against all 9 rules

**Check — First backtest gate:**
- [ ] Fold Sharpe std < 0.5 (stability)
- [ ] Random feature test scores < 55% (no data leak)
- [ ] All 9 integrity rules pass
- [ ] Beat at least Nifty buy-hold benchmark

---

### Weeks 11–12 — Exit Signal + Position Sizing

**Actions:**
- [ ] Build M-07: `models/exit/exit_signal.py`
  - LightGBM urgency regression + CoxPH survival (`lifelines`)
  - 6 exit types including P&D Exit and Promoter Pledge Risk
  - Exit TYPE always surfaced to user — never a bare "Sell"
- [ ] Build `backtest/portfolio.py` — Portfolio class with full cost accounting
- [ ] Build rules-based position sizer (replaces RL until Phase 4)
- [ ] Build `models/registry.json` — model versioning and metadata

**Check:** Exit model generates 6 distinct exit types. CoxPH survival curve output present.

---

### Weeks 13–14 — DataStore API Layer + Phase 1 Dashboard

**Actions:**
- [ ] Build `datastore/api/` — REST API exposing normalised data
  - See Platform Architecture section for full API spec
  - Phase 1 endpoints: OHLCV, delivery, features, signals, regime
  - Framework: FastAPI (fast to build, automatic docs at /docs)
- [ ] Build basic Phase 1 dashboard (Screen A: Daily Dashboard)
  - Top 5 buy signals with probabilities + conformal intervals
  - P&D alerts and blocks
  - Exit urgency for held positions
  - Market regime summary
- [ ] Deploy PSI drift monitor alongside all models
- [ ] Set up model retrain scheduler (monthly for signal models)
- [ ] Begin paper trading: track all signals in a paper portfolio

**Phase 1 Gate:**
- [ ] Daily pipeline completes within 90 minutes post market close
- [ ] Buy/Hold/Sell signals generated for all 500 stocks daily
- [ ] P&D hard block at score > 60 confirmed working
- [ ] DataStore API accessible at localhost (Phase 1: local only)
- [ ] Paper trading started and being tracked

---

## Phase 2 — Fundamentals + Multibagger
### Weeks 15–26 · Cost: ₹14,400/yr · Goal: Multibagger watchlist + forensic protection

---

### Week 15 — Screener.in Subscription + Fundamental Data Load

**Actions:**
- [ ] **Subscribe to Screener.in Premium** — ₹4,999/year
  - URL: screener.in/plans/
  - Enables bulk export to Excel for all 500 stocks
  - Export quarterly P&L, balance sheet, cashflow for 5 years per stock
- [ ] Build `pipeline/ingest/screener.py`
  - Export or scrape 10-year quarterly data for all 500 Nifty 500 stocks
  - Store in `fundamentals.db` with `announcement_date` column (PIT-critical)
  - Validate: `announcement_date` is always 30–70 days after `quarter_end_date`
- [ ] Build `pipeline/features/fundamental.py` — 28 fundamental features
  - Sector-relative z-score normalisation mandatory
  - Staleness features: `days_since_results`, `quarter_age_pct`, `results_pending_flag`
- [ ] Build `pipeline/features/governance.py` — 12 governance features from BSE shareholding
- [ ] Unit tests: `tests/unit/test_pit_alignment.py` — all PIT tests must pass

**Check:** `SELECT announcement_date, quarter_end_date FROM fundamentals LIMIT 10`
confirms announcement_date is always later than quarter_end_date by 30–70 days.

---

### Week 16 — AMFI + MF Holdings + Corporate Action Features

**Actions:**
- [ ] Build `pipeline/ingest/amfi_holdings.py`
  - Monthly AMFI portfolio disclosures: scrape all 44 AMC scheme portfolios
  - Store in `data/mf_holdings/YYYY-MM.parquet`
  - Cron on Oracle Cloud: `0 8 5 * * python3 amfi_holdings.py` (5th of month)
- [ ] Build `pipeline/features/mf_holdings.py` — 12 MF holding features
  - `mf_scheme_count`, `mf_new_entry_count`, `superstar_investor_flag`, etc.
  - PIT rule: available from ~5th of following month
- [ ] Build `pipeline/features/corporate_action_features.py` — 10 features
  - `days_to_record_date`, `post_earnings_drift_signal`, `index_inclusion_days`, etc.

---

### Weeks 17–18 — F&O Features + Signal 63d

**Actions:**
- [ ] Build `pipeline/features/fno_features.py` — 16 F&O derivative features
  - PCR, IV, max pain, OI buildup patterns
  - Source: FYERS Option Chain API (replaces manual NSE scrape)
  - Only for ~250 F&O-eligible stocks; NaN for non-F&O stocks
- [ ] Build M-03b: `models/signal/signal_63d.py`
  - Now includes 28 fundamental + 12 governance + sector z-scores
  - Retrain trigger: new quarterly fundamentals announcement
- [ ] Retrain Signal 5d and 21d with expanded Phase 2 feature set

---

### Weeks 19–21 — Multibagger Model

**Actions:**
- [ ] Build `models/multibagger/multibagger_model.py`
  - LightGBM lambdarank + CatBoost + Random Survival Forest
  - 109 features: 76 core + 33 multibagger-specific + 28 fundamental + 12 governance
  - Two-tower architecture: concatenation (Option A — start here)
  - Runs weekly (Monday, after pipeline)
  - Survival curve output at 6/12/18/24/36/60 months
  - Top-20 watchlist generation
- [ ] Build `pipeline/features/multibagger.py` — 33 multibagger-specific features
  - Base formation (6), accumulation (7), RS (5), trend quality (5), vol compression (4), etc.
- [ ] Validate: P&D episodes excluded from positive multibagger labels
- [ ] Regression test: top-20 watchlist manually reviewed for archetype coherence (HITL-03)
- [ ] Add multibagger watchlist to DataStore API

---

### Weeks 22–24 — Forensic Scoring

**Actions:**
- [ ] Build M-09: `models/forensic/classical_scores.py` — pure formula, no training
  - Beneish M-Score, Altman Z, Piotroski F, Ohlson O, Dechow F, Sloan Accrual, Benford
  - Regression test: `tests/regression/test_known_frauds.py`
  - Validate: Satyam-equivalent financials score 'amber' or 'red'
- [ ] Build M-10: `models/forensic/forensic_ml.py`
  - LightGBM + XGBoost + IsolationForest + 12 sector-specific models
  - Trained on confirmed Indian fraud cases
  - Validate: ≤ 2/50 Nifty 50 stocks get 'red' flag (Nifty 50 should be mostly clean)
- [ ] Add forensic scores to DataStore API
- [ ] HITL-06: Forensic early warning retrospective test (run manually)

---

### Weeks 25–26 — Phase 2 Integration + Trendlyne + Tijori

**Actions:**
- [ ] **Subscribe to Trendlyne StratQ** — ₹5,900/year
  - Superstar investor tracking (Dolly Khanna, Vijay Kedia, Ashish Kacholia, etc.)
  - Corporate action calendar with more detail than NSE
  - Powers: `superstar_investor_flag`, `superstar_investor_change` features
- [ ] **Subscribe to Tijori Finance Pro** — ₹3,500/year
  - Operational metrics: market share, segment breakdowns, store counts, order books
  - Sector-specific features for BFSI, IT, Pharma, Auto, FMCG, Infra
  - Powers: Phase 2 sector-specific fundamental features
- [ ] Integrate Trendlyne and Tijori data into DataStore
- [ ] Run full Phase 2 walk-forward backtest
- [ ] HITL-01: Promoter pledge spiral test (run manually)
- [ ] HITL-02: P&D false positive discrimination test (run manually)
- [ ] Expand DataStore API with Phase 2 data endpoints

**Phase 2 Gate:**
- [ ] Multibagger top-20 watchlist generating weekly
- [ ] Forensic scores for all 500 stocks computed quarterly
- [ ] Signal 63d running with fundamentals (PIT validated)
- [ ] Phase 2 backtest: Mean Sharpe > 1.0 across all folds
- [ ] Annual data cost confirmed: ~₹14,400/yr (Screener + Trendlyne + Tijori)
- [ ] DataStore API exposing all Phase 1 + Phase 2 data
- [ ] Paper trading continuing: 3+ months of tracked signals

---

## Phase 3 — Deep Learning + Technical Analysis + Damodaran Valuation
### Weeks 27–38 · Cost: ₹14,400/yr (same subscriptions)
### Goal: Deep learning ensemble + two new consumer systems on the DataStore

---

### Weeks 27–29 — Advanced Features + Deep Learning Setup

**Actions:**
- [ ] Install Phase 3 libraries
  ```bash
  pip install torch pytorch-forecasting pytorch-tabnet
  pip install mamba-ssm  # Linux/Ubuntu only
  ```
- [ ] Build Phase 3 advanced technical features (62 features):
  - Wavelet decomposition (4): `ruptures` + `PyWavelets`
  - Fractional differentiation (3): custom implementation
  - Hurst exponent + entropy features (7)
  - Pattern recognition (6): head-shoulders, double-bottom, cup-handle scores
  - Real economy macro (10): GST, PMI, IIP, auto, cement, power, rail, UPI, bank credit
  - Deep forensic features (54): Groups B–H from forensic specification
- [ ] Build M-11: `models/deep/tft_model.py` — TFT via pytorch-forecasting
  - Schedule first TFT training as overnight run
  - Validate: attention maps show temporal structure
- [ ] Build M-12: `models/deep/bilstm_model.py` + Mamba-2

---

### Weeks 30–32 — Stacking Ensemble + TabNet Feature Selection

**Actions:**
- [ ] Build M-13: `models/deep/stacking.py`
  - Logistic regression meta-learner on out-of-fold predictions
  - Validate: all 5 base models have weight ≥ 0.1
- [ ] Build M-14: `models/training/feature_selection.py` — TabNet validator
  - Research tool only; prune features where both TabNet + SHAP agree unimportant
- [ ] Run full Phase 3 walk-forward backtest with ensemble
- [ ] HITL-05: SHAP explanation quality review
- [ ] HITL-04: Market regime transition validation

---

### Weeks 33–35 — Technical Analysis System (Consumer 1 on DataStore)

**This is a separate system, not part of the ML pipeline. It consumes the DataStore API.**

**Actions:**
- [ ] Design Technical Analysis System architecture
  - Consumes: DataStore OHLCV API, features API, signals API
  - Produces: Charting, pattern detection screens, manual TA tools
- [ ] Build `systems/technical_analysis/`
  - Chart rendering: candlestick + indicators overlay
  - Pattern detection: head-shoulders, support/resistance, trend lines
  - Custom screener: user-defined technical criteria on DataStore data
  - Alert system: price alerts, breakout alerts
  - No new data ingestion — all data from DataStore
- [ ] Expose TA System outputs back to DataStore (for cross-system signal fusion)

---

### Weeks 36–38 — Damodaran Valuation System (Consumer 2 on DataStore)

**This is a separate system, not part of the ML pipeline. It consumes the DataStore API.**

**Actions:**
- [ ] Design Damodaran Valuation System architecture
  - Consumes: DataStore fundamentals API, macro API, sector API
  - Produces: Intrinsic value estimates, valuation gaps, margin of safety
- [ ] Build `systems/damodaran_valuation/`
  - Lifecycle classifier: startup/growth/mature/declining
  - DCF engine: appropriate model per lifecycle stage
  - Relative valuation: sector peer comparison
  - Output: `intrinsic_value`, `valuation_gap_pct`, `margin_of_safety`
  - Valuation outputs exposed to DataStore for consumption by ML models
- [ ] Wire valuation features into Signal 63d and Multibagger models as additional inputs
  - `valuation_gap_pct` and `margin_of_safety` become Phase 3 ML features

**Phase 3 Gate:**
- [ ] Deep learning ensemble: Sharpe improvement ≥ 0.1 vs Phase 2 LightGBM
- [ ] Technical Analysis System live and consuming DataStore
- [ ] Damodaran valuation system producing intrinsic values for Tier 1 stocks
- [ ] Valuation features wired into ML models
- [ ] Full platform DataStore API documented

---

## Phase 4 — Fundamental Analysis System + RL Agent
### Weeks 39+ · Prerequisite: 3+ months paper trading from Phase 1–2 signals

---

### Weeks 39–44 — Fundamental Analysis System (Consumer 3 on DataStore)

**Actions:**
- [ ] Build `systems/fundamental_analysis/`
  - Consumes: DataStore fundamentals API, governance API, forensic API
  - Sector-specific analysis modules (BFSI, IT, Pharma, Auto, FMCG, Infra, etc.)
  - Management quality scoring
  - Credit rating change monitoring (CRISIL, ICRA, CARE integration)
  - Peer comparison engine
  - Investment thesis builder: generates structured buy/sell thesis per stock
  - NLP on earnings call transcripts (Phase 4 text analysis)
- [ ] Wire FA System outputs back to DataStore
- [ ] FA System signals become features for Multibagger and Signal 63d models

---

### Weeks 45+ — RL Meta-Agent (only after paper trading validation)

**Prerequisite check:**
- [ ] ≥ 3 months paper trading with live signals tracked
- [ ] All Phase 1–3 models stable and backtested
- [ ] 500K+ experience tuples generated from supervised model history

**Actions:**
- [ ] Install RL libraries: `pip install stable-baselines3 gymnasium`
- [ ] Build custom Gymnasium trading environment
- [ ] 5-stage RL bootstrapping (do not shortcut this sequence):
  1. Supervised baseline backtest → 500K+ experience tuples
  2. Offline PPO training on replay buffer
  3. Synthetic scenario augmentation (crash/boom/transition)
  4. Paper trading validation (3 months minimum)
  5. Live deployment with position size safety caps
- [ ] Build 5 regime-conditioned sub-policies (Bull/Bear/Sideways/HighVol/Transition)

**Phase 4 Gate:**
- [ ] Fundamental Analysis System live on DataStore
- [ ] RL agent validated in paper trading for 3+ months before any live use
- [ ] All 4 consumer systems operational: ML, TA, Valuation, FA
- [ ] DataStore is the single source of truth for all systems

---

## Cost Summary by Phase

| Phase | Weeks | Subscriptions | Hardware | Total One-time | Annual |
|-------|-------|---------------|----------|---------------|--------|
| 0 | 1–4 | None | RAM upgrade ₹4,500 (optional) | ₹4,500 | ₹0 |
| 1 | 5–14 | None | — | — | ₹0 |
| 2 | 15–26 | Screener ₹4,999 + Trendlyne ₹5,900 + Tijori ₹3,500 | — | — | ₹14,399 |
| 3 | 27–38 | Same subscriptions renew | — | — | ₹14,399 |
| 4 | 39+ | Same subscriptions renew | — | — | ₹14,399 |
| **Oracle Cloud** | All | Always Free tier | — | — | ₹0 |

**Total annual running cost from Phase 2 onwards: ~₹14,400/year (~₹1,200/month)**

---

## Critical Path (actions that block everything downstream)

1. **Day 1:** Oracle Cloud instance must be live and scrapers running.
   Option chain data cannot be backfilled. Every day missed is lost forever.

2. **Week 2:** FYERS account must be active before historical backfill.
   FYERS account takes 2–3 business days for verification.

3. **Week 8:** Triple-barrier labels must validate before signal model training.
   A labeling bug silently corrupts every model trained on it.

4. **Week 10:** First backtest integrity check. Do not proceed to Phase 2 if
   the random feature test scores above 55% — there is a data leak.

5. **Week 15:** Screener.in subscription must be active before fundamental features.
   Quarterly data download takes 1–2 days for 500 stocks.

6. **Phase 4 RL:** Do not start until the prerequisite check passes.
   Starting RL without 3 months of real signal history is the most common
   way to build an RL agent that overfits to backtested history.
