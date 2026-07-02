# AlphaLens — Claude Code Master Context
## Indian Equity ML System · Read this file before anything else

This file is the single source of truth for the AlphaLens project. Every Claude Code
session must read this file before writing any code. All other docs in this folder
provide deeper detail on specific subsystems.

---

## What This System Is

AlphaLens is a machine-learning-based Indian equity research and stock-screening system.
It ingests daily market data, computes 330 features, runs 16 ML models, and generates
Buy/Hold/Sell signals, multibagger watchlists, exit alerts, and fraud warnings.

**This is a decision-support tool, not an autonomous trading system.** All final
investment decisions remain with the human user. The system improves decision quality
and imposes systematic discipline — it does not replace judgment.

---

## Project Status

- **Phase:** Pre-development. Starting from zero.
- **Developer:** Solo developer
- **Hardware:** HP 15 (2026), AMD Ryzen 5 7535U 6-core, 16 GB DDR5, 512 GB NVMe SSD
- **OS:** Ubuntu 22.04 LTS preferred. Windows 11 with WSL2 acceptable.
- **Architecture:** Central DataStore with API layer. 4 consumer systems (ML, TA, Valuation, FA).
  See `12_platform_architecture.md` for full architecture. See `11_phase_delivery_plan.md`
  for week-by-week build plan.
- **Python:** 3.11 (pinned — do not use 3.12+ until all libraries are tested)
- **Cloud:** None currently (laptop-only — SPEC-SCHED-009). Oracle Cloud Free Tier
  was attempted for always-on scrapers but had no available ARM A1 capacity and the
  Free Trial account blocked region switching; deferred until Phase 2+, see
  `06_deployment.md` "Oracle Cloud (deferred)". All scraping AND model training run
  on the local laptop via `ingestion/scheduler/daily_pipeline.py`.

---

## Repository Structure (Refactored — Central DataStore)

```
alphalens_platform/
├── CLAUDE.md                         ← This file (always read first)
├── README.md
├── requirements/
│   ├── datastore.txt                 ← FastAPI, uvicorn, pydantic, httpx, duckdb
│   ├── phase1.txt                    ← Phase 1 ML dependencies
│   ├── phase2.txt                    ← Phase 2 additions
│   └── phase3.txt                    ← Phase 3 deep learning
├── config/
│   ├── settings.py                   ← All constants, paths, thresholds
│   ├── universe.py                   ← Stock universe and tier definitions
│   ├── profiles.py                   ← dev/test/production config profiles
│   ├── nse_holidays.py               ← NSE trading calendar
│   └── logging_config.py
│
├── contracts/                        ← INTERFACE DEFINITIONS (SOLID-I, SOLID-D)
│   ├── interfaces.py                 ← IModel, IClassificationModel, IRegimeModel, ISurvivalModel
│   ├── data_reader.py                ← IDataStoreReader
│   └── data_writer.py                ← IDataStoreWriter
│
├── datastore/                        ← THE CENTRAL DATA LAYER
│   ├── raw/                          ← Store 1: as-is from source (gitignored)
│   ├── normalised/                   ← Store 2: cleaned DuckDB + Parquet (gitignored)
│   │   ├── alphalens.duckdb           # Analytical (DuckDB)
│   │   ├── pipeline_log.db            # Transactional (SQLite)
│   │   └── scheduler.db               # APScheduler (SQLite)
│   │   └── mf_holdings/YYYY-MM.parquet
│   ├── features/                     ← Store 3: ML feature Parquets (gitignored)
│   │   ├── daily/YYYY-MM-DD.parquet
│   │   ├── baseline/stats_baseline.pkl
│   │   └── metadata/feature_catalog.json
│   ├── signals/signals.duckdb        # Analytical (DuckDB)            ← Store 4: all system outputs (gitignored)
│   ├── models/                       ← Store 5: model files + registry.json (gitignored)
│   ├── outputs/                      ← Store 6: UI-ready aggregated JSON
│   └── api/                          ← FastAPI DataStore API
│       ├── main.py
│       ├── routers/ (ohlcv, fundamentals, features, signals, etc.)
│       ├── schemas.py
│       ├── pit.py                    ← Point-in-time enforcement
│       └── db.py
│
├── ingestion/                        ← DATA COLLECTION LAYER
│   ├── scrapers/ (bhavcopy, fno, option_chain, screener, amfi, macro, etc.)
│   ├── adjust/price_adjuster.py
│   ├── quality/ (validator, drift_monitor)
│   └── scheduler/daily_pipeline.py
│
├── features/                         ← FEATURE COMPUTATION (writes to DataStore)
│   ├── registry.py                   ← FeatureDefinition + FEATURE_REGISTRY (SOLID-O)
│   ├── technical.py                  ← 76 core features
│   ├── fundamental.py                ← 28 fundamental features
│   ├── pnd_features.py               ← 22 P&D features
│   ├── multibagger.py                ← 33 multibagger features
│   ├── forensic_classical.py         ← 30 classical forensic scores
│   ├── (intraday, governance, mf_holdings, fno, macro, calendar, corporate_action)
│   └── matrix_builder.py             ← Assembles full matrix → DataStore
│
├── systems/                          ← CONSUMER SYSTEMS (all read from DataStore API)
│   ├── ml_signal_engine/             ← System 1 (Phase 1)
│   │   ├── models/ (hmm, signal, pnd, exit, multibagger, forensic, deep)
│   │   ├── training/ (walk_forward, labeling, imbalance, hyperparams)
│   │   ├── inference/daily_inference.py
│   │   └── api_writer.py
│   ├── technical_analysis/           ← System 2 (Phase 3)
│   ├── damodaran_valuation/          ← System 3 (Phase 3)
│   └── fundamental_analysis/         ← System 4 (Phase 4)
│
├── backtest/                         ← BACKTESTING (reads from DataStore)
│   ├── engine.py, portfolio.py, costs.py, metrics.py, integrity_checker.py
│
├── dashboard/                        ← UI LAYER (reads from DataStore API)
│   └── screens/ (daily, signal_detail, multibagger, forensic, backtest, ta, valuation)
│
├── tests/
│   ├── unit/, integration/, regression/, hitl/
│
└── docs/
    ├── 01_features.md through 10_hitl_tests.md
    ├── 11_phase_delivery_plan.md     ← Week-by-week build plan with all actions
    └── 12_platform_architecture.md   ← Central DataStore architecture
```

**Architecture principle:** One DataStore, many consumers. The DataStore API enforces
point-in-time correctness centrally. Every consumer system reads via the API and writes
its outputs back via the API. No system scrapes data independently or maintains private copies.
├── models/
│   ├── hmm/
│   │   └── regime_detector.py   ← HMM regime detection (M-01)
│   ├── signal/
│   │   ├── signal_5d.py         ← Signal model 5d (M-02)
│   │   ├── signal_21d.py        ← Signal model 21d (M-03)
│   │   ├── signal_63d.py        ← Signal model 63d (M-03)
│   │   └── meta_labeler.py      ← Meta-labeler (M-04)
│   ├── uncertainty/
│   │   └── conformal.py         ← Conformal prediction CQR/ACI (M-05)
│   ├── pnd/
│   │   └── pnd_detector.py      ← Pump & dump detector (M-06)
│   ├── exit/
│   │   └── exit_signal.py       ← Exit signal + survival (M-07)
│   ├── multibagger/
│   │   └── multibagger_model.py ← Multibagger detector (M-08)
│   ├── forensic/
│   │   ├── classical_scores.py  ← Classical forensic formulas (M-09)
│   │   └── forensic_ml.py       ← ML forensic ensemble (M-10)
│   ├── deep/                    ← Phase 3 only
│   │   ├── tft_model.py         ← Temporal Fusion Transformer (M-11)
│   │   ├── bilstm_model.py      ← BiLSTM (M-12)
│   │   └── stacking.py          ← Stacking meta-learner (M-13)
│   └── training/
│       ├── walk_forward.py      ← Walk-forward backtester
│       ├── labeling.py          ← Triple-barrier label construction
│       ├── imbalance.py         ← SMOTE, focal loss, threshold opt
│       └── hyperparams.py       ← Optuna HPO protocol
├── backtest/
│   ├── engine.py                ← WalkForwardBacktester class
│   ├── portfolio.py             ← Portfolio + transaction cost model
│   ├── costs.py                 ← Indian market transaction costs
│   ├── metrics.py               ← Sharpe, drawdown, fold stability
│   └── overfit_checks.py        ← DSR, random feature test, benchmarks
├── scheduler/
│   ├── daily_pipeline.py        ← Main daily pipeline runner
│   └── cron_setup.sh            ← Cron job setup script
└── tests/
    ├── test_features.py
    ├── test_models.py
    └── test_pipeline.py
```

---

## Absolute Rules (Never Violate These)

### Data integrity
1. **Point-in-time alignment is mandatory.** Fundamentals: use `announcement_date` not
   `quarter_end_date`. Shareholding: use `filing_date`. MF holdings: available ~5th of
   following month. Violating this creates lookahead bias and inflates all backtest results.
2. **All OHLCV must be corporate-action adjusted** before any feature is computed.
   Raw bhavcopy prices corrupt every momentum and moving-average feature.
3. **Survivorship bias**: include delisted stocks in all training data.
4. **Walk-forward validation only** — never random train/test split on time-series data.
5. **P&D detection runs before any buy signal reaches the user.** It is a pre-filter,
   not a post-filter. Score > 60 = hard block.
6. **No synthetic, mocked, or procedurally-generated data anywhere in the application,
   ever — and no fallback to it.** Every model trains on real data only (real OHLCV via
   `ohlcv_adjusted`, real fundamentals, real documented archive cases such as
   `KNOWN_FRAUD_ARCHIVE`/`KNOWN_PND_TICKERS`/`HISTORICAL_MULTIBAGGER_ARCHIVE`, or real
   paper-trading history). If real data is insufficient, the loader/script **raises**
   (`RuntimeError`/`FileNotFoundError`) with an actionable message pointing to
   `BuildLog.md`'s "Real data sourcing — X" section — it never substitutes a generated,
   jittered, or randomly-sampled stand-in. This applies to training data, inference
   inputs, and backtests alike. (Deterministic, hand-built unit-test fixtures that
   exercise a function's logic in isolation — e.g. a crafted OHLCV panel to test a
   specific boundary condition — are not "synthetic data" in this sense; they never
   ship in application code paths and must be clearly labeled as test fixtures, not as
   a stand-in for real market/training data.)

### Code standards
7. Every function must have a NumPy-style docstring: Parameters, Returns, Spec References, PIT Assumptions, Raises.
8. All database operations use parameterized queries — no string interpolation in SQL.
9. Feature computation is always vectorized (pandas/numpy) — no Python loops over stocks.
10. Every model file inherits from the appropriate interface in `contracts/interfaces.py` (IModel, IClassificationModel, IRegimeModel, ISurvivalModel).
11. All paths defined in `config/settings.py` — never hardcode paths in model files.
12. Every module-level docstring includes: Phase, Specs, Owner, Consumers.
13. Inline comments reference spec IDs for non-obvious business logic.

### SOLID Principles (mandatory — see 14_engineering_standards.md for details)
14. **S — Single Responsibility:** Each class does one thing. Each function does one thing. No "and" in the description.
15. **O — Open/Closed:** Add features by adding new files, not modifying existing code. New models inherit IModel. New features register in FEATURE_REGISTRY.
16. **L — Liskov Substitution:** Any IModel subclass can replace any other wherever IModel is expected.
17. **I — Interface Segregation:** Models implement only the interfaces they need (IModel, IClassificationModel, IExplainableModel, IRegimeModel, ISurvivalModel).
18. **D — Dependency Inversion:** High-level modules depend on abstractions (interfaces), not concrete implementations. Pipeline runner receives IModel, not Signal5dModel.

### Library governance
19. All library versions pinned in requirements/*.txt.
20. Upgrades: one library at a time, on a branch, full test suite + backtest comparison.
21. Reject upgrade if Sharpe drops > 0.05 or any test fails.
22. Prefer public libraries over custom implementations (see 14_engineering_standards.md Part 4).
23. Quarterly security audit via `pip-audit`.

### Traceability
24. Every spec ID (SPEC-XXX-NNN) has at least one test (see RTM in 14_engineering_standards.md).
25. Every test references the spec it validates in its docstring.
26. Every commit message references the spec(s) it implements.
27. Requirements Traceability Matrix maintained in 14_engineering_standards.md — 100% coverage required.

### Model training
28. Optuna HPO always runs on walk-forward validation folds, never on test data.
29. SMOTE applied to training data only — never validation or test data.
30. Classification threshold is always optimized on validation fold — never use 0.5 default.
31. Model retrain protocol: snapshot → train → shadow-test → compare → promote only if better.

---

## Technology Decisions (Final, Do Not Debate)

| Decision | Choice | Reason |
|----------|--------|--------|
| Primary ML | LightGBM 4.6 | Fastest on Intel CPU, best tabular performance |
| Ensemble partner | CatBoost 1.2 | Best categorical feature handling |
| Regime detection | hmmlearn GaussianHMM | Gold standard for financial regimes |
| Uncertainty | MAPIE >= 1.3 (ACI variant) | Time-series-aware conformal prediction |
| HPO | Optuna 4.7 | Bayesian TPE, walk-forward aware |
| Imbalance | imbalanced-learn SMOTE-Tomek | P&D and forensic models |
| Survival analysis | lifelines + scikit-survival | Exit timing and multibagger duration |
| Technical indicators | TA-Lib 0.6.8 | Official wheels now on PyPI |
| Feature store | Parquet (pyarrow) | DuckDB reads Parquet natively — zero-copy |
| Analytical DB | DuckDB >= 1.2.0 | Columnar, multi-core, AsOf joins for PIT |
| Transactional DB | SQLite (pipeline log, scheduler, checkpoints) | Frequent small writes |
| Scheduler | APScheduler + custom checkpoint engine | Persistent, resume-on-failure, unlimited backfill |
| Observability | Custom structured JSON logging | On by default, configurable off for production |
| Deep learning (Phase 3) | PyTorch + pytorch-forecasting | TFT and BiLSTM |
| SSM (Phase 3) | mamba-ssm 2.x (Ubuntu only) | Mamba-2 stable; skip Mamba-1 and Mamba-3 |
| Clustering | hdbscan | Automatic k, handles outliers |
| Changepoint | ruptures (PELT) | Fast, reliable |
| Drift monitoring | PSI (custom) + river ADWIN | Phase 1 PSI, Phase 2 ADWIN |
| Backtesting | vectorbt (Phase 1-2), Backtrader (Phase 3) | Speed vs realism tradeoff |
| OS | Ubuntu 22.04 LTS | All libraries work; no Windows WSL2 needed |
| Python version | 3.11 | Maximum library compatibility |

**Dropped from scope permanently:** GNN, VAE, Knowledge Distillation, DRHP/IPO module,
Damodaran valuation (deferred to Phase 3+), LLM alphas, ESG data, satellite data,
AlphaLens 55 strategies (subsumed by ML features), DQN (replaced by PPO), Random Forest
(dominated by gradient boosting), Bayesian Neural Networks (replaced by conformal prediction).

---

## Build Phases

### Phase 1 — Weeks 1–14 (Core Signal Engine)
**Goal:** System usable for paper trading by Week 8.

**Models to build:**
- M-01: HMM Regime Detection
- M-02: Signal 5d (LightGBM primary)
- M-03: Signal 21d (LightGBM primary)
- M-04: Meta-Labeler
- M-05: Conformal Prediction (MAPIE CQR)
- M-06: Pump & Dump Detector
- M-07: Exit Signal Model

**Features:** 98 features (76 core technical + 8 intraday + 7 calendar + 14 macro + P&D features)
**Data:** FYERS historical backfill + NSE bhavcopy daily + NSE VIX/FII data
**Cost:** ₹0 (all free data sources)

### Phase 2 — Weeks 12–24 (Fundamentals + Multibagger)
**Models to add:**
- M-03b: Signal 63d (adds fundamental features)
- M-08: Multibagger Detection Model
- M-09: Forensic Classical Scores
- M-10: Forensic ML Ensemble

**Features:** +170 (28 fundamental + 12 governance + 12 MF + 22 P&D + 16 F&O +
             10 corporate actions + 8 microstructure + 8 seasonal + 30 forensic classical)
**Data:** Add Screener.in Premium (₹4,999/yr) + BSE shareholding + AMFI
**Total annual cost:** ~₹14,400

### Phase 3 — Weeks 24–36 (Deep Learning Ensemble)
**Models to add:**
- M-11: Temporal Fusion Transformer (TFT)
- M-12: BiLSTM + Mamba-2
- M-13: Stacking Meta-Learner
- M-14: TabNet (feature selection validator only)

**Additional features:** +62 (54 deep forensic + 10 real economy macro + 20 advanced technical)
**Note:** Deep learning models train overnight on CPU. Consider GPU if available.

### Phase 4 — Week 36+ (RL Meta-Agent)
**Models to add:**
- M-15: PPO Reinforcement Learning Meta-Agent
- M-16: PSI + ADWIN drift monitors (build in Phase 1 but formalize in Phase 4)

**Prerequisite:** All Phase 1–3 models must be stable and paper-trading for 3+ months
before RL development begins. Use rules-based position sizing until then.

---

## Daily Pipeline Flow (no hardcoded times — 15-hour window)

The pipeline runs linearly when triggered (APScheduler recurring job, manual, or
on-startup catch-up — never OS-level cron, see `06_deployment.md` "Running the
Scheduler"). You have from 3:30 PM to 9:15 AM next day. Steps execute in order with
checkpointing. If a run fails at step 6, the next run resumes from step 6.

```
STEP 1  → Bhavcopy + F&O + macro download (or backfill from archives) — laptop-only,
          no separate Oracle sync step (SPEC-SCHED-009; Oracle Cloud deferred)
STEP 2  → Data validation (completeness gate: ≥ 450/500 stocks)
STEP 3  → Corporate action check + retroactive adjustment
STEP 4  → Insert adjusted OHLCV into DataStore (DuckDB)
STEP 5  → Compute technical features (76 core + 8 intraday + 7 calendar)
STEP 6  → Compute macro + P&D detection features
STEP 7  → Load quarterly fundamentals (PIT join via DuckDB ASOF)
STEP 8  → Assemble feature matrix → write Parquet
STEP 9  → Quality checks + PSI drift detection
STEP 10 → M-01: HMM regime detection (today only, not backfill)
STEP 11 → M-06: P&D pre-filter — blocks score > 60 (today only)
STEP 12 → M-02/03/04/05: Signal models + meta + conformal (today only)
STEP 13 → M-07: Exit signals for held positions (today only)
STEP 14 → Generate alerts + write outputs
STEP 15 → Check retrain schedule

Monday → M-08: Multibagger weekly scan (after daily pipeline)
```

Phase 0.6 implements a simplified subset of this flow as
`ingestion/scheduler/checkpoint.py`'s STEPS list (download_bhavcopy,
download_fno, download_macro, adjust_prices, compute_features, run_models,
write_signals) — the granular STEP 1-15 breakdown above is the target
shape once features/ and systems/ml_signal_engine/ are built out; today,
compute_features/run_models/write_signals are single checkpointed steps
that raise `NotImplementedError` until Phase 1 fills them in.

**On startup:** Gap detector runs first. If laptop was off for 10 days, it backfills
~7 trading days (steps 1–8 only, no model inference) before running today's pipeline.

---

## Key Constants (set in config/settings.py)

```python
# Universe (configurable — not hardcoded)
UNIVERSE_PROFILE = 'phase_1'     # Options: phase_1, phase_2, phase_3, full_nse
TIER_THRESHOLD = 2               # Include tiers ≤ this value
MIN_ADTV_CR = 5.0                # Minimum ADTV in crores
MIN_MCAP_CR = 500                # Minimum market cap in crores
# Universe resolved dynamically from stock_master table using above filters

# Scheduler
SCHEDULER_MODE = 'linear'        # 'linear' | 'timestamp' | 'manual'

# Observability
OBSERVABILITY_ENABLED = True     # Master switch
OBSERVABILITY_LEVEL = 'info'     # 'off' | 'error' | 'warning' | 'info' | 'debug'

# Model thresholds
SIGNAL_THRESHOLD = 0.65
META_THRESHOLD = 0.50
PND_BLOCK_THRESHOLD = 60
PND_FLAG_THRESHOLD = 40
EXIT_URGENT_THRESHOLD = 80
EXIT_REDUCE_THRESHOLD = 60

# Position sizing
MAX_POSITION_PCT = 0.10
MAX_SECTOR_PCT = 0.40
MIN_ADT_INR = 1_000_000
MAX_ORDER_VS_ADTV = 0.05

# Drift monitoring
PSI_MODERATE_THRESHOLD = 0.10
PSI_SEVERE_THRESHOLD = 0.25
MIN_MODEL_ACCURACY = 0.45

# Market regime
BEAR_REGIME_POSITION_SCALE = 0.50

# Transaction costs
TOTAL_ROUNDTRIP_COST = 0.005
```

---

## Data Sources Reference

| Source | Data | Cost | Phase |
|--------|------|------|-------|
| FYERS API | Historical adjusted OHLCV (5yr backfill) | Free (demat account) | P0 |
| NSE Bhavcopy | Daily OHLCV + delivery | Free | P0 |
| NSE F&O Bhavcopy | Daily futures OI + settlement | Free | P0 |
| NSE Option Chain | PCR, IV, max pain (3:25 PM scrape) | Free | P1 |
| NSE VIX / FII-DII | India VIX, FII/DII cash flows | Free | P0 |
| BSE Shareholding | Quarterly promoter/FII/DII/MF patterns | Free | P1 |
| AMFI Monthly | Scheme-wise MF portfolio holdings | Free | P1 |
| Screener.in Premium | Quarterly P&L, balance sheet, cashflow | ₹4,999/yr | P1 |
| Trendlyne StratQ | Superstar investor tracking, corporate actions | ₹5,900/yr | P2 |
| Tijori Finance Pro | Operational metrics, segment data | ₹3,500/yr | P2 |
| RBI Website | 10yr yield, credit data | Free | P0 |
| Yahoo Finance | USD/INR, crude, gold, S&P500 | Free | P0 |

---

## File References in This Docs Folder

```
CLAUDE.md                  ← This file (read first, always)
01_features.md             ← Complete 330-feature specification
02_models.md               ← All 16 model specifications
03_data_pipeline.md        ← Database schemas, ingestion code patterns
04_backtesting.md          ← Walk-forward engine, transaction costs
05_ml_algorithms.md        ← Algorithm choices, library versions, HPO
06_deployment.md           ← Laptop scheduler setup (primary); Oracle Cloud setup (deferred)
07_truthful_expectations.md← Honest assessment of what ML can/cannot do
specs/08_specifications.md ← All SPEC-IDs for spec-driven development
tests/09_automated_tests.md← Full pytest suite (unit, integration, regression)
tests/10_hitl_tests.md     ← 7 Human-in-the-Loop test cases
11_phase_delivery_plan.md  ← Week-by-week build plan with all actions
12_platform_architecture.md← Central DataStore + multi-system architecture
13_scheduler_resilience.md ← APScheduler, gap detection, backfill, checkpoints
14_engineering_standards.md← SOLID, RTM, library governance, quality gates, coding standards
15_future_applications.md ← Requirements for TA, FA, Damodaran Valuation, Forensic apps
screens/SCREEN_INVENTORY.md← 27-screen/5-app prototype registry (design tokens, component library)
screens/alphalens_{ml,technical,fundamental,valuation,forensic}.html ← Per-app interactive HTML prototypes
```

---

## Spec-Driven Development Rules (MANDATORY)

**Every line of code must trace to a spec ID. No spec = no feature.**

The specs are in `specs/08_specifications.md`. Every PR/commit must reference
the spec ID it implements (e.g. `Implements SPEC-PIPE-002`).

### Development workflow
1. Read relevant spec(s) before writing any code
2. Write tests FIRST (tests/09_automated_tests.md) before implementation
3. Implement the code
4. Run `pytest tests/ -v --cov` — must pass before any commit
5. After major model changes, run HITL tests from `tests/10_hitl_tests.md`

### New file checklist
Before creating any new module, confirm:
- [ ] Spec ID exists in `specs/08_specifications.md`
- [ ] Unit test exists or created simultaneously
- [ ] Docstring documents PIT assumptions (if touching any data join)
- [ ] No hardcoded paths (use `config/settings.py`)
- [ ] No `print()` — use `logging`

---

## Screen References

**[AS BUILT, 2026-07-01]** The 27-screen, 5-app prototype design lives in
`screens/SCREEN_INVENTORY.md` (registry) + `screens/alphalens_{ml,
technical,fundamental,valuation,forensic}.html` (one interactive mock per
app — open any of these in a browser to see the target design). The live
dashboard at `dashboard/static/` has been rebuilt to this structure; see
`specs/08_specifications.md`'s SPEC-UI-001 through SPEC-UI-010 for the
as-built screen-to-file mapping and which screens are real-data vs.
empty-state (Technical and Valuation have no backend yet — see
SPEC-UI-008/009).

| App | Screens | Status | Spec |
|-----|---------|--------|------|
| AlphaLens.ML | 5 (Daily Insights, Signal Deep Dive, Multibagger, Positions, Backtest) | Real (Positions now includes Pending Actions review/approve, SPEC-PT-003) | SPEC-UI-001/002/003/003b/005 |
| AlphaLens.Forensic | 7 (Dashboard, Red Flags, Benford, Cash Flow, Peer Heatmap, Investigation, Universe Scan) | Real | SPEC-UI-004 |
| AlphaLens.Fundamental | 6 (Dashboard, Peers, Sector, Screener, Thesis, Management) | 4 Real, 2 Partial (Sector/Management have one empty-stated sub-panel each) | SPEC-UI-009, SPEC-FA-008 |
| AlphaLens.Technical | 5 (Chart, Screener, Compare, Alerts, Market Overview) | 3 Real (Chart/Compare/Overview), 2 Empty (Screener/Alerts need real new logic) | SPEC-UI-008, SPEC-TA-004 |
| AlphaLens.Valuation | 4 (DCF, Relative, Batch, Accuracy) | Empty (no backend) | SPEC-UI-009 |

---

## Test Execution

```bash
# Run all automated tests
pytest tests/ -v --cov=alphalens --cov-report=term-missing

# Run only unit tests (fast — use during development)
pytest tests/unit/ -v

# Run integration tests (needs database)
pytest tests/integration/ -v

# Run regression tests (needs model files)
pytest tests/regression/ -v

# Minimum bar before any commit: all unit tests pass
pytest tests/unit/ -v --tb=short
```

### HITL test execution
See `tests/10_hitl_tests.md`. Run after every major model retrain.
Record results using the template in that file.

---

## Scheduler Resilience

The laptop runs a single persistent scheduler process
(`ingestion/scheduler/daily_pipeline.py`, APScheduler with a persistent
SQLAlchemyJobStore) that catches up automatically whenever the laptop was off.
(Originally designed as a dual-scheduler architecture with Oracle Cloud running
always-on cron jobs alongside the laptop — Oracle was dropped, SPEC-SCHED-009;
the design already degraded cleanly to laptop-only since the Oracle-first
behavior was just a preferred *source*, not a structural dependency.)

**Critical behaviour:** When the laptop starts up after being off for N trading days:
1. Gap detection: find all dates between last successful pipeline run and today
2. Catch-up: process each missed date in chronological order (oldest first)
3. Data sources: NSE archives (no Oracle Object Storage step — laptop-only)
4. Option chain: non-recoverable if the laptop was off at 3:25 PM IST that day;
   F&O features set to NaN (Phase 2 scope, not a Phase 1 blocker)
5. After catch-up: run today's normal pipeline

See `SPEC-SCHED-001` through `SPEC-SCHED-010` in `specs/08_specifications.md`.

**Library:** APScheduler ≥3.11 with SQLAlchemyJobStore (replaces `schedule` library).
`schedule` is too simple — no persistence, no missed-run detection, no job recovery.
