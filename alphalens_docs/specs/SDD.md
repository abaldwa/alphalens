# AlphaLens — Software Design Document (SDD)
## Version 1.0 · Specification-Driven Development

---

## 1. System Overview

### 1.1 Purpose
AlphaLens is a machine-learning-based Indian equity research system that ingests daily
market data, computes 330 features across 500 stocks, runs 16 ML models, and produces
actionable signals for human review. It is a **decision-support tool**, not an autonomous
trading system.

### 1.2 Stakeholders
| Role | Name | Responsibility |
|------|------|----------------|
| Developer / User | Solo developer | Build, maintain, interpret outputs |
| System | AlphaLens | Data ingestion, feature computation, model inference |
| External | NSE / BSE / AMFI / Screener.in | Data providers |

### 1.3 Scope
**In scope:** Data ingestion → Feature computation → ML inference → Signal generation →
Multibagger watchlist → P&D protection → Forensic alerts → Exit signals → Backtesting

**Out of scope (deferred):** DRHP analysis, Damodaran valuation, GNN, VAE, LLM alphas,
ESG data, satellite data, autonomous order execution

---

## 2. Functional Requirements

### FR-01: Data Ingestion
| ID | Requirement | Priority | Phase |
|----|-------------|----------|-------|
| FR-01-01 | System SHALL download NSE bhavcopy daily at 4:00 PM IST for all Nifty 500 stocks | MUST | 1 |
| FR-01-02 | System SHALL apply corporate action adjustments retroactively on all affected tickers | MUST | 1 |
| FR-01-03 | System SHALL collect option chain snapshots at 3:25 PM IST for all F&O stocks | MUST | 1 |
| FR-01-04 | System SHALL load quarterly fundamentals using announcement_date for point-in-time alignment | MUST | 2 |
| FR-01-05 | System SHALL load BSE shareholding patterns using filing_date (not quarter_end_date) | MUST | 2 |
| FR-01-06 | System SHALL load AMFI MF holdings using ~5th of following month as availability date | MUST | 2 |
| FR-01-07 | System SHALL validate completeness: ≥ 450/500 stocks with non-null features before proceeding | MUST | 1 |
| FR-01-08 | System SHALL log every pipeline step with timestamp, stock count, and any errors | MUST | 1 |
| FR-01-09 | System SHALL NOT generate, fabricate, or fall back to synthetic/mocked/procedurally-sampled data in any training, scoring, or backtest code path; insufficient real data SHALL raise, never substitute (SPEC-SYS-006) | MUST | 1 |

### FR-02: Feature Computation
| ID | Requirement | Priority | Phase |
|----|-------------|----------|-------|
| FR-02-01 | System SHALL compute 76 core technical features for all 500 stocks in < 20 minutes | MUST | 1 |
| FR-02-02 | All feature computation SHALL be vectorized (no per-stock Python loops) | MUST | 1 |
| FR-02-03 | System SHALL compute sector-relative z-scores for all fundamental features | MUST | 2 |
| FR-02-04 | System SHALL compute 22 P&D detection features BEFORE any signal model runs | MUST | 1 |
| FR-02-05 | System SHALL write feature matrix as Parquet: 500 rows × N columns, named YYYY-MM-DD.parquet | MUST | 1 |
| FR-02-06 | System SHALL compute staleness features: days_since_results, quarter_age_pct, results_pending_flag | MUST | 2 |

### FR-03: ML Models
| ID | Requirement | Priority | Phase |
|----|-------------|----------|-------|
| FR-03-01 | P&D detector SHALL run BEFORE any buy signal reaches the user | MUST | 1 |
| FR-03-02 | P&D score > 60 SHALL hard-block all buy signals for that stock | MUST | 1 |
| FR-03-03 | Signal models SHALL output Buy/Hold/Sell probability + Q10/Q50/Q90 return quantiles | MUST | 1 |
| FR-03-04 | Meta-labeler SHALL only approve signals predicted profitable AFTER transaction costs | MUST | 1 |
| FR-03-05 | All models SHALL be trained on walk-forward folds only (never random splits) | MUST | 1 |
| FR-03-06 | SMOTE SHALL be applied to training data ONLY, never validation or test | MUST | 1 |
| FR-03-07 | Classification thresholds SHALL be optimized on validation fold (never use 0.5 default) | MUST | 1 |
| FR-03-08 | Multibagger model SHALL run weekly (Monday) not daily | MUST | 2 |
| FR-03-09 | Models SHALL expose SHAP explanations for every prediction | MUST | 1 |
| FR-03-10 | HMM SHALL run TWO instances: market-wide (Nifty 50) and per-stock | MUST | 1 |

### FR-04: Monitoring & Drift
| ID | Requirement | Priority | Phase |
|----|-------------|----------|-------|
| FR-04-01 | System SHALL compute PSI for top 50 features daily against baseline | MUST | 1 |
| FR-04-02 | PSI > 0.25 SHALL halt new positions and trigger immediate retrain alert | MUST | 1 |
| FR-04-03 | PSI 0.10–0.25 SHALL reduce position sizing by 50% and schedule retrain | MUST | 1 |
| FR-04-04 | System SHALL monitor rolling 63d model accuracy; alert if below 45% | MUST | 1 |

### FR-05: Outputs & Alerts
| ID | Requirement | Priority | Phase |
|----|-------------|----------|-------|
| FR-05-01 | System SHALL generate daily signal report by 5:30 PM IST | MUST | 1 |
| FR-05-02 | Exit alerts SHALL include exit TYPE (not just "Sell") | MUST | 1 |
| FR-05-03 | System SHALL maintain top-20 multibagger watchlist updated weekly | MUST | 2 |
| FR-05-04 | Forensic red flags SHALL include SHAP explanation of top 3 drivers | MUST | 2 |
| FR-05-05 | System SHALL generate monthly model transparency report | SHOULD | 1 |

---

## 3. Non-Functional Requirements

### NFR-01: Performance
| ID | Requirement |
|----|-------------|
| NFR-01-01 | Full daily pipeline (4:00 PM → 5:30 PM) SHALL complete within 90 minutes |
| NFR-01-02 | Technical feature computation for 500 stocks SHALL complete within 20 minutes |
| NFR-01-03 | Model inference for 500 stocks SHALL complete within 10 minutes total |
| NFR-01-04 | Feature Parquet files SHALL not exceed 15 MB per day |

### NFR-02: Reliability
| ID | Requirement |
|----|-------------|
| NFR-02-01 | Pipeline SHALL retry failed HTTP requests up to 3 times with exponential backoff |
| NFR-02-02 | Pipeline SHALL continue with available stocks if < 50/500 fail validation |
| NFR-02-03 | All database operations SHALL use transactions; partial writes SHALL be rolled back |
| NFR-02-04 | Models SHALL fall back to previous day's signals if inference fails |

### NFR-03: Accuracy & Integrity
| ID | Requirement |
|----|-------------|
| NFR-03-01 | Point-in-time alignment SHALL be enforced: no future data SHALL appear in features |
| NFR-03-02 | Corporate action adjustments SHALL be idempotent (safe to re-run) |
| NFR-03-03 | Backtests SHALL include delisted stocks (anti-survivorship bias) |
| NFR-03-04 | All predictions SHALL have calibrated confidence intervals (conformal coverage ≥ 90%) |

### NFR-04: Maintainability
| ID | Requirement |
|----|-------------|
| NFR-04-01 | All feature computations SHALL have unit tests with known-output fixtures |
| NFR-04-02 | Model retrain SHALL follow snapshot → train → shadow-test → compare → promote protocol |
| NFR-04-03 | All configuration values SHALL be in config/settings.py (no hardcoded values) |
| NFR-04-04 | Code coverage SHALL be ≥ 80% for pipeline and feature modules |

---

## 4. Architecture Decisions (ADRs)

### ADR-001: SQLite over PostgreSQL
**Decision:** Use SQLite for all normalized data.
**Rationale:** No server overhead, zero ops cost, Python built-in, sufficient for 500 stocks.
**Consequences:** Cannot support concurrent writes from multiple processes.
**Review trigger:** When scaling beyond 2,000 stocks or adding concurrent users.

### ADR-002: Parquet for feature store
**Decision:** Daily feature matrices stored as Parquet files, not in database.
**Rationale:** 10x faster columnar reads vs SQLite for ML training. Easy archiving.
**Consequences:** No SQL querying on features; must load full file or use pandas.

### ADR-003: LightGBM as primary model, not deep learning
**Decision:** LightGBM 4.6 is primary; deep learning added as ensemble member in Phase 3.
**Rationale:** 2025 benchmark across 111 datasets confirms gradient boosting matches or
beats deep learning on tabular financial data, while training in seconds on laptop CPU.
**Consequences:** May miss sequential patterns. BiLSTM added in Phase 3 for this.

### ADR-004: Rules-based position sizing until Phase 4
**Decision:** Position sizing via deterministic rules, not RL, through Phase 3.
**Rationale:** RL requires all supervised models to be stable first. Solo developer
cannot build RL reliably in parallel with supervised model development.
**Consequences:** Sub-optimal position sizing in Phase 1–3. Acceptable tradeoff.

### ADR-005: Ubuntu 22.04 as primary OS
**Decision:** Ubuntu 22.04 LTS is the supported OS.
**Rationale:** All 24 libraries install cleanly; mamba-ssm works natively; cron is native.
**Consequences:** Windows users must use WSL2 for Phase 3.

---

## 5. Data Flow Diagram

```
NSE/BSE/AMFI/Screener
         │
         ▼
   RAW DATA LAYER
   datastore/raw/*.csv
         │
         ▼
   INGESTION LAYER         ← ingestion/scrapers/*.py
   Corporate action adjust
   Point-in-time tag
   Quality validation
         │
         ▼
   NORMALIZED DB LAYER     ← datastore/normalised/*.db
   SQLite tables
   Parquet MF holdings
         │
         ▼
   FEATURE LAYER           ← features/*.py
   76 technical → 330 total
   Mixed-frequency join
   Sector z-scores
         │
         ▼
   MODEL LAYER             ← models/
   HMM → P&D gate → Signals
   → Meta → Conformal
   → Exit → Multibagger
         │
         ▼
   OUTPUT LAYER
   datastore/features/daily/YYYY-MM-DD.parquet
   alerts/YYYY-MM-DD.json
   reports/daily_summary.html
```

---

## 6. Security & Data Governance

- API keys (FYERS, Screener.in) stored in `.env` file, never in code
- `.env` and all `/data/` directories in `.gitignore`
- No personally identifiable information stored
- Model outputs are research opinions, not investment advice
- User is responsible for all investment decisions
