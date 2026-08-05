# AlphaLens Platform Architecture
## Central DataStore · Multi-System Design · API-First

**Architecture principle:** One DataStore, many consumers. Every system reads from
and writes back to the same data layer. No system owns data exclusively.
No system scrapes or ingests data independently. The DataStore is the single source of truth.

---

## Platform Overview

```
                          ┌─────────────────────────────────┐
                          │     EXTERNAL DATA SOURCES       │
                          │  NSE · BSE · FYERS · AMFI · RBI │
                          │  Screener · Trendlyne · Tijori  │
                          └────────────────┬────────────────┘
                                           │
                                    ┌──────▼──────┐
                                    │  INGESTION  │
                                    │   LAYER     │
                                    │ (scrapers)  │
                                    └──────┬──────┘
                                           │
              ┌────────────────────────────▼────────────────────────────┐
              │                                                         │
              │                    ALPHALENS DATASTORE                  │
              │                                                         │
              │  ┌─────────────┐ ┌──────────────┐ ┌────────────────┐   │
              │  │  RAW LAYER  │ │  NORMALISED  │ │  FEATURE STORE │   │
              │  │  (as-is     │ │  LAYER       │ │  (ML-ready     │   │
              │  │   from      │ │  (cleaned,   │ │   daily        │   │
              │  │   source)   │ │   adjusted,  │ │   parquet)     │   │
              │  │             │ │   PIT-tagged) │ │                │   │
              │  └─────────────┘ └──────────────┘ └────────────────┘   │
              │                                                         │
              │  ┌─────────────┐ ┌──────────────┐ ┌────────────────┐   │
              │  │  SIGNALS    │ │  MODEL       │ │  SYSTEM        │   │
              │  │  STORE      │ │  REGISTRY    │ │  OUTPUTS       │   │
              │  │  (all model │ │  (versions,  │ │  (TA, FA,      │   │
              │  │   outputs)  │ │   metrics,   │ │   Valuation    │   │
              │  │             │ │   SHAP)      │ │   results)     │   │
              │  └─────────────┘ └──────────────┘ └────────────────┘   │
              │                                                         │
              └────────────────────────┬───────────────────────────────┘
                                       │
                              ┌────────▼────────┐
                              │   DATASTORE     │
                              │   API LAYER     │
                              │   (FastAPI)     │
                              └────────┬────────┘
                                       │
                    ┌──────────────────┼──────────────────┐
                    │                  │                  │
              ┌─────▼─────┐    ┌──────▼──────┐    ┌──────▼──────┐
              │ SYSTEM 1  │    │ SYSTEM 2    │    │ SYSTEM 3    │
              │ ML Signal │    │ Technical   │    │ Fundamental │
              │ Engine    │    │ Analysis    │    │ Analysis    │
              │ (Phase 1) │    │ (Phase 3)   │    │ (Phase 4)   │
              └───────────┘    └─────────────┘    └─────────────┘
                                       │
                              ┌────────▼────────┐
                              │ SYSTEM 4        │
                              │ Damodaran       │
                              │ Valuation       │
                              │ (Phase 3)       │
                              └─────────────────┘
```

---

## Why This Architecture

The previous design had the ML pipeline directly owning the database, computing features,
and training models — all in one monolithic flow. That works for Phase 1, but creates
three problems when you add Technical Analysis, Fundamental Analysis, and Damodaran
Valuation as separate systems:

**Problem 1 — Data duplication.** Each system needs OHLCV, fundamentals, and governance
data. Without a shared DataStore, each system would maintain its own copy, leading to
inconsistencies (one system has yesterday's data while another has today's).

**Problem 2 — No cross-system signal fusion.** The ML engine wants Damodaran's
`valuation_gap_pct` as a feature. The Technical Analysis system wants the ML engine's
`hmm_regime` as context. The Fundamental Analysis system wants the forensic score.
Without a shared layer, you cannot wire these connections.

**Problem 3 — Point-in-time correctness per system.** PIT alignment is critical for
backtesting. If each system manages its own data timestamps, the probability of one
system introducing lookahead bias is high. A single DataStore enforces PIT rules once,
correctly, for all consumers.

---

## DataStore Architecture — Six Stores

### Store 1: Raw Data Store
**What:** Unmodified data exactly as received from external sources.
**Why keep it:** Audit trail. If a corporate action adjustment is wrong, you retrace
to the raw data and recompute.

```
datastore/raw/
├── bhavcopy/YYYY-MM-DD.csv          # NSE equity bhavcopy (as downloaded)
├── fno/YYYY-MM-DD.csv               # NSE F&O bhavcopy
├── option_chain/YYYY-MM-DD.json     # Option chain snapshot (3:25 PM)
├── screener/TICKER_quarterly.xlsx   # Screener.in Excel exports
├── amfi/YYYY-MM.json                # AMFI monthly scheme portfolios
├── bse_shareholding/YYYY-Q.json     # BSE shareholding quarterly
├── macro/                           # VIX, FX, yield, FII/DII
└── trendlyne/                       # Superstar investor data
```

**Retention:** 90 days rolling for daily files. Quarterly/annual files kept indefinitely.
**Owner:** Ingestion layer only. No consumer system writes here.

---

### Store 2: Normalised Data Store (DuckDB + Parquet)
**What:** Cleaned, validated, corporate-action-adjusted, PIT-tagged data.
**This is what all consumer systems read from.** No system should ever read from Raw.
**Technology:** DuckDB persistent database file (`alphalens.duckdb`) for all analytical tables.
DuckDB provides columnar storage, multi-core query execution, and native AsOf joins for PIT.

```sql
-- Core tables (DuckDB — analytical, columnar)

-- OHLCV: adjusted prices, delivery data, PIT-correct
ohlcv_adjusted(date, ticker, open, high, low, close, volume,
               delivery_qty, delivery_pct, adj_factor, vol_adj_factor, source)

-- Corporate actions ledger
corporate_actions(ticker, ex_date, action_type, ratio,
                  announcement_date, record_date)

-- Fundamentals: PIT via announcement_date
fundamentals(ticker, fiscal_year, quarter, quarter_end_date,
             announcement_date,   -- ← ALL consumers use THIS for PIT
             revenue, ebitda, pat, eps, operating_margin, ebitda_margin,
             net_margin, roe, roce, debt_to_equity, interest_coverage,
             fcf, asset_turnover, inventory_days, receivable_days,
             payable_days, book_value_per_share, shares_outstanding)

-- Shareholding: PIT via filing_date
shareholding(ticker, quarter_end_date, filing_date,  -- ← PIT on filing_date
             promoter_pct, promoter_pledge, fii_pct, dii_pct,
             mf_pct, retail_pct)

-- Macro indicators
macro_indicators(date, indicator, value)

-- Stock master: sector, tier, listing info
stock_master(ticker, company_name, sector, industry, nse_series,
             listing_date, market_cap_cr, adtv_cr, current_tier,
             is_fno_eligible, is_nifty500)
```

```
-- MF holdings (Parquet — large, monthly)
datastore/normalised/mf_holdings/YYYY-MM.parquet
   (month_end, scheme_name, isin, ticker, quantity, value_inr)
```

**PIT enforcement:** Every table with temporal data has a `*_date` column that represents
when the data became publicly available. The DataStore API enforces PIT queries:
`GET /fundamentals?ticker=X&as_of=2024-04-30` returns only rows where
`announcement_date <= 2024-04-30`.

---

### Store 3: Feature Store (Parquet)
**What:** Precomputed ML-ready features. 500 stocks × 330 features × daily.

```
datastore/features/
├── daily/YYYY-MM-DD.parquet    # Full feature matrix
├── baseline/stats_baseline.pkl # PSI reference distribution
└── metadata/feature_catalog.json
```

**Feature catalog (`feature_catalog.json`):**
```json
{
  "pct_rank_5d": {
    "category": "price_position",
    "update_freq": "daily",
    "source_store": "ohlcv_adjusted",
    "pit_rule": "same-day",
    "phase": 1,
    "range": [0, 1],
    "consumers": ["ml_signal_engine", "technical_analysis"]
  },
  "roe": {
    "category": "fundamental",
    "update_freq": "quarterly",
    "source_store": "fundamentals",
    "pit_rule": "announcement_date",
    "phase": 2,
    "range": [-1, 2],
    "consumers": ["ml_signal_engine", "fundamental_analysis", "damodaran_valuation"]
  }
}
```

---

### Store 4: Signals Store (DuckDB)
**What:** All model outputs from all systems. Every system writes its outputs here.
Other systems can read any system's outputs as inputs.
**Technology:** DuckDB persistent database — same engine as Store 2.

```sql
-- All signal tables in DuckDB (analytical reads, batch writes)
-- ML model outputs (written by ML Signal Engine)
ml_signals(date, ticker, model_name, model_version,
           signal_direction, buy_prob, hold_prob, sell_prob,
           q10_return, q50_return, q90_return,
           meta_label, meta_prob,
           conformal_lower, conformal_upper,
           pnd_score, pnd_phase, pnd_block,
           hmm_regime, hmm_regime_prob, hmm_stability,
           exit_urgency, exit_type,
           exit_survival_5d, exit_survival_21d, exit_survival_63d,
           shap_top5_json,
           -- [AS BUILT, ML24/ML27 2026-07-11] was this ticker in the
           -- ADTV-curated training universe (config/training_universe.py)
           -- the model was actually trained on? NULL for rows written
           -- before this column existed.
           in_training_universe)

-- Multibagger outputs (written by ML Signal Engine, weekly)
ml_multibagger(date, ticker, mb_probability, mb_tier, mb_archetype,
               survival_6m, survival_12m, survival_18m, survival_24m, survival_36m,
               shap_top5_json, analogues_json,
               in_training_universe)

-- Forensic outputs (written by ML Signal Engine, quarterly)
ml_forensic(date, ticker, beneish_m, altman_z, piotroski_f,
            ohlson_o, dechow_f, sloan_accrual, benford_mad,
            benford_detail_json, forensic_flag_label,
            forensic_composite, forensic_flag, forensic_ml_prob,
            shap_top5_json, pattern_match)

-- Technical Analysis outputs (written by TA System, Phase 3)
ta_signals(date, ticker, pattern_name, pattern_score,
           support_level, resistance_level,
           trend_direction, trend_strength,
           ta_buy_signal, ta_sell_signal)

-- Fundamental Analysis outputs (written by FA System, Phase 4)
fa_signals(date, ticker, quality_score, growth_score,
           mgmt_quality_score, sector_rank,
           investment_thesis_text, fa_rating)

-- Valuation outputs (written by Damodaran System, Phase 3)
valuation_signals(date, ticker, lifecycle_stage,
                  intrinsic_value, valuation_gap_pct,
                  margin_of_safety, wacc, cost_of_equity,
                  terminal_value_pct, dcf_model_type,
                  scenario_bull, scenario_base, scenario_bear)
```

**Cross-system wiring (the key advantage of this architecture):**
- ML Signal Engine reads `valuation_signals.valuation_gap_pct` as a feature for Signal 63d
- ML Signal Engine reads `fa_signals.quality_score` as a feature for Multibagger
- Technical Analysis System reads `ml_signals.hmm_regime` as context for chart display
- Fundamental Analysis System reads `ml_forensic.forensic_composite` as a risk overlay
- Damodaran Valuation reads `fundamentals` directly from Normalised Store

---

### Store 5: Model Registry (JSON + model files)
**What:** Versioned ML model files plus metadata tracking.

```
datastore/models/
├── registry.json                      # All model metadata
├── signal_5d/
│   ├── signal_5d_v20260520_fold3.lgbm
│   ├── signal_5d_v20260420_fold3.lgbm  # Previous version (rollback)
│   └── signal_5d_current.lgbm → signal_5d_v20260520_fold3.lgbm  # Symlink
├── hmm/
│   ├── hmm_nifty50_v20260501.pkl
│   └── hmm_per_stock/
│       ├── RELIANCE_hmm_v20260501.pkl
│       └── ...
└── ...
```

---

### Store 6: System Outputs Store (for UI and reporting)
**What:** Aggregated outputs ready for dashboard consumption.

```
datastore/outputs/
├── daily_report/YYYY-MM-DD.json       # Today's complete signal summary
├── watchlist/YYYY-MM-DD.json          # Current multibagger watchlist
├── alerts/YYYY-MM-DD.json             # P&D, exit, forensic, drift alerts
├── portfolio/positions.json           # Current paper/live portfolio state
└── backtest/                          # Walk-forward results per model
```

---

## DataStore API Layer (FastAPI)

### Why FastAPI
- Fastest Python web framework (async support)
- Automatic OpenAPI/Swagger documentation at `/docs`
- Type-validated request/response with Pydantic models
- Runs locally on laptop (Phase 1–2); can be deployed to Oracle Cloud later

### API Groups

```
/api/v1/
├── /ohlcv/                 # Normalised price data
│   ├── GET /{ticker}?from=&to=&adjusted=true
│   ├── GET /{ticker}/latest
│   └── GET /universe?tier=1&date=
│
├── /fundamentals/          # Quarterly financial data (PIT-enforced)
│   ├── GET /{ticker}?as_of=         # Returns latest as of date (PIT)
│   ├── GET /{ticker}/history?quarters=8
│   └── GET /{ticker}/staleness      # days_since_results, pending_flag
│
├── /governance/            # Shareholding patterns (PIT via filing_date)
│   ├── GET /{ticker}?as_of=
│   └── GET /{ticker}/pledge_history
│
├── /macro/                 # Macro indicators
│   ├── GET /{indicator}?from=&to=   # VIX, USD_INR, CRUDE, etc.
│   └── GET /regime                  # Current HMM market regime
│
├── /features/              # ML feature store
│   ├── GET /{ticker}/{date}         # All features for one stock on one date
│   ├── GET /matrix/{date}           # Full 500×330 matrix (Parquet download)
│   └── GET /catalog                 # Feature metadata catalog
│
├── /signals/               # ML model outputs
│   ├── GET /ml/{ticker}/{date}      # All ML signals for one stock
│   ├── GET /ml/top_buys/{date}      # Top N buy signals for a date
│   ├── GET /ta/{ticker}/{date}      # TA system outputs
│   ├── GET /fa/{ticker}/{date}      # FA system outputs
│   ├── GET /valuation/{ticker}      # Damodaran valuation outputs
│   └── GET /forensic/{ticker}       # Forensic scores and flags
│
├── /watchlist/             # Multibagger watchlist
│   ├── GET /current                 # Current top-20
│   └── GET /history                 # Watchlist changes over time
│
├── /alerts/                # P&D, exit, forensic, drift alerts
│   ├── GET /today                   # All today's alerts
│   └── GET /history?type=&days=
│
├── /portfolio/             # Position tracking
│   ├── GET /positions               # Current held positions
│   ├── POST /positions              # Add position (for tracking)
│   └── GET /performance             # Portfolio performance metrics
│
├── /backtest/              # Backtesting results
│   ├── GET /results/{model_name}    # Walk-forward fold results
│   └── GET /integrity               # Integrity rule check status
│
├── /universe/              # Stock master data
│   ├── GET /stocks                  # All stocks with tier, sector, etc.
│   ├── GET /stocks/{ticker}         # Single stock metadata
│   └── GET /tiers                   # Tier definitions and current counts
│
└── /system/                # System health
    ├── GET /health                  # Pipeline status
    ├── GET /drift                   # PSI drift status per feature
    └── GET /models                  # Model registry summary
```

### PIT Enforcement in API

Every endpoint that returns temporal data has an optional `as_of` parameter.
When provided, the API enforces point-in-time rules automatically:

```python
# datastore/api/pit.py

from fastapi import Query
from datetime import date

def get_fundamental_pit(ticker: str, as_of: date = Query(default=None)):
    """
    Returns latest fundamental where announcement_date <= as_of.
    If as_of is None, uses today's date.
    NEVER returns data with announcement_date > as_of.
    """
    if as_of is None:
        as_of = date.today()

    row = db.execute("""
        SELECT * FROM fundamentals
        WHERE ticker = ? AND announcement_date <= ?
        ORDER BY announcement_date DESC LIMIT 1
    """, (ticker, as_of.isoformat()))

    return row
```

This means every consumer system — ML, TA, FA, Valuation — gets PIT-correct data
automatically by calling the API with the appropriate date. They never need to implement
PIT logic themselves. The correctness is enforced once, centrally.

---

## Consumer Systems Architecture

### System 1: ML Signal Engine (Phase 1–2 — existing)

**Reads from DataStore:** OHLCV, features, fundamentals, governance, macro, MF holdings
**Writes to DataStore:** ML signals, multibagger watchlist, forensic scores, HMM regime, alerts
**Phase 3 also reads:** valuation_signals (from Damodaran), ta_signals (from TA System)

```
systems/ml_signal_engine/
├── models/          # All 16 ML models
├── training/        # Walk-forward, labeling, HPO
├── inference/       # Daily pipeline inference
└── monitoring/      # PSI drift, ADWIN, retrain triggers
```

**How it accesses data:**
```python
import httpx

# Get today's feature matrix
features = httpx.get("http://localhost:8000/api/v1/features/matrix/2026-05-20")

# Get latest fundamentals for a stock (PIT-correct)
fund = httpx.get("http://localhost:8000/api/v1/fundamentals/RELIANCE",
                  params={"as_of": "2026-05-20"})

# Write signal outputs back to DataStore
httpx.post("http://localhost:8000/api/v1/signals/ml",
            json={"date": "2026-05-20", "ticker": "RELIANCE",
                  "buy_prob": 0.82, "meta_label": True, ...})
```

---

### System 2: Technical Analysis System (Phase 3)

**Reads from DataStore:** OHLCV (primary), features (for overlays), ML signals (for regime context)
**Writes to DataStore:** TA signals (pattern scores, support/resistance, trend direction)

```
systems/technical_analysis/
├── charts/          # Candlestick + indicator rendering
├── patterns/        # Pattern detection (H&S, double bottom, cup)
├── screener/        # User-defined technical criteria
├── alerts/          # Price alerts, breakout alerts
└── api_writer.py    # Writes TA outputs back to DataStore
```

**What it produces:**
- Charting engine: interactive candlestick charts with indicator overlays
- Pattern detection: head-shoulders, double-bottom, cup-and-handle, flags, wedges
- Support/resistance levels: computed dynamically
- Custom screener: user defines technical criteria; screener runs on DataStore
- Trend classification: uptrend/downtrend/sideways per stock per timeframe

**How it uses DataStore data:**
```python
# Get OHLCV for charting
ohlcv = httpx.get(f"/api/v1/ohlcv/{ticker}?from=2025-01-01&to=2026-05-20")

# Get HMM regime to colour-code chart background
regime = httpx.get(f"/api/v1/signals/ml/{ticker}/2026-05-20")
# Use regime['hmm_regime'] to shade chart regions: green=bullish, red=bearish

# Write pattern detection result back
httpx.post("/api/v1/signals/ta",
            json={"date": "2026-05-20", "ticker": ticker,
                  "pattern_name": "cup_and_handle",
                  "pattern_score": 0.78,
                  "support_level": 2340.0,
                  "resistance_level": 2580.0})
```

**Cross-system value:** The ML engine reads `ta_signals.pattern_score` as an optional
feature in the multibagger model. A high cup-and-handle score reinforces the "Long Base
Breakout" archetype signal.

---

### System 3: Damodaran Valuation System (Phase 3)

**Reads from DataStore:** Fundamentals, macro (risk-free rate, equity risk premium),
stock master (sector for comparable selection)
**Writes to DataStore:** Valuation signals (intrinsic value, gap, margin of safety)

```
systems/damodaran_valuation/
├── lifecycle/       # Lifecycle classification (startup/growth/mature/decline)
├── dcf/             # DCF engine per lifecycle stage
├── relative/        # Relative valuation (sector peer multiples)
├── scenarios/       # Bull/base/bear scenario analysis
└── api_writer.py    # Writes valuation outputs back to DataStore
```

**What it produces:**
- Lifecycle stage classification for each stock
- Appropriate DCF model per stage (high-growth for startups, stable-growth for mature)
- Intrinsic value estimate with confidence range
- Valuation gap: `(intrinsic_value - current_price) / current_price`
- Margin of safety computation
- WACC, cost of equity, terminal value contribution
- 3 scenarios: bull, base, bear with probability weights

**How it uses DataStore data:**
```python
# Get last 5 years of quarterly fundamentals for DCF inputs
fund_history = httpx.get(f"/api/v1/fundamentals/{ticker}/history?quarters=20")

# Get current risk-free rate from macro store
rf = httpx.get("/api/v1/macro/BOND_YIELD_10YR?from=2026-05-01")

# Get sector peers for relative valuation
peers = httpx.get(f"/api/v1/universe/stocks?sector=IT_SERVICES&tier=1,2")

# Write valuation output back
httpx.post("/api/v1/signals/valuation",
            json={"date": "2026-05-20", "ticker": ticker,
                  "lifecycle_stage": "growth",
                  "intrinsic_value": 3200.0,
                  "valuation_gap_pct": 0.15,
                  "margin_of_safety": 0.18,
                  "dcf_model_type": "two_stage_growth"})
```

**Cross-system value:** The ML engine reads `valuation_gap_pct` and
`margin_of_safety` as Phase 3 features for Signal 63d and Multibagger models.
A high valuation gap + high multibagger probability = strongest conviction signal.

---

### System 4: Fundamental Analysis System (Phase 4)

**Reads from DataStore:** Fundamentals, governance, forensic scores, MF holdings,
Tijori operational metrics, macro indicators
**Writes to DataStore:** FA signals (quality score, growth score, management quality,
investment thesis)

```
systems/fundamental_analysis/
├── quality/         # Fundamental quality scoring
├── growth/          # Growth trajectory analysis
├── management/      # Management quality scoring
├── sector/          # Sector-specific analysis modules
│   ├── bfsi.py      # Banking/NBFC-specific analysis
│   ├── it_services.py
│   ├── pharma.py
│   ├── auto.py
│   ├── fmcg.py
│   └── infra.py
├── thesis/          # Investment thesis generator
├── peers/           # Peer comparison engine
└── api_writer.py    # Writes FA outputs back to DataStore
```

**Cross-system value:** FA quality_score becomes a feature in the multibagger model.
FA management_quality_score feeds the forensic model as a cross-validation signal.

---

## Data Flow Matrix — Who Reads What

| DataStore Table | ML Engine | TA System | Damodaran | FA System | Dashboard |
|----------------|:---------:|:---------:|:---------:|:---------:|:---------:|
| ohlcv_adjusted | ✅ read | ✅ read | — | — | — |
| fundamentals | ✅ read | — | ✅ read | ✅ read | — |
| shareholding | ✅ read | — | — | ✅ read | — |
| macro_indicators | ✅ read | — | ✅ read | — | — |
| mf_holdings | ✅ read | — | — | ✅ read | — |
| features (Parquet) | ✅ read | ✅ read | — | — | — |
| stock_master | ✅ read | ✅ read | ✅ read | ✅ read | ✅ read |
| ml_signals | — | ✅ read | — | — | ✅ read |
| ml_multibagger | — | — | — | — | ✅ read |
| ml_forensic | — | — | — | ✅ read | ✅ read |
| ta_signals | ✅ read (P3) | — | — | — | ✅ read |
| valuation_signals | ✅ read (P3) | — | — | ✅ read | ✅ read |
| fa_signals | ✅ read (P4) | — | — | — | ✅ read |

**Key insight:** Notice that each consumer system both reads and writes to the DataStore.
This creates a feedback loop where later systems enrich the data available to earlier
systems. When Damodaran Valuation writes `valuation_gap_pct`, the ML engine can read it
as a feature in its next retrain cycle.

---

## Write-Back Protocol

When a consumer system writes outputs back to the DataStore, it follows these rules:

1. **Timestamped:** Every write includes the date it was computed for
2. **Versioned:** Every write includes the model/system version that produced it
3. **Idempotent:** Writing the same output twice for the same date+ticker replaces, not duplicates
4. **Schema-validated:** API rejects writes that don't match the expected schema
5. **Logged:** Every write is logged with timestamp, system name, row count

```python
# datastore/api/write.py

@app.post("/api/v1/signals/ml")
async def write_ml_signal(signal: MLSignalInput):
    """
    Write ML signal output for a stock-date.
    Upserts: if signal already exists for this date+ticker, it is replaced.
    """
    db.execute("""
        INSERT OR REPLACE INTO ml_signals
        (date, ticker, model_name, model_version, ...)
        VALUES (?, ?, ?, ?, ...)
    """, signal.dict().values())
    log.info(f"ML signal written: {signal.ticker} {signal.date}")
    return {"status": "ok"}
```

---

## Refactored Repository Structure

```
alphalens_platform/
├── CLAUDE.md                         # Master context (updated for multi-system)
├── README.md
│
├── datastore/                        # THE CENTRAL DATA LAYER
│   ├── raw/                          # Store 1: raw as-is data (gitignored)
│   ├── normalised/                   # Store 2: cleaned DuckDB + Parquet (gitignored)
│   │   ├── alphalens.duckdb         # Analytical: OHLCV, fundamentals, shareholding, macro
│   │   ├── pipeline_log.db          # Transactional: pipeline state, checkpoints (SQLite)
│   │   ├── scheduler.db             # Transactional: APScheduler job store (SQLite)


│   │   └── mf_holdings/
│   ├── features/                     # Store 3: ML feature Parquets (gitignored)
│   │   ├── daily/
│   │   ├── baseline/
│   │   └── metadata/feature_catalog.json
│   ├── signals/                      # Store 4: all system outputs (gitignored)
│   │   └── signals.db                # ml_signals, ta_signals, fa_signals, valuation_signals
│   ├── models/                       # Store 5: model files + registry (gitignored)
│   │   ├── registry.json
│   │   └── {model_name}/
│   ├── outputs/                      # Store 6: UI-ready aggregated outputs
│   └── api/                          # FastAPI DataStore API
│       ├── main.py                   # FastAPI app entrypoint
│       ├── routers/
│       │   ├── ohlcv.py
│       │   ├── fundamentals.py
│       │   ├── governance.py
│       │   ├── macro.py
│       │   ├── features.py
│       │   ├── signals.py
│       │   ├── watchlist.py
│       │   ├── alerts.py
│       │   ├── portfolio.py
│       │   ├── backtest.py
│       │   ├── universe.py
│       │   └── system.py
│       ├── schemas.py                # Pydantic models for request/response validation
│       ├── pit.py                    # Point-in-time enforcement layer
│       └── db.py                     # Database connection management
│
├── ingestion/                        # DATA COLLECTION (runs on Oracle Cloud + laptop)
│   ├── scrapers/
│   │   ├── bhavcopy.py
│   │   ├── fno.py
│   │   ├── option_chain.py
│   │   ├── amfi_holdings.py
│   │   ├── bse_shareholding.py
│   │   ├── screener.py
│   │   ├── macro.py
│   │   └── corporate_actions.py
│   ├── adjust/
│   │   └── price_adjuster.py
│   ├── quality/
│   │   ├── validator.py
│   │   └── drift_monitor.py
│   └── scheduler/
│       ├── daily_pipeline.py         # Main pipeline orchestrator
│       └── cron_setup.sh
│
├── features/                         # FEATURE COMPUTATION (writes to DataStore)
│   ├── technical.py                  # 76 core features
│   ├── intraday.py                   # 8 intraday OHLCV patterns
│   ├── fundamental.py                # 28 fundamental features
│   ├── governance.py                 # 12 governance features
│   ├── mf_holdings.py                # 12 MF holding features
│   ├── fno_features.py               # 16 F&O derivatives
│   ├── macro_features.py             # 15 macro features
│   ├── calendar.py                   # 7 calendar/seasonal
│   ├── pnd_features.py               # 22 P&D detection features
│   ├── multibagger.py                # 33 multibagger features
│   ├── forensic_classical.py         # 30 classical forensic scores
│   ├── corporate_action_features.py  # 10 corporate action features
│   └── matrix_builder.py             # Assembles full matrix → DataStore
│
├── systems/                          # CONSUMER SYSTEMS
│   │
│   ├── ml_signal_engine/             # System 1 — Phase 1 (core)
│   │   ├── models/
│   │   │   ├── hmm/regime_detector.py
│   │   │   ├── signal/signal_5d.py
│   │   │   ├── signal/signal_21d.py
│   │   │   ├── signal/signal_63d.py
│   │   │   ├── signal/meta_labeler.py
│   │   │   ├── uncertainty/conformal.py
│   │   │   ├── pnd/pnd_detector.py
│   │   │   ├── exit/exit_signal.py
│   │   │   ├── multibagger/multibagger_model.py
│   │   │   ├── forensic/classical_scores.py
│   │   │   ├── forensic/forensic_ml.py
│   │   │   └── deep/ (Phase 3: tft, bilstm, stacking, tabnet)
│   │   ├── training/
│   │   │   ├── walk_forward.py
│   │   │   ├── labeling.py
│   │   │   ├── imbalance.py
│   │   │   └── hyperparams.py
│   │   ├── inference/daily_inference.py
│   │   └── api_writer.py             # Writes ML outputs to DataStore API
│   │
│   ├── technical_analysis/           # System 2 — Phase 3
│   │   ├── charts/chart_engine.py
│   │   ├── patterns/pattern_detector.py
│   │   ├── screener/ta_screener.py
│   │   ├── alerts/ta_alerts.py
│   │   └── api_writer.py
│   │
│   ├── damodaran_valuation/          # System 3 — Phase 3
│   │   ├── lifecycle/classifier.py
│   │   ├── dcf/dcf_engine.py
│   │   ├── relative/peer_valuation.py
│   │   ├── scenarios/scenario_builder.py
│   │   └── api_writer.py
│   │
│   └── fundamental_analysis/         # System 4 — Phase 4
│       ├── quality/quality_scorer.py
│       ├── growth/growth_analyzer.py
│       ├── management/mgmt_scorer.py
│       ├── sector/ (bfsi, it, pharma, auto, fmcg, infra)
│       ├── thesis/thesis_builder.py
│       ├── peers/peer_comparison.py
│       └── api_writer.py
│
├── backtest/                         # BACKTESTING (reads from DataStore)
│   ├── engine.py
│   ├── portfolio.py
│   ├── costs.py
│   ├── metrics.py
│   └── integrity_checker.py
│
├── dashboard/                        # UI LAYER (reads from DataStore API)
│   ├── app.py                        # Streamlit or Flask frontend
│   ├── screens/
│   │   ├── daily_dashboard.py        # Screen A
│   │   ├── signal_detail.py          # Screen B
│   │   ├── multibagger.py            # Screen C
│   │   ├── forensic.py               # Screen D
│   │   ├── backtest.py               # Screen E
│   │   ├── ta_charts.py              # Screen F (Phase 3)
│   │   └── valuation.py              # Screen G (Phase 3)
│   └── static/
│
├── config/
│   ├── settings.py
│   ├── universe.py
│   └── logging_config.py
│
├── requirements/
│   ├── datastore.txt                 # FastAPI, uvicorn, pydantic, httpx
│   ├── phase1.txt                    # ML libraries
│   ├── phase2.txt
│   └── phase3.txt
│
├── tests/
│   ├── unit/
│   ├── integration/
│   ├── regression/
│   └── hitl/
│
└── docs/
    ├── 01_features.md
    ├── 02_models.md
    ├── ... (all existing docs)
    ├── 11_phase_delivery_plan.md
    └── 12_platform_architecture.md   # This file
```

---

## Phase Delivery — DataStore Milestones

| Phase | DataStore milestone | Consumer systems active |
|-------|-------------------|------------------------|
| 0 | Stores 1 + 2 created. DuckDB + SQLite schemas deployed. Raw data flowing. | None |
| 1 | Store 3 (features) operational. Store 4 (signals) created for ML outputs. Store 5 (models) created. FastAPI running locally. | ML Signal Engine |
| 2 | All 6 stores active. Fundamentals, governance, MF holdings in normalised store. Forensic and multibagger signals in Store 4. | ML Signal Engine |
| 3 | TA System and Damodaran System added. Their outputs written to Store 4. ML Engine reads cross-system signals. | ML + TA + Damodaran |
| 4 | FA System added. Full cross-system signal fusion. All 4 consumers read and write. | ML + TA + Damodaran + FA |

---

## DataStore API — Additional Requirements (for Phase 1 requirements.txt)

```
# datastore/api requirements
fastapi>=0.115.0
uvicorn>=0.30.0
pydantic>=2.9
httpx>=0.27.0
python-multipart>=0.0.9
```

Launch DataStore API:
```bash
cd alphalens_platform
uvicorn datastore.api.main:app --host 0.0.0.0 --port 8000 --reload
# API docs available at http://localhost:8000/docs (Swagger UI)
```

---

## Migration from Previous Architecture

The previous flat design had `pipeline/` directly writing to `data/db/` and
`data/features/`. The refactored design moves this into a DataStore with an API layer.
The migration is non-breaking:

1. Rename `data/` → `datastore/normalised/` + `datastore/features/` + `datastore/raw/`
2. Move `pipeline/ingest/` → `ingestion/scrapers/`
3. Move `pipeline/features/` → `features/`
4. Move `models/` → `systems/ml_signal_engine/models/`
5. Add `datastore/api/` (new — FastAPI app)
6. Add `datastore/signals/` (new — Store 4)
7. All existing model code continues to work; only import paths change

The FastAPI layer is additive — Phase 1 ML engine can continue to read DuckDB directly
while the API is being built. Consumer systems 2/3/4 exclusively use the API.
