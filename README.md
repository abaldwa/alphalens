# AlphaLens — Nifty200 ML Trading Intelligence Platform

Personal trading intelligence system for Nifty200 stocks.
Python-native, runs entirely on your laptop (i5 / 8GB RAM / SSD recommended).

---

## What it does

- **Market cycle classification** at 3 levels (market → 12 sectors → 200 stocks) using 24 indicators including VIX, DXY, Crude, Nasdaq, Dow, FII/DII flows, and breadth
- **Autonomous strategy discovery** — genetic algorithm monthly discovers new strategies from 15yr price data
- **ML signal generation** for 4 timeframes: Intraday (3 slots), Swing (5), Medium (8), Long-term (15)
- **Entry / Target / Stop-Loss** for every recommendation, ATR-based
- **Interactive charts** with historical signal overlays (entry/exit markers per strategy)
- **Portfolio management**: manual entry + Zerodha Holdings/Tradebook CSV upload
- **P&L reporting**: Booked + Notional, STCG/LTCG breakdown
- **Alerts**: Telegram (trading signals) + Email (investment reports)
- **Stock pattern hypothesis**: HMM-based regime detection per stock
- **3× daily review**: 9:30 AM gap analysis · 3:00 PM pre-close · 6:30 PM EOD

---

## Hardware Recommendation

| Component | Current        | Recommended    |
|-----------|----------------|----------------|
| CPU       | Intel i5       | ✅ Fine          |
| RAM       | 8GB            | ✅ Fine          |
| Storage   | SATA HDD 512GB | ⚠️ Upgrade to SSD |

**SSD upgrade (Samsung 870 EVO 512GB, ~₹4,000)** gives 5× faster DuckDB backtest queries.
The system works on SATA HDD but backtests will take longer.

---

## Quick Start

```bash
# 1. Clone/copy project
cd alphalens

# 2. One-command setup
bash scripts/setup.sh

# 3. Edit credentials
nano .env   # Add Telegram token, Email, Zerodha Kite keys

# 4. Fetch 15 years of data (run once — takes 20-40 min)
python main.py --backfill

# 5. Train ML models
python main.py --train-cycles

# 6. Launch everything
python main.py
# → Dashboard: http://localhost:8050
```

---

## Application Structure

```
alphalens/
├── main.py                          # Entry point (--init, --backfill, --dashboard)
├── requirements.txt
├── .env.example                     # Copy to .env
├── config/
│   └── settings.py                  # All settings (pydantic-settings)
├── alphalens/
│   ├── core/
│   │   ├── database.py              # DuckDB + SQLite schemas + ORM
│   │   ├── ingestion/
│   │   │   ├── universe.py          # Nifty200 symbols (200 stocks)
│   │   │   ├── historical.py        # yfinance 15yr backfill + incremental
│   │   │   ├── fundamental.py       # Screener.in scraper
│   │   │   └── zerodha_import.py    # Holdings + Tradebook CSV parsers
│   │   ├── indicators/
│   │   │   └── calculator.py        # 40+ indicators (pandas-ta + custom)
│   │   ├── cycle/
│   │   │   ├── labeller.py          # Historical Bull/Bear/Neutral labels
│   │   │   ├── classifier.py        # Random Forest cycle classifier
│   │   │   └── context.py           # Real-time cycle state
│   │   ├── strategy/
│   │   │   ├── library.py           # 12 seeded strategy definitions
│   │   │   ├── backtester.py        # vectorbt walk-forward engine
│   │   │   ├── discovery.py         # DEAP genetic algorithm
│   │   │   └── mapper.py            # Cycle → best strategy mapping
│   │   ├── signals/
│   │   │   ├── generator.py         # ML ensemble signal generation
│   │   │   ├── entry.py             # Entry price calculation
│   │   │   ├── exit.py              # Target + stop-loss (ATR-based)
│   │   │   └── advisor.py           # Exit candidate (3-perspective)
│   │   ├── portfolio/
│   │   │   ├── manager.py           # Slot management + capital allocation
│   │   │   ├── reviewer.py          # 3× daily review engine
│   │   │   └── pnl.py               # P&L tracking + snapshots
│   │   ├── notifications/
│   │   │   ├── telegram.py          # Telegram Bot alerts
│   │   │   └── email.py             # SMTP email reports
│   │   └── patterns/
│   │       └── hmm.py               # Hidden Markov Model per stock
│   ├── ml/
│   │   ├── features/
│   │   │   └── pipeline.py          # Feature extraction for models
│   │   ├── training/
│   │   │   └── signal_trainer.py    # LightGBM/XGBoost/LSTM/RF trainers
│   │   └── inference/
│   │       └── predictor.py         # Model inference wrapper
│   ├── dashboard/
│   │   ├── app.py                   # Dash app factory
│   │   ├── pages/
│   │   │   ├── market_overview.py   # Page 1: Market cycle + macro
│   │   │   ├── portfolio.py         # Page 2: All portfolios
│   │   │   ├── watchlist.py         # Page 3: Buy/sell watchlist
│   │   │   ├── stock_chart.py       # Page 4: Candlestick + signals
│   │   │   ├── strategy_library.py  # Page 5: Strategy DB
│   │   │   ├── pnl_report.py        # Page 6: P&L reporting
│   │   │   ├── portfolio_entry.py   # Page 7: Manual + CSV upload
│   │   │   ├── patterns.py          # Page 8: Stock pattern analysis
│   │   │   ├── backtest.py          # Page 9: Backtest explorer
│   │   │   └── settings.py          # Page 10: Configuration
│   │   └── components/
│   │       ├── navbar.py
│   │       ├── cycle_badge.py
│   │       └── signal_table.py
│   └── scheduler/
│       └── jobs.py                  # APScheduler job definitions
├── scripts/
│   ├── setup.sh                     # One-command setup
│   └── refresh_kite_token.py        # Daily Kite token refresh
└── tests/
```

---

## Daily Schedule

| Time (IST) | Job |
|------------|-----|
| 6:30 PM    | EOD: fetch prices → indicators → cycle classify → generate signals → update watchlist/portfolio → send reports |
| 9:30 AM    | Gap analysis → intraday signals → morning Telegram alert |
| 3:00 PM    | Pre-close: check intraday exits, update SLs |
| Monday EOD | Fundamental refresh (Screener.in, ~200 stocks) |
| 1st of month | Investment portfolio monthly review |
| Last Sunday | Monthly strategy discovery (genetic algorithm) |

---

## Capital Allocation (configurable via Settings page)

| Timeframe    | Slots | Default Capital |
|--------------|-------|-----------------|
| Intraday     | 3     | ₹2,50,000 (10%) |
| Swing        | 5     | ₹5,00,000 (20%) |
| Medium-term  | 8     | ₹7,50,000 (30%) |
| Long-term    | 15    | ₹10,00,000 (40%)|
| **Total**    | **31**| **₹25,00,000**  |

---

## Build Phases

| Phase | Description | Status |
|-------|-------------|--------|
| 1 | Foundation: Data + Database + Indicators | ✅ Phase 1 |
| 2 | Market Cycle Classifier | 🔲 Next |
| 3 | Strategy Library + Backtesting | 🔲 |
| 4 | ML Signal Engine | 🔲 |
| 5 | Dash Dashboard (10 pages) | 🔲 |
| 6 | Notifications + Live Feed | 🔲 |
| 7 | Hardening + Pattern Hypothesis | 🔲 |
"# alphalens" 
