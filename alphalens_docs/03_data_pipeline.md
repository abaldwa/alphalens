# AlphaLens — Data Pipeline Specification
## Database Schemas · Ingestion Patterns · Point-in-Time Rules

---

## Three-Layer Architecture

```
LAYER 1: RAW DATA              LAYER 2: NORMALIZED DB           LAYER 3: FEATURE STORE
──────────────────────         ────────────────────────         ──────────────────────
NSE Bhavcopy (daily)      →    ohlcv_adjusted (DuckDB)     →
FYERS API (backfill)      →    corporate_actions (DuckDB)   →    features/daily/
NSE F&O bhavcopy (daily)  →    option_chain (DuckDB)        →    YYYY-MM-DD.parquet
Option chain (3:25 PM)    →    fundamentals (DuckDB)        →    500 rows × 330 cols
Screener.in (quarterly)   →    shareholding (DuckDB)        →
BSE filings (quarterly)   →    mf_holdings (Parquet)        →    baseline_stats.pkl
AMFI (monthly)            →    macro_indicators (DuckDB)    →    (for PSI drift)
RBI / Yahoo Finance       →
```

---

## Directory Structure

```
alphalens/
└── data/
    ├── raw/
    │   ├── bhavcopy/YYYY-MM-DD.csv
    │   ├── fno/YYYY-MM-DD.csv
    │   ├── option_chain/YYYY-MM-DD.json
    │   └── macro/vix.csv, usd_inr.csv
    ├── db/
    │   ├── ohlcv.db
    │   ├── corp_actions.db
    │   ├── fundamentals.db
    │   ├── shareholding.db
    │   └── macro.db
    ├── mf_holdings/YYYY-MM.parquet
    ├── features/
    │   ├── daily/YYYY-MM-DD.parquet
    │   └── baseline/stats_baseline.pkl
    ├── models/
    └── logs/pipeline_YYYY-MM-DD.log
```

---

## Database Schemas

### ohlcv_adjusted
```sql
CREATE TABLE ohlcv_adjusted (
    date         TEXT NOT NULL,
    ticker       TEXT NOT NULL,
    open         REAL,
    high         REAL,
    low          REAL,
    close        REAL NOT NULL,
    volume       INTEGER,
    delivery_qty INTEGER,
    delivery_pct REAL,
    adj_factor   REAL DEFAULT 1.0,
    PRIMARY KEY (date, ticker)
);
CREATE INDEX idx_ohlcv_ticker_date ON ohlcv_adjusted(ticker, date);
```

### corporate_actions
```sql
CREATE TABLE corporate_actions (
    ticker            TEXT NOT NULL,
    ex_date           TEXT NOT NULL,
    action_type       TEXT NOT NULL,   -- SPLIT / BONUS / RIGHTS / DIVIDEND
    ratio             REAL NOT NULL,
    announcement_date TEXT,
    record_date       TEXT,
    PRIMARY KEY (ticker, ex_date, action_type)
);
```

### fundamentals (POINT-IN-TIME — use announcement_date, never quarter_end_date)
```sql
CREATE TABLE fundamentals (
    ticker             TEXT NOT NULL,
    fiscal_year        INTEGER NOT NULL,
    quarter            INTEGER NOT NULL,
    quarter_end_date   TEXT NOT NULL,
    announcement_date  TEXT NOT NULL,   -- ← USE THIS for PIT joins
    revenue            REAL,
    ebitda             REAL,
    pat                REAL,
    eps                REAL,
    operating_margin   REAL,
    ebitda_margin      REAL,
    net_margin         REAL,
    roe                REAL,
    roce               REAL,
    debt_to_equity     REAL,
    interest_coverage  REAL,
    fcf                REAL,
    asset_turnover     REAL,
    inventory_days     REAL,
    receivable_days    REAL,
    payable_days       REAL,
    book_value_per_share REAL,
    shares_outstanding INTEGER,
    PRIMARY KEY (ticker, fiscal_year, quarter)
);
CREATE INDEX idx_fund_ticker_ann ON fundamentals(ticker, announcement_date);
```

### shareholding (use filing_date, NOT quarter_end_date)
```sql
CREATE TABLE shareholding (
    ticker           TEXT NOT NULL,
    quarter_end_date TEXT NOT NULL,
    filing_date      TEXT NOT NULL,   -- ← USE THIS for PIT joins (~21 days after QE)
    promoter_pct     REAL,
    promoter_pledge  REAL,
    fii_pct          REAL,
    dii_pct          REAL,
    mf_pct           REAL,
    retail_pct       REAL,
    PRIMARY KEY (ticker, quarter_end_date)
);
CREATE INDEX idx_sh_ticker_filing ON shareholding(ticker, filing_date);
```

### macro_indicators
```sql
CREATE TABLE macro_indicators (
    date      TEXT NOT NULL,
    indicator TEXT NOT NULL,
    value     REAL,
    PRIMARY KEY (date, indicator)
);
```

---

## Corporate Action Adjustment Logic

```python
def apply_corporate_actions(conn, ticker: str) -> None:
    """
    Retroactively adjust all historical OHLCV for a ticker.
    SPLIT ratio=0.5 means 1:2 split → pre-ex prices × 0.5
    BONUS ratio=1.0 means 1:1 bonus → pre-ex prices / (1 + 1.0) = × 0.5
    Apply most-recent action first to avoid double-adjustment.
    """
    actions = pd.read_sql(
        "SELECT * FROM corporate_actions WHERE ticker=? ORDER BY ex_date DESC",
        conn, params=[ticker]
    )
    for _, action in actions.iterrows():
        if action['action_type'] == 'SPLIT':
            adj = action['ratio']
        elif action['action_type'] == 'BONUS':
            adj = 1.0 / (1.0 + action['ratio'])
        else:
            continue  # DIVIDEND: skip for daily OHLCV
        conn.execute("""
            UPDATE ohlcv_adjusted
            SET open=open*?, high=high*?, low=low*?, close=close*?, adj_factor=adj_factor*?
            WHERE ticker=? AND date < ?
        """, (adj, adj, adj, adj, adj, ticker, action['ex_date']))
    conn.commit()
```

---

## Point-in-Time Join (Mixed-Frequency)

```python
def get_latest_as_of(conn, table: str, ticker: str, date: str,
                     date_col: str) -> Optional[pd.Series]:
    """
    Get latest row where date_col <= date.
    For fundamentals: date_col = 'announcement_date'
    For shareholding:  date_col = 'filing_date'
    For MF holdings:   date_col = 'month_end_date' (use ~5th of following month)
    """
    row = pd.read_sql(f"""
        SELECT * FROM {table}
        WHERE ticker=? AND {date_col} <= ?
        ORDER BY {date_col} DESC LIMIT 1
    """, conn, params=[ticker, date])
    return row.iloc[0] if len(row) else None
```

### Staleness features (always compute alongside fundamentals)
```python
def compute_staleness(announcement_date: str, current_date: str) -> dict:
    ann = pd.Timestamp(announcement_date)
    cur = pd.Timestamp(current_date)
    days = (cur - ann).days
    return {
        'days_since_results': days,
        'quarter_age_pct': min(days / 63.0, 1.0),
        'results_pending_flag': int(days > 70)
    }
```

---

## Daily Pipeline Execution (schedule via cron)

```python
# scheduler/daily_pipeline.py
import schedule, logging
from datetime import datetime

def run_pipeline(date: str):
    log = logging.getLogger('pipeline')
    steps = [
        ('4:00 PM', download_bhavcopy, date),
        ('4:30 PM', validate_and_adjust, date),
        ('4:45 PM', compute_technical_features, date),
        ('4:55 PM', compute_macro_pnd_features, date),
        ('5:05 PM', load_quarterly_features, date),
        ('5:15 PM', assemble_feature_matrix, date),
        ('5:20 PM', run_quality_checks, date),
        ('5:22 PM', run_hmm_regime, date),
        ('5:24 PM', run_pnd_prefilter, date),
        ('5:25 PM', run_signal_models, date),
        ('5:26 PM', run_meta_and_conformal, date),
        ('5:28 PM', run_exit_signals, date),
        ('5:30 PM', write_outputs_and_alert, date),
    ]
    for label, fn, *args in steps:
        try:
            fn(*args)
            log.info(f"[{label}] {fn.__name__} completed")
        except Exception as e:
            log.error(f"[{label}] {fn.__name__} FAILED: {e}", exc_info=True)
            raise

# Cron: 0 16 * * 1-5 python daily_pipeline.py
# Option chain scraper: 25 15 * * 1-5 python option_chain.py
```

---

## Data Quality Checks

```python
def quality_check(features_df: pd.DataFrame,
                  baseline_stats: dict) -> dict:
    report = {}
    # 1. Null check
    null_pct = features_df.isnull().sum() / len(features_df)
    report['high_null_features'] = null_pct[null_pct > 0.01].to_dict()
    # 2. PSI drift
    from pipeline.quality.drift_monitor import compute_psi
    psi = {col: compute_psi(features_df[col], baseline_stats[col])
           for col in features_df.select_dtypes('number').columns
           if col in baseline_stats}
    report['psi_breaches'] = {k: v for k, v in psi.items() if v > 0.10}
    report['psi_severe'] = {k: v for k, v in psi.items() if v > 0.25}
    # 3. Complete stocks
    complete = (features_df.isnull().sum(axis=1) == 0).sum()
    report['complete_stocks'] = complete
    report['pipeline_ok'] = (complete >= 450 and
                              not report['psi_severe'] and
                              not any(v > 0.05 for v in report['high_null_features'].values()))
    return report
```

---

## Transaction Cost Model (Indian Equities)

```python
COSTS = {
    'brokerage_pct':    0.0003,   # 0.03% each way
    'stt_sell_pct':     0.0010,   # 0.10% on sell only
    'exchange_pct':     0.0000345,# each way
    'gst_on_brokerage': 0.18,     # 18% of brokerage
    'stamp_buy_pct':    0.00015,  # 0.015% on buy
    'slippage_pct':     0.0010,   # 0.10% estimated (Tier 1-2)
    'smallcap_slip':    0.0030,   # 0.30% for stocks < ₹1Cr ADT
}

def compute_buy_cost(price, qty):
    gross = price * qty
    b = gross * COSTS['brokerage_pct']
    return gross + b + b*COSTS['gst_on_brokerage'] + \
           gross*COSTS['exchange_pct'] + gross*COSTS['stamp_buy_pct'] + \
           gross*COSTS['slippage_pct']

def compute_sell_proceeds(price, qty):
    gross = price * qty
    b = gross * COSTS['brokerage_pct']
    return gross - b - b*COSTS['gst_on_brokerage'] - \
           gross*COSTS['stt_sell_pct'] - gross*COSTS['exchange_pct'] - \
           gross*COSTS['slippage_pct']
```
