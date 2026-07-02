# AlphaLens — System Specifications
## Spec-Driven Development · All Sections · No Duplicates

**Every line of code must trace to a spec ID. No spec = no feature.**

---

## SPEC-SYS: System-Level Requirements

### SPEC-SYS-001 · Universe Coverage
- System monitors 500 stocks (Nifty 500) in Phase 1
- Each stock assigned a Tier (1–5) based on market cap and ADTV
- Tier assignment reviewed quarterly; promotion/demotion automatic

### SPEC-SYS-002 · Daily Pipeline Completion
- Full pipeline completes within 90 minutes of trigger (no fixed start time)
- Pipeline has 15 hours (3:30 PM to 9:15 AM next day) — ample time
- Only the option chain scraper has a fixed time (3:25 PM IST, before market close) —
  laptop-only (SPEC-SCHED-009): non-recoverable if the laptop is asleep/off at that
  exact time, deferred to Phase 2 (not needed for Phase 1 features)
- All other steps run linearly on trigger, not at clock times
- Pipeline failure triggers alert; checkpoint enables resume on next run

### SPEC-SYS-003 · Data Completeness Gate
- Proceed to model inference only if ≥ 450/500 stocks have complete features
- Missing stocks forward-filled from previous day with data_staleness_flag=1

### SPEC-SYS-004 · Availability
- Daily pipeline: best-effort on trading days, laptop-only (SPEC-SCHED-009)
- Option chain capture: best-effort, non-recoverable if missed (see SPEC-SYS-002)

### SPEC-SYS-005 · Storage Budgets
- Raw bhavcopy: 90-day rolling retention
- Feature Parquets: 5-year retention
- Database files (DuckDB + SQLite): periodic backup to an external drive or
  personal cloud storage of the operator's choice (Oracle Object Storage
  dropped along with Oracle Cloud — SPEC-SCHED-009); not yet automated
- Total: < 500 GB on laptop SSD

### SPEC-SYS-006 · No Synthetic Data (CRITICAL)
- The application SHALL NOT generate, fabricate, or fall back to synthetic, mocked,
  jittered, or procedurally-sampled data anywhere in a model-training, scoring, or
  backtesting code path. This applies to every model (M-01 through M-16) and every
  backtest script without exception.
- Every training-data loader (`load_pnd_training_data_from_db`,
  `load_exit_training_data_from_db`, `load_multibagger_training_data_from_db`,
  `load_forensic_training_data_from_db`, `load_ohlcv_from_db`, etc.) sources real
  data only: real OHLCV (`ohlcv_adjusted`), real fundamentals, real documented
  archive cases (`KNOWN_FRAUD_ARCHIVE`, `KNOWN_CLEAN_ARCHIVE`, `KNOWN_PND_TICKERS`,
  `HISTORICAL_MULTIBAGGER_ARCHIVE`), or real accumulated paper-trading history.
- If the real data available is insufficient (below a documented minimum-sample
  threshold), the loader/script MUST raise (`RuntimeError` or `FileNotFoundError`)
  with a message that (a) states what's missing and how much was found vs. required,
  and (b) points to the relevant `BuildLog.md` "Real data sourcing — X" section
  describing how to backfill or compute the missing real data. It MUST NOT
  silently substitute a generated/sampled/randomized stand-in.
- Known historical-archive entries (e.g. `HISTORICAL_MULTIBAGGER_ARCHIVE`'s
  feature vectors) that are themselves still a documented gap pending a real
  15-year OHLCV backfill are flagged in their own module docstrings and in
  BuildLog.md — they are not silently presented as if they were measured.
- **Exemption (test fixtures only):** deterministic, hand-built unit/regression-test
  fixtures that exercise a specific function's logic against known boundary
  conditions (e.g. a constructed OHLCV panel asserting "10x volume spike scores
  >= 70") are not in scope of this spec — they never execute in an application
  code path, are clearly named/documented as test fixtures, and do not feed any
  persisted model artifact. Test fixtures must not be confused with, or substitute
  for, the real-data loaders above; any test that trains a model under test must
  use the real loader (skipping with a clear reason if real data isn't available
  in that environment), not a synthetic generator.
- Verification: `grep -rn "generate_synthetic\|np.random.*training\|synthetic_data"
  systems/ backtest/` (excluding test-fixture helpers explicitly named/documented
  as such) must return no production training/inference code paths.

---

## SPEC-PIPE: Data Pipeline Specifications

### SPEC-PIPE-001 · OHLCV Ingestion
- Source: NSE bhavcopy (daily update), FYERS API (historical backfill)
- All prices stored as corporate-action-adjusted in DataStore normalised layer
- Raw bhavcopy retained in DataStore raw layer for audit
- Ingestion layer writes to DataStore ONLY; consumer systems read via API

### SPEC-PIPE-002 · Corporate Action Adjustment

**Direction:** Backward adjustment. Historical prices/volumes are rewritten to the
current/latest per-share basis. Today's NSE-reported price is always preserved unchanged.

**Per-action adjustment rules (confirmed P3.5):**

| Action   | price_factor                            | vol_factor   |
|----------|-----------------------------------------|--------------|
| SPLIT    | `1 / ratio`                             | `ratio`      |
| BONUS    | `1 / (1 + ratio)`                       | `1 + ratio`  |
| DIVIDEND | `1 − (dividend / raw_close_before_ex)`  | `1.0`        |
| RIGHTS   | 1.0 (subscription price not available)  | `1.0`        |
| Others   | 1.0                                     | `1.0`        |

- **SPLIT direction note (corrected P3.5):** original wording said "× ratio" which is wrong.
  A 1:5 split (ratio=5) means Rs.500 pre-split → Rs.100 adjusted → factor = 1/5 = 0.2.
  Code has always been correct; only the spec wording was wrong.

**Volume adjustment (SPLIT/BONUS only):**
- Pre-action `volume` and `delivery_qty` ×`vol_factor` (share-count units; opposite direction
  to price so volumetric comparisons remain consistent across the ex_date)
- `delivery_pct` = delivery_qty/volume — immune (same factor in numerator and denominator)
- `turnover` (price×volume) — immune (price and vol factors are inverses for SPLIT/BONUS)

**Dividend factor:**
- `price_factor = 1 − (dividend / raw_close_on_last_trading_day_before_ex_date)`
- raw_close recovered as `COALESCE(ohlcv_ca_audit.raw_close, ohlcv_adjusted.close / adj_factor)`
  so earlier split/bonus adjustments do not distort the reference price
- Vol factor = 1.0 (dividends do not change share count)

**Audit table design (P3.5 — replaces earlier raw_* column approach):**
- Original NSE-reported values are stored in `ohlcv_ca_audit`, NOT in ohlcv_adjusted
- Only rows actually modified by the adjuster appear in ohlcv_ca_audit; stocks with no
  corporate actions have zero audit rows (raw == adjusted, nothing to store)
- `raw_*` columns in ohlcv_ca_audit: first write wins (ON CONFLICT DO NOTHING) — the
  original NSE price is preserved forever regardless of adjuster re-runs
- `adj_factor / vol_adj_factor` in ohlcv_ca_audit: always updated to the latest applied
  factors (audit row is self-contained)
- `ohlcv_adjusted.adj_factor` and `ohlcv_adjusted.vol_adj_factor` carry the currently
  applied factors (1.0 = unadjusted). Features use these to know what was applied.
- Invariant: `raw_value = adjusted_value / adj_factor` (prices); same for volume
- Restore a ticker: `UPDATE ohlcv_adjusted SET open=a.raw_open, ..., adj_factor=1.0
  FROM ohlcv_ca_audit a WHERE o.date=a.date AND o.ticker=a.ticker AND a.ticker='TGT'`

**Re-download interaction (daily_pipeline.py):**
- When bhavcopy re-downloads a date (ON CONFLICT): prices updated, factors reset to 1.0
- ohlcv_ca_audit rows for that date are deleted immediately after the upsert — they were
  derived from the old NSE data and would be stale; the next `adjust_prices` run recreates
  them from the fresh NSE prices

**Idempotency:**
- Target `(adj_factor, vol_adj_factor)` computed from full corporate_actions history
- Raw values recovered as `current / adj_factor` — if adj_factor=1.0 (first run), this
  equals the NSE price exactly. Re-runs hit ON CONFLICT and the stored raw_* are preserved.
- Only rows where target ≠ stored (within 1e-9 tolerance) are updated

**Post-adjustment continuity check:**
- `|close[ex_date] − close[day_before]| / close[day_before] < 1%` — WARNING, not hard failure
  (genuine market moves on ex_date can legitimately exceed 1%)

**Feature flag:**
- `PRICE_ADJUSTMENT_ENABLED` in `config/settings.py`
- `False` = `step_adjust_prices` is a no-op; `True` (default since P3.5) = active

**Implementation:** `ingestion/adjust/price_adjuster.py`, audit table: `ohlcv_ca_audit`

### SPEC-PIPE-003 · Point-in-Time Alignment (CRITICAL)
- Fundamentals: use announcement_date (NEVER quarter_end_date)
- Shareholding: use filing_date (NEVER quarter_end_date)
- MF holdings: available from ~5th calendar day of following month
- Any code using quarter_end_date as a join key is a BUILD FAILURE
- Staleness features always computed: days_since_results, quarter_age_pct, results_pending_flag
- PIT enforced centrally by DataStore API (SPEC-DS-003)

### SPEC-PIPE-004 · Feature Computation Performance
- 76 core technical features for 500 stocks in < 15 minutes on reference hardware
- Must be fully vectorized; no Python loops over individual stocks

### SPEC-PIPE-005 · Data Quality Checks
- Null check: flag any feature with > 1% nulls
- PSI: top 50 features vs baseline; alert if > 0.10
- Range check: ratio features in [0.1, 10.0]; delivery_pct in [0, 100]
- Completeness gate: ≥ 450/500 stocks pass before model inference

### SPEC-PIPE-006 · Macro Data Ingestion
- India VIX from NSE daily; fallback to previous day if unavailable
- USD/INR, Crude, Gold from Yahoo Finance; retry 3 times on failure
- FII/DII from NSE; mark unavailable if scrape fails (non-critical)

### SPEC-PIPE-007 · Corporate Actions Ingestion
- Source: NSE JSON API (`/api/corporates-corporateActions?index=equities`), EQ series only
- Runs daily as a non-critical pipeline step (`download_corporate_actions`) after macro
  download and before `adjust_prices` — an outage must never block features/models
- Action types stored: SPLIT, BONUS, DIVIDEND, RIGHTS, BUYBACK, QIP, AGM, OTHER
- Ratio semantics per action type (all in `corporate_actions` schema comment):
  - SPLIT: new shares per old share (e.g. FV 10→2 → ratio=5)
  - BONUS: bonus shares per held share (e.g. 1:1 → ratio=1; 1:2 → ratio=0.5)
  - DIVIDEND: INR per share (e.g. Rs.10/share → ratio=10.0)
  - RIGHTS: rights shares per held share (e.g. 1:5 rights → ratio=0.2)
  - BUYBACK/QIP/AGM/OTHER: ratio=0.0 (no price-adjustment effect)
- `details VARCHAR` column stores the raw NSE purpose string verbatim for audit and
  re-parsing without a re-fetch
- Idempotent: `ON CONFLICT (ticker, ex_date, action_type) DO NOTHING`
- Raw JSON retained at `datastore/raw/corporate_actions/{YYYY-MM-DD}.json`
- `announcement_date` is NOT exposed by NSE's CA endpoint — set to NULL; features that
  require announcement_date must not use this table as a PIT-correct source without it
- Implemented in `ingestion/scrapers/corporate_actions.py`

### SPEC-PIPE-008 · Large Deals (Bulk + Block) Ingestion
- **Definitions:**
  - Bulk Deal: a single market transaction where ≥ 0.5% of a company's total listed shares
    are traded (SEBI circular). Reported to NSE/BSE by end of day.
  - Block Deal: a single transaction of ≥ 500,000 shares OR ≥ Rs. 10 crore, executed
    exclusively in the block deal window (9:15–9:30 AM IST). Reported immediately.
- **Sources:** NSE (historical bulk/block deal API + snapshot fallback) and BSE (open API)
- **Endpoint map:**
  - NSE Bulk: `https://www.nseindia.com/api/historical/bulk-deals?from=DD-MM-YYYY&to=DD-MM-YYYY`
  - NSE Block: `https://www.nseindia.com/api/historical/block-deals?from=DD-MM-YYYY&to=DD-MM-YYYY`
  - BSE Bulk: `https://api.bseindia.com/BseIndiaAPI/api/BulkDeals/w?strdate=DDMMYYYY&enddate=DDMMYYYY`
  - BSE Block: `https://api.bseindia.com/BseIndiaAPI/api/BlockDeals/w?strdate=DDMMYYYY&enddate=DDMMYYYY`
- **`large_deals` table schema:**
  - `trade_date`, `exchange` (NSE/BSE), `deal_type` (BULK/BLOCK), `ticker`
  - `client_name`, `transaction_type` (B/S), `quantity`, `price`, `remarks`
- No PRIMARY KEY — delete-then-insert per (trade_date, exchange, deal_type) per day;
  multiple deals per client per stock per day are valid
- Each of the 4 sources fetched independently; any single source failure is caught, logged,
  and skipped — the others still contribute rows (SPEC-PIPE-006 "non-critical" pattern)
- BSE ticker field uses BSE's SCRIP_ID which usually matches NSE symbol but may differ;
  no automatic reconciliation — cross-reference via company name if needed
- Raw JSON retained at `datastore/raw/large_deals/{date}_{exchange}_{type}.json`
- Implemented in `ingestion/scrapers/large_deals.py`

### SPEC-PIPE-009 · Large Deals Family Filter (Clean Layer)
- **Problem:** A significant fraction of reported bulk/block deals are intra-family
  transfers — e.g. Rakesh Jhunjhunwala selling to Rekha Jhunjhunwala, or RARE Enterprises
  (his vehicle) transferring to a family trust. These are not open-market buys/sells by
  independent participants. Including them in signals creates false activity flags.
- **Clean layer definition:** A `large_deals_clean` daily Parquet (or DuckDB view) derived
  from `large_deals` with all intra-family and intra-promoter-group pairs removed.
- **Family identification — three-tier approach (all applied in order; any match → drop):**
  1. **Entity-family registry** (primary, highest precision): A curated table
     `entity_families(entity_name VARCHAR, family_name VARCHAR, entity_type VARCHAR)`
     mapping known investor entities to their family. Seeded from Trendlyne StratQ's
     superstar investor list (which names the entity/vehicle alongside the person) and
     manually maintained. Examples: `("RARE Enterprises", "Jhunjhunwala", "HNI_VEHICLE")`,
     `("Rekha Jhunjhunwala", "Jhunjhunwala", "HNI_INDIVIDUAL")`. A deal where both buyer
     and seller map to the same `family_name` → `is_intra_family = True`.
  2. **Surname similarity** (secondary, broader net): Extract the last token of
     `client_name` after stripping common suffixes (`LLP`, `Ltd`, `PVT`, `Trust`,
     `Fund`, `Enterprises`, `Capital`). If two same-stock same-day counterparties share
     the same cleaned surname → `is_intra_family = True`. Handles new entrants not yet
     in the registry. Prone to false positives for common surnames (Patel, Sharma, Singh)
     — mitigated by requiring the match on same stock, same day.
  3. **Promoter cross-reference** (tertiary): If both client names appear in
     `shareholding` promoter disclosures for the same ticker → `is_intra_promoter = True`
     (a subset of intra-family but covers non-family promoter group transfers).
- **Output columns added to clean layer:** `is_intra_family BOOL`, `is_intra_promoter BOOL`,
  `family_name VARCHAR` (NULL if not matched), `filter_reason VARCHAR`
- **Source preference:** If Trendlyne or Tijori already tag deals with entity type
  (promoter/FII/DII/HNI/institution), persist that tag directly and use it to enrich
  the family registry rather than rebuilding from scratch.
- **Implementation target:** P3.5. `large_deals` raw table is built and populated in
  P3.4; the clean-layer filter is a separate daily post-processing step added after
  `download_large_deals` in the pipeline.
- **Spec for clean layer retention:** Raw `large_deals` rows are never deleted — only the
  clean Parquet is filtered. This preserves the ability to re-run filtering with an
  updated registry without re-fetching from NSE/BSE.

### SPEC-MFHOLD-001 · MF Holdings Sourcing Strategy (P2.2)
- AMFI does not centrally host scheme-wise portfolio holdings — SEBI's
  25-Aug-2022 circular mandates each AMC publish its own monthly
  disclosure independently, on its own website, in its own format.
- Primary source: **Groww** (groww.in) — verified live to mirror every
  AMC's real disclosure (49 AMCs) via a single consistent format
  (Next.js SSR `__NEXT_DATA__` JSON), reachable with a plain
  unauthenticated HTTP GET. Per-holding ISIN is NOT exposed by Groww —
  resolved via normalized-company-name match against `config.universe`'s
  real `isin` column. Share quantity is NOT exposed — only % of AUM
  (`corpus_per`); `quantity` is left null rather than fabricated.
- **Groww exposes only the current live snapshot — it has no historical
  archive.** A fetch for month M is only valid if Groww's live data is
  itself currently dated M; this must be checked against the live
  response's own `portfolio_date`, never assumed. A scheduled job that
  misses a given month's live window has no way to recover it via Groww.
- Secondary, higher-precision cross-check: **SBI Mutual Fund**'s own
  direct portfolio-disclosure page (real ISIN + real share quantity,
  and — unlike Groww — a genuine multi-month historical archive via its
  own year/month selector). Used to validate Groww-derived SBI rows and
  as the only fallback source if a past month is ever needed for SBI
  specifically.
- Architecture: source-agnostic `AMC_REGISTRY` (SPEC-SOLID-002,
  Open/Closed) — new AMC coverage is added by registering a
  `(fetch_fn, parse_fn)` pair, never by editing the registry/
  orchestration core.
- Ingestion runs twice monthly (config.settings.MF_HOLDINGS_SCHEDULE_DAYS,
  default day 5 and day 20) rather than once, because AMC disclosure
  timing varies and Groww's "current snapshot" can change mid-cycle.
- PIT (SPEC-PIPE-003): `availability_date` = the
  `MF_HOLDINGS_AVAILABILITY_DELAY_DAYS`-th day of the month following
  the disclosure month, stamped on every row at write time — features
  consume this column, never `month` directly.

---

## SPEC-FEAT: Feature Engineering Specifications

### SPEC-FEAT-001 · Minimum History Requirement
- Stocks with < 252 trading days excluded from model inference
- Features requiring N-day lookback return NaN if insufficient history

### SPEC-FEAT-002 · Normalization
- Price-ratio features naturally normalized; no further scaling
- Fundamental features: sector-relative z-scores (z = (x - sector_mean) / (sector_std + 1e-8))
- Z-scores clipped to [-5, +5]

### SPEC-FEAT-003 · Cyclical Encoding
- month_of_year: sin/cos pair (sin(2π×month/12), cos(2π×month/12))
- day_of_week: sin/cos pair
- Raw integer columns NOT passed to models

### SPEC-FEAT-004 · F&O Features Scope
- 16 F&O features only for ~250 F&O-eligible stocks; NaN for others
- LightGBM handles via native missing-value support

### SPEC-FEAT-005 · Sector Definitions
- NSE sector index classifications as primary labels
- Minimum: split Financials and Non-Financials for z-score normalization

---

## SPEC-MODEL: Model Specifications

### SPEC-MODEL-001 · Training Data Minimum
- Signal models: ≥ 252 trading days per fold
- Multibagger: ≥ 756 trading days for meaningful label coverage

### SPEC-MODEL-002 · Label Construction (Triple-Barrier)
- Horizon: exact trading days (5d/21d/63d)
- Barriers: 1.5× ATR (5d), 3× ATR (21d), 5× ATR (63d)
- Labels: +1 (upper hit first), -1 (lower hit first), 0 (vertical hit first)
- P&D episodes excluded from positive labels in multibagger model

### SPEC-MODEL-003 · Walk-Forward Validation
- Expanding training window, 1-year test window, minimum 3 folds
- No data from test year used in training, HPO, or threshold selection
- Selection metric: walk-forward Sharpe (not accuracy)

### SPEC-MODEL-004 · Class Imbalance Handling
- Threshold optimization mandatory; never use 0.5 default
- SMOTE on training data ONLY; never validation/test
- Class weight logging: positive/negative ratio before and after resampling

### SPEC-MODEL-005 · Model Versioning
- Every model saved as {model_name}_v{YYYYMMDD}_{fold}.pkl
- Production model symlinked as {model_name}_current.pkl
- Previous 3 versions retained for rollback
- Metadata in datastore/models/registry.json

### SPEC-MODEL-006 · P&D Pre-Filter (CRITICAL)
- P&D runs BEFORE all other models every day
- Score > 60: hard block; stock removed from all buy and multibagger candidates
- Score > 40: flag only; user sees warning
- Blocked stocks listed in daily alert

### SPEC-MODEL-007 · Conformal Prediction Coverage
- Target: 90% coverage (α = 0.10)
- ACI variant required (not standard CQR) for time-series non-exchangeability
- Validate monthly on last 63 days; alert if actual coverage < 85%

### SPEC-MODEL-008 · Retrain Protocol
- Steps: snapshot → train new → shadow-test 63 days → compare (accuracy + calibration + SHAP rank) → promote if wins ≥ 2/3
- Logged in registry.json with comparison metrics

### SPEC-MODEL-009 · Forensic Classical Scores
- Beneish M > -1.78 = likely manipulator; Altman Z < 1.81 = distress zone; Piotroski F ≥ 7 = strong
- No training required — pure formula computation from quarterly fundamentals
- 30 forensic features also feed directly into signal models as features (Phase 2)

### SPEC-MODEL-010 · Forensic ML Model
- Trained on confirmed Indian fraud cases (Satyam, DHFL, IL&FS, Yes Bank, Vakrangee, etc.)
- 84 features across 9 groups (Groups A–I from forensic specification)
- Must retrospectively flag > 70% of historical Indian frauds
- Nifty 50 false positive rate: ≤ 2/50 stocks get 'red' flag
- 12 sector-specific sub-models

---

## SPEC-BT: Backtesting Specifications

### SPEC-BT-001 · Walk-Forward Integrity
- All 9 backtesting rules are hard constraints (see 04_backtesting.md)
- Any violation invalidates the backtest

### SPEC-BT-002 · Transaction Cost Accuracy
- All 6 cost components: brokerage, STT, exchange, GST, stamp, slippage
- Small-cap slippage: 0.30% for ADTV < ₹1Cr
- Round-trip total: ~0.40–0.50%

### SPEC-BT-003 · Survivorship Bias
- Universe includes ALL stocks ever in Nifty 500 during backtest period
- Delisted stocks: forced-exit at last known price, logged as 'delisted' exit

### SPEC-BT-004 · Performance Reporting
- All 9 metrics reported per fold AND aggregated
- std(Sharpe) always reported alongside mean Sharpe
- 4 benchmarks: Nifty buy-hold, equal-weight 50, 6m momentum, random 20
- DSR applied when > 20 configurations tested

---

## SPEC-UI: User Interface Specifications

**[AS BUILT, 2026-07-01]** The dashboard was rebuilt from a single 6-page
dark-theme app into the 27-screen, 5-app structure defined in
`alphalens_docs/screens/SCREEN_INVENTORY.md` (AlphaLens.ML, .Technical,
.Fundamental, .Valuation, .Forensic), served from `dashboard/static/{ml,
technical,fundamental,valuation,forensic}/` and mounted at `/ui` by
`datastore/api/main.py`. SPEC-UI-001 through 005 below are updated in
place to point at the new screen locations (the screen *names* and IDs
they originally described map onto the ML and Forensic apps below).
SPEC-UI-006 (dark theme) is superseded — see SPEC-UI-006 itself.
SPEC-UI-007 through 010 are new, covering the app-shell/cross-link
pattern, the Technical/Fundamental/Valuation apps, and the no-fabrication
empty-state rule. Plan: `squishy-frolicking-whisper.md`.

### SPEC-UI-001 · Daily Insight Hub (ML-A, was "Daily Dashboard / Screen A")
- File: `dashboard/static/ml/index.html` + `ml/js/hub.js`
- Shows: top 5 buy signals (as insight cards + full table), today's
  alerts, multibagger top-3, active-positions top-3, regime strip
- ALL data reads from DataStore API (SPEC-DS-002); no direct database access
- Updates once per day after pipeline completes; refresh-on-demand

### SPEC-UI-002 · Signal Deep Dive (ML-B, was "Signal Detail / Screen B")
- File: `dashboard/static/ml/signal.html` + `ml/js/signal.js`
- Per-stock: SHAP bars (parsed from `shap_top5_json`, only rendered when
  present — never fabricated), all per-horizon model scores, conformal
  intervals, latest OHLCV price card, 30-day regime history chart
- Reads: `/api/v1/signals/ml/{ticker}/{date}`,
  `/api/v1/signals/ml/forensic/{ticker}`,
  `/api/v1/signals/ml/multibagger/{ticker}`, `/api/v1/ohlcv/{ticker}/latest`,
  `/api/v1/macro/regime/history`

### SPEC-UI-003 · Multibagger Watchlist (ML-C, was "Screen C")
- File: `dashboard/static/ml/multibagger.html` + `ml/js/multibagger.js`
- Top-N ranked by multibagger probability (full prototype table: tier,
  archetype, 6/12/18/24/36-month survival columns)
- Updated weekly; reads `/api/v1/watchlist/current`

### SPEC-UI-003b · Position Monitor (ML-D, net-new, folds in former
  standalone Paper Trading screen)
- File: `dashboard/static/ml/positions.html` + `ml/js/positions.js`
- Gate 7 progress, portfolio snapshot, open positions table, equity-curve
  chart, recent closed trades — `/api/v1/paper_trading/*` has no
  dedicated screen ID of its own in `SCREEN_INVENTORY.md`, so it lives
  here since it's real, ticker-position data closely tied to ML-D's
  "Position Monitor" concept

### SPEC-UI-004 · Forensic Dashboard suite (FOREN-A through G, was
  "Forensic Alert / Screen D")
- Files: `dashboard/static/forensic/{dashboard,redflag,benford,cashflow,
  heatmap,report,universe}.html` + matching `forensic/js/*.js`
- FOREN-A score ring + classical/ML scores + SHAP + pattern match;
  FOREN-B per-metric red-flag drill-down (severity thresholds are the
  real `classical_scores.py` constants — Beneish -1.78, Altman 1.81/2.99,
  Piotroski <=2, Sloan accrual >0.10 — not invented cutoffs); FOREN-C
  Benford MAD (digit-distribution histogram is empty-stated — the API
  doesn't expose per-digit counts); FOREN-D accrual-quality + FCF/PAT
  quarterly table; FOREN-E peer heatmap (N+1 fetch over `/flagged`);
  FOREN-F templated (non-generative) investigation report with
  browser-print PDF export; FOREN-G universe scan ranked table
- Reads: `/api/v1/signals/ml/forensic/{summary,flagged,{ticker}}`,
  `/api/v1/fundamentals/{ticker}/history`

### SPEC-UI-005 · Backtest Dashboard (ML-E, was "Backtest Results / Screen E")
- File: `dashboard/static/ml/backtest.html` + `ml/js/backtest.js`
- Fold-by-fold results, integrity-passed badge, aggregate stat cards —
  rendered generically over whichever `phase1`/`phase2`/`phase3` keys a
  given report JSON contains (reports are free-form dicts, no fixed schema)
- Reads: `/api/v1/backtest/reports`, `/api/v1/backtest/reports/{name}`

### SPEC-UI-006 · Design System and Performance
- **[AS BUILT, 2026-07-01] Light theme, superseding the original dark-theme
  mandate** — `dashboard/static/css/{tokens,components,shell}.css`, ported
  verbatim from the `alphalens_docs/screens/alphalens_*.html` prototypes
  (DM Sans + JetBrains Mono, `bg #F8F9FC`/`card #FFF`/`teal #0A9B8E` palette)
- Dashboard renders in < 3 seconds
- No CSV export implemented (was aspirational in the original spec; not
  built in either the dark-theme or light-theme UI — out of scope for the
  2026-07-01 rebuild, which was UI-structure/theme only)

### SPEC-UI-007 · 5-App Shell and Cross-Linking
- `dashboard/static/js/shell.js`'s `APPS` config + `renderAppShell()`
  drive a 2-tier nav: top app-bar switches between the 5 apps, a
  `.sub-tabs` row switches between that app's own screens
- `dashboard/static/js/crosslink.js`'s `buildCrossLinks(ticker)` renders
  "View in X" links to the same ticker in the other 4 apps; links to
  apps with no backend still navigate — the target renders its own
  empty-state (SPEC-UI-010) rather than the link being hidden

### SPEC-UI-008 · AlphaLens.Technical (TA-A through E)
- Files: `dashboard/static/technical/{chart,screener,compare,alerts,
  overview}.html`
- **No backend exists** (`systems/technical_analysis/` is an empty stub —
  no indicators, pattern detection, or screener engine). All 5 screens
  render SPEC-UI-010's empty-state only; do not wire raw
  `/api/v1/features/{ticker}` columns into TA-A as a faux screener (that
  would be misleading half-functionality)

### SPEC-UI-009 · AlphaLens.Fundamental (FA-A through F) and
  AlphaLens.Valuation (VAL-A through D)
- Files: `dashboard/static/fundamental/{dashboard,peers,sector,screener,
  thesis,management}.html`, `dashboard/static/valuation/{dcf,relative,
  batch,accuracy}.html`
- FA-A (raw quarterly fundamentals) and FA-F (governance/shareholding)
  are Partial: real data renders, but sub-panels needing unbuilt analysis
  (peer-relative traffic-light ranking, related-party-transaction tables)
  are empty-stated and visually separated from the real data, never blended
- FA-B/C/D/E are No-Backend (`systems/fundamental_analysis/` stub beyond
  raw fundamentals)
- All 4 Valuation screens are No-Backend (`systems/damodaran_valuation/`
  is an empty stub; Damodaran valuation remains deferred per this file's
  Phase 3+ scope note)

### SPEC-UI-010 · No-Fabrication Empty State
- `dashboard/static/js/empty_state.js`'s `renderEmptyState()` +
  `BACKEND_STATUS` map is the **only** sanctioned way to represent a
  screen or sub-panel with no real backend. Per Absolute Rule #6 (no
  synthetic/mocked data, ever), no screen renders prototype sample data
  (e.g. `TATAMOTORS`, `₹825`, `+4.2%`) as placeholder content — verified
  by a `grep` audit for known prototype sample tokens across
  `dashboard/static/` at the end of the 2026-07-01 rebuild (zero matches)

---

## SPEC-PT: Paper Trading Specifications

### SPEC-PT-003 · Pending Actions (Review/Approve)
**[AS BUILT, 2026-07-01]** `config.settings.PAPER_TRADING_REQUIRE_APPROVAL`
(default `True`) switches `scripts/run_daily_paper_trading.py` from
auto-execute to propose-then-approve: candidate exits/entries are computed
exactly as before (`systems/ml_signal_engine/inference/paper_trading_step.py`'s
new `propose_daily_exits`/`propose_daily_entries`, siblings of the
unchanged `apply_daily_exits`/`apply_daily_entries` that still back the
unattended historical-bootstrap sim) but written to
`paper_trading/pending/{date}.json` instead of executed immediately.
- `GET /api/v1/paper_trading/pending` — today's (or latest) unactioned proposals
- `POST /api/v1/paper_trading/pending/{action_id}/accept` — executes via
  `PortfolioSimulator.buy/sell/reduce_position` (re-fetching the live price,
  not the propose-time one) and logs through `PaperTradingTracker`
  identically to a bot-executed trade
- `POST /api/v1/paper_trading/pending/{action_id}/reject` — no portfolio mutation
- All reads/writes of `portfolio_state.json` (bot and accept-endpoint alike)
  are serialized through `datastore/api/utils/file_lock.py`'s `flock`-based lock
- Gate 7's "CSV exists ⇔ bot genuinely ran" invariant is unaffected — the
  heartbeat/open-row CSV log still happens every run regardless of whether
  a human has acted on that day's proposals yet
- A pending file from a prior date is discarded (not executed) at the
  start of the next run — its candidates were scored against stale signals
- UI: `dashboard/static/ml/positions.html`'s "Pending Actions" panel

### SPEC-PT-003 addendum · Backdated Entries
**[AS BUILT, 2026-07-01]** `POST /api/v1/paper_trading/backdated_buy`
`{ticker, date, quantity?}` — lets the user review a past date's real
recommendations (`GET /api/v1/signals/ml/top_buys/{date}`, which already
supports any historical date) and open a position dated to that day at
that day's real close (`GET /api/v1/ohlcv/{ticker}?from=D&to=D`). Calls
the same `PortfolioSimulator.buy()` and `PaperTradingTracker.log_trade()`
every other trade path uses — no new trade-execution logic.

**Explicit, user-confirmed trade-off:** backdated trades log into the same
`paper_trading/executions/{date}.csv` files Gate 7 counts toward its
>=90-day genuine-forward-time-operation proof, exactly like a live trade
— no separate directory, no special-casing. This means a backdated entry
inflates Gate 7's day-count with a day the bot didn't actually run live
on. Three alternatives (separate log directory; skip CSV logging
entirely; accept as-is) were presented to the user during planning; **the
user chose to accept the distortion rather than special-case backdated
trades.** This is intentional and documented (code docstring in
`datastore/api/routers/paper_trading.py`, a persistent info banner on the
UI's Backdated Entry section) — not an oversight, and must not be
"corrected" without re-confirming with the user first.

---

## SPEC-TA: Technical Analysis API Specifications

### SPEC-TA-004 · Technical Analysis API Scaffolding
**[AS BUILT, 2026-07-01]** `datastore/api/routers/technical.py`
(`/api/v1/ta/*`) exposes the 94 real technical features already computed
daily by `features/technical.py` (76 core), `features/advanced_technical.py`
(18 advanced), and `features/pattern_scores.py` (6 pattern scores) — all
already merged into the daily feature Parquet by `features/matrix_builder.py`.
This is pure API scaffolding; no new feature computation.
- `GET /{ticker}/indicators`, `GET /{ticker}/patterns` — read straight
  from the daily Parquet (`datastore/api/utils/feature_store.py`)
- `GET /compare?tickers=A,B,C` — real RS/beta/alpha plus a real pairwise
  return-correlation matrix computed from OHLCV
- `GET /market_overview` — advances/declines/sector breadth from the
  latest 2 trading days' OHLCV grouped by the real sector map
- UI: `dashboard/static/technical/{chart,compare,overview}.html` are real;
  `screener.html` (needs the still-unimplemented SPEC-TA-003 42-template
  engine) and `alerts.html` (needs stateful CRUD + a background checker)
  stay empty-state — both need real new logic, not scaffolding

---

## SPEC-FA: Fundamental Analysis API Specifications

### SPEC-FA-008 · Composite Scores, Peer Ranking, Screener, API Scaffolding
**[AS BUILT, 2026-07-01]** The 30 fundamental ratios (27 sector-relative
z-scored per SPEC-FEAT-002) and 12 governance features were already
computed daily and merged into the feature Parquet — only composite
scoring and peer-ranking were missing, never implemented in the
`systems/fundamental_analysis/{quality,growth,management,peers}/` stubs.
New `features/fundamental_composites.py`: `quality_score`/`growth_score`
(weighted sector-z-score composites, 0-100 display scale, documented
non-tuned weights), `management_quality_score` (raw governance fields —
not z-scored), `select_peers` (sector + market-cap-proximity ranking),
3 screener presets (`quality_compounder`, `garp`, `turnaround` — defined
as sector-relative z-score thresholds, since the feature store only
carries z-scores, not raw percentages; "quality compounder" means "above
sector peers," not an absolute threshold).

New endpoints in `datastore/api/routers/fundamentals.py` (registered
before the dynamic `/{ticker}` route): `GET /{ticker}/ratios`,
`GET /{ticker}/peers`, `GET /sector/{sector}` (real standard-ratio
aggregate; sector-*unique* metrics like GNPA/ANDA are never computed by
anything in this codebase, so stay unexposed), `GET /screener`,
`GET /{ticker}/scores`.

UI: `fundamental/dashboard.html` (FA-A) traffic-light now real;
`peers.html` (FA-B), `screener.html` (FA-D) fully real; `sector.html`
(FA-C) real standard-ratio aggregate with sector-unique metrics kept
separately empty-stated; `thesis.html` (FA-E) real templated
strengths/risks synthesis (non-generative, same pattern as Forensic's
FOREN-F); `management.html` (FA-F) gained the real
`management_quality_score` badge.

---

## SPEC-ALERT: Alerting Specifications

### SPEC-ALERT-001 · Alert Types
| Alert | Priority | Trigger |
|-------|----------|---------|
| P&D Block | CRITICAL | pnd_score > 60 on held position |
| Exit Urgent | HIGH | exit_urgency > 80 |
| Forensic Red | HIGH | forensic_flag → 'red' |
| Model Drift | HIGH | PSI > 0.25 |
| Pipeline Failure | HIGH | Pipeline not completed within 90 min of trigger |
| Exit Warning | MEDIUM | exit_urgency 60–80 |
| Forensic Amber | MEDIUM | forensic_flag → 'amber' |
| Buy Signal | LOW | High-conviction buy |
| Watchlist Change | LOW | Multibagger entry added/removed |

### SPEC-ALERT-002 · Delivery
- Phase 1: console + log file
- Phase 2: email (SMTP)
- All alerts include: timestamp, ticker, type, reason, action

---

## SPEC-SEC: Security Specifications

### SPEC-SEC-001 · Credentials
- No API keys, passwords, or tokens in source code or VCS
- All credentials in .env file (gitignored) or OS keychain
- config/settings.py loads via os.environ.get() only

### SPEC-SEC-002 · Data Access
- Database files: local-only, no network exposure
- Laptop-only operation (SPEC-SCHED-009) — no cloud credentials in scope
  today; if Oracle Cloud is revisited later, credentials belong in OCI CLI
  config, never in Python source

---

## SPEC-QUALITY: Code Quality Specifications

### SPEC-QUALITY-001 · Test Coverage
- pipeline/ and systems/: minimum 80% line coverage
- Critical paths (PIT join, corp action adjustment, P&D pre-filter): 100% coverage
- All tests via pytest from project root

### SPEC-QUALITY-002 · Documentation
- Every public function: docstring with inputs, outputs, PIT assumptions
- CLAUDE.md: single source of truth, updated each phase

### SPEC-QUALITY-003 · Code Style
- PEP 8 via flake8
- Type hints on all public function signatures
- No print() in production; use logging module
- No hardcoded paths; all via config/settings.py

---

## SPEC-DS: DataStore Specifications

### SPEC-DS-001 · Central Data Ownership
- All ingestion writes ONLY to DataStore raw and normalised stores
- No consumer system directly accesses external sources
- All consumer systems read exclusively through DataStore API
- All consumer systems write outputs back through DataStore API

### SPEC-DS-002 · API-First Access
- FastAPI at localhost:8000 is the sole interface for consumer systems
- Direct database file access permitted only within ingestion and feature layers
- Consumer systems use httpx to call API, never import db modules
- Swagger docs at /docs always available

### SPEC-DS-003 · Point-in-Time Enforcement at API Level
- Every temporal endpoint supports as_of query parameter
- Fundamentals: returns only rows where announcement_date <= as_of
- Shareholding: returns only rows where filing_date <= as_of
- MF holdings: returns only rows where month_end + 5 days <= as_of
- Consumer systems MUST NOT implement their own PIT logic

### SPEC-DS-004 · Write-Back Protocol
- Consumer outputs written via POST to signals store
- All writes include: date, ticker, system_name, system_version
- Upserts: same date+ticker+system replaces, never duplicates
- Schema validation: Pydantic; malformed writes rejected with 422
- All writes logged: timestamp, system, row count

### SPEC-DS-005 · Cross-System Signal Fusion
- Any consumer may read another system's outputs from signals store
- ML reads valuation_gap_pct from Damodaran (Phase 3+)
- ML reads pattern_score from TA system (Phase 3+)
- ML reads quality_score from FA system (Phase 4+)
- Access: GET /api/v1/signals/{system}/{ticker}/{date}

### SPEC-DS-006 · Feature Catalog
- Every feature documented in feature_catalog.json
- Entry: category, update_freq, source_store, pit_rule, phase, range, consumers
- New features must have catalog entry before being written

### SPEC-DS-007 · Six Stores (DuckDB + Parquet + SQLite hybrid)
- Store 1 (Raw): unmodified source data; 90d rolling daily, indefinite quarterly
- Store 2 (Normalised): cleaned, adjusted, PIT-tagged — **DuckDB** persistent database
- Store 3 (Features): daily Parquet matrices — queried directly by DuckDB (no import needed)
- Store 4 (Signals): all system outputs — **DuckDB** persistent database
- Store 5 (Models): versioned model files + registry.json
- Store 6 (Outputs): UI-ready aggregated JSON
- Pipeline log + scheduler job store: **SQLite** (transactional writes)
- DuckDB for analytical stores (OHLCV, fundamentals, features, signals)
- SQLite only for transactional stores (pipeline_log, scheduler, checkpoints)

---

## SPEC-SCHED: Scheduler & Resilience Specifications

### SPEC-SCHED-001 · Flexible Scheduling Modes
- Three modes: 'linear' (sequential on trigger), 'timestamp' (clock-based), 'manual'
- Mode configurable in settings.py per job
- Only option chain scraper uses 'timestamp' (time-sensitive: must run before market close)
- All other jobs use 'linear' — no hardcoded clock dependency
- Pipeline has 15 hours (3:30 PM to 9:15 AM) to complete; no fixed start time

### SPEC-SCHED-002 · Checkpoint-Resume on Failure
- Every pipeline step writes a checkpoint to pipeline_checkpoints table on success
- If pipeline crashes at step N, next run resumes from step N (does not re-execute 1 to N-1)
- Checkpoints tracked per run_id + step_id in SQLite (transactional — not DuckDB)
- Partial runs (some steps failed) marked as 'partial' — resume on next startup
- Each step has configurable retry_count (default 3) and retry_delay_seconds (default 60)

### SPEC-SCHED-003 · Unlimited Backfill (No Maximum Gap)
- Gap detector has NO maximum window; works for 1 day or 100 days missed
- Backfills every NSE trading day between last success and today, chronologically
- Each backfill day runs with full checkpointing (checkpoint-resume applies)
- If backfill day 3 of 7 fails: day 3 marked partial; days 4–7 still process; day 3 retried on next startup

### SPEC-SCHED-004 · Chronological Backfill Order
- Process oldest gap first; never skip, never reorder
- Corporate actions re-checked before each gap day's features
- Features use only data as-of that gap day (PIT in backfill)

### SPEC-SCHED-005 · Pipeline State Tracking
- pipeline_runs table: run_id, run_date, status, is_backfill, total/completed/failed/skipped steps
- pipeline_checkpoints table: per-step status, duration, rows_processed, error_message, retry_count
- This is the source of truth for gap detection and resume logic
- Stored in SQLite (not DuckDB) — transactional writes

### SPEC-SCHED-006 · No Model Inference During Backfill
- Signals, P&D scores, exit alerts: run ONLY for today's date
- Gap days: data + features only, NOT predictions
- Each step declares is_backfillable=True/False; non-backfillable steps skipped during backfill

### SPEC-SCHED-007 · Retrain Catch-Up
- After each daily pipeline: check if any model overdue (days > interval × 1.5)
- Trigger retrain after today's pipeline completes, never during
- Retrain jobs have their own checkpointing

### SPEC-SCHED-008 · Holiday Awareness
- NSE trading calendar in config/nse_holidays.py; updated annually
- Holidays excluded from gap detection — no backfill attempted

### SPEC-SCHED-009 · Laptop-Only Operation (formerly "Oracle Cloud Independence")
- All scraping and pipeline execution runs on the laptop, via
  ingestion/scheduler/daily_pipeline.py registered as a persistent
  APScheduler job (SPEC-SCHED-001) — not OS-level cron, not a separate
  Oracle Cloud instance.
- Oracle Cloud Free Tier provisioning was attempted and abandoned: the
  ARM A1.Flex shape had zero free capacity in ap-mumbai-1 (confirmed at
  1, then 4, OCPU), and the account's Free Trial status blocked
  subscribing to an alternate region without upgrading to Pay-As-You-Go.
  See BuildLog.md "Laptop-only pivot" for the full investigation.
- This spec originally described "Oracle-first, NSE-archive-fallback"
  sourcing. The fallback path (NSE archives, gap-detector-driven catch-up)
  was already a first-class part of the design — laptop-only operation is
  that same fallback path running as the *only* path, not a new
  architecture. No ingestion code depended on Oracle directly (no `oci`
  SDK calls anywhere in ingestion/); only a few settings constants
  referenced Oracle and have been updated (config/settings.py).
- Known consequence: NSE's live option-chain endpoint (PCR, IV, max pain,
  captured intraday at 3:25 PM IST) is non-recoverable if the laptop is
  asleep/off at that time — `OPTION_CHAIN_RECOVERABLE = False` in
  config/settings.py. This is a Phase 2 concern (F&O features); Phase 1
  needs only FYERS backfill + daily bhavcopy + VIX/FII, all of which are
  recoverable via NSE archives through the gap-detector's unlimited,
  chronological backfill (SPEC-SCHED-003, SPEC-SCHED-004).
- Revisit Oracle Cloud only if/when always-on intraday capture becomes
  necessary (Phase 2+) — not before.

### SPEC-SCHED-010 · Atomic Writes
- Feature Parquets: write to temp file, then rename (atomic)
- No partial files ever appear in feature store
- DuckDB inserts: wrapped in transactions; rollback on failure

### SPEC-SCHED-011 · Step Dependencies
**[AS BUILT, 2026-07-02]** Each step in `ingestion/scheduler/checkpoint.py`'s
`STEPS` list declares `depends_on: List[str]` (list of prerequisite step names).
`pipeline_scheduler.run_steps_for_date` implements the fallback mechanism:
- When a step fails, the pipeline does NOT abort immediately.
- Each subsequent step's `depends_on` is checked against the set of steps that
  have `status='success'` for this date (across prior runs AND the current run).
- If all dependencies are met → step runs normally.
- If any dependency is unmet → step is recorded as `'skipped'` with reason logged; pipeline continues.
- `'skipped'` steps (unlike `'success'`) are retried on the next run, so fixing
  a prerequisite automatically unlocks its dependents.
- Return value: `False` if any attempted step raised (even if later steps ran).

**Dependency graph (2026-07-02):**
```
download_bhavcopy:            depends_on: []  (foundation — any failure here is critical)
download_fno:                 depends_on: []  (independent, non-critical)
download_macro:               depends_on: []  (independent, non-critical)
download_corporate_actions:   depends_on: []  (independent, non-critical)
download_large_deals:         depends_on: []  (independent, non-critical)
adjust_prices:                depends_on: [download_bhavcopy]
compute_features:             depends_on: [adjust_prices]
run_models:                   depends_on: [compute_features]
write_signals:                depends_on: [run_models]
paper_trade:                  depends_on: [write_signals]
```

Key invariant: the 4 independent downloaders (fno/macro/corporate_actions/
large_deals) never block `compute_features`. Their data is consumed as
NaN-tolerant soft inputs within the feature computation itself.

### SPEC-SCHED-015 · 23-Hour Pipeline Window and Overnight Training
**[AS BUILT, 2026-07-02]** Replaces the earlier 15-hour window (SPEC-SYS-002).
- **Trigger**: 18:00 IST (6 PM) daily on trading days
- **Deadline**: 17:00 IST next day (23 hours) — user-confirmed acceptable window
- **Model training**: Separate `model_training` job fires at 20:00 IST (mon-fri),
  after the daily pipeline completes. Checks `datastore/models/registry.json` for
  overdue models (SPEC-SCHED-007) and triggers retraining via subprocess.
  Each model has an 8-hour subprocess timeout — well within the 23-hour window.
  Implemented in `ingestion/scheduler/pipeline_scheduler.py::_execute_model_training_job`.
- **Weekend jobs** (Saturday only, market closed, full CPU available):
  - `weekend_feature_backfill`: 09:00 IST — runs `scripts/feature_backfill_hybrid.py`
    with `--stage2-chunk-size 400` to fill any feature Parquet gaps from the week
  - `weekend_fundamentals`: 10:30 IST — runs `scripts/backfill_fundamentals_trendlyne.py`
    to refresh fundamentals from Screener.in / Trendlyne for newly-published quarters
- Settings constants: `MODEL_TRAINING_SCHEDULE_TIME`, `WEEKEND_FEATURE_BACKFILL_TIME`,
  `WEEKEND_FUNDAMENTALS_TIME` in `config/settings.py`
- Heartbeat staleness thresholds updated in
  `datastore/api/utils/scheduler_status.py::HEARTBEAT_STALE_AFTER`

### SPEC-SCHED-012 · Backfill Catch-Up Scheduling
- Registered as its own recurring job (`backfill_catchup`), separate from
  the daily pipeline job (`daily_pipeline`) — distinct purpose, distinct
  failure modes, distinct schedule
- Daily (not weekly), 20:00 IST, after the 18:00 daily pipeline — at full
  target scope (5,500 BSE stocks x 15 years, SPEC-SYS-012), FYERS'
  ~1,000-calls/day budget and ~1 call/ticker/year history limit imply
  roughly 80+ days of sustained daily budget to complete a from-empty
  backfill; weekly cadence would stretch that past a year
- Re-runs `ingestion.backfill_runner.run_backfill` against the full
  current universe (`config.universe.get_tickers()`) every time it fires —
  relies entirely on the existing coverage skip (`has_sufficient_history`,
  >=90% coverage) and resume checkpoint to make already-backfilled tickers
  a fast no-op; only newly-added tickers or outstanding budget-exhausted
  ones do real work each day
- Does not call FYERS during the daily pipeline's own steps — the two jobs
  never compete for the same call budget
- **Critical constraint — no unattended renewal:** FYERS' retail API has
  no refresh-token mechanism; an access token expires daily and can only
  be renewed via an interactive OAuth2 login (browser + FYERS login). This
  job MUST check for a valid, already-cached same-day token *before*
  attempting any backfill call, and skip cleanly (logged, not raised) if
  none is available — it must never reach the interactive OAuth2 fallback,
  which blocks forever on `input()` with no connected stdin in a scheduler
  thread. True unattended daily automation is not possible under FYERS'
  current auth model; this job only removes the need to manually
  re-trigger the backfill command itself once the operator has logged in
  that day — the daily interactive login step remains a manual operator
  action until/unless FYERS offers a non-interactive auth path

### SPEC-SCHED-013 · DuckDB Concurrency Resilience & Scheduler Heartbeats
- **Root cause this spec exists for:** DuckDB allows multiple concurrent
  read-only connections to a file OR exactly one read-write connection —
  never both at once, even across separate processes. The DataStore API
  (long-lived) and the ingestion scheduler (also long-lived) both touch
  `DUCKDB_PATH`; a naive "keep every connection open for the life of the
  process" pool means the API holding so much as one read-only connection
  permanently blocks the scheduler from ever opening a read-write
  connection to the same file, and vice versa
- `datastore.api.db.get_duckdb_connection(..., persist=False)`: opens a
  connection, yields it, and closes it again on exit — never cached —
  so a file's lock is held only for the duration of one request/step, not
  the process's entire lifetime. Required on both sides of any
  cross-process-shared DuckDB file:
  - API endpoints reading a file the scheduler also writes (e.g. every
    `DUCKDB_PATH`-touching route in `datastore/api/routers/ohlcv.py`,
    `system.py`)
  - Scheduler/ingestion steps writing a file the API also reads (e.g.
    `ingestion/scheduler/daily_pipeline.py`'s `step_download_bhavcopy`,
    `step_adjust_prices`, `step_download_macro`;
    `ingestion/backfill_runner.py`'s `run_backfill`)
  - Does NOT apply to a file only one process ever touches (e.g. the
    API's own exclusive ownership of `ml_signals` in `SIGNALS_DUCKDB_PATH`)
    or to in-memory (`:memory:`) connections, which have no cross-process
    file lock to release in the first place — `persist=False` is a no-op
    (treated as `True`) for `:memory:` so tests that seed an in-memory DB
    in one call and read it back in another keep working
- `get_duckdb_connection` retries a lock-conflict `IOException` with
  exponential backoff (`DUCKDB_LOCK_RETRY_ATTEMPTS=4`,
  `DUCKDB_LOCK_RETRY_BASE_DELAY_S=0.5` → ~3.5s worst case) before raising
  — turns a write step that is *actively* in progress when a read arrives
  into a short delay instead of a hard failure
- **Scheduler heartbeats:** every invocation attempt of a recurring job
  (`daily_pipeline`, `backfill_catchup`) — success, failure, or a
  deliberate early skip — is upserted to `scheduler_heartbeats`
  (`job_id` PRIMARY KEY; `last_attempt_at`, `last_status`, `last_error`,
  `last_success_at`). `last_success_at` only advances on an actual
  success, so "last successful run" and "last attempt at all" stay
  independently queryable
- Both recurring job functions (`_execute_daily_job`,
  `_execute_backfill_catchup`) are wrapped in try/except: no exception
  may propagate past the job target function itself, regardless of root
  cause — a single job's failure must never risk destabilizing the
  scheduler's ability to fire its *next* scheduled occurrence
- `GET /health` exposes a `scheduler` field: one entry per known job,
  including a computed `is_stale` flag (no attempt recorded within the
  job's expected interval — 4 days for the Mon-Fri daily pipeline to
  absorb a normal weekend without a false positive, 26 hours for the
  daily backfill catch-up)
- **Incident this spec formalizes:** a real, multi-day-running scheduler
  process's `backfill_catchup` job crashed against exactly the lock
  conflict above (the API process was holding a persistent read-only
  connection); the crash itself was caught and logged by APScheduler
  correctly, but the scheduler then stopped firing *either* recurring
  job entirely, with no heartbeat or any other record anywhere to make
  that observable short of reading the scheduler process's own log file
  via `/proc/<pid>/fd` by hand. See BuildLog.md "Scheduler/DuckDB
  concurrency resilience" for the full investigation and fix.

### SPEC-SCHED-014 · Job Autoruns (Ops) API and Page
**[AS BUILT, 2026-07-01]** Not part of the 27-screen prototype spec — an
operational page the user asked for directly. Pure API scaffolding over
the scheduler infrastructure SPEC-SCHED-002/005/013 above already built;
no new scheduling logic.
- `GET /api/v1/ops/heartbeats` — the same data `GET /health`'s `scheduler`
  field exposes, both now sourced from one shared
  `datastore/api/utils/scheduler_status.get_scheduler_heartbeats()`
- `GET /api/v1/ops/runs?limit=` — recent `pipeline_runs` rows
- `GET /api/v1/ops/steps?date=` — every `checkpoint.STEPS` entry's
  checkpoint status for one date (`'never_run'` if no row exists)
- `POST /api/v1/ops/steps/{step_name}/force?date=` — runs exactly one
  step via the real `daily_pipeline.step_runner`/`_STEP_DISPATCH`, guarded
  the same way `pipeline_scheduler.run_steps_for_date()` guards its own
  loop: 409 if any lower-`step_index` step hasn't succeeded yet for that
  date (SPEC-SCHED-011's step-dependency ordering applies here too — a
  force-run must never skip ahead of an unmet prerequisite). Runs via
  `asyncio.to_thread` so it doesn't block the event loop for other
  requests while a slow step executes.
- UI: `dashboard/static/ops/index.html`, a 6th entry in `js/shell.js`'s
  `APPS` (id `"ops"`) — deliberately excluded from `crosslink.js`'s
  "View in X" links (no per-ticker meaning) via an explicit
  `CROSSLINK_APP_IDS` allowlist rather than "all apps except current."

---

## SPEC-OBS: Observability Specifications

### SPEC-OBS-001 · Master Switch
- OBSERVABILITY_ENABLED (bool) in settings.py — turns entire system on/off
- When off: zero performance overhead (NoOpObservability class)
- Default: True in development, configurable in production

### SPEC-OBS-002 · Observability Levels
- 5 levels: 'off', 'error', 'warning', 'info', 'debug'
- 'off': no logging, no metrics, no structured events
- 'error': only errors and critical failures
- 'warning': + data quality warnings, drift alerts
- 'info': + step start/complete, pipeline progress, backfill progress (production default)
- 'debug': + per-stock timings, feature-level diagnostics
- Level configurable via OBSERVABILITY_LEVEL in settings.py

### SPEC-OBS-003 · Structured Logging
- All observability events emitted as JSON lines to datastore/logs/observability.jsonl
- Each event: {event_type, timestamp, step_id, duration, rows_processed, ...}
- Console output: human-readable format (configurable)
- Log rotation: daily, 30-day retention

### SPEC-OBS-004 · Metrics Collection
- Per-step: duration_seconds, rows_processed, retry_count
- Per-pipeline: total_duration, steps_completed, steps_failed, steps_skipped
- Per-backfill: days_processed, days_remaining, estimated_completion
- Exposed via DataStore API: GET /api/v1/system/health

### SPEC-OBS-005 · Production Mode
- In production (OBSERVABILITY_LEVEL='error' or 'warning'):
  - No per-stock logging (reduces log volume 500×)
  - No debug-level metrics
  - Only errors, data quality warnings, and drift alerts
  - Console output minimal or silent
- Switch between modes without code changes — config only

---

## SPEC-SYS-011 · Configurable Universe Expansion

### Universe is data-driven, not hardcoded
- UNIVERSE_SIZE constant REMOVED from settings.py
- Universe determined by query on stock_master table with tier/mcap/adtv filters
- Tier thresholds, min mcap, min ADTV configurable in settings.py
- Expansion happens by changing config parameters, not code

### Expansion stages
| Config Profile | Tier Threshold | Min ADTV (Cr) | Min Mcap (Cr) | Approx Stocks |
|---------------|:-:|:-:|:-:|:-:|
| phase_1 (default) | ≤ 2 | 5.0 | 500 | ~500 |
| phase_2 | ≤ 3 | 0.5 | 100 | ~2,000 |
| phase_3 | ≤ 4 | 0.1 | 50 | ~3,500 |
| full_nse | ≤ 5 | 0.0 | 0 | ~5,000+ |

### Gate before expansion
- Pipeline must complete within 90 min at current universe size before expanding
- Feature computation must scale linearly (verify with timing benchmarks)
- Storage must have 50% headroom for expanded feature store

---

## SPEC-SYS-012 · Multi-Exchange Universe (NSE + BSE)

**Status: drafted, not yet implemented.** Written ahead of need, in response to
an operator question about eventual BSE expansion (~5,500 listings) on top of
NSE (~2,000 full / 500 Nifty-500). No code in this repo implements this yet —
`stock_master`/`ohlcv_adjusted` are still NSE-only, single-exchange schemas
(see "Migration required" below). Treat this section as a proposal: confirm
or amend the design choices before any implementation work starts against it.

### Why a ticker alone is not a safe key across exchanges
- The same company has a *different* symbol on each exchange (e.g. NSE
  `RELIANCE` vs BSE scrip code `500325` — BSE primarily identifies securities
  by numeric scrip code, with an alphanumeric "Security Id" as a secondary
  label that does not always match the NSE symbol).
- A large fraction of NSE-listed companies are also BSE-listed (dual listing
  is the norm for mid/large caps, not the exception). Treating each
  exchange's listing as an independent "stock" would double-count the same
  company in the universe, in feature computation, and — critically — in
  position sizing (SPEC-ALERT, MAX_POSITION_PCT): the system must never hold
  what it thinks are two positions in what is actually one company.
- **Canonical identity is ISIN** (International Securities Identification
  Number), not ticker. ISIN is exchange-agnostic and already present in NSE's
  own index-constituent exports (`ind_nifty500list.csv`'s "ISIN Code" column,
  already fetched by `config/build_universe.py._fetch_index_csv()` today but
  currently discarded — not in `OUTPUT_COLUMNS`).

### De-duplication rule
- One company = one row in `stock_master`, keyed by `isin`, regardless of how
  many exchanges list it.
- Each row carries a `primary_exchange` ('NSE' | 'BSE'), defaulting to NSE
  for any dual-listed security — NSE has materially deeper liquidity than
  BSE for nearly all Indian equities, so NSE's price/volume series is
  authoritative for `ohlcv_adjusted`, feature computation, and signals.
- A BSE-only listing (no NSE listing at all) has `primary_exchange = 'BSE'`
  and BSE is then authoritative for that row.
- Ingesting the *secondary* exchange's series (e.g. BSE prices for an
  NSE-primary stock) is optional, for cross-validation/liquidity aggregation
  only — it must never create a second tradeable "position" for the same
  ISIN. Any future feature/signal that reads OHLCV reads the primary
  exchange's series unless explicitly asking for the secondary one.

### Schema changes required (migration, not yet applied)
- `stock_master`: add `isin VARCHAR NOT NULL UNIQUE` (replaces `ticker` as
  the conceptual primary key — `ticker` alone is no longer guaranteed
  unique once BSE-only scrip codes are added), `nse_ticker VARCHAR`
  (nullable — NULL if not NSE-listed), `bse_ticker VARCHAR` (nullable —
  BSE scrip code or Security Id; NULL if not BSE-listed), `primary_exchange
  VARCHAR NOT NULL CHECK (primary_exchange IN ('NSE','BSE'))`.
- `ohlcv_adjusted`: add `exchange VARCHAR NOT NULL DEFAULT 'NSE'`; primary
  key changes from `(date, ticker)` to `(date, isin, exchange)` so a
  secondary-exchange series (if ever ingested) can coexist with the primary
  one without a key collision, while every consumer query still filters to
  `primary_exchange` by default (PIT/consumer-facing reads are unaffected —
  this is additive, not a behavior change for existing NSE-only data).
- Existing NSE-only rows backfill as `exchange='NSE'`, `isin` populated by
  joining the current `ticker` against NSE's own ISIN export (no real
  ambiguity — every currently-loaded ticker is NSE-listed today).

### Universe-scope flags decoupled from `tier`
- SPEC-SYS-001 defines `tier` as a market-cap/ADTV-derived liquidity ranking
  (1-5) across the *whole* monitored universe — not an index-membership
  label. `config/build_universe.py`'s current implementation (tier 1=Nifty50,
  2=NiftyNext50, 3=Midcap150, 4=Smallcap250, 5=rest of Nifty 500) is a
  documented, temporary proxy for that real definition, used only because
  free NSE archives don't publish bulk market cap (`market_cap_cr` is 0 /
  unsourced for every row today — see that module's own docstring). It
  already cannot represent "is this stock part of the Nifty 500 at all"
  once non-Nifty-500 NSE stocks or BSE stocks are added to the same table —
  there's no tier value left to mean "outside Nifty 500 entirely" without
  colliding with tier 5's existing meaning ("smallest *Nifty-500* members").
- Fix: index/exchange *membership* is tracked as independent boolean
  columns on `stock_master` — `is_nifty500` (already in the schema, but
  currently unused by `config/universe.py`'s filter `WHERE` clause; this
  spec makes it a real, active filter), plus new `is_bse_500` / similar
  BSE-index flags as needed once a BSE index source is chosen. `tier`
  remains purely a size/liquidity ranking, computed the same way regardless
  of exchange or index membership, once real `market_cap_cr`/`adtv_cr` are
  sourced (SPEC-SYS-011's existing gap — not introduced by this spec).
- `UNIVERSE_PROFILES` (`config/settings.py`) gains an optional
  `universe_scope` filter per profile (e.g. `is_nifty500=True` for
  `phase_1`, unset/no scope filter for an eventual `nse_bse_full` profile)
  — applied in `config/universe.py`'s `load_universe()` alongside the
  existing `tier_threshold`/`min_adtv_cr`/`min_mcap_cr` filters, not
  instead of them.

### Expansion stages (extends SPEC-SYS-011's table)
| Config Profile | Universe Scope | Approx Stocks |
|---|---|---|
| phase_1 (current default) | NSE, `is_nifty500=True` | ~500 |
| full_nse | NSE, no index-scope filter | ~2,000 |
| nse_bse_full | NSE ∪ BSE, de-duplicated by ISIN | ~7,000 minus dual-listed overlap (NSE ~2,000 + BSE ~5,500, large intersection) |

### Ingestion implications (not built; future phase)
- BSE does not publish through NSE's archive endpoints — a new module
  (e.g. `ingestion/scrapers/bse_bhavcopy.py`) is required, mirroring
  `ingestion/scrapers/bhavcopy.py`'s structure (raw retention under
  `datastore/raw/`, SPEC-PIPE-005-equivalent validation) against BSE's own
  bhavcopy format and URL scheme, which differs from NSE's.
- FYERS' symbology differs by exchange (`NSE:TICKER-EQ` vs a BSE-specific
  prefix/scrip-code format) — `ingestion/scrapers/fyers_backfill.py`'s
  ticker formatting (currently NSE-only) needs an `exchange` parameter
  before it can backfill BSE-primary securities.
- Out of scope for this spec: corporate-action reconciliation when the same
  ISIN's NSE and BSE listings report a split/bonus on different ex-dates
  (rare but possible) — flagged as a known open question, not resolved here.

---

## SPEC-SOLID: SOLID Coding Principles

### SPEC-SOLID-001 · Single Responsibility
- Each class/module does exactly one thing
- Functions: ≤ 50 lines. Classes: ≤ 300 lines. Modules: ≤ 600 lines.
- If a class description uses "and", it violates SRP — split it

### SPEC-SOLID-002 · Open/Closed
- New features: add new files, do not modify existing code
- New models: inherit from IModel interface; never modify base class
- New feature categories: add to FEATURE_REGISTRY; never modify existing features
- New consumer systems: add directory in systems/; never modify DataStore API

### SPEC-SOLID-003 · Liskov Substitution
- Any IModel subclass can be substituted wherever IModel is expected
- Pipeline runner, stacking meta-learner, and backtester all accept IModel

### SPEC-SOLID-004 · Interface Segregation
- IModel: core (train, predict, save, load) — all models
- IClassificationModel(IModel): adds predict_proba — classification models only
- IExplainableModel(IModel): adds get_shap_values — SHAP-capable models only
- IRegimeModel: fit, predict_regime — HMM only (different interface)
- ISurvivalModel(IModel): adds predict_survival — Cox/RSF only

### SPEC-SOLID-005 · Dependency Inversion
- High-level modules depend on interfaces (contracts/), not concrete implementations
- Pipeline runner receives IModel, not Signal5dModel
- DataStore API depends on IDataStoreReader, not DuckDB directly
- Tests inject mock implementations via interfaces

---

## SPEC-LIB: Library Governance

### SPEC-LIB-001 · Version Pinning
- All library versions pinned with == in requirements/*.txt
- No >= or ~= version specifiers in production requirements

### SPEC-LIB-002 · Upgrade Protocol
- One library at a time, on a dedicated branch
- Full test suite must pass before and after upgrade
- Walk-forward backtest comparison: reject if Sharpe drops > 0.05
- Merge only if all tests pass AND backtest is stable

### SPEC-LIB-003 · Quarterly Security Audit
- Run pip-audit quarterly against all requirements files
- Security patches applied within 1 week of disclosure
- Security upgrades follow normal upgrade protocol (branch, test, compare)

### SPEC-LIB-004 · Prefer Public Libraries
- Use public, well-maintained libraries over custom implementations
- Custom code only where no suitable library exists or domain logic is unique
- See 14_engineering_standards.md Part 4 for the full approved library list

---

## SPEC-TRACE: Requirements Traceability

### SPEC-TRACE-001 · RTM Coverage
- Every spec ID has at least one test in the RTM (14_engineering_standards.md)
- RTM maintained with 100% coverage — no spec without a test
- RTM reviewed and updated at each phase boundary

### SPEC-TRACE-002 · Docstring Spec References
- Every public function docstring includes "Spec References" section
- Lists all spec IDs the function implements or supports
- Automated scan verifies no public function is missing spec references

### SPEC-TRACE-003 · Commit Traceability
- Every commit message references the spec(s) it implements
- Format: "Implement SPEC-PIPE-003: PIT alignment for fundamentals"
- Commits without spec references are rejected in code review

### SPEC-TRACE-004 · Test-Spec Linkage
- Every test docstring starts with the spec ID it validates
- Format: "SPEC-PIPE-003: Q4 results announced May 15 must NOT appear before May 15."
- Tests without spec references are flagged during CI
