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

---

## SPEC-PIPE: Data Pipeline Specifications

### SPEC-PIPE-001 · OHLCV Ingestion
- Source: NSE bhavcopy (daily update), FYERS API (historical backfill)
- All prices stored as corporate-action-adjusted in DataStore normalised layer
- Raw bhavcopy retained in DataStore raw layer for audit
- Ingestion layer writes to DataStore ONLY; consumer systems read via API

### SPEC-PIPE-002 · Corporate Action Adjustment
- Applied retroactively to all historical prices on ex-date
- SPLIT: pre-ex prices × ratio. BONUS: pre-ex prices / (1 + ratio)
- Must be idempotent (safe to call multiple times)
- Post-adjustment: price continuity at ex-date < 1% gap
- Logged with before/after price sample

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

### SPEC-UI-001 · Daily Dashboard (Screen A)
- Shows: top 5 buy signals, exit urgency, P&D warnings, multibagger top-5, regime summary
- ALL data reads from DataStore API (SPEC-DS-002); no direct database access
- Updates once per day after pipeline completes; refresh-on-demand

### SPEC-UI-002 · Signal Detail (Screen B)
- Per-stock: SHAP waterfall, all model scores, conformal intervals, regime history
- Reads: /api/v1/signals/ml/{ticker}, /api/v1/features/{ticker}

### SPEC-UI-003 · Multibagger Watchlist (Screen C)
- Top 20 ranked by multibagger probability
- Survival curves, archetypes, historical analogues
- Updated weekly; reads /api/v1/watchlist/current

### SPEC-UI-004 · Forensic Alert (Screen D)
- All stocks with forensic_flag red or amber
- Classical score breakdown, trend chart, pattern match
- Reads: /api/v1/signals/forensic/{ticker}

### SPEC-UI-005 · Backtest Results (Screen E)
- Fold-by-fold results, integrity checks, benchmark comparisons
- Reads: /api/v1/backtest/results/{model}

### SPEC-UI-006 · Performance
- Dashboard renders in < 3 seconds
- Exportable to CSV: signal list, positions, watchlist
- Dark theme

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
- Each pipeline step declares depends_on (list of prerequisite step IDs)
- Step only executes if all dependencies have status='success' in current run
- If dependency failed/skipped: dependent step is skipped with reason logged

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
