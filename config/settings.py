"""
config/settings.py

Phase: 0
Specs: SPEC-SYS-001, SPEC-SYS-002, SPEC-SYS-003, SPEC-SYS-004, SPEC-SYS-005, SPEC-SYS-011,
       SPEC-SCHED-001, SPEC-SCHED-007, SPEC-SCHED-008, SPEC-OBS-001, SPEC-OBS-002,
       SPEC-MODEL-006, SPEC-MODEL-007, SPEC-PIPE-005, SPEC-BT-002, SPEC-SEC-001,
       SPEC-DS-002, SPEC-QUALITY-003
Owner: Platform / DataStore
Consumers: ingestion, features, systems/*, backtest, datastore/api, scheduler, dashboard

Single source of truth for every constant, path, and threshold used across AlphaLens.
No other module may hardcode a path, threshold, or magic number (SPEC-QUALITY-003).
All credentials are loaded from the environment only (SPEC-SEC-001) — never hardcoded here.
"""

import os
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()

# ---------------------------------------------------------------------------
# SPEC-SYS-001: Universe Coverage
# SPEC-SYS-011: Configurable Universe Expansion (universe is query-driven, not hardcoded)
# ---------------------------------------------------------------------------
UNIVERSE_PROFILE = os.environ.get("UNIVERSE_PROFILE", "full_nse")  # phase_1 | phase_2 | phase_3 | full_nse

# tier_threshold: include tiers <= this value
# min_adtv_cr: minimum average daily traded value, INR crore
# min_mcap_cr: minimum market capitalisation, INR crore
#
# Tier scheme (config/build_universe.py):
#   1 = Nifty 50, 2 = NiftyNext50, 3 = Midcap150, 4 = Smallcap250,
#   5 = every other Nifty 500 member, 6 = broader NSE active (non-Nifty500).
# build_full_nse_universe_from_db() in build_universe.py populates tier=6 for
# tickers present in ohlcv_adjusted but absent from the Nifty 500 constituent
# list, giving us the full ~2492-stock active NSE universe in the CSV.
# full_nse uses tier_threshold=6 to capture all tiers including the broader market.
UNIVERSE_PROFILES = {
    "phase_1": {"tier_threshold": 5, "min_adtv_cr": 5.0, "min_mcap_cr": 500},
    "phase_2": {"tier_threshold": 3, "min_adtv_cr": 0.5, "min_mcap_cr": 100},
    "phase_3": {"tier_threshold": 4, "min_adtv_cr": 0.1, "min_mcap_cr": 50},
    "full_nse": {"tier_threshold": 6, "min_adtv_cr": 0.0, "min_mcap_cr": 0},
}

if UNIVERSE_PROFILE not in UNIVERSE_PROFILES:
    raise ValueError(
        f"Unknown UNIVERSE_PROFILE '{UNIVERSE_PROFILE}'. "
        f"Must be one of {list(UNIVERSE_PROFILES)}."
    )

TIER_THRESHOLD = UNIVERSE_PROFILES[UNIVERSE_PROFILE]["tier_threshold"]
MIN_ADTV_CR = UNIVERSE_PROFILES[UNIVERSE_PROFILE]["min_adtv_cr"]
MIN_MCAP_CR = UNIVERSE_PROFILES[UNIVERSE_PROFILE]["min_mcap_cr"]

NIFTY_500_SIZE = 500        # Nifty 500 index size (used for phase_1 profile reference only)
NSE_ACTIVE_UNIVERSE_SIZE = 2492  # Full active NSE universe (ohlcv_adjusted, non-delisted); see SPEC-SYS-011
TIER_COUNT = 6              # tiers 1-5 = Nifty 500 sub-indices; tier 6 = broader NSE active
TIER_REVIEW_FREQUENCY = "quarterly"

# ---------------------------------------------------------------------------
# SPEC-SYS-002: Daily Pipeline Completion
#
# PIPELINE_WINDOW_START/PIPELINE_WINDOW_HOURS were originally set here to
# "15:30"/15 (3:30 PM, 15-hour window). Consolidated below (previously a
# duplicate reassignment further down this file silently overrode these
# values with the current 18:00/23-hour window — see the "23-hour pipeline
# window (user-confirmed, 2026-07-02)" section) into a single definition to
# avoid two competing constants for the same setting in one file.
# ---------------------------------------------------------------------------
PIPELINE_MAX_DURATION_MINUTES = 90
PIPELINE_WINDOW_START = "18:00"  # 6:00 PM IST (user-confirmed 2026-07-02; was 3:30 PM)
PIPELINE_WINDOW_END = "09:15"  # 9:15 AM next day
PIPELINE_WINDOW_HOURS = 23  # 23-hour window (user-confirmed 2026-07-02; was 15h)
OPTION_CHAIN_SCRAPE_TIME = "15:25"  # Only fixed-time job; must run before market close

# ---------------------------------------------------------------------------
# SPEC-SYS-003: Data Completeness Gate
# ---------------------------------------------------------------------------
MIN_STOCKS_FOR_INFERENCE = 2000  # out of NSE_ACTIVE_UNIVERSE_SIZE (was 450 out of 500 under phase_1)
DATA_STALENESS_FLAG_COLUMN = "data_staleness_flag"

# ---------------------------------------------------------------------------
# SPEC-SYS-004: Availability — laptop-only (SPEC-SCHED-009); Oracle Cloud
# deferred (no available ARM A1 capacity, Free Trial blocked region
# switching — see BuildLog.md "Laptop-only pivot")
# ---------------------------------------------------------------------------
LAPTOP_SCHEDULER_UPTIME_TARGET = 0.99  # formerly ORACLE_SCRAPER_UPTIME_TARGET
OPTION_CHAIN_RECOVERABLE = False  # Non-recoverable if the laptop is off/asleep at 3:25 PM IST

# ---------------------------------------------------------------------------
# SPEC-SYS-005: Storage Budgets
# ---------------------------------------------------------------------------
RAW_BHAVCOPY_RETENTION_DAYS = 90
FEATURE_PARQUET_RETENTION_YEARS = 5
DB_BACKUP_FREQUENCY = "weekly"
DB_BACKUP_TARGET = "local_external_drive"  # formerly "oracle_object_storage" — not yet automated
MAX_TOTAL_STORAGE_GB = 500

# ---------------------------------------------------------------------------
# Paths — SPEC-QUALITY-003: no hardcoded paths anywhere else in the codebase
# ---------------------------------------------------------------------------
PROJECT_ROOT = Path(__file__).resolve().parent.parent
CONFIG_DIR = PROJECT_ROOT / "config"

DATASTORE_DIR = PROJECT_ROOT / "datastore"
RAW_DIR = DATASTORE_DIR / "raw"
NORMALISED_DIR = DATASTORE_DIR / "normalised"
# 2026-07-08: local cache of raw NSE Integrated Filing HTML, keyed by NSE's
# own seq_id — scripts/backfill_fundamentals_nse_xbrl.py downloads a filing
# at most once ever (reported figures don't change once filed, per explicit
# operator instruction), re-reading from this cache on any re-run instead of
# re-fetching from NSE.
NSE_XBRL_RAW_CACHE_DIR = RAW_DIR / "nse_xbrl_filings"
FEATURES_DIR = DATASTORE_DIR / "features"
SIGNALS_DIR = DATASTORE_DIR / "signals"
MODELS_DIR = DATASTORE_DIR / "models"
OUTPUTS_DIR = DATASTORE_DIR / "outputs"
LOGS_DIR = DATASTORE_DIR / "logs"
# ML24 (2026-07-11): versioned weekly ADTV-ranked training-universe lists
# (config/training_universe.py) — one dated JSON per refresh, so a given
# model's training run can be traced back to the exact ticker list used.
TRAINING_UNIVERSE_DIR = MODELS_DIR / "training_universe"

# Store 2: Normalised (DuckDB analytical + SQLite transactional) — SPEC-DS-007
DUCKDB_PATH = NORMALISED_DIR / "alphalens.duckdb"
# A50 (2026-07-10): fno_data (121M rows) lives in its OWN DuckDB file,
# ATTACHed transparently by datastore/api/db.py::get_duckdb_connection
# whenever a caller connects to DUCKDB_PATH (via `SET search_path`, so
# every existing unqualified `FROM fno_data`/`INSERT INTO fno_data`/
# `DELETE FROM fno_data` continues to work unchanged — see that module's
# docstring). This lets datastore/staging/publish.py publish a brand-new
# fno_data.duckdb file and atomically os.replace() it into place instead of
# rewriting all 121M rows in-place via `CREATE OR REPLACE TABLE fno_data
# AS SELECT * FROM staging.fno_data` on every publish — the prior approach
# held the DuckDB write lock for however long a 121M-row physical rewrite
# takes, even when only one trade_date's ~50k rows actually changed.
FNO_DATA_DB_PATH = NORMALISED_DIR / "fno_data.duckdb"
PIPELINE_LOG_DB_PATH = NORMALISED_DIR / "pipeline_log.db"
SCHEDULER_DB_PATH = NORMALISED_DIR / "scheduler.db"
# [2026-07-28] Event-driven fundamental-feature cache (features/fundamental_cache.py):
# ~44 of FUNDAMENTAL_FEATURES' 51 ratios only change when a ticker's
# announcement_date advances (quarterly), yet the daily feature build
# recomputed all of them for every ticker every day. Keyed by
# (ticker, fiscal_year, quarter) — persists across process restarts so a
# multi-day backfill doesn't lose its warm cache on a crash/reboot.
FUNDAMENTAL_RAW_CACHE_DB_PATH = NORMALISED_DIR / "fundamental_raw_cache.duckdb"
# 2026-07-05: cross-process advisory lock (fcntl.flock) so run_steps_for_date
# can never execute concurrently from two OS processes (the scheduler's own
# daily_pipeline + morning_catchup jobs both call it for "today", and the
# Ops API's force_run_step is a separate process again) — see
# pipeline_scheduler.py's pipeline_run_lock(). Root cause of pipeline_runs
# rows recorded 'failed' on 2026-07-02/03 despite every step's own
# checkpoint showing 'success': two concurrent invocations racing on the
# same date's pipeline_checkpoints rows.
PIPELINE_RUN_LOCK_PATH = NORMALISED_DIR / ".pipeline_run.lock"
MF_HOLDINGS_DIR = NORMALISED_DIR / "mf_holdings"

# Store 3: Features
FEATURES_DAILY_DIR = FEATURES_DIR / "daily"
# [AS BUILT, P1.7] PND_FEATURES (features/pnd_features.py) aren't part of
# ALL_FEATURE_COLUMNS (features/matrix_builder.py) — daily_pipeline.py's
# step_compute_features saves them to a sibling daily Parquet directory.
FEATURES_PND_DAILY_DIR = FEATURES_DIR / "daily_pnd"
FEATURE_CATALOG_PATH = FEATURES_DIR / "metadata" / "feature_catalog.json"
PSI_BASELINE_PATH = FEATURES_DIR / "baseline" / "stats_baseline.pkl"

# Store 4: Signals (DuckDB) — SPEC-DS-007
SIGNALS_DUCKDB_PATH = SIGNALS_DIR / "signals.duckdb"

# Store 5: Models
MODEL_REGISTRY_PATH = MODELS_DIR / "registry.json"

# Store 6: Backtest (DuckDB) — Unified Backtest & Paper Trading Umbrella, Phase 1
# (BacktestUmbrellaPlan.md at the repo root). Own file rather than reusing
# SIGNALS_DUCKDB_PATH: backtest_feature_log is a per-decision write-heavy log
# (one row per candidate signal per rebalance date, across every channel's
# every run), a different write pattern from signals.duckdb's once-daily
# batch upserts, and keeping it separate means Phase-6's fine-tuning loop can
# query/purge backtest history without contending with live signal reads.
BACKTEST_DIR = DATASTORE_DIR / "backtest_store"
BACKTEST_DUCKDB_PATH = BACKTEST_DIR / "backtest.duckdb"

# Observability
OBSERVABILITY_LOG_PATH = LOGS_DIR / "observability.jsonl"

# Universe definition file (config/universe.py reads this)
UNIVERSE_CSV_PATH = CONFIG_DIR / "nifty500_universe.csv"

# ---------------------------------------------------------------------------
# Scheduler — SPEC-SCHED-001 through SPEC-SCHED-012
# ---------------------------------------------------------------------------
SCHEDULER_MODE = "linear"  # 'linear' | 'timestamp' | 'manual'
DEFAULT_RETRY_COUNT = 3

# SPEC-SCHED-012: backfill catch-up. Daily (not weekly): at full BSE
# expansion scope (5,500 stocks x 15 years), FYERS_MAX_CALLS_PER_DAY=1000
# means roughly 1 call/ticker/year (FYERS_HISTORY_MAX_DAYS_PER_CALL=365),
# so 5500 x 15 = ~82,500 calls needed -> ~83 days of daily budget to
# complete a full backfill from empty. A weekly cadence would stretch that
# to well over a year. Runs after the main 18:00 daily pipeline so the two
# never compete for the same window; they don't compete for FYERS budget
# either (the daily pipeline's bhavcopy/macro/F&O steps never call FYERS —
# only this job and the one-time/manual backfill_runner CLI do).
BACKFILL_CATCHUP_TIME = "20:00"  # HH:MM, Asia/Kolkata, daily
# ingestion/scheduler/daily_pipeline.py main()'s schedule_daily_pipeline()
# call and datastore/api/utils/scheduler_status.py's next-run-time
# computation both read this single constant so they can never drift apart.
DAILY_PIPELINE_SCHEDULE_TIME = "18:00"  # HH:MM, Asia/Kolkata, mon-fri
# 2026-07: second, earlier trigger of the same catch-up-then-today logic
# (schedule_morning_catchup) -- NSE-sourced steps (download_fno/macro/
# corporate_actions/large_deals etc.) that failed on an earlier date get
# retried here instead of sitting idle until the 18:00 run. "Today" itself
# will still 404 at this hour (NSE publishes a trading day's bhavcopy after
# that day's own market close, not before) -- the real value is the
# gap-backfill portion. See schedule_morning_catchup's docstring.
MORNING_CATCHUP_SCHEDULE_TIME = "07:30"  # HH:MM, Asia/Kolkata, mon-fri
DEFAULT_RETRY_DELAY_SECONDS = 60
# Pipeline & Monitoring Remediation Phase 1 (2026-07-10): a pipeline_runs
# row inserted with status='running' (at the start of
# pipeline_scheduler.py::run_startup_sequence) and never updated to a
# terminal status within this many minutes is treated as a crashed run
# (e.g. OOM-killed process) by GET /api/v1/ops/runs's `is_stale` flag,
# rather than silently continuing to look like "in progress". Set well
# above the pipeline's own expected run time (typically well under an
# hour) so a genuinely still-running pipeline is never flagged stale.
PIPELINE_STALE_RUN_THRESHOLD_MINUTES = 180
# Pipeline & Monitoring Remediation Phase 2 (2026-07-10): single, uniform
# memory ceiling shared by ingestion/scheduler/resource_guard.py's
# adaptive chunk-sizing (self-heal, triggers at 80% of this by default),
# and intended as the same figure a future DuckDB `memory_limit` PRAGMA
# and the real-time Ops resource monitor's alert threshold should read
# from — replacing today's scattered, independently-chosen constants
# (chunk sizes, worker counts) that don't share a common basis. Sized
# conservatively below this machine's typical available RAM; adjust if
# the laptop's real headroom is measured to be meaningfully different.
PIPELINE_MEMORY_CEILING_MB = 6144
RETRAIN_OVERDUE_MULTIPLIER = 1.5  # days_since_retrain > interval * this => overdue
# 2026-07-07: retrain cadence for all registry-tracked models (hmm_market,
# pnd_detector, signal_5d/21d/63d, meta_labeler, conformal_signal5d,
# multibagger). 28 days (4 weeks) lines up with the weekly Saturday
# training-check job below — most Saturdays are a fast no-op, and an
# actual multi-hour retrain fires roughly once a month. Short enough to
# track regime drift, long enough that a single volatile week of data
# doesn't get chased into every model.
DEFAULT_TRAINING_INTERVAL_DAYS = 28

# ---------------------------------------------------------------------------
# 23-hour pipeline window (user-confirmed, 2026-07-02)
# Trigger at 18:00 IST; all pipeline + model training must complete by
# 17:00 IST the next day.  Heavy tasks (feature backfill, fundamentals
# scraping) are pushed to the weekend when the laptop isn't needed for
# real-time signal generation. (PIPELINE_WINDOW_START/PIPELINE_WINDOW_HOURS
# for this window are defined once, above, under SPEC-SYS-002.)
# ---------------------------------------------------------------------------
# Model training (2026-07-07: moved off weekdays onto the weekend — a real
# production-grade retrain with real Optuna trials runs 3-4+ hours per
# model and was contending with the 18:00 daily pipeline / DuckDB's
# single-writer lock on trading days; see BuildLog.md 2026-07-07). Fires
# Saturday after WEEKEND_FEATURE_BACKFILL_TIME/WEEKEND_FUNDAMENTALS_TIME
# below have finished, markets closed, full CPU/DB available all weekend.
# Checks RETRAIN_OVERDUE_MULTIPLIER x DEFAULT_TRAINING_INTERVAL_DAYS.
MODEL_TRAINING_SCHEDULE_TIME = "12:00"     # HH:MM, Asia/Kolkata, saturday
MODEL_TRAINING_DAY_OF_WEEK = "sat"
# Pipeline & Monitoring Remediation Phase 4 (A52, 2026-07-10):
# pipeline_scheduler.py::schedule_model_training_nightly's alternative to
# the single weekly Saturday job above — spreads training across Mon-Thu
# 11pm-ish nights (_MODEL_TRAINING_GROUPS), well clear of the 18:00 daily
# pipeline's own window.
MODEL_TRAINING_NIGHTLY_TIME = "23:00"      # HH:MM, Asia/Kolkata
# Weekend jobs fire Saturday morning — markets are closed, full CPU available.
WEEKEND_FEATURE_BACKFILL_TIME = "09:00"   # HH:MM, Asia/Kolkata, saturday
WEEKEND_FUNDAMENTALS_TIME = "10:30"       # HH:MM, Asia/Kolkata, saturday
# FutureDevelopment.md #14: multibagger/forensic scoring is operator-CLI
# only today (score_multibagger.py/score_forensic.py), never scheduled.
# BuildLog.md documents a "weekly cadence" design intent for the
# multibagger watchlist (2026-06's M-08 build notes: "known historical
# multibaggers score > 0.30; weekly cadence"/"weekly refresh") — both
# scoring scripts are cheap ("trains fresh at scoring time", seconds to
# low-minutes per BuildLog.md), so Sunday morning (markets closed, no
# daily-pipeline contention) is used for both.
MULTIBAGGER_SCORING_SCHEDULE_TIME = "09:30"  # HH:MM, Asia/Kolkata, sunday
FORENSIC_SCORING_SCHEDULE_TIME = "10:00"     # HH:MM, Asia/Kolkata, sunday
# 2026-07-08: NSE Integrated Filing — IndAS is a real, regulator-authoritative
# fundamentals source (ingestion/scrapers/nse_xbrl_financials.py) that only
# publishes new filings quarterly per company, but different companies file on
# different real-world days throughout each quarter — a weekly scan (not
# quarterly) is what actually catches newly-published filings promptly.
# [REVISED 2026-07-08] Originally scheduled Sunday 10:30 (after
# forensic_scoring) — wrong per explicit operator instruction: this must run
# AHEAD OF forensic scoring, valuation modeling, and every model that reads
# `fundamentals`, not after. A full-universe scan takes ~2-3h (real
# measurement, ~2,700 tickers), so a same-morning 30-minute gap before
# multibagger/forensic (09:30/10:00 Sunday) can't possibly finish in time —
# this codebase has no job-dependency/wait-for-completion scheduler
# primitive (see schedule_model_training's docstring: everything here is
# fixed-time cron with a generous gap, not a DAG), so the only real fix is
# starting early enough that it's done well before EVERY downstream
# consumer across the whole weekend batch: WEEKEND_FEATURE_BACKFILL_TIME/
# WEEKEND_FUNDAMENTALS_TIME (09:00/10:30 Saturday — WEEKEND_FUNDAMENTALS_TIME
# is Screener/Trendlyne, the FALLBACK source, so it should also run after
# this), MODEL_TRAINING_SCHEDULE_TIME (12:00 Saturday), and Sunday's
# MULTIBAGGER_SCORING_SCHEDULE_TIME/FORENSIC_SCORING_SCHEDULE_TIME. Moved to
# 05:00 Saturday — a single early run covers the entire weekend, rather than
# duplicating it into two slots.
NSE_XBRL_FUNDAMENTALS_SCHEDULE_TIME = "05:00"  # HH:MM, Asia/Kolkata, saturday
# A54 (2026-07-10): scripts/backfill_promoter_pledge_nse.py and
# scripts/backfill_balance_sheet_from_screener.py are real, live-verified
# (2026-07-07) backfills that were simply never scheduled — 71% of
# shareholding.promoter_pledge rows were NULL purely because of that, not
# because the underlying NSE/Screener sources are actually unavailable.
# Both are per-ticker HTTP loops over the full universe (not bulk
# endpoints), so scheduled Saturday alongside weekend_fundamentals — after
# nse_xbrl_fundamentals/weekend_feature_backfill/weekend_fundamentals have
# refreshed the base fundamentals/shareholding rows these two enrich, and
# before model_training (12:00 Saturday).
PROMOTER_PLEDGE_BACKFILL_SCHEDULE_TIME = "11:00"    # HH:MM, Asia/Kolkata, saturday
BALANCE_SHEET_BACKFILL_SCHEDULE_TIME = "11:30"      # HH:MM, Asia/Kolkata, saturday

# ---------------------------------------------------------------------------
# Observability — SPEC-OBS-001 through SPEC-OBS-005
# ---------------------------------------------------------------------------
OBSERVABILITY_ENABLED = True
OBSERVABILITY_LEVEL = "info"  # 'off' | 'error' | 'warning' | 'info' | 'debug'
OBSERVABILITY_LOG_RETENTION_DAYS = 30

# ---------------------------------------------------------------------------
# Model thresholds — SPEC-MODEL-006, SPEC-MODEL-007, SPEC-ALERT-001
# ---------------------------------------------------------------------------
SIGNAL_THRESHOLD = 0.65
META_THRESHOLD = 0.50
PND_BLOCK_THRESHOLD = 60
PND_FLAG_THRESHOLD = 40
EXIT_URGENT_THRESHOLD = 80
EXIT_REDUCE_THRESHOLD = 60

CONFORMAL_TARGET_COVERAGE = 0.90  # alpha = 0.10
CONFORMAL_MIN_COVERAGE_ALERT = 0.85
CONFORMAL_VALIDATION_WINDOW_DAYS = 63

# ---------------------------------------------------------------------------
# Position sizing
# ---------------------------------------------------------------------------
MAX_POSITION_PCT = 0.10
MAX_SECTOR_PCT = 0.40
MIN_ADT_INR = 1_000_000
MAX_ORDER_VS_ADTV = 0.05

# ---------------------------------------------------------------------------
# Paper trading — SPEC-PT-003 (Pending Actions / review-approve)
# ---------------------------------------------------------------------------
# When True, scripts/run_daily_paper_trading.py computes candidate entries/
# exits but does not execute them — it writes paper_trading/pending/{date}.json
# and a human accepts/rejects each one via POST /api/v1/paper_trading/pending/
# {action_id}/{accept,reject}. When False, the bot auto-executes immediately
# (the original Phase 3.x behavior), e.g. for unattended historical replay.
PAPER_TRADING_REQUIRE_APPROVAL = os.environ.get("PAPER_TRADING_REQUIRE_APPROVAL", "true").lower() == "true"

# ---------------------------------------------------------------------------
# Drift monitoring — SPEC-PIPE-005
# ---------------------------------------------------------------------------
PSI_MODERATE_THRESHOLD = 0.10  # warning: reduce position sizing 50% (SPEC-ALERT-001)
PSI_SEVERE_THRESHOLD = 0.25  # halt + retrain (SPEC-ALERT-001: "Model Drift | HIGH")
PSI_TOP_N_FEATURES = 50  # SPEC-PIPE-005: "PSI: top 50 features vs baseline"
MIN_MODEL_ACCURACY = 0.45
NULL_RATE_ALERT_THRESHOLD = 0.01  # 1%
RATIO_FEATURE_RANGE = (0.1, 10.0)
DELIVERY_PCT_RANGE = (0, 100)

# ---------------------------------------------------------------------------
# Market regime
# ---------------------------------------------------------------------------
BEAR_REGIME_POSITION_SCALE = 0.50

# ---------------------------------------------------------------------------
# Transaction cost model — SPEC-BT-002
# ---------------------------------------------------------------------------
TOTAL_ROUNDTRIP_COST = 0.005
SMALL_CAP_SLIPPAGE_PCT = 0.0030  # Applies when ADTV < INR 1 Cr

# ---------------------------------------------------------------------------
# Strategy confidence framework — backtest/strategy_confidence.py
# ---------------------------------------------------------------------------
# A strategy's win rate isn't shown to the user until it clears this many
# independent trading dates of history (not signal-row count — see module
# docstring for why that distinction matters).
CONFIDENCE_MIN_INDEPENDENT_DATES = 60
# Minimum dates within a single market regime for that regime's bucket to
# count toward the "spans >=2 regimes" VALIDATED requirement.
CONFIDENCE_MIN_DATES_PER_REGIME = 15
# Deflated Sharpe Ratio threshold (SPEC-BT-001 rule 8) a strategy must clear,
# after correcting for how many strategies were compared side by side, to
# reach VALIDATED tier.
CONFIDENCE_DSR_THRESHOLD = 0.95

# ---------------------------------------------------------------------------
# Credentials — SPEC-SEC-001: never hardcoded, loaded from environment only
# ---------------------------------------------------------------------------
FYERS_APP_ID = os.environ.get("FYERS_APP_ID")
FYERS_SECRET_ID = os.environ.get("FYERS_SECRET_ID")
FYERS_ACCESS_TOKEN = os.environ.get("FYERS_ACCESS_TOKEN")
FYERS_REDIRECT_URI = os.environ.get("FYERS_REDIRECT_URI", "https://127.0.0.1")
SCREENER_USERNAME = os.environ.get("SCREENER_USERNAME")
SCREENER_PASSWORD = os.environ.get("SCREENER_PASSWORD")
VALUERESEARCH_USERNAME = os.environ.get("VALUERESEARCH_USERNAME")
VALUERESEARCH_PASSWORD = os.environ.get("VALUERESEARCH_PASSWORD")
TRENDLYNE_USERNAME = os.environ.get("TRENDLYNE_USERNAME")
TRENDLYNE_PASSWORD = os.environ.get("TRENDLYNE_PASSWORD")
TIJORI_USERNAME = os.environ.get("TIJORI_USERNAME")
TIJORI_PASSWORD = os.environ.get("TIJORI_PASSWORD")
OPENROUTER_API_KEY = os.environ.get("OPENROUTER_API_KEY")

# ---------------------------------------------------------------------------
# FYERS historical backfill — SPEC-PIPE-001, SPEC-PIPE-002
# ---------------------------------------------------------------------------
FYERS_RAW_DIR = RAW_DIR / "fyers"
FYERS_TOKEN_CACHE_PATH = FYERS_RAW_DIR / "access_token.json"
FYERS_RESUME_CHECKPOINT_PATH = FYERS_RAW_DIR / "backfill_resume.txt"
FYERS_MAX_CALLS_PER_DAY = 1000
FYERS_RATE_LIMIT_SLEEP_SECONDS = 0.5
FYERS_HISTORY_MAX_DAYS_PER_CALL = 365  # FYERS daily-resolution history API window limit
BACKFILL_YEARS = 5

# ---------------------------------------------------------------------------
# Screener.in fundamentals ingestion — SPEC-PIPE-003 (PIT, CRITICAL)
# ---------------------------------------------------------------------------
SCREENER_RAW_DIR = RAW_DIR / "screener"
SCREENER_RATE_LIMIT_SLEEP_SECONDS = 1.0
# [AS BUILT, A35 fix 2026-07-09] batch_export() accumulates fundamentals
# records in memory and flushes via one write_fundamentals_batch() call
# every N tickers, instead of one HTTP POST per ticker (see
# ingestion/scrapers/screener.py::batch_export and FeatureBacklog.md's
# A35 entry). N=50 is a deliberate partial-checkpoint compromise per A35's
# own tradeoff note: a crash mid-run loses at most one chunk's worth of
# already-fetched-but-unflushed tickers, not the "each ticker lands the
# moment it's fetched" durability of the old per-row-POST design, but also
# not "lose the entire multi-hour run" if it were one single end-of-run
# flush.
SCREENER_BATCH_EXPORT_CHUNK_SIZE = 50
# Conservative PIT defaults when Screener.in doesn't expose the real
# disclosure date directly (SPEC-PIPE-003): SEBI LODR Reg. 33 gives
# companies up to 45 days after quarter-end to announce Q1-Q3 results,
# but 60 days for Q4/annual results — a flat 45-day constant applied to
# every quarter (as this was before 2026-07-19) under-delays Q4 rows by
# up to 15 days, letting Q4-driven fundamentals (shares_outstanding
# changes from buybacks/QIPs/bonus around fiscal year-end, etc.) leak
# into PIT-filtered data before they were genuinely public knowledge.
# BSE shareholding filings are due ~21 days after quarter-end for every
# quarter, no Q4 exception (alphalens_docs/03_data_pipeline.md). Using
# the regulatory deadline rather than a shorter guess means a feature can
# never be backdated to a date earlier than the data was truly knowable.
FUNDAMENTALS_ANNOUNCEMENT_DELAY_DAYS_BY_QUARTER = {1: 45, 2: 45, 3: 45, 4: 60}
SHAREHOLDING_FILING_DELAY_DAYS = 21
RESULTS_PENDING_THRESHOLD_DAYS = 70  # SPEC-PIPE-003: results_pending_flag
# features/fundamental.py's ROIC: Screener.in exposes no reported EBIT or
# effective-tax-rate line item, so NOPAT is approximated as
# (operating_margin * revenue) * (1 - ASSUMED_TAX_RATE) — India's
# standard corporate tax rate under the concessional regime (Section
# 115BAA), used as a flat approximation across all sectors/years.
ASSUMED_TAX_RATE = 0.25
Z_SCORE_CLIP = 5.0  # SPEC-FEAT-002: sector-relative z-scores clipped to [-5, +5]

# ---------------------------------------------------------------------------
# AMFI MF holdings — SPEC-PIPE-003 (PIT), P2.2
# ---------------------------------------------------------------------------
MF_HOLDINGS_AVAILABILITY_DELAY_DAYS = 5  # available from ~5th of the following month
AMFI_FETCH_RATE_LIMIT_SLEEP_SECONDS = 1.0
# [AS BUILT, Big Investor Activity Phase C, 2026-07-05] Weekly, not
# twice-monthly: user decision — refresh every Saturday, independent of
# whether Groww's underlying AMC disclosure actually changed that week
# (re-ingesting an unchanged month is a safe no-op per
# save_monthly_parquet's merge-not-overwrite behavior). Cron
# day-of-week field syntax, passed straight to APScheduler's
# CronTrigger(day_of_week=...).
MF_HOLDINGS_SCHEDULE_DAY_OF_WEEK = "sat"
AMFI_SCHEDULE_TIME = "13:00"  # HH:MM, Asia/Kolkata
AMFI_RAW_DIR = RAW_DIR / "amfi_holdings"  # SPEC-PIPE-001: raw per-AMC disclosure files
# groww.in: primary MF-holdings source (P2.2 continued) — one scheme-detail
# HTTP request per scheme across ~49 AMCs x ~80-100 schemes each is several
# thousand requests for a full run; keep this polite.
GROWW_RATE_LIMIT_SLEEP_SECONDS = 0.5

# ---------------------------------------------------------------------------
# Price adjustment — disabled pending deliberation on adjustment logic
# ---------------------------------------------------------------------------
# SPEC-PIPE-002: backward adjustment; raw NSE prices preserved in raw_* columns.
# Set False to disable all adjustment (raw == adjusted); True for full CA adjustment.
PRICE_ADJUSTMENT_ENABLED = True

# ---------------------------------------------------------------------------
# 2026-07-09: was put on hold at user request (alongside 800-ticker cap on
# the model_training job) while investigating that day's memory issues.
# Reinstated 2026-07-11 at user request. The 800-ticker model_training cap
# (DEFAULT_MAX_TICKERS in retrain_phase2.py) was NOT part of this
# reinstatement — left as-is since it wasn't asked for.
# ---------------------------------------------------------------------------
MORNING_CATCHUP_ENABLED = True

# ---------------------------------------------------------------------------
# Corporate actions ingestion — NSE corporate action filings
# ---------------------------------------------------------------------------
NSE_CA_RAW_DIR = RAW_DIR / "corporate_actions"
NSE_CA_RATE_LIMIT_SLEEP_SECONDS = 1.0

# ---------------------------------------------------------------------------
# Large deals (bulk + block deals) ingestion — NSE and BSE
# ---------------------------------------------------------------------------
LARGE_DEALS_RAW_DIR = RAW_DIR / "large_deals"
LARGE_DEALS_RATE_LIMIT_SLEEP_SECONDS = 1.0

# ---------------------------------------------------------------------------
# Corporate action features — P2.2
# ---------------------------------------------------------------------------
IPO_LOCKIN_DAYS = 180  # typical SEBI-mandated minimum promoter/anchor lock-in
POST_EARNINGS_DRIFT_WINDOW_DAYS = 5  # PEAD measurement window after announcement_date
CORP_ACTION_ANTICIPATION_WINDOW_DAYS = 5  # run-up window before the nearest ex_date
# NSE's corporate-actions bulk endpoint does not expose a true announcement
# date (caBroadcastDate is present in the raw JSON schema but has been
# confirmed live, across every sample checked 2006-2026, to always be null
# — NSE simply does not populate it on this feed). record_date IS reliably
# populated (SEBI LODR Reg 42(2) requires listed companies to give the
# exchange written notice at least CORP_ACTION_NOTICE_DAYS working days
# before fixing a record date), so announcement_date is derived here as
# record_date - CORP_ACTION_NOTICE_DAYS: a documented, regulation-based
# conservative lower bound on when the action became public knowledge, not
# the fabricated true date. This can only make features/
# corporate_action_features.py's PIT gate MORE conservative (later/equal
# to the true announcement date), never introduce lookahead bias.
CORP_ACTION_NOTICE_DAYS = 7

# features/corporate_action_features.py's dividend_yield_vs_fd_rate: no
# trailing-dividend or live FD-rate data source exists yet in this
# codebase (corporate_actions has no DIVIDEND rows ingested; no bank
# FD-rate feed) — flat approximate reference rate, same documented-
# approximation precedent as ASSUMED_TAX_RATE, used only once a real
# dividend yield is available to compare against.
ASSUMED_FD_RATE = 0.07

# ---------------------------------------------------------------------------
# Trendlyne StratQ (superstar investor tracking) — SPEC-PIPE-003, P2.6
# ---------------------------------------------------------------------------
TRENDLYNE_RAW_DIR = RAW_DIR / "trendlyne"
TRENDLYNE_RATE_LIMIT_SLEEP_SECONDS = 1.0
# StratQ portfolio disclosures lag the underlying quarter-end the same way
# shareholding filings do (both ultimately trace back to BSE/NSE bulk/block
# deal + shareholding-pattern disclosures) — reuses SHAREHOLDING_FILING_DELAY_DAYS
# as the conservative PIT default when Trendlyne doesn't expose its own
# "last updated" timestamp directly, same reasoning as
# FUNDAMENTALS_ANNOUNCEMENT_DELAY_DAYS_BY_QUARTER above.

# ---------------------------------------------------------------------------
# Tijori Finance Pro (sector-specific operational metrics) — SPEC-PIPE-003, P2.6
# ---------------------------------------------------------------------------
TIJORI_RAW_DIR = RAW_DIR / "tijori"
TIJORI_RATE_LIMIT_SLEEP_SECONDS = 1.0

# ---------------------------------------------------------------------------
# Co-Pilot LLM (OpenRouter) — natural-language query to strategy spec
# ---------------------------------------------------------------------------
# OPENROUTER_API_KEY is defined in the Credentials block above. No fallback
# key: llm_client.py raises if it's unset, never falls back to a canned
# response (Absolute Rule 6 — no synthetic stand-ins for real LLM output).
OPENROUTER_MODEL = os.environ.get("OPENROUTER_MODEL", "anthropic/claude-sonnet-4.5")
OPENROUTER_BASE_URL = os.environ.get("OPENROUTER_BASE_URL", "https://openrouter.ai/api/v1")
OPENROUTER_TIMEOUT_SECONDS = 60
COPILOT_DEDUP_SIMILARITY_THRESHOLD = 0.8
# Backtest window when a strategy spec doesn't otherwise imply one — kept
# short/deliberate rather than "as much history as exists" so a Co-Pilot
# backtest returns quickly; real historical prices only, no synthetic fill.
COPILOT_BACKTEST_YEARS = 3

# ---------------------------------------------------------------------------
# F&O features — SPEC-FEAT-004, P2.3
# ---------------------------------------------------------------------------
# features/fno_features.py's Black-Scholes IV inversion: NSE's F&O bhavcopy
# has no risk-free-rate field — flat approximate reference rate, same
# documented-approximation precedent as ASSUMED_FD_RATE/ASSUMED_TAX_RATE
# (India's ~10yr G-Sec yield has hovered close to this band).
INDIA_RISK_FREE_RATE = 0.07
# F&O eligibility lookback: a ticker is treated as F&O-eligible as of date
# D if fno_data has any contract row for it within this many calendar days
# before D (NSE revises the F&O-eligible list quarterly; this window is
# comfortably wider than one expiry cycle so a ticker isn't wrongly flagged
# ineligible just because today happens to fall between two of its expiries).
FNO_ELIGIBILITY_LOOKBACK_DAYS = 35
# IV solver: Brent's method bracket and tolerance for Black-Scholes
# inversion — vol below 1%/above 500% is treated as un-solvable (NaN), not
# clamped, since premiums that don't bracket a root in this range are
# usually a stale/zero-volume quote, not real implied volatility.
IV_SOLVER_MIN_VOL = 0.01
IV_SOLVER_MAX_VOL = 5.0

# features/matrix_builder.py compute_features step, live/daily path only
# (2026-07 perf fix — see BuildLog.md). HMM fitting is CPU-bound (one
# GaussianHMM fit per ticker, hmmlearn has no batch API), so it needs real
# processes; the fundamentals/shareholding pre-load is pure network I/O
# against the local DataStore API, so threads are fine and far cheaper.
# HMM_FEATURE_WORKERS=3 (not the 10 scripts/feature_backfill_hybrid.py's
# --help suggests for a 14-core box) because 10 spawn-context workers
# OOM-killed this machine twice on 2026-06-26 against the 501-ticker
# universe (confirmed via journalctl); the following day's run against the
# full ~2,644-ticker universe used 3 workers with no OOM.
# Env-overridable so scripts/monitor_scheduler_resources.py can dial these
# down at runtime under memory pressure without editing this file.
HMM_FEATURE_WORKERS = int(os.environ.get("HMM_FEATURE_WORKERS", "3"))
FEATURE_CACHE_PRELOAD_WORKERS = int(os.environ.get("FEATURE_CACHE_PRELOAD_WORKERS", "16"))

# ---------------------------------------------------------------------------
# Off-machine backup — rclone to Backblaze B2 (2026-07-04 architecture
# review; switched from an initial Google Drive design after the OAuth
# setup proved impractical to automate headlessly — see
# scripts/backup_to_b2.py's module docstring)
# ---------------------------------------------------------------------------
# False by default so a fresh checkout never fails a scheduled backup run
# before B2 credentials exist. Flip to true in .env once BACKBLAZE_KEY_ID/
# BACKBLAZE_APPLICATION_KEY/BACKBLAZE_BUCKET are set and verified.
BACKUP_ENABLED = os.environ.get("BACKUP_ENABLED", "false").lower() == "true"
# SPEC-SEC-001: credentials from environment only, never hardcoded — same
# convention as every other credential block in this file. Created once on
# backblaze.com (App Keys page); no OAuth, no interactive rclone config.
BACKBLAZE_KEY_ID = os.environ.get("BACKBLAZE_KEY_ID")
BACKBLAZE_APPLICATION_KEY = os.environ.get("BACKBLAZE_APPLICATION_KEY")
BACKBLAZE_BUCKET = os.environ.get("BACKBLAZE_BUCKET")
# Destination folder path within the bucket.
BACKUP_REMOTE_PATH = os.environ.get("BACKUP_REMOTE_PATH", "AlphaLens_Backup")
# Daily, off-hours — after the 20:00 model-training check has had time to
# finish on a normal day, comfortably inside the 23-hour pipeline window.
BACKUP_SCHEDULE_TIME = "22:30"  # HH:MM, Asia/Kolkata, every day

# A21 (Pipeline Health Checker): weekly job-completeness audit. Fires
# after Saturday's weekend batch and Sunday's multibagger/forensic
# scoring jobs have had a chance to record their own job_run_log rows for
# the week, so this audit isn't racing the very jobs it's checking.
JOB_HEALTH_CHECK_DAY_OF_WEEK = "sun"
JOB_HEALTH_CHECK_SCHEDULE_TIME = "11:00"  # HH:MM, Asia/Kolkata, sunday

# ---------------------------------------------------------------------------
# DataStore API — SPEC-DS-002
# ---------------------------------------------------------------------------
DATASTORE_API_HOST = os.environ.get("DATASTORE_API_HOST", "localhost")
DATASTORE_API_PORT = int(os.environ.get("DATASTORE_API_PORT", "8000"))
# NOTE: no "/api/v1" suffix here — datastore/client.py's methods already
# pass full "/api/v1/..." paths to self._get(). A previous version of this
# constant included the suffix, which silently doubled it into
# ".../api/v1/api/v1/ohlcv/..." for every DataStoreClient call (caught
# while wiring features/matrix_builder.py, P1.1 — see BuildLog.md).
DATASTORE_API_BASE_URL = f"http://{DATASTORE_API_HOST}:{DATASTORE_API_PORT}"

# CORS origins allowed to call the API. Defaults cover the Vite dashboard's
# dev server (5173) and local preview build (4173); production origins are
# added via the FRONTEND_ORIGINS env var (comma-separated), SPEC-SEC-003.
DATASTORE_API_CORS_ORIGINS = [
    "http://localhost:5173",
    "http://localhost:4173",
] + [o.strip() for o in os.environ.get("FRONTEND_ORIGINS", "").split(",") if o.strip()]

# ---------------------------------------------------------------------------
# Big Investor Activity — bulk/block deals + MF holdings (Phase A)
# ---------------------------------------------------------------------------
# Fixed Rs. crore market-cap bands, independent of stock_master.current_tier
# (which is rank-based). Placeholders — tune to taste.
BIG_INVESTOR_CAP_LARGE_CR = 20000   # > this = Large
BIG_INVESTOR_CAP_MID_CR = 5000      # 5000-20000 = Mid
BIG_INVESTOR_CAP_SMALL_CR = 1000    # 1000-5000 = Small
# < 1000 = Micro

# Phase B: same-day BUY vs SELL by the same client/ticker/deal_type within
# this fraction of quantity is treated as a wash trade (nets to the
# residual, not double-counted as both a real buy and a real sell).
INTRADAY_NETTING_QTY_TOLERANCE_PCT = 0.02

# Seed file for the investor_family table — reviewed/edited manually,
# never auto-loaded. See scripts/load_investor_family_seed.py.
BIG_INVESTOR_FAMILY_SEED_PATH = "datastore/seed/investor_family_seed.yaml"

# ---------------------------------------------------------------------------
# A25: Write-audit-publish — staging schema + atomic publish + rollback
# snapshots (2026-07-09). Pilot scope: fno_data, ohlcv_adjusted — the two
# tables that change daily and drive the real incremental snapshot cost
# (see FeatureBacklog.md A25). Staging itself lives inside the DuckDB
# `staging` schema (alphalens.duckdb), not a separate directory — STAGING_DIR
# is reserved for any on-disk staging artifacts a future source may need.
# ---------------------------------------------------------------------------
STAGING_DIR = DATASTORE_DIR / "staging"
SNAPSHOT_DIR = DATASTORE_DIR / "snapshots"
# N=7 daily rollback snapshots, per A25's storage-budget design (15GB cap,
# incremental/hardlinked — not full DB copies). Env-overridable so the
# budget can be tightened (first lever per the design doc) without a code
# change if measured deltas run over budget.
SNAPSHOT_RETENTION_N = int(os.environ.get("SNAPSHOT_RETENTION_N", "7"))
if SNAPSHOT_RETENTION_N <= 0:
    raise ValueError(f"SNAPSHOT_RETENTION_N must be positive, got {SNAPSHOT_RETENTION_N}")
# Cross-process advisory lock guarding the staging→publish sequence — same
# fcntl.flock pattern as PIPELINE_RUN_LOCK_PATH above, and for the same
# reason (commit 8147579: two processes independently opening a writable
# DuckDB connection to the same file is unsafe). Separate lock file since
# publish can run outside the daily pipeline (e.g. a manual backfill).
PUBLISH_RUN_LOCK_PATH = NORMALISED_DIR / ".publish_run.lock"

# ---------------------------------------------------------------------------
# REV27 (2026-07-21 review): DuckDB lock-conflict retry budget
# (datastore/api/db.py::_connect_with_retry, SPEC-SCHED-013). Previously
# hardcoded (4 attempts, 0.5s base -> ~3.5s worst-case wait). Moved here,
# env-overridable, so an operator can extend the budget for a known-long
# write (a full backfill, a universe-wide compute_features run) without a
# code change. Default attempts raised 4 -> 6 (worst case ~15.5s:
# 0.5+1+2+4+8) — still short enough not to hang an API request badly, long
# enough to ride out more real write-step handoffs than before.
#
# This is a bounded retry, not a guarantee: a write step that holds the
# write lock LONGER than this budget will still hard-fail a concurrent read
# (the same failure class that crashed the scheduler once — see BuildLog.md
# project memory). Operational rule: avoid starting a long write (full
# backfill, universe-wide compute_features) while API read traffic is
# expected, rather than relying on this retry alone.
# ---------------------------------------------------------------------------
DUCKDB_LOCK_RETRY_ATTEMPTS = int(os.environ.get("DUCKDB_LOCK_RETRY_ATTEMPTS", "6"))
DUCKDB_LOCK_RETRY_BASE_DELAY_S = float(os.environ.get("DUCKDB_LOCK_RETRY_BASE_DELAY_S", "0.5"))

# 2026-07-26 fix: a separate, longer retry budget for backtest jobs' own
# read-write connection to BACKTEST_DUCKDB_PATH specifically (wired via
# get_duckdb_connection(..., retry_attempts=..., retry_base_delay_s=...) in
# backtest/run_orchestrator_backtest.py) — NOT the default above, which is
# still what the API's read-only polling endpoints use. Reviewed by
# ml-rigor-reviewer + backtest-reviewer: with 3+ browser tabs polling
# queue/orchestrator status every ~3s, the API's near-continuous read-only
# connections to backtest.duckdb increasingly starved out backtest jobs'
# write-connection attempts before DUCKDB_LOCK_RETRY_ATTEMPTS's ~15.5s
# budget elapsed (6+ job failures in ~2h, all
# "Could not set lock ... Conflicting lock is held" against the API's PID).
# 8 attempts / 1.0s base -> worst case ~127s (1+2+4+8+16+32+64), long enough
# to ride out sustained read-lock churn; kept separate from the default so
# the API's own read-only path (which shares _connect_with_retry) is not
# slowed down by this widening.
#
# 2026-07-26 follow-up: 8 uncapped-exponential attempts still wasn't
# enough — job[53] failed after all 8 attempts (~127s) with the SAME
# polling-driven contention, because the later delays (16s/32s/64s) are
# far wider than the observed ~6-7s frontend polling cycle, so most of the
# 127s budget was spent NOT even trying. The fix is more attempts spread
# across the window, not more total wait: cap each delay at
# DUCKDB_WRITE_LOCK_RETRY_MAX_DELAY_S so backoff still ramps up for the
# first few tries (avoiding a thundering-herd retry burst) but then holds
# at a delay shorter than a polling cycle, giving many more chances to
# land in a gap. 16 attempts / 1.0s base / 10s cap -> worst case ~125s
# (1+2+4+8+10*11), same order of total wait as before but double the
# distinct attempts.
DUCKDB_WRITE_LOCK_RETRY_ATTEMPTS = int(os.environ.get("DUCKDB_WRITE_LOCK_RETRY_ATTEMPTS", "16"))
DUCKDB_WRITE_LOCK_RETRY_BASE_DELAY_S = float(os.environ.get("DUCKDB_WRITE_LOCK_RETRY_BASE_DELAY_S", "1.0"))
DUCKDB_WRITE_LOCK_RETRY_MAX_DELAY_S = float(os.environ.get("DUCKDB_WRITE_LOCK_RETRY_MAX_DELAY_S", "10.0"))
