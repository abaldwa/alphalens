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
# ---------------------------------------------------------------------------
PIPELINE_MAX_DURATION_MINUTES = 90
PIPELINE_WINDOW_START = "15:30"  # 3:30 PM
PIPELINE_WINDOW_END = "09:15"  # 9:15 AM next day
PIPELINE_WINDOW_HOURS = 15
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
FEATURES_DIR = DATASTORE_DIR / "features"
SIGNALS_DIR = DATASTORE_DIR / "signals"
MODELS_DIR = DATASTORE_DIR / "models"
OUTPUTS_DIR = DATASTORE_DIR / "outputs"
LOGS_DIR = DATASTORE_DIR / "logs"

# Store 2: Normalised (DuckDB analytical + SQLite transactional) — SPEC-DS-007
DUCKDB_PATH = NORMALISED_DIR / "alphalens.duckdb"
PIPELINE_LOG_DB_PATH = NORMALISED_DIR / "pipeline_log.db"
SCHEDULER_DB_PATH = NORMALISED_DIR / "scheduler.db"
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
DEFAULT_RETRY_DELAY_SECONDS = 60
RETRAIN_OVERDUE_MULTIPLIER = 1.5  # days_since_retrain > interval * this => overdue

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
# Conservative PIT defaults when Screener.in doesn't expose the real
# disclosure date directly (SPEC-PIPE-003): NSE listing rules give
# companies up to 45 days after quarter-end to announce results, and BSE
# shareholding filings are due ~21 days after quarter-end
# (alphalens_docs/03_data_pipeline.md). Using the regulatory deadline
# rather than a shorter guess means a feature can never be backdated to a
# date earlier than the data was truly knowable.
FUNDAMENTALS_ANNOUNCEMENT_DELAY_DAYS = 45
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
# [AS BUILT, P2.2 continued] Twice a month, not once: Groww (the primary
# source as of the pivot to Groww) exposes only its current live
# snapshot, no historical archive, and AMC disclosure timing varies
# enough that a single monthly check risks landing on a stale/transitional
# snapshot. Cron day-of-month field syntax (comma-separated), passed
# straight to APScheduler's CronTrigger(day=...).
MF_HOLDINGS_SCHEDULE_DAYS = "5,20"
AMFI_SCHEDULE_TIME = "08:00"  # HH:MM, Asia/Kolkata
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
# FUNDAMENTALS_ANNOUNCEMENT_DELAY_DAYS above.

# ---------------------------------------------------------------------------
# Tijori Finance Pro (sector-specific operational metrics) — SPEC-PIPE-003, P2.6
# ---------------------------------------------------------------------------
TIJORI_RAW_DIR = RAW_DIR / "tijori"
TIJORI_RATE_LIMIT_SLEEP_SECONDS = 1.0

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
