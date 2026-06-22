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
UNIVERSE_PROFILE = os.environ.get("UNIVERSE_PROFILE", "phase_1")  # phase_1 | phase_2 | phase_3 | full_nse

# tier_threshold: include tiers <= this value
# min_adtv_cr: minimum average daily traded value, INR crore
# min_mcap_cr: minimum market capitalisation, INR crore
#
# NOTE on phase_1's tier_threshold (5, not the original 2): config/build_universe.py
# assigns tier purely from NSE's own Nifty 500 sub-index membership (1=Nifty50,
# 2=NiftyNext50, 3=Midcap150, 4=Smallcap250, 5=every other Nifty 500 member) —
# all 5 tiers are slices *within* the Nifty 500, never a broader NSE universe.
# tier_threshold=2 therefore structurally caps phase_1 at 102 stocks
# (Nifty50+Next50), conflicting with SPEC-SYS-001 ("System monitors 500 stocks
# (Nifty 500) in Phase 1") and CLAUDE.md's NIFTY_500_SIZE=500. tier_threshold=5
# is the value that actually reaches the full Nifty 500 under this tier scheme.
# phase_2/phase_3/full_nse's tier_threshold values (3/4/5) are NOT fixed here —
# build_universe.py has no source for a broader-than-Nifty-500 universe yet
# (SPEC-SYS-011's ~2,000/~3,500/~5,000+ "Approx Stocks" column assumes one), so
# those three profiles remain aspirational/Phase 2+ scope, unchanged by this fix.
UNIVERSE_PROFILES = {
    "phase_1": {"tier_threshold": 5, "min_adtv_cr": 5.0, "min_mcap_cr": 500},
    "phase_2": {"tier_threshold": 3, "min_adtv_cr": 0.5, "min_mcap_cr": 100},
    "phase_3": {"tier_threshold": 4, "min_adtv_cr": 0.1, "min_mcap_cr": 50},
    "full_nse": {"tier_threshold": 5, "min_adtv_cr": 0.0, "min_mcap_cr": 0},
}

if UNIVERSE_PROFILE not in UNIVERSE_PROFILES:
    raise ValueError(
        f"Unknown UNIVERSE_PROFILE '{UNIVERSE_PROFILE}'. "
        f"Must be one of {list(UNIVERSE_PROFILES)}."
    )

TIER_THRESHOLD = UNIVERSE_PROFILES[UNIVERSE_PROFILE]["tier_threshold"]
MIN_ADTV_CR = UNIVERSE_PROFILES[UNIVERSE_PROFILE]["min_adtv_cr"]
MIN_MCAP_CR = UNIVERSE_PROFILES[UNIVERSE_PROFILE]["min_mcap_cr"]

NIFTY_500_SIZE = 500  # Phase 1 reference universe size; not a hard cap — see SPEC-SYS-011
TIER_COUNT = 5
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
MIN_STOCKS_FOR_INFERENCE = 450  # out of NIFTY_500_SIZE
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
