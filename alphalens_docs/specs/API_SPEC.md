# AlphaLens — API & Module Contract Specification

Every module MUST implement the interfaces defined here.
Claude Code SHALL refuse to generate code that violates these contracts.

---

## Pipeline Module Contracts

### bhavcopy.py
```python
def download_bhavcopy(date: str) -> pd.DataFrame:
    """
    Download NSE equity bhavcopy for given date.
    Args:
        date: YYYY-MM-DD format
    Returns:
        DataFrame with columns: ticker, open, high, low, close, volume,
                                 traded_qty, delivery_qty, series
        Raises: ConnectionError if download fails after 3 retries
                ValueError if fewer than 450 stocks found in result
    """

def validate_bhavcopy(df: pd.DataFrame, expected_tickers: List[str]) -> dict:
    """
    Returns: {'ok': bool, 'missing': List[str], 'anomalies': List[str]}
    anomalies = stocks with > 30% single-day price change without known corp action
    """
```

### price_adjuster.py
```python
def adjust_for_corporate_actions(conn: sqlite3.Connection,
                                   ticker: str) -> None:
    """
    Apply all retroactive price adjustments for a ticker.
    MUST be idempotent — safe to call multiple times.
    Uses adj_factor column to track cumulative adjustments.
    """

def get_adjustment_factor(conn, ticker: str, as_of_date: str) -> float:
    """
    Returns cumulative adjustment factor for ticker as of date.
    Used to convert raw prices to adjusted prices without re-running full adjustment.
    """
```

### feature_matrix.py
```python
def build_feature_matrix(date: str,
                           tickers: List[str],
                           db_path: str) -> pd.DataFrame:
    """
    Assemble complete feature matrix for a date.
    Args:
        date: YYYY-MM-DD
        tickers: list of NSE tickers
        db_path: path to SQLite databases
    Returns:
        DataFrame: shape (len(tickers), N_FEATURES)
                   columns: all feature names
                   index: ticker
    Raises:
        ValueError if complete_stocks < 450
    Contract:
        - All features use only data where effective_date <= date
        - No future data permitted (enforced by SQL date filters)
        - NaN allowed for stocks missing fundamental data
    """
```

---

## Model Module Contracts

### BaseModel (all models must inherit)
```python
class BaseModel(ABC):
    @abstractmethod
    def train(self, X_train: pd.DataFrame, y_train: pd.Series,
              X_val: pd.DataFrame, y_val: pd.Series) -> None:
        """Train model. Must call Optuna HPO internally for tunable models."""

    @abstractmethod
    def predict(self, X: pd.DataFrame) -> pd.Series:
        """Return point prediction (class label or continuous value)."""

    @abstractmethod
    def predict_proba(self, X: pd.DataFrame) -> np.ndarray:
        """Return probability array. Shape: (n_samples, n_classes)."""

    @abstractmethod
    def save(self, path: str) -> None:
        """Serialize model to path. Must include metadata (train_date, version, features)."""

    @classmethod
    @abstractmethod
    def load(cls, path: str) -> 'BaseModel':
        """Deserialize model from path."""

    def get_shap_values(self, X: pd.DataFrame) -> pd.DataFrame:
        """Return SHAP values. Shape: (n_samples, n_features). Required for all models."""

    def get_feature_importance(self) -> pd.Series:
        """Return feature importance sorted descending."""
```

### PnDDetector specific contract
```python
def predict_full(self, X: pd.DataFrame) -> pd.DataFrame:
    """
    Returns DataFrame with columns:
        pnd_score: float 0–100
        pnd_phase: str in {'normal','accumulation','pump','dump','aftermath'}
        pnd_block: bool (True if score > 60)
        pnd_flag:  bool (True if score > 40)
    MUST be called BEFORE any signal model in the daily pipeline.
    """
```

### SignalModel specific contract
```python
def predict_signals(self, X: pd.DataFrame) -> pd.DataFrame:
    """
    Returns DataFrame with columns:
        signal_buy_prob:  float 0–1
        signal_hold_prob: float 0–1
        signal_sell_prob: float 0–1
        signal_q10:       float (10th percentile expected return)
        signal_q50:       float (median expected return)
        signal_q90:       float (90th percentile expected return)
    """
```

### ExitSignalModel specific contract
```python
def predict_exit(self, X: pd.DataFrame,
                  positions: Dict[str, Position]) -> pd.DataFrame:
    """
    Returns DataFrame with columns:
        exit_urgency: float 0–100
        exit_type:    str in {'thesis_broken','momentum_exhaustion',
                               'risk_management','target_achieved',
                               'opportunity_cost','pnd_exit'}
        exit_survival_5d:  float 0–1  (probability position still profitable in 5d)
        exit_survival_21d: float 0–1
        exit_survival_63d: float 0–1
    """
```

### MultibaggerModel specific contract
```python
def predict_watchlist(self, X: pd.DataFrame) -> pd.DataFrame:
    """
    Returns DataFrame with columns:
        mb_probability:   float 0–1
        mb_tier:          str in {'2x','3x','5x','10x','none'}
        mb_archetype:     str in {'long_base_breakout','post_crash_recovery',
                                   'quiet_accumulator','sector_rotation_leader'}
        mb_survival_12m:  float (probability hits 2x within 12 months)
        mb_survival_36m:  float (probability hits 2x within 36 months)
        mb_shap_json:     str (JSON array of top 5 SHAP drivers)
    Frequency: WEEKLY only (called Monday). Not daily.
    Scope: Tier 1–3 only.
    """
```

---

## Settings Contract

```python
# config/settings.py — ALL constants here, nothing hardcoded elsewhere

# Universe
# UNIVERSE_TICKERS_PATH (deprecated — now from stock_master table)
# UNIVERSE_TICKERS_PATH = 'config/nifty500_tickers.csv'
UNIVERSE_PROFILE = 'phase_1'  # Configurable: phase_1, phase_2, phase_3, full_nse

# Paths
DATA_ROOT = 'datastore/'
DB_PATH = 'datastore/normalised/'
FEATURES_PATH = 'datastore/features/daily/'
MODELS_PATH = 'datastore/models/'
LOGS_PATH = 'datastore/logs/'

# Pipeline thresholds
MIN_COMPLETE_STOCKS = 450
MIN_HISTORY_DAYS = 252

# Model thresholds
SIGNAL_BUY_THRESHOLD = 0.65
META_ACT_THRESHOLD = 0.50
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

# Transaction costs (see 03_data_pipeline.md for full breakdown)
TOTAL_ROUNDTRIP_COST = 0.005
```

---

## Error Handling Contract

All pipeline functions must follow this error handling pattern:

```python
import logging
from functools import wraps

def pipeline_step(func):
    """Decorator for pipeline steps. Logs timing and handles errors."""
    @wraps(func)
    def wrapper(*args, **kwargs):
        log = logging.getLogger('pipeline')
        start = time.time()
        try:
            result = func(*args, **kwargs)
            log.info(f"{func.__name__} completed in {time.time()-start:.1f}s")
            return result
        except Exception as e:
            log.error(f"{func.__name__} FAILED: {e}", exc_info=True)
            raise PipelineStepError(func.__name__, e) from e
    return wrapper
```

---

## DataStore API Endpoints (FastAPI)

### Launch
```bash
uvicorn datastore.api.main:app --host 0.0.0.0 --port 8000 --reload
# Swagger docs: http://localhost:8000/docs
```

### Endpoint Contracts

#### /api/v1/ohlcv
```python
@router.get("/{ticker}")
async def get_ohlcv(
    ticker: str,
    from_date: date = Query(None),
    to_date: date = Query(None),
    adjusted: bool = Query(True),
) -> List[OHLCVRow]:
    """
    Returns adjusted (default) or raw OHLCV + delivery data.
    Includes: date, open, high, low, close, volume, delivery_qty, delivery_pct, adj_factor
    """

@router.get("/{ticker}/latest")
async def get_latest_ohlcv(ticker: str) -> OHLCVRow:
    """Returns most recent trading day's data for a ticker."""

@router.get("/universe")
async def get_universe_ohlcv(
    date: date,
    tier: Optional[int] = Query(None),
) -> List[OHLCVRow]:
    """Returns OHLCV for all tickers in universe (or filtered by tier) for a date."""
```

#### /api/v1/fundamentals
```python
@router.get("/{ticker}")
async def get_fundamentals(
    ticker: str,
    as_of: date = Query(None),   # PIT enforcement: returns latest where announcement_date <= as_of
) -> FundamentalRow:
    """
    CRITICAL PIT RULE: Joins on announcement_date, never quarter_end_date.
    If as_of is None, uses today.
    Returns: all 28 fundamental metrics + announcement_date + staleness features.
    """

@router.get("/{ticker}/history")
async def get_fundamental_history(
    ticker: str,
    quarters: int = Query(8),
) -> List[FundamentalRow]:
    """Returns last N quarters of fundamentals, ordered by announcement_date desc."""

@router.get("/{ticker}/staleness")
async def get_staleness(ticker: str) -> StalenessInfo:
    """Returns: days_since_results, quarter_age_pct, results_pending_flag."""
```

#### /api/v1/governance
```python
@router.get("/{ticker}")
async def get_governance(
    ticker: str,
    as_of: date = Query(None),   # PIT enforcement: joins on filing_date
) -> GovernanceRow:
    """
    PIT RULE: Joins on filing_date (~21 days after quarter_end_date).
    Returns: promoter_pct, promoter_pledge, fii_pct, dii_pct, mf_pct, changes.
    """
```

#### /api/v1/features
```python
@router.get("/{ticker}/{date}")
async def get_features(ticker: str, date: date) -> Dict[str, float]:
    """Returns all features for one stock on one date as key-value dict."""

@router.get("/matrix/{date}")
async def get_feature_matrix(date: date) -> FileResponse:
    """Returns full 500×330 Parquet file for download. Used by ML engine."""

@router.get("/catalog")
async def get_feature_catalog() -> Dict[str, FeatureMeta]:
    """Returns feature_catalog.json with metadata for all 330 features."""
```

#### /api/v1/signals
```python
@router.get("/ml/{ticker}/{date}")
async def get_ml_signals(ticker: str, date: date) -> MLSignalRow:
    """Returns all ML model outputs for a stock on a date."""

@router.get("/ml/top_buys/{date}")
async def get_top_buys(date: date, n: int = Query(10)) -> List[MLSignalRow]:
    """Returns top N buy signals ranked by probability × meta_prob."""

@router.post("/ml")
async def write_ml_signal(signal: MLSignalInput) -> StatusResponse:
    """Upsert ML signal. Idempotent: same date+ticker replaces existing."""

@router.get("/ta/{ticker}/{date}")
async def get_ta_signals(ticker: str, date: date) -> TASignalRow:
    """Returns TA system outputs. Available from Phase 3."""

@router.post("/ta")
async def write_ta_signal(signal: TASignalInput) -> StatusResponse:
    """Write TA system output. Called by systems/technical_analysis/."""

@router.get("/valuation/{ticker}")
async def get_valuation(ticker: str) -> ValuationRow:
    """Returns latest Damodaran valuation. Available from Phase 3."""

@router.post("/valuation")
async def write_valuation(signal: ValuationInput) -> StatusResponse:
    """Write valuation output. Called by systems/damodaran_valuation/."""

@router.get("/fa/{ticker}/{date}")
async def get_fa_signals(ticker: str, date: date) -> FASignalRow:
    """Returns FA system outputs. Available from Phase 4."""

@router.post("/fa")
async def write_fa_signal(signal: FASignalInput) -> StatusResponse:
    """Write FA system output. Called by systems/fundamental_analysis/."""

@router.get("/forensic/{ticker}")
async def get_forensic(ticker: str) -> ForensicRow:
    """Returns forensic composite + all 7 classical scores + ML prob."""
```

#### /api/v1/watchlist
```python
@router.get("/current")
async def get_watchlist() -> List[MultibaggerRow]:
    """Returns current top-20 multibagger watchlist (updated weekly)."""

@router.get("/history")
async def get_watchlist_history(weeks: int = Query(12)) -> List[WatchlistSnapshot]:
    """Returns watchlist changes over time (additions, removals, rank changes)."""
```

#### /api/v1/alerts
```python
@router.get("/today")
async def get_today_alerts() -> List[AlertRow]:
    """Returns all P&D, exit, forensic, drift alerts for today."""

@router.get("/history")
async def get_alert_history(
    alert_type: Optional[str] = Query(None),
    days: int = Query(30),
) -> List[AlertRow]:
    """Returns alert history filtered by type and lookback days."""
```

#### /api/v1/universe
```python
@router.get("/stocks")
async def get_stocks(
    sector: Optional[str] = Query(None),
    tier: Optional[int] = Query(None),
) -> List[StockMasterRow]:
    """Returns stock master data. Filterable by sector and/or tier."""

@router.get("/stocks/{ticker}")
async def get_stock(ticker: str) -> StockMasterRow:
    """Single stock metadata: sector, tier, listing date, market cap, ADTV."""

@router.get("/tiers")
async def get_tiers() -> Dict[int, TierInfo]:
    """Returns tier definitions with current stock counts."""
```

#### /api/v1/system
```python
@router.get("/health")
async def health_check() -> SystemHealthResponse:
    """
    Returns: pipeline_status (ok/degraded/failed), last_run_date,
    last_run_duration_sec, complete_stocks_count, scraper_status.
    """

@router.get("/drift")
async def get_drift_status() -> Dict[str, float]:
    """Returns PSI values for top 50 features. Flags >0.10 and >0.25."""

@router.get("/models")
async def get_model_registry() -> Dict[str, ModelInfo]:
    """Returns registry.json summary: model name, version, train date, accuracy."""

@router.get("/scheduler")
async def get_scheduler_status() -> SchedulerStatus:
    """
    Returns: last_successful_run, pending_backfill_dates, next_scheduled_run,
    oracle_scraper_status, missed_days_count.
    """
```
