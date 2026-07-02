"""
datastore/api/schemas.py

Phase: 0.1 (Project Skeleton)
Specs: SPEC-DS-001, SPEC-DS-002, SPEC-DS-004, SPEC-DS-005, SPEC-DS-007
Owner: Platform / DataStore
Consumers: datastore/api/main, datastore/api/routers, systems/ml_signal_engine, backtest

Pydantic models for FastAPI request/response schemas.
Single source of truth for API contracts across all endpoints.
SOLID: Each schema is focused on one resource type.
"""

from datetime import datetime
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, ConfigDict, Field


# ===== OHLCV (SPEC-DS-001) =====
class OHLCVRow(BaseModel):
    """Single OHLCV record (open, high, low, close, volume)."""

    date: datetime
    ticker: str
    open: float = Field(gt=0, description="Opening price in INR")
    high: float = Field(gt=0, description="High price in INR")
    low: float = Field(gt=0, description="Low price in INR")
    close: float = Field(gt=0, description="Closing price in INR")
    volume: int = Field(ge=0, description="Volume in shares")
    adjusted_close: Optional[float] = Field(default=None, description="Split/dividend adjusted")
    delivery_pct: Optional[float] = Field(
        default=None, description="Delivery quantity as % of traded volume (SPEC-PIPE-005 range [0,100])"
    )
    adj_factor: Optional[float] = Field(
        default=None,
        description="Cumulative corporate-action adjustment factor applied to this row "
        "(ingestion/adjust/price_adjuster.py); 1.0 if no adjustment has been applied yet.",
    )


class OHLCVResponse(BaseModel):
    """Response for OHLCV query endpoint."""

    ticker: str
    start_date: datetime
    end_date: datetime
    data: List[OHLCVRow]
    record_count: int


class OHLCVUniverseResponse(BaseModel):
    """Distinct tickers present in ohlcv_adjusted, with row counts — GET /api/v1/ohlcv/_meta/tickers."""

    tickers: List[str]
    row_counts: Dict[str, int]


# ===== Fundamentals — full row schema (SPEC-DS-003, SPEC-PIPE-003, P2.1) =====
# [AS BUILT, P2.1] Replaced the original P0.1 narrow metric_name/metric_value
# pair design with a wide-table schema mirroring datastore/schema/
# create_normalised.py's `fundamentals` table columns directly — same
# precedent as P1.7's MLSignalWrite/MLSignalRow superseding the old narrow
# SignalWrite/SignalResponse (see BuildLog.md "P1.7"). Never had a live
# caller (grepped before changing), so this is a clean replacement, not a
# migration.
class FundamentalsWrite(BaseModel):
    """One quarterly fundamentals row — upsert unit for POST /api/v1/fundamentals/write."""

    ticker: str
    fiscal_year: int
    quarter: int = Field(ge=1, le=4)
    quarter_end_date: datetime
    announcement_date: datetime  # SPEC-PIPE-003: the PIT key — NEVER quarter_end_date
    revenue: Optional[float] = None
    ebitda: Optional[float] = None
    pat: Optional[float] = None
    eps: Optional[float] = None
    operating_margin: Optional[float] = None
    ebitda_margin: Optional[float] = None
    net_margin: Optional[float] = None
    roe: Optional[float] = None
    roce: Optional[float] = None
    debt_to_equity: Optional[float] = None
    interest_coverage: Optional[float] = None
    fcf: Optional[float] = None
    asset_turnover: Optional[float] = None
    inventory_days: Optional[float] = None
    receivable_days: Optional[float] = None
    payable_days: Optional[float] = None
    book_value_per_share: Optional[float] = None
    shares_outstanding: Optional[int] = None
    # [AS BUILT, P2.1] Added beyond the original P0.2 schema: features/
    # fundamental.py's gross_margin, capex_intensity, roic, net_debt_to_ebitda,
    # and current_ratio (P2.1 build prompt) need raw line items the original
    # 19-column table never carried. Safe in-place addition — the
    # `fundamentals` table has had zero rows since P0.2 (screener.py, built
    # this phase, is its first-ever writer); see BuildLog.md "P2.1".
    gross_profit: Optional[float] = None
    capex: Optional[float] = None
    current_assets: Optional[float] = None
    current_liabilities: Optional[float] = None
    total_debt: Optional[float] = None
    cash_and_equivalents: Optional[float] = None
    # [AS BUILT, P2.5] Already parsed by screener.py (used internally to derive
    # ebitda) but never persisted until now — exposed for systems/ml_signal_engine/
    # models/forensic/classical_scores.py's Beneish DEPI and Ohlson FFO inputs.
    depreciation: Optional[float] = None
    # [AS BUILT, P2.6] ingestion/scrapers/tijori.py's sector-specific operational
    # metrics (ARPU/NPA/ANDA/etc.) — generic numbered columns since the metric
    # *meaning* varies by sector; see tijori.py's _SECTOR_METRICS map.
    sector_specific_metric_1: Optional[float] = None
    sector_specific_metric_2: Optional[float] = None
    sector_specific_metric_3: Optional[float] = None
    sector_specific_metric_4: Optional[float] = None
    sector_specific_metric_5: Optional[float] = None
    sector_specific_metric_6: Optional[float] = None


class FundamentalsRow(FundamentalsWrite):
    """A stored fundamentals row, as returned by GET /api/v1/fundamentals/{ticker}."""


class FundamentalsWriteResult(BaseModel):
    """Confirmation response for a single FundamentalsWrite upsert."""

    ticker: str
    fiscal_year: int
    quarter: int
    written: bool


class FundamentalsResponse(BaseModel):
    """Response for GET /api/v1/fundamentals/{ticker} — PIT-filtered, ascending by announcement_date."""

    ticker: str
    as_of: datetime
    data: List[FundamentalsRow]
    record_count: int


# ===== Shareholding — full row schema (SPEC-DS-003, SPEC-PIPE-003, P2.1) =====
class ShareholdingWrite(BaseModel):
    """One quarterly shareholding row — upsert unit for POST /api/v1/shareholding/write."""

    ticker: str
    quarter_end_date: datetime
    filing_date: datetime  # SPEC-PIPE-003: the PIT key — NEVER quarter_end_date
    promoter_pct: Optional[float] = Field(default=None, ge=0.0, le=100.0)
    promoter_pledge: Optional[float] = Field(default=None, ge=0.0, le=100.0)
    fii_pct: Optional[float] = Field(default=None, ge=0.0, le=100.0)
    dii_pct: Optional[float] = Field(default=None, ge=0.0, le=100.0)
    mf_pct: Optional[float] = Field(default=None, ge=0.0, le=100.0)
    retail_pct: Optional[float] = Field(default=None, ge=0.0, le=100.0)
    # [AS BUILT, P2.6] ingestion/scrapers/trendlyne.py's superstar-investor
    # tracking — `shareholding` IS this project's "governance" store
    # (12_platform_architecture.md line 320), so these land here rather than
    # on a new standalone table. superstar_change is the QoQ change in the
    # combined superstar-holding stake (percentage points), signed.
    superstar_flag: Optional[bool] = None
    superstar_change: Optional[float] = None


class ShareholdingRow(ShareholdingWrite):
    """A stored shareholding row, as returned by GET /api/v1/shareholding/{ticker}."""


class ShareholdingWriteResult(BaseModel):
    """Confirmation response for a single ShareholdingWrite upsert."""

    ticker: str
    quarter_end_date: datetime
    written: bool


class ShareholdingResponse(BaseModel):
    """Response for GET /api/v1/shareholding/{ticker} — PIT-filtered, ascending by filing_date."""

    ticker: str
    as_of: datetime
    data: List[ShareholdingRow]
    record_count: int


# ===== Corporate Actions (SPEC-DS-001, SPEC-PIPE-002, P2.2) — read-only =====
# [AS BUILT, P2.2] No write endpoint: ingestion/scrapers/bhavcopy.py and
# ingestion/adjust/price_adjuster.py already write `corporate_actions`
# directly (established P0.4 precedent — ingestion writes to DataStore
# directly, consumers read via API, SPEC-PIPE-001). This is purely the
# missing READ side, needed by features/corporate_action_features.py
# (SPEC-DS-002: features must read via the API, never a direct DuckDB query).
class CorporateActionRow(BaseModel):
    """One corporate action record, as returned by GET /api/v1/corporate_actions/{ticker}."""

    ticker: str
    ex_date: datetime
    action_type: str  # 'SPLIT' | 'BONUS' | 'BUYBACK' | 'DIVIDEND' | 'QIP' | ...
    ratio: float
    announcement_date: Optional[datetime] = None
    record_date: Optional[datetime] = None


class CorporateActionResponse(BaseModel):
    """Response for GET /api/v1/corporate_actions/{ticker} — ascending by ex_date."""

    ticker: str
    data: List[CorporateActionRow]
    record_count: int


# ===== F&O (SPEC-DS-001, SPEC-PIPE-001, P2.3) — read-only =====
# [AS BUILT, P2.3] No write endpoint: ingestion/scheduler/daily_pipeline.py's
# step_download_fno already writes fno_data directly (same established
# P0.4/P2.2 precedent as corporate_actions above). This is the read side,
# needed by features/fno_features.py (SPEC-DS-002).
class FNORow(BaseModel):
    """One F&O contract record, as returned by GET /api/v1/fno/{ticker}."""

    trade_date: datetime
    ticker: str
    instrument: str  # STF | STO | IDF | IDO (NSE UDiFF FinInstrmTp codes)
    expiry: datetime
    strike: Optional[float] = None
    option_type: Optional[str] = None  # 'CE' | 'PE' | None (futures)
    oi: Optional[int] = None
    oi_change: Optional[int] = None
    volume: Optional[int] = None
    settle_price: Optional[float] = None
    close_price: Optional[float] = None
    underlying_price: Optional[float] = None


class FNOResponse(BaseModel):
    """Response for GET /api/v1/fno/{ticker} — ascending by trade_date, expiry, strike."""

    ticker: str
    start_date: datetime
    end_date: datetime
    data: List[FNORow]
    record_count: int


# ===== Features (SPEC-FEAT-001, SPEC-DS-006) =====
class FeatureMatrixRow(BaseModel):
    """Single row of computed feature matrix."""

    date: datetime
    ticker: str
    # All feature columns follow; shape is (date, ticker) -> {feature_1, feature_2, ...}
    # Example: rsi_14=65.5, macd=-2.3, pe_ratio=25.4, ...
    feature_values: Dict[str, Optional[float]] = Field(description="Feature name -> value mapping")
    data_staleness_flag: int = Field(default=0, description="0=fresh, 1=stale (SPEC-SYS-003)")
    missing_feature_count: int = Field(
        default=0, description="Number of NaN features on this row"
    )


class FeatureMatrixResponse(BaseModel):
    """Response for feature matrix query."""

    ticker: str
    start_date: datetime
    end_date: datetime
    feature_names: List[str]
    data: List[FeatureMatrixRow]
    record_count: int


# ===== Signals (SPEC-PIPE-002, SPEC-MODEL-001) =====
class SignalWrite(BaseModel):
    """Request payload for writing ML signals."""

    # model_version intentionally matches the ml_signals DuckDB column name
    # (datastore/schema/create_signals.py) — silence pydantic's "model_"
    # protected-namespace warning rather than rename away from the schema.
    model_config = ConfigDict(protected_namespaces=())

    ticker: str
    date: datetime
    signal_name: str  # e.g., 'ml_classifier_v2'
    signal_value: float = Field(ge=0.0, le=1.0, description="Signal magnitude [0, 1]")
    model_version: str  # e.g., '2.1.0'
    probability: Optional[float] = Field(
        default=None, ge=0.0, le=1.0, description="Confidence [0, 1]"
    )
    shap_values: Optional[Dict[str, float]] = Field(
        default=None, description="Per-feature importance"
    )
    metadata: Optional[Dict[str, Any]] = None  # e.g., training_date, latency_ms


class SignalResponse(BaseModel):
    """Response after writing signal."""

    model_config = ConfigDict(protected_namespaces=())

    ticker: str
    date: datetime
    signal_name: str
    signal_value: float
    model_version: str
    written_at: datetime
    signal_id: Optional[str] = None  # Database ID if applicable


# ===== Model Registry (SPEC-MODEL-001, SPEC-DS-004) =====
class ModelMetadata(BaseModel):
    """Metadata for a trained model."""

    # model_type intentionally matches the model_registry SQLite column name
    # (datastore/api/db.py) — silence pydantic's "model_" protected-namespace
    # warning rather than rename away from the schema.
    model_config = ConfigDict(protected_namespaces=())

    name: str  # e.g., 'ClassificationModel'
    version: str  # e.g., '2.1.0'
    model_type: str  # e.g., 'xgboost_classifier'
    created_at: datetime
    features_used: List[str]
    accuracy_on_validation: Optional[float] = None
    additional_metrics: Optional[Dict[str, float]] = None
    hyperparameters: Optional[Dict[str, Any]] = None
    training_samples: Optional[int] = None
    training_time_seconds: Optional[float] = None


class ModelRegistry(BaseModel):
    """Model registry response."""

    models: List[ModelMetadata]
    total_models: int
    latest_model_by_name: Dict[str, ModelMetadata]


# ===== Pipeline Status (SPEC-PIPE-001, SPEC-SYS-002) =====
class PipelineStatus(BaseModel):
    """Status of daily pipeline execution."""

    date: datetime
    status: str  # 'running', 'completed', 'failed', 'partial'
    stage: str  # e.g., 'ingestion', 'feature_engineering', 'inference', 'output'
    records_processed: int
    records_skipped: int
    records_failed: int
    data_completeness_pct: float = Field(ge=0.0, le=100.0)
    started_at: datetime
    completed_at: Optional[datetime] = None
    duration_seconds: Optional[float] = None
    error_summary: Optional[str] = None
    notes: Optional[str] = None


# ===== ML Signals — full row schema (SPEC-DS-004, P1.7) =====
# [AS BUILT, P1.7] Added rather than reusing SignalWrite/SignalResponse above:
# those model one (ticker, date, signal_name) -> scalar signal_value pair,
# but datastore/schema/create_signals.py's ml_signals table (Phase 0.2) is a
# WIDE table — one row per (date, ticker, model_name) carrying that model's
# full typed output (buy_prob, exit_urgency, pnd_score, etc. as named
# columns, not a generic name/value pair). MLSignalWrite/MLSignalRow mirror
# that table directly so systems/ml_signal_engine/inference/daily_inference.py
# can upsert one full row per model per ticker per day, matching SPEC-DS-004's
# "same date+ticker+system replaces, never duplicates" upsert unit exactly
# (system_name == model_name here).
class MLSignalWrite(BaseModel):
    """One model's full output row for one (date, ticker) — upsert unit for POST /api/v1/signals/ml/write."""

    model_config = ConfigDict(protected_namespaces=())

    date: datetime
    ticker: str
    model_name: str
    model_version: str
    signal_direction: Optional[str] = None
    buy_prob: Optional[float] = Field(default=None, ge=0.0, le=1.0)
    hold_prob: Optional[float] = Field(default=None, ge=0.0, le=1.0)
    sell_prob: Optional[float] = Field(default=None, ge=0.0, le=1.0)
    q10_return: Optional[float] = None
    q50_return: Optional[float] = None
    q90_return: Optional[float] = None
    meta_label: Optional[str] = None
    meta_prob: Optional[float] = Field(default=None, ge=0.0, le=1.0)
    conformal_lower: Optional[float] = None
    conformal_upper: Optional[float] = None
    pnd_score: Optional[float] = Field(default=None, ge=0.0, le=100.0)
    pnd_phase: Optional[str] = None
    pnd_block: Optional[bool] = None
    hmm_regime: Optional[str] = None
    hmm_regime_prob: Optional[float] = Field(default=None, ge=0.0, le=1.0)
    hmm_stability: Optional[float] = None
    exit_urgency: Optional[float] = Field(default=None, ge=0.0, le=100.0)
    exit_type: Optional[str] = None
    exit_survival_5d: Optional[float] = Field(default=None, ge=0.0, le=1.0)
    exit_survival_21d: Optional[float] = Field(default=None, ge=0.0, le=1.0)
    exit_survival_63d: Optional[float] = Field(default=None, ge=0.0, le=1.0)
    shap_top5_json: Optional[str] = None


class MLSignalRow(MLSignalWrite):
    """Same shape as MLSignalWrite, returned by GET /api/v1/signals/ml/* endpoints."""


class MLSignalWriteResult(BaseModel):
    """Confirmation response for a single MLSignalWrite upsert."""

    model_config = ConfigDict(protected_namespaces=())

    date: datetime
    ticker: str
    model_name: str
    written: bool


# ===== Multibagger (SPEC-DS-004, SPEC-UI-003, P2.6) =====
class MultibaggerWrite(BaseModel):
    """One M-08 MultibaggerModel.predict_full() row for one (date, ticker) —
    upsert unit for POST /api/v1/signals/ml/multibagger/write."""

    date: datetime
    ticker: str
    mb_probability: Optional[float] = Field(default=None, ge=0.0, le=1.0)
    mb_tier: Optional[str] = None
    mb_archetype: Optional[str] = None
    survival_6m: Optional[float] = Field(default=None, ge=0.0, le=1.0)
    survival_12m: Optional[float] = Field(default=None, ge=0.0, le=1.0)
    survival_18m: Optional[float] = Field(default=None, ge=0.0, le=1.0)
    survival_24m: Optional[float] = Field(default=None, ge=0.0, le=1.0)
    survival_36m: Optional[float] = Field(default=None, ge=0.0, le=1.0)
    shap_top5_json: Optional[str] = None
    analogues_json: Optional[str] = None


class MultibaggerRow(MultibaggerWrite):
    """Same shape as MultibaggerWrite, returned by GET /api/v1/signals/ml/multibagger/{ticker}."""


class MultibaggerWriteResult(BaseModel):
    """Confirmation response for a single MultibaggerWrite upsert."""

    date: datetime
    ticker: str
    written: bool


# ===== Forensic (SPEC-DS-004, SPEC-MODEL-009/010, P2.6) =====
class ForensicWrite(BaseModel):
    """One M-09/M-10 forensic scoring row for one (date, ticker) —
    upsert unit for POST /api/v1/signals/ml/forensic/write."""

    date: datetime
    ticker: str
    beneish_m: Optional[float] = None
    altman_z: Optional[float] = None
    piotroski_f: Optional[float] = None
    ohlson_o: Optional[float] = None
    dechow_f: Optional[float] = None
    sloan_accrual: Optional[float] = None
    benford_mad: Optional[float] = None
    forensic_composite: Optional[float] = Field(default=None, ge=0.0, le=100.0)
    forensic_flag: Optional[bool] = None  # "blocked" — composite > FORENSIC_BLOCK_THRESHOLD (60)
    # [AS BUILT, P2.6] forensic_ml.py's 5-level taxonomy (green/yellow/orange/red/black)
    forensic_flag_label: Optional[str] = None
    forensic_ml_prob: Optional[float] = Field(default=None, ge=0.0, le=1.0)
    shap_top5_json: Optional[str] = None
    pattern_match: Optional[str] = None


class ForensicRow(ForensicWrite):
    """Same shape as ForensicWrite, returned by GET /api/v1/signals/ml/forensic/{ticker}."""


class ForensicWriteResult(BaseModel):
    """Confirmation response for a single ForensicWrite upsert."""

    date: datetime
    ticker: str
    written: bool


class ForensicFlaggedRow(BaseModel):
    """One flagged ticker, for GET /api/v1/signals/ml/forensic/flagged."""

    ticker: str
    date: datetime
    forensic_composite: Optional[float] = None
    forensic_flag_label: Optional[str] = None


class ForensicFlaggedResponse(BaseModel):
    """All tickers carrying any of the requested flag labels on the most recent scored date."""

    as_of_date: Optional[datetime] = None
    rows: List[ForensicFlaggedRow] = Field(default_factory=list)


class ForensicSummaryResponse(BaseModel):
    """Universe-wide forensic flag counts for the most recent scored date.
    Labels: green/yellow/orange/red/black (M-09/M-10 5-level taxonomy).
    red_count = red + black (critical risk); amber_count = orange + yellow (elevated risk).
    """

    as_of_date: Optional[datetime] = None
    red_count: int = 0
    amber_count: int = 0
    green_count: int = 0
    total_scored: int = 0
    available: bool = False


# ===== Governance (SPEC-DS-003, P2.6) =====
class GovernanceRow(BaseModel):
    """One quarterly governance row — `shareholding` IS this project's governance
    store (12_platform_architecture.md line 320) — returned by GET /api/v1/governance/{ticker}."""

    ticker: str
    quarter_end_date: datetime
    filing_date: datetime
    promoter_pct: Optional[float] = None
    promoter_pledge: Optional[float] = None
    fii_pct: Optional[float] = None
    dii_pct: Optional[float] = None
    mf_pct: Optional[float] = None
    retail_pct: Optional[float] = None
    superstar_flag: Optional[bool] = None
    superstar_change: Optional[float] = None


class GovernanceResponse(BaseModel):
    """Response for GET /api/v1/governance/{ticker} — PIT-filtered, ascending by filing_date."""

    ticker: str
    as_of: datetime
    data: List[GovernanceRow]
    record_count: int


# ===== Regime (SPEC-DS-002, /api/v1/macro/regime) =====
class RegimeResponse(BaseModel):
    """Current market-wide HMM regime state."""

    date: Optional[datetime] = None
    hmm_regime: Optional[str] = None
    hmm_regime_prob: Optional[float] = None
    hmm_stability: Optional[float] = None
    available: bool = Field(description="False if no regime row has been written yet")


class RegimeHistoryRow(BaseModel):
    """One day's market-wide HMM regime state, for GET /api/v1/macro/regime/history."""

    date: Optional[datetime] = None
    hmm_regime: Optional[str] = None
    hmm_regime_prob: Optional[float] = None
    hmm_stability: Optional[float] = None


class RegimeHistoryResponse(BaseModel):
    """Last N days of market-wide HMM regime state (SPEC-UI-002 Signal Detail screen)."""

    days: List[RegimeHistoryRow] = Field(default_factory=list)


# ===== Watchlist (SPEC-UI-003 — Phase 1 stub, M-08 multibagger model is Phase 2) =====
class WatchlistResponse(BaseModel):
    """Phase 1 stub — ml_multibagger (M-08) is not built until Phase 2."""

    tickers: List[Dict[str, Any]] = Field(default_factory=list)
    implemented: bool = Field(default=False, description="True once Phase 2's multibagger model is wired in")
    notes: str = "Phase 1 stub — multibagger watchlist (M-08) is Phase 2 scope (SPEC-UI-003)."


# ===== Alerts (SPEC-ALERT) =====
class AlertRow(BaseModel):
    """One synthesized alert (P&D, exit, drift) for the alerts feed."""

    date: datetime
    ticker: Optional[str] = None
    alert_type: str  # 'pnd_block', 'pnd_flag', 'exit_urgent', 'drift_halt', 'drift_warning'
    severity: str  # 'high', 'medium', 'low'
    message: str


class AlertsResponse(BaseModel):
    date: datetime
    alerts: List[AlertRow]
    count: int


# ===== System Health (SPEC-DS-002 /system group) =====
class DriftStatus(BaseModel):
    date: Optional[datetime] = None
    worst_status: str = "unknown"  # 'ok' | 'warning' | 'halt' | 'unknown' (no PSI run recorded yet)
    worst_feature: Optional[str] = None
    worst_psi: Optional[float] = None


class SchedulerJobHeartbeat(BaseModel):
    """One recurring scheduler job's last-known state (SPEC-SCHED-013)."""

    job_id: str
    last_attempt_at: Optional[datetime] = None
    last_status: Optional[str] = None  # 'success' | 'failed' | 'skipped' | None (never attempted)
    last_error: Optional[str] = None
    last_success_at: Optional[datetime] = None
    next_run_time: Optional[datetime] = Field(
        default=None,
        description="Next scheduled fire time (Asia/Kolkata), computed analytically from the "
        "job's cron config — not read from the persisted APScheduler job store.",
    )
    is_stale: bool = Field(
        default=False,
        description="True if no attempt has been recorded within the job's expected interval "
        "(e.g. > 26h for a daily job) — the symptom that originally went undetected.",
    )


class SystemHealthResponse(BaseModel):
    status: str
    timestamp: datetime
    version: str
    last_pipeline_run: Optional[Dict[str, Any]] = None
    stock_count: int = 0
    drift: DriftStatus = Field(default_factory=DriftStatus)
    scheduler: List[SchedulerJobHeartbeat] = Field(default_factory=list)


# ===== Paper Trading (Automated Daily Paper Trading, P3.x) =====
class PaperTradingPosition(BaseModel):
    """One open position, as persisted in paper_trading/portfolio_state.json."""

    ticker: str
    sector: str
    entry_date: str
    entry_price: float
    quantity: int
    peak_price: float
    current_price: Optional[float] = None
    unrealised_pnl_pct: Optional[float] = None


class PaperTradingStateResponse(BaseModel):
    """GET /api/v1/paper_trading/state — current portfolio snapshot."""

    as_of_date: Optional[str] = None
    cash: float = 0.0
    total_equity: float = 0.0
    initial_capital: float = 0.0
    positions: List[PaperTradingPosition] = Field(default_factory=list)
    available: bool = Field(
        default=False, description="False if no portfolio state has been written yet"
    )


class PaperTradingTrade(BaseModel):
    """One closed trade row, as logged by scripts/paper_trading_tracker.py."""

    date: str
    ticker: str
    signal_type: str
    entry_price: float
    quantity: int
    entry_time: str
    exit_price: Optional[float] = None
    exit_time: Optional[str] = None
    exit_date: Optional[str] = None
    exit_type: Optional[str] = None
    pnl: Optional[float] = None
    pnl_pct: Optional[float] = None


class PaperTradingTradesResponse(BaseModel):
    """GET /api/v1/paper_trading/trades — closed trades, sorted by exit_date desc."""

    trades: List[PaperTradingTrade] = Field(default_factory=list)
    count: int = 0


class EquityCurvePoint(BaseModel):
    date: str
    equity: float


class EquityCurveResponse(BaseModel):
    """GET /api/v1/paper_trading/equity_curve."""

    points: List[EquityCurvePoint] = Field(default_factory=list)


class GateStatusResponse(BaseModel):
    """GET /api/v1/paper_trading/gate_status — Phase 3 Gate 7 progress
    (>=90 NSE trading days of continuous live daily pipeline runs)."""

    days_count: int = 0
    gate_threshold: int = 90
    gate_cleared: bool = False


# ===== Paper Trading Pending Actions (SPEC-PT-003, review/approve) =====
class PendingActionRow(BaseModel):
    """One proposed trade awaiting human accept/reject, as persisted in
    paper_trading/pending/{date}.json by scripts/run_daily_paper_trading.py
    when PAPER_TRADING_REQUIRE_APPROVAL is set."""

    action_id: str
    date: str
    action_type: str  # 'buy' | 'sell' | 'reduce'
    ticker: str
    sector: Optional[str] = None
    price: Optional[float] = None  # propose-time price, display only — accept re-fetches live price
    reason: str
    status: str = "pending"  # 'pending' | 'accepted' | 'rejected'


class PendingActionsResponse(BaseModel):
    """GET /api/v1/paper_trading/pending."""

    date: Optional[str] = None
    actions: List[PendingActionRow] = Field(default_factory=list)


class ActionDecisionResponse(BaseModel):
    """POST /api/v1/paper_trading/pending/{action_id}/{accept,reject}."""

    action_id: str
    status: str  # 'accepted' | 'rejected'
    executed: bool = False
    detail: Optional[str] = None


# ===== Technical Analysis API scaffolding (SPEC-TA-004) — the 94 real
# features computed by features/technical.py, features/advanced_technical.py,
# features/pattern_scores.py already exist in the daily feature Parquets;
# these endpoints shape them for the UI instead of exposing the raw
# /api/v1/features/{ticker} dict directly. =====
class TAIndicatorsResponse(BaseModel):
    """GET /api/v1/ta/{ticker}/indicators."""

    ticker: str
    date: Optional[str] = None
    available: bool = False
    indicators: Dict[str, Optional[float]] = Field(default_factory=dict)


class TAPatternsResponse(BaseModel):
    """GET /api/v1/ta/{ticker}/patterns."""

    ticker: str
    date: Optional[str] = None
    available: bool = False
    patterns: Dict[str, Optional[float]] = Field(default_factory=dict)


class TACompareTickerRow(BaseModel):
    ticker: str
    rs_vs_nifty500_21d: Optional[float] = None
    beta_63d: Optional[float] = None
    alpha_21d: Optional[float] = None


class TACompareResponse(BaseModel):
    """GET /api/v1/ta/compare?tickers=A,B,C."""

    date: Optional[str] = None
    rows: List[TACompareTickerRow] = Field(default_factory=list)
    correlation: Dict[str, Dict[str, float]] = Field(
        default_factory=dict, description="Pairwise close-to-close return correlation, computed from real OHLCV"
    )


class TASectorBreadthRow(BaseModel):
    sector: str
    advances: int = 0
    declines: int = 0
    unchanged: int = 0
    avg_change_pct: Optional[float] = None


class TAMarketOverviewResponse(BaseModel):
    """GET /api/v1/ta/market_overview."""

    date: Optional[str] = None
    advances: int = 0
    declines: int = 0
    unchanged: int = 0
    sector_breadth: List[TASectorBreadthRow] = Field(default_factory=list)
    available: bool = False


# ===== Technical Analysis Screener & Alerts (SPEC-TA-005, SPEC-TA-006) =====
# Schemas for the screener (named templates + custom conditions) and the
# daily alert checker (ta_signals table read-back). Added alongside the
# existing TA scaffolding schemas above; keeps all TA schemas together in
# one block for easy navigation.

class TATemplateInfo(BaseModel):
    """One template's summary metadata, for GET /api/v1/ta/screener/templates."""

    name: str
    category: str
    description: str
    condition_count: int


class TATemplateListResponse(BaseModel):
    """GET /api/v1/ta/screener/templates — all 42 pre-built templates."""

    templates: List[TATemplateInfo] = Field(default_factory=list)
    count: int = 0


class TAScreenerCondition(BaseModel):
    """One condition in a custom screener request.

    op: "lt" | "gt" | "lte" | "gte" | "eq" | "between" | "top_pct" | "bottom_pct"
    value: scalar for lt/gt/lte/gte/eq/top_pct/bottom_pct; [lo, hi] list for between.
    feature2: optional second column name for col-vs-col ops (gt_col etc.).
    """

    feature: str
    op: str
    value: Optional[Any] = None
    feature2: Optional[str] = None


class TAScreenerRequest(BaseModel):
    """POST /api/v1/ta/screener/custom body."""

    conditions: List[TAScreenerCondition]
    date: Optional[str] = Field(default=None, description="YYYY-MM-DD; defaults to latest")
    limit: int = Field(default=50, ge=1, le=500)


class TAScreenerRow(BaseModel):
    """One result row from a screener run."""

    ticker: str
    date: str
    template_name: str
    matched_conditions: int
    total_conditions: int
    score: float
    key_values: Dict[str, Optional[float]] = Field(default_factory=dict)


class TAScreenerResponse(BaseModel):
    """Response for /screener/run/{template_name} and /screener/custom."""

    template_name: str
    date: Optional[str] = None
    rows: List[TAScreenerRow] = Field(default_factory=list)
    count: int = 0


class TAAlertRow(BaseModel):
    """One row from the ta_signals table (daily alert match)."""

    date: str
    ticker: str
    template_name: str
    category: str
    score: float
    matched_conditions: int
    total_conditions: int
    key_values: Dict[str, Optional[float]] = Field(default_factory=dict)


class TAAlertResponse(BaseModel):
    """Response for /alerts/today and /alerts/{ticker}."""

    as_of_date: Optional[str] = None
    rows: List[TAAlertRow] = Field(default_factory=list)
    count: int = 0


class TAUserAlertCreate(BaseModel):
    """Request body for POST /api/v1/ta/user-alerts."""

    ticker: str
    template_name: str


class TAUserAlertRow(BaseModel):
    """One user-defined alert (ta_alerts), enriched with trigger state."""

    alert_id: int
    ticker: str
    template_name: str
    category: str
    active: bool
    last_triggered_date: Optional[str] = None
    triggered_today: bool = False


class TAUserAlertResponse(BaseModel):
    """Response for GET/POST /api/v1/ta/user-alerts."""

    rows: List[TAUserAlertRow] = Field(default_factory=list)
    count: int = 0


# ===== Fundamental Analysis API scaffolding (SPEC-FA-008) — the 27
# sector-relative z-scored ratios + 3 staleness flags (features/fundamental.py)
# and 12 governance features (features/governance.py) already exist in the
# daily feature Parquet; features/fundamental_composites.py adds the small
# net-new composite-scoring/peer-selection/screener logic. =====
class FARatiosResponse(BaseModel):
    """GET /api/v1/fundamentals/{ticker}/ratios."""

    ticker: str
    date: Optional[str] = None
    available: bool = False
    ratios: Dict[str, Optional[float]] = Field(
        default_factory=dict, description="27 sector-relative z-scored ratios + 3 staleness flags"
    )


class FAPeerRow(BaseModel):
    ticker: str
    roe: Optional[float] = None
    roce: Optional[float] = None
    debt_to_equity: Optional[float] = None
    pe_ratio: Optional[float] = None


class FAPeersResponse(BaseModel):
    """GET /api/v1/fundamentals/{ticker}/peers."""

    ticker: str
    date: Optional[str] = None
    sector: Optional[str] = None
    peers: List[FAPeerRow] = Field(default_factory=list)


class FASectorResponse(BaseModel):
    """GET /api/v1/fundamentals/sector/{sector}."""

    sector: str
    date: Optional[str] = None
    ticker_count: int = 0
    avg_ratios: Dict[str, Optional[float]] = Field(default_factory=dict)
    note: str = "Sector-unique metrics (e.g. GNPA for banks, ANDA for pharma) are not computed anywhere yet — only sector aggregates of the standard ratio set are shown."


class FAScreenerResponse(BaseModel):
    """GET /api/v1/fundamentals/screener?preset=."""

    preset: str
    date: Optional[str] = None
    tickers: List[str] = Field(default_factory=list)


class FAScoresResponse(BaseModel):
    """GET /api/v1/fundamentals/{ticker}/scores."""

    ticker: str
    date: Optional[str] = None
    quality_score: Optional[float] = None
    growth_score: Optional[float] = None
    management_quality_score: Optional[float] = None


# ===== Job Autoruns / Ops API (SPEC-SCHED-014) — exposes the scheduler
# infrastructure that already exists (ingestion/scheduler/checkpoint.py's
# STEPS, the pipeline_checkpoints/scheduler_heartbeats tables) rather than
# building any new scheduling logic. =====
class OpsStepRow(BaseModel):
    step_name: str
    step_index: int
    is_backfillable: bool
    status: str = "never_run"  # 'never_run' | 'running' | 'success' | 'failed' | 'skipped'
    started_at: Optional[str] = None
    completed_at: Optional[str] = None
    error_message: Optional[str] = None
    last_success_date: Optional[str] = Field(
        default=None,
        description="Most recent date (any date, not just the one this row is scoped to) this "
        "step's checkpoint recorded status='success'.",
    )
    next_scheduled_run: Optional[datetime] = Field(
        default=None,
        description="Earliest next fire time across every recurring job that can run this step "
        "(daily_pipeline, morning_catchup) — same value for every row.",
    )


class OpsStepsResponse(BaseModel):
    """GET /api/v1/ops/steps."""

    date: str
    steps: List[OpsStepRow] = Field(default_factory=list)


class OpsFailedStepInfo(BaseModel):
    """One step's failed checkpoint, attached to an OpsRunRow with status='failed'."""

    step_name: str
    error_message: Optional[str] = None


class OpsRunRow(BaseModel):
    run_id: Optional[int] = None
    date: Optional[str] = None
    status: Optional[str] = None
    stocks_processed: Optional[int] = None
    started_at: Optional[str] = None
    completed_at: Optional[str] = None
    error_message: Optional[str] = None
    failed_steps: List[OpsFailedStepInfo] = Field(
        default_factory=list,
        description="Every step with a 'failed' checkpoint for this run's date (pipeline_runs "
        "itself never recorded which step failed or why — error_message above is always None; "
        "this is looked up from pipeline_checkpoints instead).",
    )


class OpsRunsResponse(BaseModel):
    """GET /api/v1/ops/runs."""

    runs: List[OpsRunRow] = Field(default_factory=list)


class OpsForceStepResult(BaseModel):
    """One (step, date) outcome within an OpsForceStepResponse.results list."""

    step_name: str
    date: str
    status: str  # 'success' | 'failed'
    error_message: Optional[str] = None


class OpsForceStepResponse(BaseModel):
    """POST /api/v1/ops/steps/{step_name}/force.

    date/status/error_message describe the most recent date attempted
    (kept for backward compatibility with callers reading a single result);
    results carries every date's outcome when an explicit `date` query param
    was omitted and the endpoint auto-backfilled multiple missing trading days.
    """

    step_name: str
    date: str
    status: str  # 'success' | 'failed'
    error_message: Optional[str] = None
    results: List[OpsForceStepResult] = Field(default_factory=list)


# ===== Paper Trading Backdated Entries (SPEC-PT-003 addendum) =====
class BackdatedBuyRequest(BaseModel):
    """POST /api/v1/paper_trading/backdated_buy."""

    ticker: str
    date: str  # YYYY-MM-DD
    quantity: Optional[int] = None  # if omitted, PortfolioSimulator sizes it


class BackdatedBuyResponse(BaseModel):
    ticker: str
    date: str
    entry_price: Optional[float] = None
    quantity: Optional[int] = None
    executed: bool = False
    detail: Optional[str] = None
