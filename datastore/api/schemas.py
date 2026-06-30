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
