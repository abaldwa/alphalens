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


# ===== Fundamentals (SPEC-DS-003) =====
class FundamentalRow(BaseModel):
    """Single fundamental data record (earnings, ratios, shareholding, etc)."""

    date: datetime
    ticker: str
    fiscal_year: int
    fiscal_quarter: int
    announcement_date: Optional[datetime] = None
    metric_name: str  # e.g., 'eps', 'pe_ratio', 'roe', 'debt_to_equity', 'promoter_holding'
    metric_value: float
    unit: str = ""  # e.g., '%', 'INR', 'ratio'
    data_source: str = ""  # e.g., 'bse_filing', 'company_investor_relations'
    filing_date: Optional[datetime] = None  # For shareholding (SPEC-DS-003 PIT)
    month_end: Optional[datetime] = None  # For MF holdings


class FundamentalResponse(BaseModel):
    """Response for fundamentals query endpoint."""

    ticker: str
    start_date: datetime
    end_date: datetime
    as_of: Optional[datetime] = None
    data: List[FundamentalRow]
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


class SystemHealthResponse(BaseModel):
    status: str
    timestamp: datetime
    version: str
    last_pipeline_run: Optional[Dict[str, Any]] = None
    stock_count: int = 0
    drift: DriftStatus = Field(default_factory=DriftStatus)
