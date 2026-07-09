"""
datastore.api package.

Phase: 0.1 (Project Skeleton)
Specs: SPEC-DS-001, SPEC-DS-002, SPEC-DS-003, SPEC-DS-004, SPEC-DS-005, SPEC-DS-007
Owner: Platform / DataStore
Consumers: systems/ml_signal_engine, systems/fundamental_analysis, systems/technical_analysis,
           backtest, dashboard, features/*, ingestion/*

FastAPI REST API for data access and signal writing.
Enforces PIT correctness, rate limiting, and authentication.
"""

from . import db, pit, schemas
from .db import close_all_connections, get_duckdb_connection, get_sqlite_connection
from .pit import compute_staleness_flags, enforce_pit_fundamentals, enforce_pit_mf_holdings, enforce_pit_shareholding
from .schemas import (
    CorporateActionResponse,
    CorporateActionRow,
    FeatureMatrixResponse,
    FeatureMatrixRow,
    FundamentalsResponse,
    FundamentalsRow,
    FundamentalsWrite,
    FundamentalsWriteResult,
    ModelMetadata,
    ModelRegistry,
    OHLCVResponse,
    OHLCVRow,
    PipelineStatus,
    ShareholdingResponse,
    ShareholdingRow,
    ShareholdingWrite,
    ShareholdingWriteResult,
    SignalResponse,
    SignalWrite,
)

__all__ = [
    # Modules
    "db",
    "pit",
    "schemas",
    # DB functions
    "get_duckdb_connection",
    "get_sqlite_connection",
    "close_all_connections",
    # PIT functions
    "enforce_pit_fundamentals",
    "enforce_pit_shareholding",
    "enforce_pit_mf_holdings",
    "compute_staleness_flags",
    # Schema classes
    "OHLCVRow",
    "OHLCVResponse",
    "FundamentalsRow",
    "FundamentalsWrite",
    "FundamentalsWriteResult",
    "FundamentalsResponse",
    "ShareholdingRow",
    "ShareholdingWrite",
    "ShareholdingWriteResult",
    "ShareholdingResponse",
    "CorporateActionRow",
    "CorporateActionResponse",
    "FeatureMatrixRow",
    "FeatureMatrixResponse",
    "SignalWrite",
    "SignalResponse",
    "ModelMetadata",
    "ModelRegistry",
    "PipelineStatus",
]
