"""
datastore/api/main.py

Phase: 0.1 (Project Skeleton); routers wired in 1.7 (DataStore API Full)
Specs: SPEC-DS-001, SPEC-DS-002, SPEC-DS-003, SPEC-DS-004, SPEC-DS-005, SPEC-PIPE-001
Owner: Platform / DataStore
Consumers: systems/ml_signal_engine, dashboard, backtest, ingestion/scheduler

FastAPI application entry point for DataStore API.
Provides REST endpoints for querying OHLCV, fundamentals, features, and writing signals.
Enforces PIT correctness and staleness checks (SPEC-DS-003, SPEC-SYS-003).
SOLID: Routing and business logic are separate; controllers are thin.

[AS BUILT, P1.7] OHLCV, ML Signals, Macro Regime, Watchlist, Alerts, and
System Health endpoints now live in datastore/api/routers/ (ohlcv.py,
signals.py, regime.py, watchlist.py, alerts.py, system.py) per this
prompt's "implement all Phase 1 API endpoints" instruction, included
below via app.include_router(). The bare `/health` route and the
`/api/v1/ohlcv/{ticker}` / `/api/v1/signals/ml/*` routes that used to be
defined inline in this file were removed in favor of the router versions
(same paths, equivalent-or-richer behavior — see each router's
docstring) to avoid a duplicate-route conflict. Features, Models, and
Pipeline Status endpoints below are untouched — out of P1.7's explicit
router-file list.

[AS BUILT, P2.1] Fundamentals and Shareholding moved into routers/
(fundamentals.py, shareholding.py) the same way — the old inline
`/api/v1/fundamentals/{ticker}` GET below was a permanent stub (always
returned an empty list, `# TODO: Phase 1 — implement actual query`),
never wired to the `fundamentals`/`shareholding` DuckDB tables that have
existed since P0.2. Removed in favor of the real, PIT-enforcing router
versions.
"""

import logging
from contextlib import asynccontextmanager
from datetime import datetime
from typing import List, Optional

from fastapi import FastAPI, HTTPException, Path, Query
from fastapi.middleware.cors import CORSMiddleware

from config.settings import DATASTORE_API_HOST, DATASTORE_API_PORT
from config.timezone import now_ist

from . import schemas
from .routers import (
    alerts,
    corporate_actions,
    fno,
    forensic,
    fundamentals,
    governance,
    multibagger,
    ohlcv,
    regime,
    shareholding,
    signals,
    system,
    watchlist,
)

logger = logging.getLogger(__name__)


# [AS BUILT] Replaces the old @app.on_event("startup")/("shutdown") handlers —
# FastAPI deprecated on_event in favor of this single lifespan context manager
# (everything before `yield` runs at startup, everything after at shutdown).
# Same behavior, just the registration mechanism; no real resource setup/
# teardown logic existed in either handler to preserve.
@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info(f"DataStore API starting up (host={DATASTORE_API_HOST}, port={DATASTORE_API_PORT})")
    # TODO: Phase 1 — initialize database connections
    yield
    logger.info("DataStore API shutting down")
    # TODO: Phase 1 — close database connections


# ===== FastAPI App Initialization =====
app = FastAPI(
    title="AlphaLens DataStore API",
    description="SPEC-DS-001, SPEC-DS-002, SPEC-DS-003, SPEC-DS-004, SPEC-DS-005: "
    "REST API for querying market data, fundamentals, features, and writing ML signals. "
    "All queries enforce point-in-time (PIT) correctness and data staleness tracking.",
    version="0.1",
    lifespan=lifespan,
)

# ===== CORS Middleware =====
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # TODO: SPEC-SEC-003 — restrict in production
    allow_credentials=True,
    allow_methods=["GET", "POST", "PUT"],
    allow_headers=["*"],
)

# ===== Routers (P1.7; fundamentals/shareholding added P2.1; corporate_actions
# added P2.2; fno added P2.3; forensic/multibagger/governance added P2.6) =====
app.include_router(system.router)
app.include_router(ohlcv.router)
# [AS BUILT, P2.6] forensic.router and multibagger.router MUST be registered
# before signals.router: their literal "/forensic/{ticker}" and
# "/multibagger/{ticker}" paths would otherwise structurally collide with
# signals.router's wildcard "/ml/{ticker}/{date}" pattern (both shapes are
# "/api/v1/signals/ml/<segment>/<segment>") — FastAPI/Starlette matches by
# registration order, not specificity, so a request for
# /api/v1/signals/ml/forensic/RELIANCE would otherwise be swallowed by
# get_ml_signals(ticker="forensic", date="RELIANCE") and fail date
# validation with a confusing 422, never reaching get_forensic_score().
# Same route-ordering discipline signals.py's own docstring already
# documents for its internal top_buys-vs-{ticker}/{date} ordering.
app.include_router(forensic.router)
app.include_router(multibagger.router)
app.include_router(signals.router)
app.include_router(regime.router)
app.include_router(watchlist.router)
app.include_router(alerts.router)
app.include_router(fundamentals.router)
app.include_router(shareholding.router)
app.include_router(governance.router)
app.include_router(corporate_actions.router)
app.include_router(fno.router)


# ===== Features Endpoints =====
@app.get(
    "/api/v1/features/{ticker}",
    response_model=schemas.FeatureMatrixResponse,
    tags=["Features"],
)
async def get_features(
    ticker: str,
    start_date: datetime = Query(..., description="Inclusive start date (YYYY-MM-DD)"),
    end_date: datetime = Query(..., description="Inclusive end date (YYYY-MM-DD)"),
    feature_names: Optional[List[str]] = Query(
        None, description="Subset of features (default: all)"
    ),
) -> schemas.FeatureMatrixResponse:
    """
    Query precomputed feature matrix for a ticker.

    SPEC-FEAT-001, SPEC-DS-006: Returns all technical indicators, fundamental ratios,
    and derived features. Includes data_staleness_flag (SPEC-SYS-003) for filtering
    stale observations.

    Args:
        ticker: Stock ticker
        start_date: Inclusive start date
        end_date: Inclusive end date
        feature_names: If provided, return only these features (else return all)

    Returns:
        FeatureMatrixResponse with rows sorted by date ascending
        Each row includes all features + staleness flag

    Raises:
        HTTPException 404: If ticker not found
        HTTPException 400: If date range or feature names invalid
    """
    # TODO: Phase 1 — implement actual query
    if not ticker:
        raise HTTPException(status_code=400, detail="Ticker cannot be empty")

    return schemas.FeatureMatrixResponse(
        ticker=ticker,
        start_date=start_date,
        end_date=end_date,
        feature_names=feature_names or [],
        data=[],
        record_count=0,
    )


# ===== Model Registry Endpoints =====
@app.get(
    "/api/v1/models",
    response_model=schemas.ModelRegistry,
    tags=["Models"],
)
async def get_models(
    model_name: Optional[str] = Query(
        None, description="Filter by model name (optional)"
    ),
) -> schemas.ModelRegistry:
    """
    Query model registry.

    SPEC-DS-004: Returns metadata for all trained models (versions, features,
    validation accuracy, hyperparameters).

    Args:
        model_name: If provided, return only this model's versions

    Returns:
        ModelRegistry with all models and latest-by-name index

    Raises:
        HTTPException 404: If model_name provided but not found
    """
    # TODO: Phase 1 — implement actual query
    return schemas.ModelRegistry(
        models=[],
        total_models=0,
        latest_model_by_name={},
    )


# ===== Pipeline Status Endpoints =====
@app.get(
    "/api/v1/pipeline/status/{date}",
    response_model=schemas.PipelineStatus,
    tags=["Pipeline"],
)
async def get_pipeline_status(
    date: datetime = Path(..., description="Pipeline date (YYYY-MM-DD)"),
) -> schemas.PipelineStatus:
    """
    Query daily pipeline execution status.

    SPEC-PIPE-001, SPEC-SYS-002: Returns overall pipeline health (ingestion,
    feature engineering, inference, output generation).

    Args:
        date: Date of pipeline run

    Returns:
        PipelineStatus with run summary

    Raises:
        HTTPException 404: If no pipeline run on this date
    """
    # TODO: Phase 1 — implement actual query
    return schemas.PipelineStatus(
        date=date,
        status="pending",
        stage="not_started",
        records_processed=0,
        records_skipped=0,
        records_failed=0,
        data_completeness_pct=0.0,
        started_at=now_ist(),
        completed_at=None,
        duration_seconds=None,
        error_summary=None,
        notes="Placeholder",
    )


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(
        "datastore.api.main:app",
        host=DATASTORE_API_HOST,
        port=DATASTORE_API_PORT,
        reload=False,
    )
