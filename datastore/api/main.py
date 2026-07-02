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

import json
import logging
from contextlib import asynccontextmanager
from datetime import datetime
from typing import List, Optional

import pandas as pd
from fastapi import FastAPI, HTTPException, Path, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles

from config.settings import (
    DATASTORE_API_HOST,
    DATASTORE_API_PORT,
    FEATURES_DAILY_DIR,
    MODEL_REGISTRY_PATH,
    PIPELINE_LOG_DB_PATH,
)
from datastore.api.db import get_sqlite_connection

from . import schemas
from .routers import (
    alerts,
    backtest_reports,
    corporate_actions,
    fno,
    forensic,
    fundamentals,
    governance,
    multibagger,
    ohlcv,
    ops,
    paper_trading,
    regime,
    shareholding,
    signals,
    system,
    technical,
    valuation,
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
# [AS BUILT, P3.x] paper_trading/backtest_reports added for the Automated
# Daily Paper Trading + Web UI build (see plan: scalable-bubbling-reddy.md).
app.include_router(paper_trading.router)
app.include_router(backtest_reports.router)
# [AS BUILT, SPEC-TA-004] Technical Analysis API scaffolding over the
# already-computed features/{technical,advanced_technical,pattern_scores}.py
# output — see plan: squishy-frolicking-whisper.md.
app.include_router(technical.router)
# [AS BUILT, SPEC-SCHED-014] Job Autoruns / Ops page — API scaffolding over
# the pre-existing scheduler infrastructure (ingestion/scheduler/checkpoint.py,
# pipeline_checkpoints/scheduler_heartbeats tables).
app.include_router(ops.router)
# [AS BUILT, 2026-07-02] Damodaran Valuation API — lifecycle classification,
# FCFF/ExcessReturn/CommodityNormalized DCF, Monte Carlo, relative PE.
# See systems/damodaran_valuation/ and tests/unit/test_damodaran.py (44 tests).
app.include_router(valuation.router)

# ===== Static UI (P3.x — zero-new-dependency web UI, StaticFiles ships with
# Starlette/FastAPI already; rebuilt to the 27-screen/5-app prototype layout
# per plan squishy-frolicking-whisper.md) — dashboard/static/index.html is a
# 5-app launcher (ml/, technical/, fundamental/, valuation/, forensic/
# subdirectories, one HTML+JS page per screen, light theme per
# alphalens_docs/screens/SCREEN_INVENTORY.md). Only ml/ (5 screens) and
# forensic/ (7 screens) are fully real-data; fundamental/ is partially real
# (dashboard.html, management.html); technical/ and valuation/ render an
# honest "not yet available" empty-state since their backends
# (systems/technical_analysis/, systems/damodaran_valuation/) don't exist
# yet — see dashboard/static/js/empty_state.js's BACKEND_STATUS map. All
# served read-only from this same API process. =====
app.mount("/ui", StaticFiles(directory="dashboard/static", html=True), name="ui")


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
    if not ticker:
        raise HTTPException(status_code=400, detail="Ticker cannot be empty")
    if end_date < start_date:
        raise HTTPException(status_code=400, detail="end_date must be >= start_date")

    # Feature matrices are written one Parquet per calendar day under
    # FEATURES_DAILY_DIR (features/matrix_builder.py's
    # _save_feature_matrix()) — no DuckDB feature table exists, so this
    # reads each day's file in range rather than issuing a SQL query.
    rows: List[schemas.FeatureMatrixRow] = []
    for day in pd.date_range(start_date, end_date, freq="D"):
        day_path = FEATURES_DAILY_DIR / f"{day.date().isoformat()}.parquet"
        if not day_path.exists():
            continue
        day_df = pd.read_parquet(day_path)
        ticker_rows = day_df[day_df["ticker"] == ticker]
        if ticker_rows.empty:
            continue
        row = ticker_rows.iloc[0]
        all_feature_cols = [c for c in day_df.columns if c not in ("date", "ticker")]
        cols = feature_names if feature_names else all_feature_cols
        missing_cols = [c for c in cols if c not in day_df.columns]
        if missing_cols:
            raise HTTPException(
                status_code=400, detail=f"Unknown feature name(s): {missing_cols}"
            )
        feature_values = {c: (None if pd.isna(row[c]) else float(row[c])) for c in cols}
        rows.append(
            schemas.FeatureMatrixRow(
                date=day,
                ticker=ticker,
                feature_values=feature_values,
                missing_feature_count=sum(1 for v in feature_values.values() if v is None),
            )
        )

    if not rows:
        raise HTTPException(
            status_code=404, detail=f"No feature data for {ticker} in [{start_date}, {end_date}]"
        )

    return schemas.FeatureMatrixResponse(
        ticker=ticker,
        start_date=start_date,
        end_date=end_date,
        feature_names=feature_names or [],
        data=rows,
        record_count=len(rows),
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
    # train_all_phase1.py / retrain_phase2.py write one entry per model to
    # MODEL_REGISTRY_PATH (datastore/models/registry.json) keyed by model
    # name — no DuckDB/SQLite table backs this, the JSON file IS the
    # registry. Older entries (hmm_market, conformal_signal5d) predate this
    # endpoint's contract and only carry saved_path/saved_at — these still
    # surface (version/model_type default to "unknown") rather than being
    # silently dropped.
    if not MODEL_REGISTRY_PATH.exists():
        raise HTTPException(status_code=404, detail="Model registry not found")

    raw_registry = json.loads(MODEL_REGISTRY_PATH.read_text())
    models: List[schemas.ModelMetadata] = []
    for key, entry in raw_registry.items():
        name = entry.get("name", key)
        if model_name and name != model_name:
            continue
        created_at_raw = entry.get("created_at") or entry.get("saved_at")
        models.append(
            schemas.ModelMetadata(
                name=name,
                version=entry.get("version", "unknown"),
                model_type=entry.get("model_type", "unknown"),
                created_at=created_at_raw,
                features_used=entry.get("feature_names", []),
                accuracy_on_validation=entry.get("accuracy_on_validation"),
                # additional_metrics is Dict[str, float] — registry.json's
                # "diagnostics" is a nested dict of dicts (class ratios,
                # best params, per-class F1), not a flat float map, so it
                # doesn't fit this schema field as-is.
                additional_metrics=None,
                hyperparameters=entry.get("hyperparams"),
                training_samples=entry.get("training_samples"),
                training_time_seconds=entry.get("training_time_seconds"),
            )
        )

    if model_name and not models:
        raise HTTPException(status_code=404, detail=f"Model '{model_name}' not found in registry")

    latest_by_name: dict = {}
    for m in models:
        existing = latest_by_name.get(m.name)
        if existing is None or m.created_at > existing.created_at:
            latest_by_name[m.name] = m

    return schemas.ModelRegistry(
        models=models,
        total_models=len(models),
        latest_model_by_name=latest_by_name,
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
    # ingestion/scheduler/checkpoint.py writes one pipeline_runs row per day
    # and one pipeline_checkpoints row per pipeline step that day, both into
    # PIPELINE_LOG_DB_PATH — same store routers/system.py's /health reads
    # for the latest run; this endpoint adds per-date and per-step detail.
    date_str = date.strftime("%Y-%m-%d")
    with get_sqlite_connection(PIPELINE_LOG_DB_PATH) as conn:
        cursor = conn.cursor()
        cursor.execute(
            "SELECT status, stocks_processed, started_at, completed_at, error_message "
            "FROM pipeline_runs WHERE date = ? ORDER BY run_id DESC LIMIT 1",
            (date_str,),
        )
        run_row = cursor.fetchone()
        if run_row is None:
            raise HTTPException(status_code=404, detail=f"No pipeline run found for {date_str}")
        status, stocks_processed, started_at, completed_at, error_message = run_row

        cursor.execute(
            "SELECT step_name FROM pipeline_checkpoints WHERE date = ? ORDER BY step_index DESC LIMIT 1",
            (date_str,),
        )
        step_row = cursor.fetchone()
        stage = step_row[0] if step_row else "unknown"

        cursor.execute(
            "SELECT COUNT(*), SUM(CASE WHEN status = 'success' THEN 1 ELSE 0 END), "
            "SUM(CASE WHEN status = 'failed' THEN 1 ELSE 0 END) "
            "FROM pipeline_checkpoints WHERE date = ?",
            (date_str,),
        )
        total_steps, completed_steps, failed_steps = cursor.fetchone()
        completed_steps = completed_steps or 0
        failed_steps = failed_steps or 0
        completeness_pct = (completed_steps / total_steps * 100.0) if total_steps else 0.0

    duration_seconds = None
    if started_at and completed_at:
        duration_seconds = (
            datetime.fromisoformat(completed_at) - datetime.fromisoformat(started_at)
        ).total_seconds()

    return schemas.PipelineStatus(
        date=date,
        status=status,
        stage=stage,
        records_processed=stocks_processed or 0,
        records_skipped=0,
        records_failed=failed_steps,
        data_completeness_pct=completeness_pct,
        started_at=started_at,
        completed_at=completed_at,
        duration_seconds=duration_seconds,
        error_summary=error_message,
        notes=None,
    )


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(
        "datastore.api.main:app",
        host=DATASTORE_API_HOST,
        port=DATASTORE_API_PORT,
        reload=False,
    )
