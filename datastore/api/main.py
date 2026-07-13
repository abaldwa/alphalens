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

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles

from config.settings import DATASTORE_API_HOST, DATASTORE_API_PORT

from .routers import (
    alerts,
    backtest_reports,
    big_investors,
    corporate_actions,
    corporate_announcements,
    features,
    fno,
    forensic,
    fundamentals,
    governance,
    macro,
    models,
    multibagger,
    ohlcv,
    ops,
    paper_trading,
    pipeline,
    regime,
    sector_rotation,
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
    # 2026-07-07: ensure the DuckDB schema is fully provisioned before the API
    # serves any request. create_schema() is idempotent (CREATE TABLE IF NOT
    # EXISTS) — this is the fix for index_ohlcv having been added to
    # _ALL_TABLES but never actually created against the live DB, since
    # nothing previously called create_schema() outside manual/ad-hoc runs.
    from datastore.schema.create_normalised import create_schema

    create_schema()
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
app.include_router(corporate_announcements.router)
app.include_router(fno.router)
app.include_router(macro.router)
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
# [AS BUILT, item #6] Features/Models/Pipeline Status — moved out of this
# file's former inline @app.get() routes into their own router files (see
# each router's module docstring); same paths, same behavior. Features
# also picked up item #10/AF-3's DuckDB-glob perf fix in the same move
# (see routers/features.py + utils/feature_store.py).
app.include_router(features.router)
app.include_router(models.router)
app.include_router(pipeline.router)
# [AS BUILT, Phase A — Big Investor Activity] bulk/block deals + MF holdings
# screen. See plan: gentle-wobbling-swing.md.
app.include_router(big_investors.router)
# [AS BUILT, ML12 steps 4-6] Daily Sector Rotation report — trailing-21d
# relative strength per sector (config/sector_index_map.py) vs Nifty 500,
# ranked, with top stocks per in-favor sector.
app.include_router(sector_rotation.router)

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

if __name__ == "__main__":
    import uvicorn

    uvicorn.run(
        "datastore.api.main:app",
        host=DATASTORE_API_HOST,
        port=DATASTORE_API_PORT,
        reload=False,
    )
