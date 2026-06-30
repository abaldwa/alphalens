"""
datastore/api/routers/watchlist.py

Phase: 1.7 (DataStore API Full + Daily Pipeline + Dashboard); made real P2.6
Specs: SPEC-DS-002, SPEC-UI-003
Owner: Platform / DataStore
Consumers: dashboard/screens/daily_dashboard.py

GET /api/v1/watchlist/current — top-20 ranked by mb_probability, from the
most recent date present in ml_multibagger (Store 4,
datastore/schema/create_signals.py), written weekly by
systems/ml_signal_engine/inference/score_multibagger.py (M-08).

[AS BUILT, P2.6] Was a Phase 1 stub (always returned an empty
WatchlistResponse with implemented=False) because M-08 didn't exist yet.
Now reads the real table — SPEC-UI-003's "Top 20 ranked by multibagger
probability... survival curves, archetypes". implemented=True is returned
whenever ml_multibagger has at least one row for the latest date,
implemented=False (honest empty response, same as the old stub) if the
table is still empty (e.g. score_multibagger.py has never been run) —
never a fabricated placeholder list.
"""

import logging

from fastapi import APIRouter

from config.settings import SIGNALS_DUCKDB_PATH
from datastore.api.db import get_duckdb_connection
from datastore.api.schemas import WatchlistResponse

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/v1/watchlist", tags=["Watchlist"])

_COLUMNS = [
    "date", "ticker", "mb_probability", "mb_tier", "mb_archetype",
    "survival_6m", "survival_12m", "survival_18m", "survival_24m", "survival_36m",
]
_SELECT_COLS = ", ".join(_COLUMNS)
_TOP_N = 20


@router.get("/current", response_model=WatchlistResponse)
async def get_watchlist_current() -> WatchlistResponse:
    """Top 20 tickers by mb_probability, from the most recent ml_multibagger date."""
    with get_duckdb_connection(SIGNALS_DUCKDB_PATH) as conn:
        latest = conn.execute("SELECT MAX(date) FROM ml_multibagger").fetchone()
        latest_date = latest[0] if latest else None
        if latest_date is None:
            return WatchlistResponse()

        rows = conn.execute(
            f"""
            SELECT {_SELECT_COLS} FROM ml_multibagger
            WHERE date = ? AND mb_probability IS NOT NULL
            ORDER BY mb_probability DESC
            LIMIT ?
            """,
            [latest_date, _TOP_N],
        ).fetchall()

    tickers = [dict(zip(_COLUMNS, r)) for r in rows]
    return WatchlistResponse(
        tickers=tickers,
        implemented=True,
        notes=f"Top {len(tickers)} multibagger watchlist for {latest_date} (SPEC-UI-003).",
    )
