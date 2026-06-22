"""
datastore/api/routers/watchlist.py

Phase: 1.7 (DataStore API Full + Daily Pipeline + Dashboard)
Specs: SPEC-DS-002, SPEC-UI-003
Owner: Platform / DataStore
Consumers: dashboard/screens/daily_dashboard.py

GET /api/v1/watchlist/current — explicit Phase 1 stub per the build
prompt. The multibagger model (M-08) that would actually populate this
(SPEC-UI-003: "Top 20 ranked by multibagger probability... survival
curves, archetypes") is Phase 2 scope (P2.4) and doesn't exist yet.
Returns an honestly-empty response with implemented=False rather than
fabricating placeholder tickers — a caller checking `implemented` can
distinguish "no watchlist data" from "watchlist not built yet".
"""

import logging

from fastapi import APIRouter

from datastore.api.schemas import WatchlistResponse

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/v1/watchlist", tags=["Watchlist"])


@router.get("/current", response_model=WatchlistResponse)
async def get_watchlist_current() -> WatchlistResponse:
    """Phase 1 stub — see module docstring. ml_multibagger table exists (Store 4) but nothing writes to it yet."""
    return WatchlistResponse()
