"""
datastore/api/routers/paper_trading_unified.py

Phase: Unified Backtest & Paper Trading Umbrella, Phase 5
(BacktestUmbrellaPlan.md at the repo root)
Owner: Platform / DataStore
Consumers: Phase 4's Backtest frontend page (a future "Paper Trading" tab
within it, per the plan's "same nav section, not a separate menu item")

Channel-aware paper-trading API, generalizing the existing ML-only
/api/v1/paper_trading/* router (datastore/api/routers/paper_trading.py,
left completely untouched — "wrap, don't refactor") to any of the four
channels via backtest/paper_trading/live_runner.py +
backtest/paper_trading/approval_queue.py.

Distinct prefix (/api/v1/paper_trading2, not /api/v1/paper_trading) to
avoid any path collision with the existing router's routes (both
routers' path shapes — {id}/accept, gate_status, etc. — would otherwise
collide once mounted on the same prefix). "2" is an intentionally
unglamorous placeholder name — a future full retirement of the ML-only
router (out of scope for this initiative; ML's paper trading has a real
production track record it shouldn't lose) would be the natural time to
rename this to the plain prefix.

Every endpoint is scoped to a (channel, strategy_id) pair, since a run is
now always exactly one strategy with its own capital base (confirmed
2026-07-20, no pooled capital) — there is no "the" paper trading state
the way the ML-only router's singular portfolio_state.json assumes.
"""

import logging
from typing import Optional

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from backtest.core.horizon import HorizonBucket
from backtest.paper_trading.approval_queue import gate_status as _gate_status
from backtest.paper_trading.approval_queue import read_pending_actions
from backtest.paper_trading.live_runner import PaperTradingRunner

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/v1/paper_trading2", tags=["Paper Trading (Unified)"])


class PendingActionResponse(BaseModel):
    action_id: str
    channel: str
    strategy_id: str
    as_of_date: str
    ticker: str
    action: str
    sector: str
    conviction: float
    adtv_cr: Optional[float] = None
    status: str
    proposed_at: str
    decided_at: Optional[str] = None
    executed_price: Optional[float] = None
    executed_quantity: Optional[int] = None


class PendingActionsListResponse(BaseModel):
    channel: str
    strategy_id: str
    as_of_date: str
    actions: list[PendingActionResponse]


class GateStatusResponse(BaseModel):
    channel: str
    strategy_id: str
    days_completed: int
    gate_threshold: int
    gate_passed: bool


class StateSummaryResponse(BaseModel):
    channel: str
    strategy_id: str
    cash: float
    initial_capital: float
    total_contributed: float
    n_open_positions: int
    n_closed_trades: int


class AcceptActionRequest(BaseModel):
    as_of_date: str
    price: float
    prices: dict[str, float] = {}
    # Only required the FIRST time this (channel, strategy_id) accepts an
    # action, before any portfolio state file exists for it — ignored (the
    # persisted state wins) on every subsequent call. See
    # PaperTradingRunner._portfolio()'s docstring.
    horizon_bucket: Optional[str] = None
    initial_capital: Optional[float] = None


class RejectActionRequest(BaseModel):
    as_of_date: str


class ProposeRequest(BaseModel):
    as_of_date: str
    horizon_bucket: str
    # Required for technical/fundamental, ignored for momentum: those two
    # channels' registry rows declare the entry rule but not how many names to
    # hold. See backtest/core/live_adapter_factory.py.
    top_n: Optional[int] = None


class ProposeResponse(BaseModel):
    channel: str
    strategy_id: str
    as_of_date: str
    universe_size: int
    actions: list[PendingActionResponse]


@router.get("/{channel}/{strategy_id}/pending", response_model=PendingActionsListResponse)
async def list_pending_actions(channel: str, strategy_id: str, as_of_date: str) -> PendingActionsListResponse:
    actions = read_pending_actions(channel, strategy_id, as_of_date)
    return PendingActionsListResponse(
        channel=channel, strategy_id=strategy_id, as_of_date=as_of_date,
        actions=[PendingActionResponse(**a.__dict__) for a in actions],
    )


@router.post("/{channel}/{strategy_id}/pending/{action_id}/accept", response_model=PendingActionResponse)
async def accept_pending_action(channel: str, strategy_id: str, action_id: str, body: AcceptActionRequest) -> PendingActionResponse:
    from datetime import date as date_type

    horizon_bucket = HorizonBucket(body.horizon_bucket) if body.horizon_bucket else None
    runner = PaperTradingRunner(channel, strategy_id, horizon_bucket=horizon_bucket, initial_capital=body.initial_capital)
    try:
        decided = runner.accept(action_id, date_type.fromisoformat(body.as_of_date), body.price, body.prices)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc))
    return PendingActionResponse(**decided.__dict__)


@router.post("/{channel}/{strategy_id}/pending/{action_id}/reject", response_model=PendingActionResponse)
async def reject_pending_action(channel: str, strategy_id: str, action_id: str, body: RejectActionRequest) -> PendingActionResponse:
    from datetime import date as date_type

    # reject() never touches portfolio state (see live_runner.py), so no
    # horizon_bucket/initial_capital is ever needed here.
    runner = PaperTradingRunner(channel, strategy_id)
    try:
        decided = runner.reject(action_id, date_type.fromisoformat(body.as_of_date))
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc))
    return PendingActionResponse(**decided.__dict__)


@router.get("/{channel}/{strategy_id}/gate_status", response_model=GateStatusResponse)
async def get_gate_status(channel: str, strategy_id: str) -> GateStatusResponse:
    """Phase 3 Gate 7, generalized: >=90 real forward paper-trading days
    for THIS strategy specifically, before it may ever be considered for
    live capital (still requires separate human sign-off regardless —
    this endpoint never sets any live-eligibility flag itself)."""
    return GateStatusResponse(**_gate_status(channel, strategy_id))


@router.get("/{channel}/{strategy_id}/state", response_model=StateSummaryResponse)
async def get_state_summary(channel: str, strategy_id: str) -> StateSummaryResponse:
    runner = PaperTradingRunner(channel, strategy_id)  # read-only: no fallback portfolio creation
    try:
        summary = runner.state_summary()
    except ValueError:
        raise HTTPException(status_code=404, detail=f"No paper-trading state yet for {channel}/{strategy_id}")
    return StateSummaryResponse(**summary)


@router.post("/{channel}/{strategy_id}/propose", response_model=ProposeResponse)
async def propose_today(channel: str, strategy_id: str, body: ProposeRequest) -> ProposeResponse:
    """Generate today's proposals for one strategy and queue them for review.

    F1: the endpoint that connects paper trading to the same adapter the
    backtests run. It owns no selection logic — live_adapter_factory builds
    the strategy's adapter from its registry row, PaperTradingRunner calls
    generate_signals and records the result (ledger source="paper"), and every
    proposed action still requires an explicit human accept() before it moves
    any capital, simulated or otherwise.

    An empty `actions` list is a normal outcome, not an error: the A103
    readiness gate returns nothing on a day whose inputs are incomplete, and
    a strategy can simply have no move to make.
    """
    from datetime import date as _date

    from backtest.core.live_adapter_factory import build_live_adapter
    from config.settings import DUCKDB_PATH
    from datastore.api.db import get_duckdb_connection
    from features.momentum_live import StrategyNotRunnableLive
    from strategies.definitions import DefinitionNotFound

    try:
        as_of = _date.fromisoformat(body.as_of_date)
        horizon = HorizonBucket(body.horizon_bucket)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))

    runner = PaperTradingRunner(channel, strategy_id, horizon_bucket=horizon)
    try:
        with get_duckdb_connection(DUCKDB_PATH, read_only=True, persist=False) as conn:
            adapter, universe = build_live_adapter(
                channel, strategy_id, as_of, conn=conn, top_n=body.top_n,
            )
            # Inside the connection's scope: the three bespoke fundamental
            # presets read raw PIT financials during generate_signals, so the
            # connection must still be open when the adapter runs, not just
            # when it is built.
            actions = runner.propose_today(adapter, universe, as_of)
    except StrategyNotRunnableLive as exc:
        # 409, not 500: the strategy is well-formed and the request is valid;
        # this path simply cannot honour one of its declared filters today,
        # and generating anyway would run a different strategy.
        raise HTTPException(status_code=409, detail=str(exc))
    except DefinitionNotFound as exc:
        raise HTTPException(status_code=404, detail=str(exc))
    except (ValueError, KeyError) as exc:
        raise HTTPException(status_code=400, detail=str(exc))

    return ProposeResponse(
        channel=channel, strategy_id=strategy_id, as_of_date=body.as_of_date,
        universe_size=len(universe),
        actions=[PendingActionResponse(**a.__dict__) for a in actions],
    )
