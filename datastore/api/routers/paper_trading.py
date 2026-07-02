"""
datastore/api/routers/paper_trading.py

Phase: 3.x (Automated Daily Paper Trading); SPEC-PT-003 Pending Actions added 2026-07-01
Specs: SPEC-DS-002, SPEC-OBS-004, SPEC-PT-003
Owner: Platform / DataStore
Consumers: dashboard/static (Paper Trading screen), scripts/run_daily_paper_trading.py (reads
back via backtest/portfolio_state.py directly, not this router — this router is read-only)

Read-only endpoints exposing the automated daily paper-trading bot's state
to the UI: current open positions/cash (paper_trading/portfolio_state.json,
written by backtest/portfolio_state.py), closed trades
(paper_trading/executions/*.csv, written by scripts/paper_trading_tracker.py),
the equity curve, and Phase 3 Gate 7 progress (>=90 NSE trading days of
continuous live daily pipeline runs, counted as distinct dated CSVs under
paper_trading/executions/).

[AS BUILT, SPEC-PT-003] When config.settings.PAPER_TRADING_REQUIRE_APPROVAL
is set, scripts/run_daily_paper_trading.py no longer auto-executes its
candidate entries/exits — it writes them to paper_trading/pending/{date}.json
instead (systems/ml_signal_engine/inference/paper_trading_step.py's
propose_daily_exits/propose_daily_entries). The 3 endpoints below are the
*only* write path into portfolio_state.json other than the bot itself: GET
/pending lists today's proposals, POST /pending/{id}/accept executes one via
PortfolioSimulator (same mechanics the bot itself uses) and logs it exactly
like a bot-executed trade, POST /pending/{id}/reject just marks it decided.
Every read-modify-write of portfolio_state.json (here and in the bot) goes
through datastore/api/utils/file_lock.py's flock-based lock so the two
writers (bot, accept-endpoint) can't race.
"""

import csv
import json
import logging
from datetime import date as date_type
from pathlib import Path
from typing import Optional

from fastapi import APIRouter, HTTPException, Query

from backtest.portfolio_state import load_portfolio_state, save_portfolio_state
from config.timezone import now_ist
from config.universe import load_universe_raw
from datastore.api.schemas import (
    ActionDecisionResponse,
    BackdatedBuyRequest,
    BackdatedBuyResponse,
    EquityCurvePoint,
    EquityCurveResponse,
    GateStatusResponse,
    PaperTradingPosition,
    PaperTradingStateResponse,
    PaperTradingTrade,
    PaperTradingTradesResponse,
    PendingActionRow,
    PendingActionsResponse,
)
from datastore.api.utils.file_lock import locked_file
from scripts.paper_trading_tracker import PaperTradingTracker

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/v1/paper_trading", tags=["Paper Trading"])

PORTFOLIO_STATE_PATH = Path("paper_trading/portfolio_state.json")
EXECUTIONS_DIR = Path("paper_trading/executions")
PENDING_DIR = Path("paper_trading/pending")
GATE_THRESHOLD = 90


@router.get("/state", response_model=PaperTradingStateResponse)
async def get_paper_trading_state() -> PaperTradingStateResponse:
    """Current portfolio snapshot — available=False if the bot has never run."""
    if not PORTFOLIO_STATE_PATH.exists():
        return PaperTradingStateResponse()

    state = json.loads(PORTFOLIO_STATE_PATH.read_text())
    positions = [PaperTradingPosition(**p) for p in state.get("positions", [])]
    positions_value = sum(p.quantity * p.entry_price for p in positions)
    return PaperTradingStateResponse(
        as_of_date=state.get("as_of_date"),
        cash=state.get("cash", 0.0),
        total_equity=state.get("cash", 0.0) + positions_value,
        initial_capital=state.get("initial_capital", 0.0),
        positions=positions,
        available=True,
    )


@router.get("/trades", response_model=PaperTradingTradesResponse)
async def get_paper_trading_trades(
    start_date: Optional[str] = Query(None, description="Inclusive start date (YYYY-MM-DD)"),
    end_date: Optional[str] = Query(None, description="Inclusive end date (YYYY-MM-DD)"),
) -> PaperTradingTradesResponse:
    """Closed trades across all paper_trading/executions/*.csv, sorted by exit_date desc."""
    trades = []
    if EXECUTIONS_DIR.exists():
        for log_file in sorted(EXECUTIONS_DIR.glob("*.csv")):
            with open(log_file, "r") as f:
                for row in csv.DictReader(f):
                    if not row.get("exit_date"):
                        continue  # still open — not a closed trade
                    if start_date and row["exit_date"] < start_date:
                        continue
                    if end_date and row["exit_date"] > end_date:
                        continue
                    trades.append(
                        PaperTradingTrade(
                            date=row["date"],
                            ticker=row["ticker"],
                            signal_type=row["signal_type"],
                            entry_price=float(row["entry_price"]),
                            quantity=int(row["quantity"]),
                            entry_time=row["entry_time"],
                            exit_price=float(row["exit_price"]) if row.get("exit_price") else None,
                            exit_time=row.get("exit_time") or None,
                            exit_date=row.get("exit_date") or None,
                            exit_type=row.get("exit_type") or None,
                            pnl=float(row["pnl"]) if row.get("pnl") else None,
                            pnl_pct=float(row["pnl_pct"]) if row.get("pnl_pct") else None,
                        )
                    )

    trades.sort(key=lambda t: t.exit_date or "", reverse=True)
    return PaperTradingTradesResponse(trades=trades, count=len(trades))


@router.get("/equity_curve", response_model=EquityCurveResponse)
async def get_paper_trading_equity_curve() -> EquityCurveResponse:
    """Equity curve recorded by the daily bot, for the UI's equity chart."""
    if not PORTFOLIO_STATE_PATH.exists():
        return EquityCurveResponse()

    state = json.loads(PORTFOLIO_STATE_PATH.read_text())
    points = [EquityCurvePoint(**p) for p in state.get("equity_curve", [])]
    return EquityCurveResponse(points=points)


@router.get("/gate_status", response_model=GateStatusResponse)
async def get_paper_trading_gate_status() -> GateStatusResponse:
    """Phase 3 Gate 7 progress: distinct dated CSVs under paper_trading/executions/."""
    days_count = len(list(EXECUTIONS_DIR.glob("*.csv"))) if EXECUTIONS_DIR.exists() else 0
    return GateStatusResponse(
        days_count=days_count,
        gate_threshold=GATE_THRESHOLD,
        gate_cleared=days_count >= GATE_THRESHOLD,
    )


def _latest_pending_path() -> Optional[Path]:
    """Most recent paper_trading/pending/{date}.json, or None if none exist."""
    if not PENDING_DIR.exists():
        return None
    files = sorted(PENDING_DIR.glob("*.json"))
    return files[-1] if files else None


def _load_pending(path: Path) -> list:
    return json.loads(path.read_text()) if path.exists() else []


def _save_pending(path: Path, rows: list) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(rows, indent=2))


@router.get("/pending", response_model=PendingActionsResponse)
async def get_pending_actions() -> PendingActionsResponse:
    """Today's (or the latest unactioned date's) proposed trades awaiting
    accept/reject — written by scripts/run_daily_paper_trading.py when
    PAPER_TRADING_REQUIRE_APPROVAL is set (SPEC-PT-003)."""
    path = _latest_pending_path()
    if path is None:
        return PendingActionsResponse()
    rows = _load_pending(path)
    return PendingActionsResponse(
        date=path.stem,
        actions=[PendingActionRow(**r) for r in rows if r.get("status") == "pending"],
    )


@router.post("/pending/{action_id}/accept", response_model=ActionDecisionResponse)
async def accept_pending_action(action_id: str) -> ActionDecisionResponse:
    """Execute one proposed trade via PortfolioSimulator (same mechanics the
    bot itself uses) and log it like a normal bot-executed trade. Locked
    against the bot's own portfolio_state.json writes (SPEC-PT-003)."""
    path = _latest_pending_path()
    if path is None:
        raise HTTPException(status_code=404, detail="No pending actions found")
    rows = _load_pending(path)
    match = next((r for r in rows if r["action_id"] == action_id), None)
    if match is None:
        raise HTTPException(status_code=404, detail=f"Pending action {action_id} not found")
    if match["status"] != "pending":
        raise HTTPException(status_code=409, detail=f"Action {action_id} already {match['status']}")

    # Re-fetch live price rather than trusting the (possibly stale) propose-time
    # price — same reasoning a real broker would re-quote at execution time.
    from datastore.api.routers.ohlcv import get_ohlcv_latest

    latest = await get_ohlcv_latest(match["ticker"])
    if latest is None:
        raise HTTPException(status_code=422, detail=f"No current price available for {match['ticker']}")
    price = latest.close
    today = now_ist().date()

    with locked_file(PORTFOLIO_STATE_PATH):
        portfolio = load_portfolio_state(PORTFOLIO_STATE_PATH)
        if portfolio is None:
            raise HTTPException(status_code=409, detail="No portfolio state exists yet — the bot hasn't run")

        executed = False
        if match["action_type"] == "buy":
            position = portfolio.buy(match["ticker"], match.get("sector", "UNKNOWN"), price, today, {match["ticker"]: price})
            executed = position is not None
            detail = None if executed else "can_buy() gates rejected this trade (cash/sector/position cap)"
        elif match["action_type"] == "sell":
            trade = portfolio.sell(match["ticker"], price, today, reason="manual_accept")
            executed = trade is not None
            detail = None if executed else f"{match['ticker']} is no longer held"
        elif match["action_type"] == "reduce":
            trade = portfolio.reduce_position(match["ticker"], price, today, reason="manual_accept")
            executed = trade is not None
            detail = None if executed else f"{match['ticker']} is no longer held"
        else:
            raise HTTPException(status_code=400, detail=f"Unknown action_type: {match['action_type']}")

        if executed:
            tracker = PaperTradingTracker(logs_dir=str(EXECUTIONS_DIR))
            if match["action_type"] == "buy":
                pos = portfolio.positions[match["ticker"]]
                tracker.log_trade(
                    date=str(today), ticker=match["ticker"], signal_type="BUY",
                    entry_price=pos.entry_price, quantity=pos.quantity, entry_time="manual",
                )
            elif executed and match["ticker"] not in portfolio.positions:
                # sell, or a reduce that exactly zeroed the position — log the
                # closed trade, same rule apply_daily_exits() uses.
                closed = portfolio.trades[-1]
                tracker.log_trade(
                    date=str(closed.entry_date), ticker=match["ticker"], signal_type="BUY",
                    entry_price=closed.entry_price, quantity=closed.quantity, entry_time="manual",
                    exit_price=closed.exit_price, exit_time="manual", exit_date=str(closed.exit_date),
                    exit_type=closed.exit_reason, pnl=closed.pnl_inr, pnl_pct=closed.pnl_pct,
                )
            save_portfolio_state(portfolio, PORTFOLIO_STATE_PATH, as_of_date=str(today))

        match["status"] = "accepted" if executed else "rejected"
        _save_pending(path, rows)

    return ActionDecisionResponse(action_id=action_id, status="accepted" if executed else "rejected", executed=executed, detail=detail)


@router.post("/pending/{action_id}/reject", response_model=ActionDecisionResponse)
async def reject_pending_action(action_id: str) -> ActionDecisionResponse:
    """Mark a proposed trade rejected — no portfolio mutation."""
    path = _latest_pending_path()
    if path is None:
        raise HTTPException(status_code=404, detail="No pending actions found")
    rows = _load_pending(path)
    match = next((r for r in rows if r["action_id"] == action_id), None)
    if match is None:
        raise HTTPException(status_code=404, detail=f"Pending action {action_id} not found")
    if match["status"] != "pending":
        raise HTTPException(status_code=409, detail=f"Action {action_id} already {match['status']}")

    match["status"] = "rejected"
    _save_pending(path, rows)
    return ActionDecisionResponse(action_id=action_id, status="rejected", executed=False)


@router.post("/backdated_buy", response_model=BackdatedBuyResponse)
async def backdated_buy(request: BackdatedBuyRequest) -> BackdatedBuyResponse:
    """
    Open a paper-trading position dated to a past day, at that day's real
    close price — lets a user review a historical date's ML recommendations
    (GET /api/v1/signals/ml/top_buys/{date}, which already supports any past
    date) and act on one retroactively.

    [AS BUILT] User-confirmed design decision (2026-07-01): this logs to
    paper_trading/executions/{date}.csv exactly like a live bot-executed
    trade, via the same PaperTradingTracker.log_trade() every other trade
    path uses — no special-casing. Gate 7 (>=90 NSE trading days of
    continuous live daily pipeline runs, GET /gate_status below) counts
    every distinct dated CSV under that directory, so a backdated entry's
    CSV counts toward that total even though the bot didn't genuinely run
    live on that date. This was flagged explicitly during planning and the
    user chose to accept it rather than route backdated trades to a
    separate directory — do not "fix" this without re-confirming with the
    user, since it's a deliberate accepted trade-off, not an oversight.
    """
    try:
        entry_date = date_type.fromisoformat(request.date)
    except ValueError:
        raise HTTPException(status_code=400, detail=f"Invalid date '{request.date}', expected YYYY-MM-DD")

    from datastore.api.routers.ohlcv import get_ohlcv

    ohlcv_resp = await get_ohlcv(request.ticker, from_date=entry_date, to_date=entry_date)
    if not ohlcv_resp.data:
        raise HTTPException(status_code=422, detail=f"No OHLCV data for {request.ticker} on {request.date}")
    price = ohlcv_resp.data[0].close

    universe = load_universe_raw()
    sector_map = dict(zip(universe["ticker"], universe["sector"]))
    sector = sector_map.get(request.ticker, "UNKNOWN")

    with locked_file(PORTFOLIO_STATE_PATH):
        portfolio = load_portfolio_state(PORTFOLIO_STATE_PATH)
        if portfolio is None:
            raise HTTPException(status_code=409, detail="No portfolio state exists yet — the bot hasn't run")

        position = portfolio.buy(request.ticker, sector, price, entry_date, {request.ticker: price})
        if position is None:
            return BackdatedBuyResponse(
                ticker=request.ticker, date=request.date, entry_price=price, executed=False,
                detail="can_buy() gates rejected this trade (already held, cash/sector/position cap)",
            )

        tracker = PaperTradingTracker(logs_dir=str(EXECUTIONS_DIR))
        tracker.log_trade(
            date=request.date, ticker=request.ticker, signal_type="BUY",
            entry_price=position.entry_price, quantity=position.quantity, entry_time="backdated",
        )
        save_portfolio_state(portfolio, PORTFOLIO_STATE_PATH, as_of_date=str(now_ist().date()))

    return BackdatedBuyResponse(
        ticker=request.ticker, date=request.date, entry_price=position.entry_price,
        quantity=position.quantity, executed=True,
    )
